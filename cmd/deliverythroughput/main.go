// cmd/deliverythroughput is the 15,000 webhooks/sec claim's exact
// measurement: hammer the delivery-stats edge's own dedup+produce path
// directly, in process, with a courtesy-capped number of goroutines (6 of
// this WSL instance's 12 logical cores, half again of the already-halved
// per-build-job courtesy budget, since this is sustained load generation
// running while Manas may be using the machine, not a one-shot build), for
// a fixed duration, and report webhooks/sec actually sustained.
//
// Each of the 6 goroutines batches many webhooks into one Kafka
// WriteMessages call (internal/kafkaclient.DeliveryBatchProducer) rather
// than issuing one network round trip per webhook: a hard-capped goroutine
// count can only reach five-figure throughput if each goroutine amortizes
// its own network cost across many messages per call, not by adding more
// concurrent senders.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"device-telemetry-gateway/internal/deliverystats"
	"device-telemetry-gateway/internal/kafkaclient"

	pb "device-telemetry-gateway/proto"
)

func main() {
	broker := flag.String("broker", "127.0.0.1:9092", "Kafka broker address")
	topic := flag.String("topic", "deliverystats-throughput-bench", "Kafka topic")
	duration := flag.Duration("duration", 20*time.Second, "how long to sustain the offered load")
	workers := flag.Int("workers", 6, "concurrent sender goroutines (courtesy-capped, see README Resource courtesy)")
	batchSize := flag.Int("batch-size", 400, "webhooks per WriteMessages call, per worker")
	outFile := flag.String("out", "", "path to also write the full report to")
	flag.Parse()

	var out *os.File
	if *outFile != "" {
		f, err := os.Create(*outFile)
		if err != nil {
			log.Fatalf("create out file: %v", err)
		}
		defer f.Close()
		out = f
	}
	w := func(format string, args ...interface{}) {
		s := fmt.Sprintf(format, args...)
		fmt.Print(s)
		if out != nil {
			fmt.Fprint(out, s)
		}
	}

	w("=== device-telemetry-gateway delivery-stats edge throughput benchmark ===\n")
	w("workers: %d (courtesy cap), batch size: %d, offered duration: %s\n", *workers, *batchSize, *duration)

	if err := kafkaclient.EnsureTopic(*broker, *topic, 6); err != nil {
		w("topic create note (may already exist): %v\n", err)
	}

	ctx := context.Background()
	var sent, errs int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	start := time.Now()

	for wk := 0; wk < *workers; wk++ {
		producer := kafkaclient.NewDeliveryBatchProducer(*broker, *topic, *batchSize)
		dedup := deliverystats.NewWebhookDedup()
		wg.Add(1)
		go func(workerID int, producer *kafkaclient.DeliveryBatchProducer, dedup *deliverystats.WebhookDedup) {
			defer wg.Done()
			defer producer.Close()
			i := 0
			batch := make([]*pb.DeliveryWebhook, 0, *batchSize)
			flush := func() {
				if len(batch) == 0 {
					return
				}
				n := len(batch)
				if err := producer.ProduceBatch(ctx, batch); err != nil {
					atomic.AddInt64(&errs, int64(n))
				} else {
					atomic.AddInt64(&sent, int64(n))
				}
				batch = batch[:0]
			}
			for {
				select {
				case <-stop:
					flush()
					return
				default:
				}
				ch := deliverystats.Channels[i%len(deliverystats.Channels)]
				wh := &pb.DeliveryWebhook{
					WebhookId:   fmt.Sprintf("tb-%d-%d", workerID, i),
					MessageId:   fmt.Sprintf("tb-msg-%d-%d", workerID, i/2),
					Channel:     ch.Name,
					Status:      "delivered",
					TimestampMs: time.Now().UnixMilli(),
					Provider:    ch.Provider,
				}
				if !dedup.Admit(wh.WebhookId) {
					i++
					continue
				}
				batch = append(batch, wh)
				if len(batch) >= *batchSize {
					flush()
				}
				i++
			}
		}(wk, producer, dedup)
	}

	time.Sleep(*duration)
	close(stop)
	wg.Wait()
	elapsed := time.Since(start)

	rate := float64(atomic.LoadInt64(&sent)) / elapsed.Seconds()
	w("sent (accepted+produced): %d, errors: %d, elapsed: %s\n", sent, errs, elapsed)
	w("RESULT: sustained throughput: %.0f webhooks/sec\n", rate)
}
