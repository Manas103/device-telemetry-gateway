// cmd/deliveryburst is the 10x burst backlog drain claim's exact
// measurement: send a baseline sustained stream of delivery webhooks through
// the real edge into a real Kafka topic while a real consumer drains it into
// ClickHouse, measure the baseline sustained rate, then offer a burst at 10x
// that rate for a fixed window and measure how long the consumer takes to
// work the resulting backlog back down to zero lag. Same shape as
// cmd/segmentbench's burst-drain section, standalone here because this
// extension's edge and store are its own package (internal/deliverystats),
// not internal/segment's.
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

	"github.com/segmentio/kafka-go"

	pb "device-telemetry-gateway/proto"
)

func ensureTopic(broker, topic string, partitions int) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	ctrlConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return err
	}
	defer ctrlConn.Close()
	return ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
}

func main() {
	broker := flag.String("broker", "127.0.0.1:9092", "Kafka broker address")
	topic := flag.String("topic", "deliverystats-burst", "Kafka topic (a unique suffix is appended unless this is set explicitly)")
	partitions := flag.Int("partitions", 6, "Kafka partitions for the topic")
	chAddr := flag.String("clickhouse", "127.0.0.1:8123", "ClickHouse HTTP address")
	chDatabase := flag.String("clickhouse-db", "deliverystats", "ClickHouse database")
	baselineSeconds := flag.Int("baseline-seconds", 8, "how long to measure the baseline sustained rate")
	burstMultiplier := flag.Float64("burst-multiplier", 10, "burst offered rate as a multiple of the measured baseline rate")
	burstSeconds := flag.Int("burst-seconds", 8, "how long to offer burst traffic")
	burstDrainCap := flag.Duration("burst-drain-cap", 4*time.Minute, "how long to wait for the backlog to drain before giving up")
	sendWorkers := flag.Int("send-workers", 6, "concurrent sender goroutines (courtesy cap, see README)")
	outFile := flag.String("out", "", "path to also write the full report to")
	flag.Parse()

	runTopic := *topic
	if runTopic == "deliverystats-burst" {
		runTopic = fmt.Sprintf("deliverystats-burst-%d", time.Now().UnixNano())
	}
	topic = &runTopic

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

	ctx := context.Background()
	w("=== device-telemetry-gateway delivery-stats 10x burst backlog drain benchmark ===\n")

	ch := deliverystats.NewClickHouseStore(*chAddr, *chDatabase)
	if err := ch.EnsureSchema(ctx); err != nil {
		log.Fatalf("clickhouse ensure schema: %v", err)
	}
	if err := ch.TruncateAll(ctx); err != nil {
		log.Fatalf("clickhouse truncate: %v", err)
	}

	if err := ensureTopic(*broker, *topic, *partitions); err != nil {
		w("topic create note (may already exist): %v\n", err)
	}

	reader := kafkaclient.NewDeliveryStreamReader(*broker, *topic, *partitions)
	consumeCtx, cancelConsume := context.WithCancel(ctx)
	msgs := reader.Messages(consumeCtx)

	var consumed int64
	const chBatchSize = 500
	chBatch := make([]*pb.DeliveryWebhook, 0, chBatchSize)
	var chMu sync.Mutex
	flushCH := func() {
		chMu.Lock()
		batch := chBatch
		chBatch = make([]*pb.DeliveryWebhook, 0, chBatchSize)
		chMu.Unlock()
		if len(batch) == 0 {
			return
		}
		if err := ch.InsertBatch(ctx, batch); err != nil {
			log.Printf("clickhouse insert batch: %v", err)
		}
	}

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		flushTicker := time.NewTicker(200 * time.Millisecond)
		defer flushTicker.Stop()
		for {
			select {
			case wh, ok := <-msgs:
				if !ok {
					flushCH()
					return
				}
				chMu.Lock()
				chBatch = append(chBatch, wh)
				full := len(chBatch) >= chBatchSize
				chMu.Unlock()
				if full {
					flushCH()
				}
				atomic.AddInt64(&consumed, 1)
			case <-flushTicker.C:
				flushCH()
			case <-consumeCtx.Done():
				flushCH()
				return
			}
		}
	}()

	producer := kafkaclient.NewDeliveryProducer(*broker, *topic)
	pipeline := deliverystats.NewPipeline(deliverystats.NewWebhookDedup(), producer)

	sendAt := func(durationSecs int, ratePerSec float64, labelPrefix string) (int64, time.Duration) {
		var count int64
		var wg sync.WaitGroup
		stop := make(chan struct{})
		start := time.Now()
		perWorkerInterval := time.Duration(float64(*sendWorkers) / ratePerSec * float64(time.Second))
		if perWorkerInterval <= 0 {
			perWorkerInterval = time.Microsecond
		}
		for wk := 0; wk < *sendWorkers; wk++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				i := 0
				ticker := time.NewTicker(perWorkerInterval)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						ch := deliverystats.Channels[i%len(deliverystats.Channels)]
						e := &pb.DeliveryWebhook{
							WebhookId:   fmt.Sprintf("%s-%d-%d", labelPrefix, workerID, i),
							MessageId:   fmt.Sprintf("%s-msg-%d-%d", labelPrefix, workerID, i),
							Channel:     ch.Name,
							Status:      "delivered",
							TimestampMs: time.Now().UnixMilli(),
							Provider:    ch.Provider,
						}
						if _, err := pipeline.Ingest(ctx, e); err == nil {
							atomic.AddInt64(&count, 1)
						}
						i++
					}
				}
			}(wk)
		}
		time.AfterFunc(time.Duration(durationSecs)*time.Second, func() { close(stop) })
		wg.Wait()
		return atomic.LoadInt64(&count), time.Since(start)
	}

	w("baseline phase: offering unthrottled for %ds to measure sustained rate\n", *baselineSeconds)
	baselineCount, baselineElapsed := sendAt(*baselineSeconds, 1_000_000, "baseline")
	baselineRate := float64(baselineCount) / baselineElapsed.Seconds()
	w("baseline: %d webhooks sent in %s (%.0f/sec sustained)\n", baselineCount, baselineElapsed, baselineRate)

	burstRate := baselineRate * *burstMultiplier
	w("burst phase: offering ~%.0f/sec (%.0fx baseline) for %ds\n", burstRate, *burstMultiplier, *burstSeconds)
	burstCount, burstElapsed := sendAt(*burstSeconds, burstRate, "burst")
	w("burst: %d webhooks sent in %s\n", burstCount, burstElapsed)

	drainStart := time.Now()
	drainDeadline := drainStart.Add(*burstDrainCap)
	var finalLag int64 = -1
	for time.Now().Before(drainDeadline) {
		offsets, err := kafkaclient.DeliveryPartitionOffsets(ctx, *broker, *topic)
		if err != nil {
			log.Printf("partition offsets: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		cur := reader.CurrentOffsets()
		var lag int64
		for pid, bounds := range offsets {
			c := cur[pid]
			l := bounds.Last - c
			if l > 0 {
				lag += l
			}
		}
		finalLag = lag
		if lag <= 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	drainElapsed := time.Since(drainStart)
	w("RESULT: backlog drained (lag=%d) in %s (cap %s)\n", finalLag, drainElapsed, *burstDrainCap)
	w("total webhooks consumed by end of run: %d\n", atomic.LoadInt64(&consumed))

	cancelConsume()
	<-consumerDone
	reader.Close()
	producer.Close()

	if finalLag > 0 {
		w("NOTE: drain did not reach zero lag within the cap; see README Findings/Limitations.\n")
	}
	w("done.\n")
}
