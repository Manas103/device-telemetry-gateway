// cmd/throughputbench is the sustained-throughput claim's exact
// measurement: hammer the storefront edge's own dedup+produce path
// (internal/eventingest.Pipeline backed by a real Kafka producer) directly,
// in process, with as much concurrency as this machine's cores support, for
// a fixed duration, and report events/sec actually sustained. Direct calls
// rather than a network hop to a running edge server measure the edge's
// own code path, not client-to-edge transport, matching this repo's
// existing cmd/loadcurve and cmd/soak precedent of measuring
// internal/ingest.Pipeline directly for a throughput or volume number.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"device-telemetry-gateway/internal/eventingest"
	"device-telemetry-gateway/internal/kafkaclient"

	pb "device-telemetry-gateway/proto"
)

func main() {
	broker := flag.String("broker", "127.0.0.1:9092", "Kafka broker address")
	topic := flag.String("topic", "storefront-events-throughput-bench", "Kafka topic")
	duration := flag.Duration("duration", 20*time.Second, "how long to sustain the offered load")
	workers := flag.Int("workers", 0, "concurrent sender goroutines (0 = half of GOMAXPROCS, per this project's resource-courtesy rule)")
	outFile := flag.String("out", "", "path to also write the full report to")
	flag.Parse()

	if *workers <= 0 {
		*workers = runtime.GOMAXPROCS(0) / 2
		if *workers < 1 {
			*workers = 1
		}
	}

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

	producer := kafkaclient.NewStorefrontProducer(*broker, *topic)
	defer producer.Close()
	pipeline := eventingest.NewPipeline(eventingest.NewEventDedup(), producer)

	w("=== device-telemetry-gateway storefront edge sustained throughput benchmark ===\n")
	w("workers: %d, offered duration: %s\n", *workers, *duration)

	ctx := context.Background()
	var sent, errs int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	start := time.Now()

	for wk := 0; wk < *workers; wk++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				e := &pb.StorefrontEvent{
					EventId:     fmt.Sprintf("tb-%d-%d", workerID, i),
					ProfileId:   fmt.Sprintf("tb-profile-%d", i%1000),
					EventType:   "page_view",
					TimestampMs: time.Now().UnixMilli(),
					Attributes:  map[string]string{"category": "electronics"},
				}
				if _, err := pipeline.Ingest(ctx, e); err != nil {
					atomic.AddInt64(&errs, 1)
				} else {
					atomic.AddInt64(&sent, 1)
				}
				i++
			}
		}(wk)
	}

	time.Sleep(*duration)
	close(stop)
	wg.Wait()
	elapsed := time.Since(start)

	rate := float64(atomic.LoadInt64(&sent)) / elapsed.Seconds()
	w("sent (accepted+produced): %d, errors: %d, elapsed: %s\n", sent, errs, elapsed)
	w("RESULT: sustained throughput: %.0f events/sec\n", rate)
}
