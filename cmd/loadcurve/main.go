// cmd/loadcurve is the staged-load-curve claim's exact measurement: ramp
// the offered ingest rate in stages against a producer with a fixed,
// known sustained capacity, and report the first stage at which the
// pipeline could no longer produce every reading directly within its
// 200ms window (internal/ingest.Pipeline.Ingest) and had to fall back to
// the spool.
//
// "Loss" here means exactly that: a reading falling back to the spool
// within a stage, not a permanently dropped reading. This gateway's whole
// design (see README) is that nothing is ever permanently dropped once it
// passes dedup, so "0 lost, ever" is true by construction at any offered
// rate and is not the useful number; the operationally meaningful number
// is the rate at which the pipeline first could not keep up with Kafka
// directly and had to lean on the spool, which is what a real capacity
// planning exercise would call "loss" (a reading that is late, not gone).
// A fixed-capacity fake producer stands in for Kafka here so the result is
// deterministic and reproducible rather than a measurement of whatever
// this session's local Kafka instance happened to sustain.
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

	pb "device-telemetry-gateway/proto"

	"device-telemetry-gateway/internal/ingest"
)

// cappedProducer simulates a downstream with a fixed sustained throughput
// of capacity/serviceLatency readings/sec: at most `capacity` readings are
// "in flight" at once, each taking serviceLatency to complete. Offered
// traffic below that implied rate is served within the 200ms window every
// time; offered traffic above it queues until Ingest's own timeout fires.
type cappedProducer struct {
	sem            chan struct{}
	serviceLatency time.Duration
	succeeded      int64
	timedOut       int64
}

func newCappedProducer(capacity int, serviceLatency time.Duration) *cappedProducer {
	return &cappedProducer{sem: make(chan struct{}, capacity), serviceLatency: serviceLatency}
}

// Produce returns an error (which is exactly what makes Pipeline.Ingest
// fall back to the spool) whenever it could not complete within its own
// simulated service latency inside the caller's context deadline. The
// success/timeout counters are this benchmark's direct, race-free way of
// knowing whether a given reading was produced straight through or had to
// be spooled, rather than inferring it from a shared Spool's size, which
// many concurrent goroutines racing on the same stage would make noisy.
func (p *cappedProducer) Produce(ctx context.Context, r *pb.Reading) error {
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		atomic.AddInt64(&p.timedOut, 1)
		return ctx.Err()
	}
	defer func() { <-p.sem }()
	select {
	case <-time.After(p.serviceLatency):
		atomic.AddInt64(&p.succeeded, 1)
		return nil
	case <-ctx.Done():
		atomic.AddInt64(&p.timedOut, 1)
		return ctx.Err()
	}
}

func main() {
	capacity := flag.Int("capacity", 5, "simulated producer's max concurrent in-flight requests")
	serviceLatency := flag.Duration("service-latency", 50*time.Millisecond, "simulated producer's per-request latency")
	stageDuration := flag.Duration("stage-duration", time.Second, "how long each offered-rate stage runs")
	outFile := flag.String("out", "", "path to also write the full report to (docs/loadcurve_output.txt)")
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

	impliedCapacityPerSec := float64(*capacity) / serviceLatency.Seconds()
	w("=== device-telemetry-gateway staged load curve ===\n")
	w("simulated producer capacity: %d in flight, %s service latency (implied sustained throughput: %.0f/sec)\n\n",
		*capacity, *serviceLatency, impliedCapacityPerSec)

	stages := []int{30, 50, 70, 90, 110, 150, 200, 300}
	firstLossyRate := -1

	for _, rate := range stages {
		spoolFile, err := os.CreateTemp("", "dtg-loadcurve-spool-*.dat")
		if err != nil {
			log.Fatalf("create temp spool: %v", err)
		}
		spoolFile.Close()
		defer os.Remove(spoolFile.Name())

		spool, err := ingest.NewSpool(spoolFile.Name())
		if err != nil {
			log.Fatalf("open spool: %v", err)
		}
		producer := newCappedProducer(*capacity, *serviceLatency)
		pipeline := ingest.NewPipeline(ingest.NewDedup(), spool, producer)

		total := int(float64(rate) * stageDuration.Seconds())
		var sent int64
		var wg sync.WaitGroup
		interval := time.Duration(float64(time.Second) / float64(rate))
		ticker := time.NewTicker(interval)
		ctx := context.Background()
		_ = spool // kept open so Ingest's spool-fallback path is real, not just discarded

		for i := 0; i < total; i++ {
			<-ticker.C
			wg.Add(1)
			go func(seq int) {
				defer wg.Done()
				r := &pb.Reading{DeviceId: "loadcurve-dev", Sequence: uint64(seq), TimestampMs: int64(seq), Value: float64(seq)}
				if err := pipeline.Ingest(ctx, r); err != nil {
					log.Printf("ingest error at seq %d: %v", seq, err)
				}
				atomic.AddInt64(&sent, 1)
			}(i)
		}
		wg.Wait()
		ticker.Stop()

		spooled := atomic.LoadInt64(&producer.timedOut)
		lossFraction := float64(spooled) / float64(sent)
		w("offered rate %6d/sec: sent %6d, fell back to spool %6d (%.2f%%)\n", rate, sent, spooled, lossFraction*100)
		if spooled > 0 && firstLossyRate == -1 {
			firstLossyRate = rate
		}
	}

	w("\nRESULT: offered rate where loss first went nonzero: ")
	if firstLossyRate == -1 {
		w("none found in tested range (%d to %d/sec)\n", stages[0], stages[len(stages)-1])
	} else {
		w("%d/sec\n", firstLossyRate)
	}
}
