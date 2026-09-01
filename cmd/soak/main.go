// cmd/soak is the six-hour soak claim's exact measurement: replay the
// traffic volume of a six-hour soak at the outage benchmark's own device
// count and rate (50 devices, 1 reading/sec/device: cmd/outagebench), and
// sample the dedup cache's entry count over the course of the run.
//
// "Six-hour" describes the *traffic volume* replayed, not wall-clock
// duration: like the outage benchmark's 60-second outage (simulated at the
// producer, not waited out in real time, and disclosed as such in the
// README), this soak replays 6 hours' worth of readings as fast as the
// pipeline can accept them rather than sleeping for 6 real hours, so the
// measurement is reproducible in a single session. This benchmark uses an
// in-memory always-succeeds producer rather than a real Kafka broker: what
// it measures is the dedup cache's memory behavior under sustained volume,
// which does not depend on where Ingest hands a Reading off to next; real
// Kafka production throughput is already measured by cmd/loadgen and
// cmd/outagebench.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	pb "device-telemetry-gateway/proto"

	"device-telemetry-gateway/internal/ingest"
)

type alwaysSucceedsProducer struct{}

func (alwaysSucceedsProducer) Produce(ctx context.Context, r *pb.Reading) error { return nil }

func main() {
	devices := flag.Int("devices", 50, "device count, matching cmd/outagebench")
	ratePerSec := flag.Float64("rate-per-device", 1.0, "readings per second per device, matching cmd/outagebench")
	hours := flag.Float64("hours", 6.0, "soak duration to replay, in traffic-volume hours (not wall-clock)")
	sampleEvery := flag.Int("sample-every", 50000, "sample dedup.EntryCount() every this many readings ingested")
	spoolPath := flag.String("spool", "", "spool file path (defaults to a temp file)")
	outFile := flag.String("out", "", "path to also write the full report to (docs/soak_output.txt)")
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

	spoolFile := *spoolPath
	if spoolFile == "" {
		f, err := os.CreateTemp("", "dtg-soak-spool-*.dat")
		if err != nil {
			log.Fatalf("create temp spool: %v", err)
		}
		spoolFile = f.Name()
		f.Close()
		defer os.Remove(spoolFile)
	}
	spool, err := ingest.NewSpool(spoolFile)
	if err != nil {
		log.Fatalf("open spool: %v", err)
	}

	dedup := ingest.NewDedup()
	pipeline := ingest.NewPipeline(dedup, spool, alwaysSucceedsProducer{})

	totalReadingsPerDevice := int64(*ratePerSec * *hours * 3600)
	totalReadings := totalReadingsPerDevice * int64(*devices)

	w("=== device-telemetry-gateway soak benchmark ===\n")
	w("devices: %d, rate: %.2f readings/sec/device, soak duration: %.1f hours (traffic volume, not wall-clock)\n",
		*devices, *ratePerSec, *hours)
	w("readings per device: %d, total readings: %d\n", totalReadingsPerDevice, totalReadings)
	w("dedup cache cap (MaxSequencesPerDevice): %d\n\n", ingest.MaxSequencesPerDevice)

	ctx := context.Background()
	maxEntryCountSeen := 0
	var ingested int64
	for seq := int64(0); seq < totalReadingsPerDevice; seq++ {
		for d := 0; d < *devices; d++ {
			deviceID := fmt.Sprintf("soak-dev-%03d", d)
			r := &pb.Reading{DeviceId: deviceID, Sequence: uint64(seq), TimestampMs: seq * 1000, Value: float64(seq)}
			if err := pipeline.Ingest(ctx, r); err != nil {
				log.Fatalf("ingest failed at reading %d: %v", ingested, err)
			}
			ingested++
			if ingested%int64(*sampleEvery) == 0 {
				c := dedup.EntryCount()
				if c > maxEntryCountSeen {
					maxEntryCountSeen = c
				}
			}
		}
	}
	finalCount := dedup.EntryCount()
	if finalCount > maxEntryCountSeen {
		maxEntryCountSeen = finalCount
	}

	boundedCap := *devices * ingest.MaxSequencesPerDevice
	w("total readings ingested: %d\n", ingested)
	w("dedup cache entry count, final: %d\n", finalCount)
	w("dedup cache entry count, peak observed: %d\n", maxEntryCountSeen)
	w("theoretical bound (devices * MaxSequencesPerDevice): %d\n", boundedCap)
	w("unbounded-cache entry count would have been: %d (one entry per unique reading ever ingested)\n", ingested)
	pass := maxEntryCountSeen <= boundedCap
	w("RESULT: cache stayed bounded: %v\n", pass)
	if !pass {
		os.Exit(1)
	}
}
