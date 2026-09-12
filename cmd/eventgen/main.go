// cmd/eventgen is the storefront-event dedup claim's exact measurement:
// build a corpus of simulated storefront events, duplicate a fraction of
// the delivered stream (a client-side retry resending the same event_id),
// send everything through the real edge (internal/eventingest.Pipeline,
// backed by a real Kafka producer), then read every message actually
// landed in Kafka back out and diff it against an independently written
// reference dedup over the same delivered stream.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"device-telemetry-gateway/internal/eventingest"
	"device-telemetry-gateway/internal/kafkaclient"
	"device-telemetry-gateway/internal/simstorefront"

	pb "device-telemetry-gateway/proto"
)

func main() {
	profiles := flag.Int("profiles", 500, "distinct simulated profiles")
	avgEvents := flag.Int("avg-events", 6, "average events per profile")
	dupFraction := flag.Float64("dup-fraction", 0.05, "fraction of delivered traffic that is duplicate")
	broker := flag.String("broker", "127.0.0.1:9092", "Kafka broker address")
	topic := flag.String("topic", "storefront-events-dedup-bench", "Kafka topic")
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

	corpus := simstorefront.Build(*profiles, *avgEvents, 30, time.Now(), 42)
	unique := simstorefront.Flatten(corpus)
	delivered := simstorefront.WithDuplicates(unique, *dupFraction, 43)

	w("=== device-telemetry-gateway storefront event dedup benchmark ===\n")
	w("profiles: %d, unique events: %d, delivered (with duplicates): %d (target dup fraction %.2f%%)\n",
		*profiles, len(unique), len(delivered), *dupFraction*100)

	producer := kafkaclient.NewStorefrontProducer(*broker, *topic)
	defer producer.Close()
	pipeline := eventingest.NewPipeline(eventingest.NewEventDedup(), producer)

	ctx := context.Background()
	start := time.Now()
	accepted, rejected, produceErrs := 0, 0, 0
	for _, e := range delivered {
		ok, err := pipeline.Ingest(ctx, e)
		if err != nil {
			produceErrs++
			continue
		}
		if ok {
			accepted++
		} else {
			rejected++
		}
	}
	sendElapsed := time.Since(start)
	w("send phase: %d accepted, %d rejected as duplicates by the edge, %d produce errors, in %s\n",
		accepted, rejected, produceErrs, sendElapsed)

	time.Sleep(2 * time.Second) // let the broker settle before read-back
	landed, err := kafkaclient.ConsumeAvailableStorefront(ctx, *broker, *topic)
	if err != nil {
		log.Fatalf("read back from kafka: %v", err)
	}

	gotIDs := idSet(landed)
	wantIDs := idSet(eventingest.ReferenceDedup(delivered))

	w("kafka-materialized unique event ids: %d\n", len(gotIDs))
	w("reference-dedup unique event ids (independent, over the same delivered stream): %d\n", len(wantIDs))

	match := equalIDSets(gotIDs, wantIDs)
	w("RESULT: edge-dedup-into-kafka matches independent reference exactly: %v\n", match)
	if !match {
		os.Exit(1)
	}
}

func idSet(events []*pb.StorefrontEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.EventId
	}
	sort.Strings(out)
	return out
}

func equalIDSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
