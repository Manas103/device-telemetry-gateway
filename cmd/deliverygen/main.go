// cmd/deliverygen is the delivery-webhook dedup claim's exact measurement:
// build a corpus of simulated email/SMS/WhatsApp delivery webhooks, retry a
// fraction of the delivered stream (a provider resending an unacknowledged
// webhook with its original webhook_id), send everything through the real
// edge (internal/deliverystats.Pipeline, backed by a real Kafka producer),
// then read every message actually landed in Kafka back out and diff it
// against an independently written reference dedup over the same delivered
// stream. Same shape as the existing cmd/eventgen, for the same reason.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"device-telemetry-gateway/internal/deliverystats"
	"device-telemetry-gateway/internal/kafkaclient"

	pb "device-telemetry-gateway/proto"
)

func main() {
	messages := flag.Int("messages", 2000, "distinct simulated outbound messages")
	historyMinutes := flag.Int("history-minutes", 240, "synthetic webhook history window in minutes")
	dupFraction := flag.Float64("dup-fraction", 0.05, "fraction of delivered traffic that is a provider retry")
	broker := flag.String("broker", "127.0.0.1:9092", "Kafka broker address")
	topic := flag.String("topic", "deliverystats-dedup-bench", "Kafka topic")
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

	corpus := deliverystats.Build(*messages, *historyMinutes, time.Now(), 42)
	delivered := deliverystats.WithRetries(corpus, *dupFraction, 43)

	w("=== device-telemetry-gateway delivery-stats webhook dedup benchmark ===\n")
	w("messages: %d, unique webhooks: %d, delivered (with retries): %d (target retry fraction %.2f%%)\n",
		*messages, len(corpus), len(delivered), *dupFraction*100)

	channelCounts := map[string]int{}
	for _, wh := range corpus {
		channelCounts[wh.Channel]++
	}
	for _, ch := range deliverystats.Channels {
		w("  channel %-10s unique webhooks: %d\n", ch.Name, channelCounts[ch.Name])
	}

	if err := kafkaclient.EnsureTopic(*broker, *topic, 6); err != nil {
		w("topic create note (may already exist): %v\n", err)
	}

	producer := kafkaclient.NewDeliveryProducer(*broker, *topic)
	defer producer.Close()
	pipeline := deliverystats.NewPipeline(deliverystats.NewWebhookDedup(), producer)

	ctx := context.Background()
	start := time.Now()
	accepted, rejected, produceErrs := 0, 0, 0
	for _, wh := range delivered {
		ok, err := pipeline.Ingest(ctx, wh)
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

	time.Sleep(2 * time.Second)
	landed, err := kafkaclient.ConsumeAvailableDelivery(ctx, *broker, *topic)
	if err != nil {
		log.Fatalf("read back from kafka: %v", err)
	}

	gotIDs := idSet(landed)
	wantIDs := idSet(deliverystats.ReferenceDedup(delivered))

	w("kafka-materialized unique webhook ids: %d\n", len(gotIDs))
	w("reference-dedup unique webhook ids (independent, over the same delivered stream): %d\n", len(wantIDs))

	match := equalIDSets(gotIDs, wantIDs)
	w("RESULT: edge-dedup-into-kafka matches independent reference exactly: %v\n", match)
	if !match {
		os.Exit(1)
	}
}

func idSet(webhooks []*pb.DeliveryWebhook) []string {
	out := make([]string, len(webhooks))
	for i, w := range webhooks {
		out[i] = w.WebhookId
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
