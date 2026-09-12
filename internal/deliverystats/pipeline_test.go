package deliverystats

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	pb "device-telemetry-gateway/proto"
)

type fakeProducer struct {
	mu        sync.Mutex
	delivered []*pb.DeliveryWebhook
	failEvery int
	calls     int
}

func (f *fakeProducer) Produce(_ context.Context, w *pb.DeliveryWebhook) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failEvery > 0 && f.calls%f.failEvery == 0 {
		return fmt.Errorf("simulated producer failure")
	}
	f.delivered = append(f.delivered, w)
	return nil
}

func (f *fakeProducer) snapshot() []*pb.DeliveryWebhook {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*pb.DeliveryWebhook, len(f.delivered))
	copy(out, f.delivered)
	return out
}

func TestWebhookDedupAdmitsEachWebhookIDExactlyOnce(t *testing.T) {
	d := NewWebhookDedup()
	if !d.Admit("w1") {
		t.Fatal("first admission of a fresh webhook id should succeed")
	}
	if d.Admit("w1") {
		t.Fatal("second admission of the same webhook id should be rejected")
	}
	if !d.Admit("w2") {
		t.Fatal("a different webhook id is not a duplicate")
	}
}

func TestWebhookDedupBookkeepingStaysConsistentAtSmallScale(t *testing.T) {
	d := NewWebhookDedup()
	for i := 0; i < 1000; i++ {
		d.Admit(fmt.Sprintf("w%d", i))
	}
	if got := d.EntryCount(); got != 1000 {
		t.Fatalf("expected 1000 entries, got %d", got)
	}
}

func TestDuplicateWebhooksAreNeverForwardedTwice(t *testing.T) {
	producer := &fakeProducer{}
	p := NewPipeline(NewWebhookDedup(), producer)

	w := &pb.DeliveryWebhook{WebhookId: "w1", MessageId: "m1", Channel: "email", Status: "delivered"}
	for i := 0; i < 5; i++ {
		accepted, err := p.Ingest(context.Background(), w)
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		if i == 0 && !accepted {
			t.Fatal("first send of a fresh webhook id should be accepted")
		}
		if i > 0 && accepted {
			t.Fatal("a repeated webhook id should not be re-accepted")
		}
	}
	if got := len(producer.snapshot()); got != 1 {
		t.Fatalf("expected exactly 1 delivery of a 5-times-retried webhook, got %d", got)
	}
}

func TestPipelinePropagatesProducerErrorsWithoutSuppressingDedup(t *testing.T) {
	producer := &fakeProducer{failEvery: 1}
	p := NewPipeline(NewWebhookDedup(), producer)

	w := &pb.DeliveryWebhook{WebhookId: "w1", MessageId: "m1", Channel: "sms", Status: "sent"}
	accepted, err := p.Ingest(context.Background(), w)
	if !accepted {
		t.Fatal("webhook id should have been admitted even though the producer failed")
	}
	if err == nil {
		t.Fatal("expected the producer's error to propagate")
	}

	accepted, err = p.Ingest(context.Background(), w)
	if accepted {
		t.Fatal("a repeated webhook id should not be re-admitted even after a producer failure")
	}
	if err != nil {
		t.Fatalf("a duplicate should short-circuit before ever calling the producer again: %v", err)
	}
}

func TestReferenceDedupKeepsFirstOccurrenceOnly(t *testing.T) {
	delivered := []*pb.DeliveryWebhook{
		{WebhookId: "w1", MessageId: "m1"},
		{WebhookId: "w1", MessageId: "m1"},
		{WebhookId: "w2", MessageId: "m1"},
	}
	out := ReferenceDedup(delivered)
	if len(out) != 2 {
		t.Fatalf("expected 2 unique webhooks, got %d", len(out))
	}
}

// TestPipelineAgreesWithReferenceOverRandomRetriedDeliveries is the
// differential test behind the dedup claim: feed the same randomized,
// retried delivery stream into (a) the real Pipeline backed by a fake
// in-memory producer, and (b) the independent ReferenceDedup, and check the
// two sets of webhook ids that got through always agree exactly.
func TestPipelineAgreesWithReferenceOverRandomRetriedDeliveries(t *testing.T) {
	rng := rand.New(rand.NewSource(20260912))

	for trial := 0; trial < 100; trial++ {
		numUnique := 1 + rng.Intn(200)
		var unique []*pb.DeliveryWebhook
		for i := 0; i < numUnique; i++ {
			unique = append(unique, &pb.DeliveryWebhook{
				WebhookId: fmt.Sprintf("t%d-w%d", trial, i),
				MessageId: fmt.Sprintf("m%d", i%7),
				Channel:   Channels[i%len(Channels)].Name,
				Status:    "delivered",
			})
		}
		delivered := append([]*pb.DeliveryWebhook{}, unique...)
		for i := 0; i < rng.Intn(numUnique+1); i++ {
			delivered = append(delivered, unique[rng.Intn(len(unique))])
		}
		rng.Shuffle(len(delivered), func(i, j int) { delivered[i], delivered[j] = delivered[j], delivered[i] })

		producer := &fakeProducer{}
		p := NewPipeline(NewWebhookDedup(), producer)
		for _, w := range delivered {
			if _, err := p.Ingest(context.Background(), w); err != nil {
				t.Fatalf("trial %d: Ingest: %v", trial, err)
			}
		}

		got := idSet(producer.snapshot())
		want := idSet(ReferenceDedup(delivered))
		if !equalSets(got, want) {
			t.Fatalf("trial %d: pipeline admitted ids %v != reference %v", trial, got, want)
		}
	}
}

func TestBuildProducesTwoOrThreeWebhooksPerMessageAcrossAllChannels(t *testing.T) {
	webhooks := Build(500, 60, time.Now(), 7)
	if len(webhooks) < 1000 || len(webhooks) > 1500 {
		t.Fatalf("expected between 1000 and 1500 webhooks for 500 messages, got %d", len(webhooks))
	}
	seenChannels := map[string]bool{}
	for _, w := range webhooks {
		seenChannels[w.Channel] = true
	}
	for _, ch := range Channels {
		if !seenChannels[ch.Name] {
			t.Fatalf("channel %s never appeared in a 500-message corpus", ch.Name)
		}
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

func equalSets(a, b []string) bool {
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
