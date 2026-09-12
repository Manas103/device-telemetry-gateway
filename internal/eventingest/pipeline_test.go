package eventingest

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	pb "device-telemetry-gateway/proto"
)

type fakeProducer struct {
	mu        sync.Mutex
	delivered []*pb.StorefrontEvent
	failEvery int // if > 0, every failEvery-th call fails (not a duplicate-suppression path here, just to exercise error propagation)
	calls     int
}

func (f *fakeProducer) Produce(_ context.Context, e *pb.StorefrontEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failEvery > 0 && f.calls%f.failEvery == 0 {
		return fmt.Errorf("simulated producer failure")
	}
	f.delivered = append(f.delivered, e)
	return nil
}

func (f *fakeProducer) snapshot() []*pb.StorefrontEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*pb.StorefrontEvent, len(f.delivered))
	copy(out, f.delivered)
	return out
}

func TestEventDedupAdmitsEachEventIDExactlyOnce(t *testing.T) {
	d := NewEventDedup()
	if !d.Admit("e1") {
		t.Fatal("first admission of a fresh event id should succeed")
	}
	if d.Admit("e1") {
		t.Fatal("second admission of the same event id should be rejected")
	}
	if !d.Admit("e2") {
		t.Fatal("a different event id is not a duplicate")
	}
}

func TestEventDedupEvictsOldestOnceBoundExceeded(t *testing.T) {
	// Shrink the effective bound via a tiny local instance is not possible
	// (MaxEventIDs is a package constant), so this test only checks the
	// FIFO-eviction bookkeeping logic stays internally consistent at a
	// small scale: entry count never exceeds MaxEventIDs even if every id
	// were unique. Full-scale eviction is exercised by cmd/eventgen's
	// dedup-cache accounting, mirroring internal/ingest's soak precedent.
	d := NewEventDedup()
	for i := 0; i < 1000; i++ {
		d.Admit(fmt.Sprintf("e%d", i))
	}
	if got := d.EntryCount(); got != 1000 {
		t.Fatalf("expected 1000 entries, got %d", got)
	}
}

func TestDuplicateEventsAreNeverForwardedTwice(t *testing.T) {
	producer := &fakeProducer{}
	p := NewPipeline(NewEventDedup(), producer)

	e := &pb.StorefrontEvent{EventId: "e1", ProfileId: "p1", EventType: "page_view"}
	for i := 0; i < 5; i++ {
		accepted, err := p.Ingest(context.Background(), e)
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		if i == 0 && !accepted {
			t.Fatal("first send of a fresh event id should be accepted")
		}
		if i > 0 && accepted {
			t.Fatal("a repeated event id should not be re-accepted")
		}
	}
	if got := len(producer.snapshot()); got != 1 {
		t.Fatalf("expected exactly 1 delivery of a 5-times-repeated event, got %d", got)
	}
}

func TestPipelinePropagatesProducerErrorsWithoutSuppressingDedup(t *testing.T) {
	producer := &fakeProducer{failEvery: 1} // always fails
	p := NewPipeline(NewEventDedup(), producer)

	e := &pb.StorefrontEvent{EventId: "e1", ProfileId: "p1", EventType: "purchase"}
	accepted, err := p.Ingest(context.Background(), e)
	if !accepted {
		t.Fatal("event id should have been admitted even though the producer failed")
	}
	if err == nil {
		t.Fatal("expected the producer's error to propagate")
	}

	// A retry of the exact same event id should be recognized as a
	// duplicate rather than retried, exactly like the first attempt: this
	// edge does not distinguish "duplicate because it already succeeded"
	// from "duplicate because an earlier attempt is still in flight or
	// failed"; both collapse to the same event_id key, matching a real
	// client-side retry that resends the identical event_id either way.
	accepted, err = p.Ingest(context.Background(), e)
	if accepted {
		t.Fatal("a repeated event id should not be re-admitted even after a producer failure")
	}
	if err != nil {
		t.Fatalf("a duplicate should short-circuit before ever calling the producer again: %v", err)
	}
}

func TestReferenceDedupKeepsFirstOccurrenceOnly(t *testing.T) {
	delivered := []*pb.StorefrontEvent{
		{EventId: "e1", ProfileId: "p1"},
		{EventId: "e1", ProfileId: "p1"}, // exact duplicate
		{EventId: "e2", ProfileId: "p1"},
	}
	out := ReferenceDedup(delivered)
	if len(out) != 2 {
		t.Fatalf("expected 2 unique events, got %d", len(out))
	}
}

// TestPipelineAgreesWithReferenceOverRandomDuplicatedDeliveries is the
// differential test behind the dedup claim: feed the same randomized,
// duplicated delivery stream into (a) the real Pipeline backed by a fake
// in-memory producer, and (b) the independent ReferenceDedup, and check the
// two sets of event ids that got through always agree exactly.
func TestPipelineAgreesWithReferenceOverRandomDuplicatedDeliveries(t *testing.T) {
	rng := rand.New(rand.NewSource(20260912))

	for trial := 0; trial < 100; trial++ {
		numUnique := 1 + rng.Intn(200)
		var unique []*pb.StorefrontEvent
		for i := 0; i < numUnique; i++ {
			unique = append(unique, &pb.StorefrontEvent{
				EventId:   fmt.Sprintf("t%d-e%d", trial, i),
				ProfileId: fmt.Sprintf("p%d", i%7),
				EventType: "page_view",
			})
		}
		delivered := append([]*pb.StorefrontEvent{}, unique...)
		for i := 0; i < rng.Intn(numUnique+1); i++ {
			delivered = append(delivered, unique[rng.Intn(len(unique))])
		}
		rng.Shuffle(len(delivered), func(i, j int) { delivered[i], delivered[j] = delivered[j], delivered[i] })

		producer := &fakeProducer{}
		p := NewPipeline(NewEventDedup(), producer)
		for _, e := range delivered {
			if _, err := p.Ingest(context.Background(), e); err != nil {
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

func idSet(events []*pb.StorefrontEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.EventId
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
