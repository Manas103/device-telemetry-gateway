package ingest

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	pb "device-telemetry-gateway/proto"
)

// fakeProducer is an in-memory stand-in for Kafka. failUntilCall makes the
// first N calls fail, to exercise the spool/drain path deterministically.
type fakeProducer struct {
	mu            sync.Mutex
	delivered     []*pb.Reading
	failUntilCall int
	calls         int
}

func (f *fakeProducer) Produce(_ context.Context, r *pb.Reading) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failUntilCall {
		return fmt.Errorf("simulated producer failure")
	}
	f.delivered = append(f.delivered, r)
	return nil
}

func (f *fakeProducer) snapshot() []*pb.Reading {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*pb.Reading, len(f.delivered))
	copy(out, f.delivered)
	return out
}

func newTempSpool(t *testing.T) *Spool {
	t.Helper()
	path := t.TempDir() + "/spool.dat"
	s, err := NewSpool(path)
	if err != nil {
		t.Fatalf("NewSpool: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestDedupAdmitsEachSequenceExactlyOnce(t *testing.T) {
	d := NewDedup()
	if !d.Admit("d1", 5) {
		t.Fatal("first admission of a fresh sequence should succeed")
	}
	if d.Admit("d1", 5) {
		t.Fatal("second admission of the same sequence should be rejected")
	}
	if !d.Admit("d1", 3) {
		t.Fatal("a lower, but never-seen, sequence should still be admitted (out-of-order delivery is allowed)")
	}
	if !d.Admit("d2", 5) {
		t.Fatal("the same sequence number on a different device is not a duplicate")
	}
}

func TestPipelineSpillsToDiskWhenProducerFails(t *testing.T) {
	spool := newTempSpool(t)
	producer := &fakeProducer{failUntilCall: 1000} // always fails
	p := NewPipeline(NewDedup(), spool, producer)

	r := &pb.Reading{DeviceId: "d1", Sequence: 1, Value: 42}
	if err := p.Ingest(context.Background(), r); err != nil {
		t.Fatalf("Ingest should not itself fail when the spool write succeeds: %v", err)
	}
	if len(producer.snapshot()) != 0 {
		t.Fatal("producer should not have received anything while failing")
	}

	spooled, err := spool.Drain()
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(spooled) != 1 || spooled[0].DeviceId != "d1" || spooled[0].Sequence != 1 {
		t.Fatalf("expected the failed reading in the spool, got %+v", spooled)
	}
}

func TestPipelineDrainerDeliversSpooledReadingsOnceTheProducerRecovers(t *testing.T) {
	spool := newTempSpool(t)
	producer := &fakeProducer{failUntilCall: 3} // first 3 calls fail, then recover
	p := NewPipeline(NewDedup(), spool, producer)

	for i := 1; i <= 3; i++ {
		if err := p.Ingest(context.Background(), &pb.Reading{DeviceId: "d1", Sequence: uint64(i), Value: float64(i)}); err != nil {
			t.Fatalf("Ingest %d: %v", i, err)
		}
	}
	if len(producer.snapshot()) != 0 {
		t.Fatal("nothing should have been delivered yet")
	}

	p.drainOnce(context.Background())

	delivered := producer.snapshot()
	if len(delivered) != 3 {
		t.Fatalf("expected all 3 spooled readings delivered after recovery, got %d", len(delivered))
	}

	remaining, err := spool.Drain()
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("spool should be empty after a successful drain, found %d leftover", len(remaining))
	}
}

func TestDuplicateReadingsAreNeverForwardedTwice(t *testing.T) {
	spool := newTempSpool(t)
	producer := &fakeProducer{}
	p := NewPipeline(NewDedup(), spool, producer)

	r := &pb.Reading{DeviceId: "d1", Sequence: 7, Value: 1}
	for i := 0; i < 5; i++ { // send the exact same reading 5 times
		if err := p.Ingest(context.Background(), r); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	if got := len(producer.snapshot()); got != 1 {
		t.Fatalf("expected exactly 1 delivery of a 5-times-repeated reading, got %d", got)
	}
}

func TestFinalStateDigestIsOrderIndependent(t *testing.T) {
	a := []*pb.Reading{
		{DeviceId: "d2", Sequence: 1, Value: 1},
		{DeviceId: "d1", Sequence: 2, Value: 2},
		{DeviceId: "d1", Sequence: 1, Value: 1},
	}
	b := []*pb.Reading{ // same set, different order
		{DeviceId: "d1", Sequence: 1, Value: 1},
		{DeviceId: "d1", Sequence: 2, Value: 2},
		{DeviceId: "d2", Sequence: 1, Value: 1},
	}
	da, db := FinalStateDigest(a), FinalStateDigest(b)
	if string(da) != string(db) {
		t.Fatalf("digest should not depend on slice order: %x != %x", da, db)
	}

	c := append(append([]*pb.Reading{}, a...), &pb.Reading{DeviceId: "d3", Sequence: 1, Value: 99})
	if string(FinalStateDigest(c)) == string(da) {
		t.Fatal("adding a reading should change the digest")
	}
}

func TestReferenceDedupKeepsFirstOccurrenceOnly(t *testing.T) {
	delivered := []*pb.Reading{
		{DeviceId: "d1", Sequence: 1, Value: 100},
		{DeviceId: "d1", Sequence: 1, Value: 100}, // exact duplicate
		{DeviceId: "d1", Sequence: 2, Value: 200},
	}
	out := ReferenceDedup(delivered)
	if len(out) != 2 {
		t.Fatalf("expected 2 unique readings, got %d", len(out))
	}
}

// TestPipelineAgreesWithReferenceOverRandomDuplicatedOutOfOrderStreams is the
// differential test behind the replay claim: feed the same randomized,
// duplicated, reordered stream into (a) the real Pipeline backed by a fake
// in-memory producer, and (b) the independent ReferenceDedup, and check the
// two final-state digests always agree.
func TestPipelineAgreesWithReferenceOverRandomDuplicatedOutOfOrderStreams(t *testing.T) {
	rng := rand.New(rand.NewSource(20260829))

	for trial := 0; trial < 100; trial++ {
		numDevices := 1 + rng.Intn(5)
		readingsPerDevice := 1 + rng.Intn(20)

		var truth []*pb.Reading
		var delivered []*pb.Reading
		for d := 0; d < numDevices; d++ {
			deviceID := fmt.Sprintf("d%d", d)
			var unique []*pb.Reading
			for s := 1; s <= readingsPerDevice; s++ {
				r := &pb.Reading{DeviceId: deviceID, Sequence: uint64(s), Value: float64(s)}
				unique = append(unique, r)
				truth = append(truth, r)
			}
			// Duplicate a random subset and shuffle the delivery order.
			dup := append([]*pb.Reading{}, unique...)
			for i := 0; i < rng.Intn(readingsPerDevice+1); i++ {
				dup = append(dup, unique[rng.Intn(len(unique))])
			}
			rng.Shuffle(len(dup), func(i, j int) { dup[i], dup[j] = dup[j], dup[i] })
			delivered = append(delivered, dup...)
		}

		spool := newTempSpool(t)
		producer := &fakeProducer{}
		p := NewPipeline(NewDedup(), spool, producer)
		for _, r := range delivered {
			if err := p.Ingest(context.Background(), r); err != nil {
				t.Fatalf("trial %d: Ingest: %v", trial, err)
			}
		}

		gotDigest := FinalStateDigest(producer.snapshot())
		wantDigest := FinalStateDigest(ReferenceDedup(delivered))
		if string(gotDigest) != string(wantDigest) {
			t.Fatalf("trial %d: pipeline digest %x != reference digest %x (delivered %d, truth %d)",
				trial, gotDigest, wantDigest, len(delivered), len(truth))
		}
	}
}
