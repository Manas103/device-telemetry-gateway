package ingest

import (
	"context"
	"log"
	"sync"
	"time"

	pb "device-telemetry-gateway/proto"
)

// Producer is the narrow interface Pipeline needs from a Kafka writer. Tests
// and the outage benchmark substitute a fault-injecting implementation
// without touching the pipeline logic itself.
type Producer interface {
	Produce(ctx context.Context, r *pb.Reading) error
}

// Pipeline is the ingest path: dedup, then hand off to Kafka; if Kafka
// cannot take it right now, spill to disk and let the drainer retry later.
// Nothing here ever silently drops a reading that passed dedup.
type Pipeline struct {
	dedup    *Dedup
	spool    *Spool
	producer Producer
}

func NewPipeline(dedup *Dedup, spool *Spool, producer Producer) *Pipeline {
	return &Pipeline{dedup: dedup, spool: spool, producer: producer}
}

// Ingest is called once per Reading arriving over a device's WebSocket
// connection. It returns quickly: a Kafka failure never blocks the caller,
// it spills to disk and returns.
func (p *Pipeline) Ingest(ctx context.Context, r *pb.Reading) error {
	if !p.dedup.Admit(r.DeviceId, r.Sequence) {
		return nil // exact duplicate of an already-forwarded reading; dropped by design
	}

	produceCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err := p.producer.Produce(produceCtx, r); err != nil {
		if spoolErr := p.spool.Write(r); spoolErr != nil {
			log.Printf("ingest: spool write failed after produce failure (produce error: %v): %v", err, spoolErr)
			return spoolErr
		}
		return nil
	}
	return nil
}

// RunDrainer periodically retries every spooled reading against the
// producer until ctx is cancelled. A reading that still cannot be produced
// (the outage is still ongoing) goes straight back into the spool, so a
// reading is never lost between drain attempts.
func (p *Pipeline) RunDrainer(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.drainOnce(ctx)
		}
	}
}

// drainConcurrency bounds how many spooled readings are retried against the
// producer at once. A long outage can leave thousands of readings queued;
// retrying them one at a time, each paying a network round trip, made
// recovery take far longer than the outage itself. Retrying in parallel
// keeps recovery time proportional to the backlog divided by concurrency,
// not to the backlog alone.
const drainConcurrency = 32

func (p *Pipeline) drainOnce(ctx context.Context) {
	records, err := p.spool.Drain()
	if err != nil {
		log.Printf("ingest: spool drain failed: %v", err)
		return
	}

	sem := make(chan struct{}, drainConcurrency)
	var wg sync.WaitGroup
	for _, r := range records {
		wg.Add(1)
		sem <- struct{}{}
		go func(r *pb.Reading) {
			defer wg.Done()
			defer func() { <-sem }()

			produceCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			err := p.producer.Produce(produceCtx, r)
			cancel()
			if err != nil {
				if spoolErr := p.spool.Write(r); spoolErr != nil {
					log.Printf("ingest: re-spool failed for %s/%d: %v", r.DeviceId, r.Sequence, spoolErr)
				}
			}
		}(r)
	}
	wg.Wait()
}
