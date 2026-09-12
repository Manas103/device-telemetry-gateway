package eventingest

import (
	"context"

	pb "device-telemetry-gateway/proto"
)

// Producer is the narrow interface Pipeline needs from a Kafka writer.
type Producer interface {
	Produce(ctx context.Context, e *pb.StorefrontEvent) error
}

// Pipeline is the storefront-event edge: dedup on event_id, then hand off to
// Kafka. Unlike internal/ingest.Pipeline, this pipeline does not spool to
// disk on a Kafka failure; that durability path was in scope for the device
// telemetry gateway's own claims and is not one of this extension's claims
// (see README Limitations). A Kafka produce error is returned to the caller
// as-is.
type Pipeline struct {
	dedup    *EventDedup
	producer Producer
}

func NewPipeline(dedup *EventDedup, producer Producer) *Pipeline {
	return &Pipeline{dedup: dedup, producer: producer}
}

// Ingest is called once per StorefrontEvent arriving at the edge. It returns
// (accepted=false, err=nil) for an exact duplicate event_id, which the
// caller treats as a success (the event is already on its way, or already
// landed, from an earlier send).
func (p *Pipeline) Ingest(ctx context.Context, e *pb.StorefrontEvent) (accepted bool, err error) {
	if !p.dedup.Admit(e.EventId) {
		return false, nil
	}
	if err := p.producer.Produce(ctx, e); err != nil {
		return true, err
	}
	return true, nil
}
