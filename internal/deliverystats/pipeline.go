package deliverystats

import (
	"context"

	pb "device-telemetry-gateway/proto"
)

// Producer is the narrow interface Pipeline needs from a Kafka writer.
type Producer interface {
	Produce(ctx context.Context, w *pb.DeliveryWebhook) error
}

// Pipeline is the delivery-webhook edge: dedup on webhook_id, then hand off
// to Kafka. Like internal/eventingest.Pipeline (and unlike
// internal/ingest.Pipeline) this does not spool to disk on a Kafka failure;
// that durability path is the original telemetry gateway's own claim, not
// this extension's. A Kafka produce error is returned to the caller as-is.
type Pipeline struct {
	dedup    *WebhookDedup
	producer Producer
}

func NewPipeline(dedup *WebhookDedup, producer Producer) *Pipeline {
	return &Pipeline{dedup: dedup, producer: producer}
}

// Ingest is called once per DeliveryWebhook arriving at the edge. It returns
// (accepted=false, err=nil) for an exact duplicate webhook_id, which the
// caller treats as a success (the webhook is already on its way, or already
// landed, from an earlier delivery attempt).
func (p *Pipeline) Ingest(ctx context.Context, w *pb.DeliveryWebhook) (accepted bool, err error) {
	if !p.dedup.Admit(w.WebhookId) {
		return false, nil
	}
	if err := p.producer.Produce(ctx, w); err != nil {
		return true, err
	}
	return true, nil
}
