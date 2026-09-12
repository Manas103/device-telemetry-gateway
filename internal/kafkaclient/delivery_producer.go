package kafkaclient

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	pb "device-telemetry-gateway/proto"
)

// DeliveryProducer publishes each DeliveryWebhook as one Kafka message, keyed
// by message_id so every webhook belonging to one outbound message lands in
// the same partition (a consumer that ever wants "this message's webhooks in
// order" gets that for free from partition ordering, the same reasoning
// StorefrontProducer applies per-profile).
type DeliveryProducer struct {
	writer *kafka.Writer
}

func NewDeliveryProducer(brokerAddr, topic string) *DeliveryProducer {
	return &DeliveryProducer{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokerAddr),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    500,
		WriteTimeout: 5 * time.Second,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}}
}

func (p *DeliveryProducer) Produce(ctx context.Context, w *pb.DeliveryWebhook) error {
	value, err := proto.Marshal(w)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(w.MessageId),
		Value: value,
	})
}

func (p *DeliveryProducer) Close() error {
	return p.writer.Close()
}

// DeliveryBatchProducer is a second, batching producer used only by the
// throughput benchmark (cmd/deliverythroughput). Unlike DeliveryProducer
// (one WriteMessages call per webhook, matching this repo's existing
// StorefrontProducer/eventingest precedent for correctness benchmarks where
// per-call latency does not matter), the throughput claim is required to be
// measured under a hard cap on concurrent goroutines (6, see README
// "Resource courtesy"), and a handful of goroutines each issuing one
// network round trip per webhook cannot plausibly reach a five-figure
// events/sec rate. Batching many webhooks into one WriteMessages call per
// goroutine is what makes a small, courteous number of goroutines able to
// sustain high throughput: the concurrency limit is on OS threads doing
// work, not on how many messages one of them can hand to the client library
// per call.
type DeliveryBatchProducer struct {
	writer *kafka.Writer
}

func NewDeliveryBatchProducer(brokerAddr, topic string, batchSize int) *DeliveryBatchProducer {
	return &DeliveryBatchProducer{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokerAddr),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 20 * time.Millisecond,
		BatchSize:    batchSize,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}}
}

// ProduceBatch marshals and writes every webhook in one WriteMessages call.
func (p *DeliveryBatchProducer) ProduceBatch(ctx context.Context, webhooks []*pb.DeliveryWebhook) error {
	msgs := make([]kafka.Message, len(webhooks))
	for i, w := range webhooks {
		value, err := proto.Marshal(w)
		if err != nil {
			return err
		}
		msgs[i] = kafka.Message{Key: []byte(w.MessageId), Value: value}
	}
	return p.writer.WriteMessages(ctx, msgs...)
}

func (p *DeliveryBatchProducer) Close() error {
	return p.writer.Close()
}
