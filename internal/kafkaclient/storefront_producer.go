package kafkaclient

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	pb "device-telemetry-gateway/proto"
)

// StorefrontProducer publishes each StorefrontEvent as one Kafka message,
// keyed by profile id so every event for one profile lands in the same
// partition and the segment engine's single-partition consumer sees a
// stable relative order for that profile's own event history (it does not
// need cross-profile ordering: segment predicates are evaluated per
// profile).
type StorefrontProducer struct {
	writer *kafka.Writer
}

func NewStorefrontProducer(brokerAddr, topic string) *StorefrontProducer {
	return &StorefrontProducer{writer: &kafka.Writer{
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

func (p *StorefrontProducer) Produce(ctx context.Context, e *pb.StorefrontEvent) error {
	value, err := proto.Marshal(e)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(e.ProfileId),
		Value: value,
	})
}

func (p *StorefrontProducer) Close() error {
	return p.writer.Close()
}
