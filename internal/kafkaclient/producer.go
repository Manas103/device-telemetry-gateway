// Package kafkaclient wraps segmentio/kafka-go for the two things this
// project needs: publish one Reading per message, keyed by device id, and
// read everything currently on a topic back out for verification.
package kafkaclient

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	pb "device-telemetry-gateway/proto"
)

// Producer publishes each Reading as one Kafka message, keyed by device id
// so every reading from one device lands in the same partition and a
// per-partition consumer sees a stable relative order for that device.
type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokerAddr, topic string) *Producer {
	return &Producer{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokerAddr),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 2 * time.Second,
		RequiredAcks: kafka.RequireOne,
	}}
}

func (p *Producer) Produce(ctx context.Context, r *pb.Reading) error {
	value, err := proto.Marshal(r)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(r.DeviceId),
		Value: value,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

// OutageInjectingProducer wraps a real Producer and fails every call while
// the outage window is active, so a test can exercise the spool/drain path
// deterministically without needing to stop the actual shared Kafka broker.
// The 60-second "simulated broker outage" in the resume claim is simulated
// exactly here: the producer, not the broker, refuses to accept writes.
type OutageInjectingProducer struct {
	inner       *Producer
	outageUntil func() time.Time
}

func NewOutageInjectingProducer(inner *Producer, outageUntil func() time.Time) *OutageInjectingProducer {
	return &OutageInjectingProducer{inner: inner, outageUntil: outageUntil}
}

func (p *OutageInjectingProducer) Produce(ctx context.Context, r *pb.Reading) error {
	if time.Now().Before(p.outageUntil()) {
		return errBrokerUnreachable
	}
	return p.inner.Produce(ctx, r)
}

func (p *OutageInjectingProducer) Close() error {
	return p.inner.Close()
}

var errBrokerUnreachable = &brokerUnreachableError{}

type brokerUnreachableError struct{}

func (e *brokerUnreachableError) Error() string {
	return "simulated broker outage: producer refusing writes"
}
