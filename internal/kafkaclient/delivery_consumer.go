package kafkaclient

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	pb "device-telemetry-gateway/proto"
)

// ConsumeAvailableDelivery reads every message currently on every partition
// of a topic, from the earliest offset up to each partition's high
// watermark at the moment this function is called. Same manual,
// no-consumer-group approach as ConsumeAvailable/ConsumeAvailableStorefront,
// for the same reason: a fresh consumer group's rebalance handshake is
// exactly the kind of timing dependency a "did everything I sent arrive"
// check should not have (see README Findings on the original gateway).
func ConsumeAvailableDelivery(ctx context.Context, brokerAddr, topic string) ([]*pb.DeliveryWebhook, error) {
	conn, err := kafka.DialContext(ctx, "tcp", brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	var out []*pb.DeliveryWebhook
	for _, part := range partitions {
		webhooks, err := readDeliveryPartition(ctx, brokerAddr, topic, part.ID)
		if err != nil {
			return nil, fmt.Errorf("partition %d: %w", part.ID, err)
		}
		out = append(out, webhooks...)
	}
	return out, nil
}

func readDeliveryPartition(ctx context.Context, brokerAddr, topic string, partition int) ([]*pb.DeliveryWebhook, error) {
	pconn, err := kafka.DialLeader(ctx, "tcp", brokerAddr, topic, partition)
	if err != nil {
		return nil, fmt.Errorf("dial leader: %w", err)
	}
	defer pconn.Close()

	first, last, err := pconn.ReadOffsets()
	if err != nil {
		return nil, fmt.Errorf("read offsets: %w", err)
	}
	if last <= first {
		return nil, nil
	}
	if _, err := pconn.Seek(first, kafka.SeekStart); err != nil {
		return nil, fmt.Errorf("seek: %w", err)
	}

	var out []*pb.DeliveryWebhook
	batch := pconn.ReadBatch(1, 100<<20)
	defer batch.Close()

	buf := make([]byte, 4<<20)
	for {
		n, err := batch.Read(buf)
		if n > 0 {
			var w pb.DeliveryWebhook
			if unmarshalErr := proto.Unmarshal(buf[:n], &w); unmarshalErr == nil {
				out = append(out, &w)
			}
		}
		if err != nil {
			break
		}
		if int64(len(out)) >= last-first {
			break
		}
	}
	return out, nil
}

// DeliveryPartitionOffsets returns each partition id in topic paired with
// the current [first, last) offset bounds, used by the burst-drain
// benchmark to compute how far behind the newest produced offset a
// consumer's own progress is.
func DeliveryPartitionOffsets(ctx context.Context, brokerAddr, topic string) (map[int]struct{ First, Last int64 }, error) {
	conn, err := kafka.DialContext(ctx, "tcp", brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	out := make(map[int]struct{ First, Last int64 }, len(partitions))
	for _, part := range partitions {
		pconn, err := kafka.DialLeader(ctx, "tcp", brokerAddr, topic, part.ID)
		if err != nil {
			return nil, fmt.Errorf("dial leader %d: %w", part.ID, err)
		}
		first, last, err := pconn.ReadOffsets()
		pconn.Close()
		if err != nil {
			return nil, fmt.Errorf("read offsets %d: %w", part.ID, err)
		}
		out[part.ID] = struct{ First, Last int64 }{first, last}
	}
	return out, nil
}

// DeliveryStreamReader consumes a topic continuously, one kafka.Reader per
// partition in simple (no consumer group) mode.
type DeliveryStreamReader struct {
	readers []*kafka.Reader
}

func NewDeliveryStreamReader(brokerAddr, topic string, partitionCount int) *DeliveryStreamReader {
	readers := make([]*kafka.Reader, partitionCount)
	for i := 0; i < partitionCount; i++ {
		readers[i] = kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{brokerAddr},
			Topic:     topic,
			Partition: i,
			MinBytes:  1,
			MaxBytes:  10 << 20,
		})
		readers[i].SetOffset(kafka.FirstOffset)
	}
	return &DeliveryStreamReader{readers: readers}
}

// Messages returns a channel of every webhook across all partitions,
// decoded, closed when ctx is cancelled.
func (r *DeliveryStreamReader) Messages(ctx context.Context) <-chan *pb.DeliveryWebhook {
	out := make(chan *pb.DeliveryWebhook, 4096)
	for _, reader := range r.readers {
		go func(rd *kafka.Reader) {
			for {
				msg, err := rd.ReadMessage(ctx)
				if err != nil {
					return
				}
				var w pb.DeliveryWebhook
				if err := proto.Unmarshal(msg.Value, &w); err != nil {
					continue
				}
				select {
				case out <- &w:
				case <-ctx.Done():
					return
				}
			}
		}(reader)
	}
	return out
}

// CurrentOffsets returns this reader's own last-consumed offset per
// partition.
func (r *DeliveryStreamReader) CurrentOffsets() map[int]int64 {
	out := make(map[int]int64, len(r.readers))
	for i, reader := range r.readers {
		out[i] = reader.Offset()
	}
	return out
}

func (r *DeliveryStreamReader) Close() error {
	var firstErr error
	for _, reader := range r.readers {
		if err := reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
