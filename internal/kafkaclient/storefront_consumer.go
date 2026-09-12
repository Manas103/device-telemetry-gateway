package kafkaclient

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	pb "device-telemetry-gateway/proto"
)

// ConsumeAvailableStorefront reads every message currently on every
// partition of a topic, from the earliest offset up to each partition's
// high watermark at the moment this function is called. Same manual,
// no-consumer-group approach as ConsumeAvailable, and for the same reason
// (see internal/kafkaclient/consumer.go and the README's Findings): a
// fresh consumer group's rebalance handshake is exactly the kind of timing
// dependency a "did everything I sent arrive" check should not have.
func ConsumeAvailableStorefront(ctx context.Context, brokerAddr, topic string) ([]*pb.StorefrontEvent, error) {
	conn, err := kafka.DialContext(ctx, "tcp", brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	var out []*pb.StorefrontEvent
	for _, part := range partitions {
		events, err := readStorefrontPartition(ctx, brokerAddr, topic, part.ID)
		if err != nil {
			return nil, fmt.Errorf("partition %d: %w", part.ID, err)
		}
		out = append(out, events...)
	}
	return out, nil
}

func readStorefrontPartition(ctx context.Context, brokerAddr, topic string, partition int) ([]*pb.StorefrontEvent, error) {
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

	var out []*pb.StorefrontEvent
	batch := pconn.ReadBatch(1, 100<<20)
	defer batch.Close()

	buf := make([]byte, 4<<20)
	for {
		n, err := batch.Read(buf)
		if n > 0 {
			var e pb.StorefrontEvent
			if unmarshalErr := proto.Unmarshal(buf[:n], &e); unmarshalErr == nil {
				out = append(out, &e)
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

// PartitionOffsets returns each partition id in topic paired with the
// current [first, last) offset bounds, used by the segment engine to
// compute how far behind the newest produced offset a slow consumer is
// (the burst-backlog-drain benchmark's "lag" number) without depending on a
// broker-side consumer group's committed offsets.
func PartitionOffsets(ctx context.Context, brokerAddr, topic string) (map[int]struct{ First, Last int64 }, error) {
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

// StorefrontStreamReader consumes a topic continuously, one kafka.Reader per
// partition in simple (no consumer group) mode, so there is no rebalance
// handshake and no broker-side committed offset: the caller's own process
// memory is the only place progress is tracked, which is exactly right for
// a benchmark that measures this run's own lag and throughput.
type StorefrontStreamReader struct {
	readers []*kafka.Reader
}

func NewStorefrontStreamReader(brokerAddr, topic string, partitionCount int) *StorefrontStreamReader {
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
	return &StorefrontStreamReader{readers: readers}
}

// Messages returns a channel of every event across all partitions, decoded,
// closed when ctx is cancelled. Decode errors are dropped (logged by the
// caller if it wants), matching this project's existing tolerance for a
// corrupt individual record over aborting a whole stream.
func (r *StorefrontStreamReader) Messages(ctx context.Context) <-chan *pb.StorefrontEvent {
	out := make(chan *pb.StorefrontEvent, 4096)
	for _, reader := range r.readers {
		go func(rd *kafka.Reader) {
			for {
				msg, err := rd.ReadMessage(ctx)
				if err != nil {
					return // ctx cancelled or reader closed
				}
				var e pb.StorefrontEvent
				if err := proto.Unmarshal(msg.Value, &e); err != nil {
					continue
				}
				select {
				case out <- &e:
				case <-ctx.Done():
					return
				}
			}
		}(reader)
	}
	return out
}

// CurrentOffsets returns this reader's own last-consumed offset per
// partition (not the broker's high watermark; pair with PartitionOffsets to
// compute lag).
func (r *StorefrontStreamReader) CurrentOffsets() map[int]int64 {
	out := make(map[int]int64, len(r.readers))
	for i, reader := range r.readers {
		out[i] = reader.Offset()
	}
	return out
}

func (r *StorefrontStreamReader) Close() error {
	var firstErr error
	for _, reader := range r.readers {
		if err := reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
