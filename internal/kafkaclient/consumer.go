package kafkaclient

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	pb "device-telemetry-gateway/proto"
)

// ConsumeAvailable reads every message currently on every partition of a
// topic, from the earliest offset up to each partition's high watermark at
// the moment this function is called. It deliberately avoids kafka-go's
// consumer-group reader for this: group-coordinator join and partition
// assignment adds a rebalance round trip whose latency is exactly the kind
// of timing noise a "did everything I sent arrive" verification pass should
// not depend on. Reading each partition directly by offset is slower to
// write but has no such race.
func ConsumeAvailable(ctx context.Context, brokerAddr, topic string) ([]*pb.Reading, error) {
	conn, err := kafka.DialContext(ctx, "tcp", brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	var out []*pb.Reading
	for _, part := range partitions {
		readings, err := readPartition(ctx, brokerAddr, topic, part.ID)
		if err != nil {
			return nil, fmt.Errorf("partition %d: %w", part.ID, err)
		}
		out = append(out, readings...)
	}
	return out, nil
}

func readPartition(ctx context.Context, brokerAddr, topic string, partition int) ([]*pb.Reading, error) {
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
		return nil, nil // empty partition
	}

	if _, err := pconn.Seek(first, kafka.SeekStart); err != nil {
		return nil, fmt.Errorf("seek: %w", err)
	}

	var out []*pb.Reading
	batch := pconn.ReadBatch(1, 50<<20)
	defer batch.Close()

	buf := make([]byte, 1<<20)
	for {
		n, err := batch.Read(buf)
		if n > 0 {
			var reading pb.Reading
			if unmarshalErr := proto.Unmarshal(buf[:n], &reading); unmarshalErr == nil {
				out = append(out, &reading)
			}
		}
		if err != nil {
			break // end of the batch's available bytes (we asked for exactly [first, last))
		}
		if int64(len(out)) >= last-first {
			break
		}
	}
	return out, nil
}
