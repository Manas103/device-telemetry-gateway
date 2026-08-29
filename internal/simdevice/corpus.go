// Package simdevice builds and drives simulated devices over real WebSocket
// connections, for both the replay-correctness benchmark and the
// broker-outage benchmark.
package simdevice

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	pb "device-telemetry-gateway/proto"
)

// DeviceStream is one simulated device's delivery order: what it will
// actually send, over its own WebSocket connection, in send order.
type DeviceStream struct {
	DeviceID string
	Delivery []*pb.Reading
}

// AllUnique returns every distinct (device_id, sequence) Reading across all
// streams, i.e. what a perfect dedup should end up with. Used as the input
// to the reference oracle.
func AllUnique(streams []DeviceStream) []*pb.Reading {
	var out []*pb.Reading
	for _, s := range streams {
		seen := make(map[uint64]bool)
		for _, r := range s.Delivery {
			if seen[r.Sequence] {
				continue
			}
			seen[r.Sequence] = true
			out = append(out, r)
		}
	}
	return out
}

// BuildReplayCorpus builds numDevices independent devices, each with
// readingsPerDevice true sequential readings, then perturbs each device's
// own delivery order so that exactly dupFraction of the full delivered
// stream (not the unique stream) are duplicates, and shuffles every
// device's delivery into a genuinely non-monotonic send order.
func BuildReplayCorpus(numDevices, readingsPerDevice int, dupFraction float64, seed int64) []DeviceStream {
	rng := rand.New(rand.NewSource(seed))
	streams := make([]DeviceStream, numDevices)
	now := time.Now().UnixMilli()

	// dupCount is chosen so dupCount / (readingsPerDevice + dupCount) ==
	// dupFraction, i.e. dupFraction really is the fraction of the delivered
	// (not the unique) stream that is duplicate traffic.
	dupCount := int(math.Round(dupFraction * float64(readingsPerDevice) / (1 - dupFraction)))

	for d := 0; d < numDevices; d++ {
		deviceID := fmt.Sprintf("dev-%05d", d)
		unique := make([]*pb.Reading, readingsPerDevice)
		for i := 0; i < readingsPerDevice; i++ {
			unique[i] = &pb.Reading{
				DeviceId:    deviceID,
				Sequence:    uint64(i + 1),
				TimestampMs: now,
				Value:       float64(i+1) * 1.5,
			}
		}

		delivery := make([]*pb.Reading, 0, readingsPerDevice+dupCount)
		delivery = append(delivery, unique...)
		for i := 0; i < dupCount; i++ {
			delivery = append(delivery, unique[rng.Intn(readingsPerDevice)])
		}
		rng.Shuffle(len(delivery), func(i, j int) { delivery[i], delivery[j] = delivery[j], delivery[i] })

		streams[d] = DeviceStream{DeviceID: deviceID, Delivery: delivery}
	}
	return streams
}

// Send opens one WebSocket connection per stream (bounded to `concurrency`
// connections open at a time) and writes every Reading in that device's
// delivery order as a single binary protobuf frame.
func Send(ctx context.Context, wsURL string, streams []DeviceStream, concurrency int) error {
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	errCh := make(chan error, len(streams))

	for _, stream := range streams {
		wg.Add(1)
		sem <- struct{}{}
		go func(s DeviceStream) {
			defer wg.Done()
			defer func() { <-sem }()

			conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
			if err != nil {
				errCh <- fmt.Errorf("device %s: dial: %w", s.DeviceID, err)
				return
			}
			defer conn.Close()

			for _, r := range s.Delivery {
				b, err := proto.Marshal(r)
				if err != nil {
					errCh <- fmt.Errorf("device %s: marshal: %w", s.DeviceID, err)
					return
				}
				if err := conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
					errCh <- fmt.Errorf("device %s: write: %w", s.DeviceID, err)
					return
				}
			}
		}(stream)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}
