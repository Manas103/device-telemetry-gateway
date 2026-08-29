// outagebench is the reproducible measurement behind the resume claim: "0
// records lost across a 60-second simulated broker outage". Devices send
// continuously, at a steady rate, before, during and after a window in which
// the producer is made to fail every write (simulating the broker being
// unreachable). Records that fail to produce during the outage are expected
// to spool to disk and drain back out once the window ends; the benchmark
// waits for that drain and then counts what actually landed in Kafka against
// what was actually sent.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"device-telemetry-gateway/internal/ingest"
	"device-telemetry-gateway/internal/kafkaclient"
	"device-telemetry-gateway/internal/wsserver"
	pb "device-telemetry-gateway/proto"
)

func main() {
	numDevices := flag.Int("devices", 50, "number of simulated devices sending continuously")
	rate := flag.Duration("interval", 1*time.Second, "interval between readings per device")
	preOutage := flag.Duration("pre-outage", 10*time.Second, "normal operation before the outage starts")
	outageDuration := flag.Duration("outage", 60*time.Second, "duration of the simulated broker outage")
	postOutage := flag.Duration("post-outage", 15*time.Second, "continued sending after the outage ends")
	maxDrainWait := flag.Duration("max-drain-wait", 5*time.Minute, "upper bound on how long to poll for the spool to empty after sending stops")
	broker := flag.String("broker", "127.0.0.1:9092", "kafka broker address")
	topic := flag.String("topic", "telemetry-outage-bench", "kafka topic")
	spoolPath := flag.String("spool", "/tmp/outagebench-spool.dat", "backpressure spool path")
	flag.Parse()

	realProducer := kafkaclient.NewProducer(*broker, *topic)
	defer realProducer.Close()

	var mu sync.Mutex
	var outageUntil time.Time
	getOutageUntil := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return outageUntil
	}

	producer := kafkaclient.NewOutageInjectingProducer(realProducer, getOutageUntil)

	spool, err := ingest.NewSpool(*spoolPath)
	if err != nil {
		log.Fatalf("spool: %v", err)
	}
	defer spool.Close()

	pipeline := ingest.NewPipeline(ingest.NewDedup(), spool, producer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pipeline.RunDrainer(ctx, 500*time.Millisecond)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: wsserver.NewServer(pipeline).Handler()}
	go srv.Serve(listener)
	defer srv.Close()

	wsURL := fmt.Sprintf("ws://%s/ingest", listener.Addr())
	totalDuration := *preOutage + *outageDuration + *postOutage
	fmt.Printf("outage benchmark: %d devices, 1 reading every %s, total send window %s (outage %s starting at t=%s)\n",
		*numDevices, *rate, totalDuration, *outageDuration, *preOutage)

	testStart := time.Now()
	mu.Lock()
	outageUntil = testStart.Add(*preOutage).Add(*outageDuration)
	mu.Unlock()

	var sentCount int64
	var wg sync.WaitGroup
	for d := 0; d < *numDevices; d++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			deviceID := fmt.Sprintf("dev-%03d", idx)
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err != nil {
				log.Printf("device %s: dial: %v", deviceID, err)
				return
			}
			defer conn.Close()

			ticker := time.NewTicker(*rate)
			defer ticker.Stop()
			deadline := testStart.Add(totalDuration)
			var seq uint64
			for time.Now().Before(deadline) {
				seq++
				r := &pb.Reading{DeviceId: deviceID, Sequence: seq, TimestampMs: time.Now().UnixMilli(), Value: float64(seq)}
				b, _ := proto.Marshal(r)
				if err := conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
					log.Printf("device %s: write: %v", deviceID, err)
					return
				}
				atomic.AddInt64(&sentCount, 1)
				<-ticker.C
			}
		}(d)
	}

	go func() {
		time.Sleep(*preOutage)
		fmt.Printf("[t=%s] simulated outage begins\n", time.Since(testStart).Round(time.Second))
		time.Sleep(*outageDuration)
		fmt.Printf("[t=%s] simulated outage ends, producer accepting writes again\n", time.Since(testStart).Round(time.Second))
	}()

	wg.Wait()
	fmt.Printf("send phase complete: %d readings sent over %s\n", atomic.LoadInt64(&sentCount), time.Since(testStart).Round(time.Second))

	fmt.Printf("waiting for the drainer to flush the spool backlog (up to %s)...\n", *maxDrainWait)
	drainDeadline := time.Now().Add(*maxDrainWait)
	for {
		size, err := spool.SizeBytes()
		if err != nil {
			log.Fatalf("spool size: %v", err)
		}
		if size == 0 {
			break
		}
		if time.Now().After(drainDeadline) {
			fmt.Printf("WARNING: spool still has %d bytes pending after the %s wait bound\n", size, *maxDrainWait)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Printf("spool drained after %s\n", time.Since(testStart).Round(time.Second))

	got, err := kafkaclient.ConsumeAvailable(context.Background(), *broker, *topic)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}

	sent := atomic.LoadInt64(&sentCount)
	lost := sent - int64(len(got))
	fmt.Printf("\nsent: %d, landed in kafka: %d\n", sent, len(got))
	fmt.Printf("RESULT: records lost across the %s simulated broker outage: %d\n", *outageDuration, lost)
	if lost != 0 {
		log.Fatalf("MISMATCH: sent %d but kafka has %d (lost=%d)", sent, len(got), lost)
	}
}
