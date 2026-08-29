// loadgen is the reproducible measurement behind the resume claim: "final
// state was bit-identical after a replay 5% duplicated and out of order".
// It starts a real gateway (real WebSocket server, real dedup, real Kafka
// producer against the already-running broker), drives numDevices simulated
// devices over real WebSocket connections with a delivery order that is 5%
// duplicated and fully shuffled per device, waits for Kafka to settle, reads
// everything back with a real consumer, and diffs the result against an
// independently computed reference digest.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"device-telemetry-gateway/internal/ingest"
	"device-telemetry-gateway/internal/kafkaclient"
	"device-telemetry-gateway/internal/simdevice"
	"device-telemetry-gateway/internal/wsserver"
)

func main() {
	numDevices := flag.Int("devices", 5000, "number of simulated devices")
	readingsPerDevice := flag.Int("readings", 20, "true readings per device")
	dupFraction := flag.Float64("dup-fraction", 0.05, "fraction of the delivered stream that is duplicate traffic")
	concurrency := flag.Int("concurrency", 400, "max concurrent device connections")
	broker := flag.String("broker", "127.0.0.1:9092", "kafka broker address")
	topic := flag.String("topic", "telemetry-replay-bench", "kafka topic (should be empty/fresh for a clean measurement)")
	spoolPath := flag.String("spool", "/tmp/loadgen-spool.dat", "backpressure spool path")
	flag.Parse()

	producer := kafkaclient.NewProducer(*broker, *topic)
	defer producer.Close()

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
	fmt.Printf("gateway listening on %s, kafka broker %s, topic %q\n", listener.Addr(), *broker, *topic)

	streams := simdevice.BuildReplayCorpus(*numDevices, *readingsPerDevice, *dupFraction, 20260829)

	totalUnique := 0
	totalDelivered := 0
	for _, s := range streams {
		totalDelivered += len(s.Delivery)
	}
	totalUnique = *numDevices * *readingsPerDevice
	measuredDupFraction := float64(totalDelivered-totalUnique) / float64(totalDelivered)

	fmt.Printf("corpus: %d devices x %d readings = %d unique readings; %d delivered (%.2f%% duplicates, targeting %.2f%%)\n",
		*numDevices, *readingsPerDevice, totalUnique, totalDelivered, measuredDupFraction*100, *dupFraction*100)

	start := time.Now()
	if err := simdevice.Send(ctx, wsURL, streams, *concurrency); err != nil {
		log.Fatalf("send: %v", err)
	}
	sendElapsed := time.Since(start)
	fmt.Printf("send complete in %s over up to %d concurrent WebSocket connections\n", sendElapsed, *concurrency)

	// Let the producer's batch timeout flush and any last drain tick run.
	time.Sleep(2 * time.Second)

	expected := ingest.ReferenceDedup(simdevice.AllUnique(streams))
	expectedDigest := ingest.FinalStateDigest(expected)
	fmt.Printf("reference oracle: %d unique readings expected, digest %x\n", len(expected), expectedDigest)

	got, err := kafkaclient.ConsumeAvailable(context.Background(), *broker, *topic)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}
	gotDigest := ingest.FinalStateDigest(got)
	fmt.Printf("kafka consumer:   %d readings actually landed in kafka, digest %x\n", len(got), gotDigest)

	match := string(expectedDigest) == string(gotDigest)
	fmt.Printf("\nRESULT: final state bit-identical after a %.2f%%-duplicated, out-of-order replay: %v\n", measuredDupFraction*100, match)
	if !match {
		log.Fatalf("DIGESTS DO NOT MATCH")
	}
}
