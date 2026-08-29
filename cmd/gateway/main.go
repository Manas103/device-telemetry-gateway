// gateway runs the WebSocket ingest edge, dedup, and Kafka fanout as a
// standalone service, the way it would run in a real deployment (see
// deploy/gateway-deployment.yaml for the Kubernetes shape this targets).
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"time"

	"device-telemetry-gateway/internal/ingest"
	"device-telemetry-gateway/internal/kafkaclient"
	"device-telemetry-gateway/internal/wsserver"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "listen address")
	broker := flag.String("broker", "127.0.0.1:9092", "kafka broker address")
	topic := flag.String("topic", "telemetry", "kafka topic")
	spoolPath := flag.String("spool", "spool.dat", "path to the backpressure spool file")
	drainInterval := flag.Duration("drain-interval", 1*time.Second, "how often to retry spooled readings")
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
	go pipeline.RunDrainer(ctx, *drainInterval)

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("gateway listening on %s, publishing to kafka %s topic %q", listener.Addr(), *broker, *topic)
	log.Fatal(http.Serve(listener, wsserver.NewServer(pipeline).Handler()))
}
