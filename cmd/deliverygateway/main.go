// cmd/deliverygateway is the standalone deployable service for the
// delivery-stats extension: an HTTP endpoint simulated email/SMS/WhatsApp
// providers POST webhooks to, a deduplicating edge that forwards each
// unique webhook to Kafka, a background consumer that fans Kafka out to
// ClickHouse (raw log + rollup table, refreshed on a short interval), and a
// small JSON API the React dashboard (web/deliverydashboard) polls for
// live counts. One process runs all four roles for this project's scope
// (a real deployment would likely split the HTTP receiver from the
// consumer so each scales independently; see README Limitations).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"device-telemetry-gateway/internal/deliverystats"
	"device-telemetry-gateway/internal/kafkaclient"

	pb "device-telemetry-gateway/proto"
)

// webhookPayload is the simulated provider's JSON POST body. A real
// provider's exact schema (SendGrid, Twilio, and the WhatsApp Business API
// all differ) is out of scope for this project; this is a normalized shape
// the simulated senders in internal/deliverystats.Build already produce
// values compatible with, disclosed as a simplification in README
// Limitations.
type webhookPayload struct {
	WebhookID string `json:"webhook_id"`
	MessageID string `json:"message_id"`
	Channel   string `json:"channel"`
	Status    string `json:"status"`
	Provider  string `json:"provider"`
	Timestamp int64  `json:"timestamp_ms"`
}

func main() {
	addr := flag.String("addr", ":8090", "HTTP listen address for the webhook receiver and dashboard API")
	broker := flag.String("broker", "127.0.0.1:9092", "Kafka broker address")
	topic := flag.String("topic", "deliverystats-live", "Kafka topic")
	partitions := flag.Int("partitions", 6, "Kafka partitions for the topic")
	chAddr := flag.String("clickhouse", "127.0.0.1:8123", "ClickHouse HTTP address")
	chDatabase := flag.String("clickhouse-db", "deliverystats", "ClickHouse database")
	rollupInterval := flag.Duration("rollup-interval", 5*time.Second, "how often the background consumer refreshes the rollup table")
	staticDir := flag.String("static-dir", "", "path to the built React dashboard (web/deliverydashboard/dist) to serve at /; empty disables static serving")
	flag.Parse()

	ctx := context.Background()

	ch := deliverystats.NewClickHouseStore(*chAddr, *chDatabase)
	if err := ch.EnsureSchema(ctx); err != nil {
		log.Fatalf("clickhouse ensure schema: %v", err)
	}

	if err := ensureTopic(*broker, *topic, *partitions); err != nil {
		log.Printf("topic create note (may already exist): %v", err)
	}

	producer := kafkaclient.NewDeliveryProducer(*broker, *topic)
	defer producer.Close()
	pipeline := deliverystats.NewPipeline(deliverystats.NewWebhookDedup(), producer)

	reader := kafkaclient.NewDeliveryStreamReader(*broker, *topic, *partitions)
	defer reader.Close()
	consumeCtx, cancelConsume := context.WithCancel(ctx)
	defer cancelConsume()
	msgs := reader.Messages(consumeCtx)

	const chBatchSize = 200
	chBatch := make([]*pb.DeliveryWebhook, 0, chBatchSize)
	var chMu sync.Mutex
	flushCH := func() {
		chMu.Lock()
		batch := chBatch
		chBatch = make([]*pb.DeliveryWebhook, 0, chBatchSize)
		chMu.Unlock()
		if len(batch) == 0 {
			return
		}
		if err := ch.InsertBatch(ctx, batch); err != nil {
			log.Printf("clickhouse insert batch: %v", err)
		}
	}

	go func() {
		flushTicker := time.NewTicker(500 * time.Millisecond)
		defer flushTicker.Stop()
		for {
			select {
			case wh, ok := <-msgs:
				if !ok {
					return
				}
				chMu.Lock()
				chBatch = append(chBatch, wh)
				full := len(chBatch) >= chBatchSize
				chMu.Unlock()
				if full {
					flushCH()
				}
			case <-flushTicker.C:
				flushCH()
			case <-consumeCtx.Done():
				flushCH()
				return
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(*rollupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := ch.RefreshRollup(ctx); err != nil {
					log.Printf("rollup refresh: %v", err)
				}
			case <-consumeCtx.Done():
				return
			}
		}
	}()

	mux := http.NewServeMux()

	mux.HandleFunc("/webhook/", func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		if err != nil {
			http.Error(rw, "read error", http.StatusBadRequest)
			return
		}
		var p webhookPayload
		if err := json.Unmarshal(body, &p); err != nil {
			http.Error(rw, "invalid json", http.StatusBadRequest)
			return
		}
		if p.WebhookID == "" || p.Channel == "" {
			http.Error(rw, "webhook_id and channel are required", http.StatusBadRequest)
			return
		}
		ts := p.Timestamp
		if ts == 0 {
			ts = time.Now().UnixMilli()
		}
		w := &pb.DeliveryWebhook{
			WebhookId:   p.WebhookID,
			MessageId:   p.MessageID,
			Channel:     p.Channel,
			Status:      p.Status,
			Provider:    p.Provider,
			TimestampMs: ts,
		}
		accepted, err := pipeline.Ingest(r.Context(), w)
		if err != nil {
			http.Error(rw, fmt.Sprintf("produce error: %v", err), http.StatusBadGateway)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(map[string]bool{"accepted": accepted})
	})

	mux.HandleFunc("/api/summary", func(rw http.ResponseWriter, r *http.Request) {
		rows, err := ch.Summary(r.Context())
		if err != nil {
			http.Error(rw, fmt.Sprintf("summary error: %v", err), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(rw).Encode(map[string]interface{}{
			"generated_at": time.Now().UTC().Format(time.RFC3339),
			"rows":         rows,
		})
	})

	mux.HandleFunc("/healthz", func(rw http.ResponseWriter, r *http.Request) {
		rw.Write([]byte("ok"))
	})

	if *staticDir != "" {
		mux.Handle("/", http.FileServer(http.Dir(*staticDir)))
	}

	srv := &http.Server{Addr: *addr, Handler: mux}
	log.Printf("deliverygateway listening on %s (topic=%s, clickhouse db=%s)", *addr, *topic, *chDatabase)
	log.Fatal(srv.ListenAndServe())
}

func ensureTopic(broker, topic string, partitions int) error {
	return kafkaclient.EnsureTopic(broker, topic, partitions)
}
