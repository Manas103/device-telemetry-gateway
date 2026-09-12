// cmd/segmentbench drives the whole segment-membership engine end to end in
// one process: build a corpus, send it through the real edge into a real
// Kafka topic, consume it into a real ClickHouse event store while
// incrementally updating real Redis segment membership, measure
// event-to-segment lag, diff the incremental result against an independent
// from-scratch ClickHouse recompute over all 50 test segments, then offer a
// burst of extra traffic and measure how long the consumer takes to drain
// the resulting backlog back to caught up.
//
// One binary orchestrates all of this (rather than one binary per phase)
// because every phase needs the same live Kafka+ClickHouse+Redis state as
// the previous one; splitting it into separate processes would only add
// process-startup and reconnection overhead, not independence, since
// nothing here is measured by mocking any of these three systems.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"device-telemetry-gateway/internal/eventingest"
	"device-telemetry-gateway/internal/kafkaclient"
	"device-telemetry-gateway/internal/segment"
	"device-telemetry-gateway/internal/simstorefront"

	pb "device-telemetry-gateway/proto"
)

func ensureTopic(broker, topic string, partitions int) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	ctrlConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return err
	}
	defer ctrlConn.Close()
	return ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
}

func main() {
	profiles := flag.Int("profiles", 3000, "distinct simulated profiles")
	avgEvents := flag.Int("avg-events", 5, "average events per profile")
	historyDays := flag.Int("history-days", 120, "synthetic event history window in days")
	broker := flag.String("broker", "127.0.0.1:9092", "Kafka broker address")
	topic := flag.String("topic", "storefront-events", "Kafka topic")
	partitions := flag.Int("partitions", 6, "Kafka partitions for the topic")
	chAddr := flag.String("clickhouse", "127.0.0.1:8123", "ClickHouse HTTP address")
	redisAddr := flag.String("redis", "127.0.0.1:6380", "Redis address")
	burstMultiplier := flag.Float64("burst-multiplier", 10, "burst offered rate as a multiple of the measured sustained consume rate")
	burstSeconds := flag.Int("burst-seconds", 8, "how long to offer burst traffic")
	burstDrainCap := flag.Duration("burst-drain-cap", 3*time.Minute, "how long to wait for the backlog to drain before giving up")
	outFile := flag.String("out", "", "path to also write the full report to")
	flag.Parse()

	// A fixed topic name across repeated runs left leftover messages from a
	// prior run on the topic (Kafka topics are not truncated by
	// ensureTopic, which only creates-if-absent), so a rerun's consumer
	// would read its own new messages plus every prior run's, which showed
	// up as "consumed by segment engine: 30778 / 16559" (more consumed than
	// sent) and polluted the lag and segment-membership numbers with stale
	// data. Each run now gets its own topic unless -topic is set explicitly,
	// so repeated runs never share Kafka state. See README Findings.
	runTopic := *topic
	if runTopic == "storefront-events" {
		runTopic = fmt.Sprintf("storefront-events-%d", time.Now().UnixNano())
	}
	topic = &runTopic

	var out *os.File
	if *outFile != "" {
		f, err := os.Create(*outFile)
		if err != nil {
			log.Fatalf("create out file: %v", err)
		}
		defer f.Close()
		out = f
	}
	w := func(format string, args ...interface{}) {
		s := fmt.Sprintf(format, args...)
		fmt.Print(s)
		if out != nil {
			fmt.Fprint(out, s)
		}
	}

	ctx := context.Background()

	w("=== device-telemetry-gateway segment membership engine benchmark ===\n")
	w("profiles: %d, avg events/profile: %d, history window: %d days\n", *profiles, *avgEvents, *historyDays)

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		log.Fatalf("redis flushdb: %v", err)
	}

	ch := segment.NewClickHouseStore(*chAddr)
	if err := ch.EnsureSchema(ctx); err != nil {
		log.Fatalf("clickhouse ensure schema: %v", err)
	}
	if err := ch.TruncateEvents(ctx); err != nil {
		log.Fatalf("clickhouse truncate: %v", err)
	}

	if err := ensureTopic(*broker, *topic, *partitions); err != nil {
		w("topic create note (may already exist): %v\n", err)
	}

	segments := segment.GenerateTestSegments()
	w("test segments: %d\n", len(segments))

	corpusProfiles := simstorefront.Build(*profiles, *avgEvents, *historyDays, time.Now(), 7)
	var simNow time.Time
	totalEvents := 0
	for _, p := range corpusProfiles {
		totalEvents += len(p.Events)
		for _, e := range p.Events {
			t := time.UnixMilli(e.TimestampMs)
			if t.After(simNow) {
				simNow = t
			}
		}
	}
	w("corpus: %d events across %d profiles\n", totalEvents, *profiles)

	// --- consumer: Kafka -> ClickHouse (batched) + Redis (incremental) ---
	reader := kafkaclient.NewStorefrontStreamReader(*broker, *topic, *partitions)
	engine := segment.NewIncrementalEngine(rdb, segments)

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	msgs := reader.Messages(consumeCtx)

	var consumed int64
	var lagMu sync.Mutex
	var lagSamplesMs []float64
	var sentAtMu sync.Mutex
	sentAt := make(map[string]time.Time, totalEvents)
	const chBatchSize = 500
	chBatch := make([]*pb.StorefrontEvent, 0, chBatchSize)
	var chMu sync.Mutex
	flushCH := func() {
		chMu.Lock()
		batch := chBatch
		chBatch = make([]*pb.StorefrontEvent, 0, chBatchSize)
		chMu.Unlock()
		if len(batch) == 0 {
			return
		}
		if err := ch.InsertBatch(ctx, batch); err != nil {
			log.Printf("clickhouse insert batch: %v", err)
		}
	}

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		flushTicker := time.NewTicker(200 * time.Millisecond)
		defer flushTicker.Stop()
		for {
			select {
			case e, ok := <-msgs:
				if !ok {
					flushCH()
					return
				}
				recvAt := time.Now()
				chMu.Lock()
				chBatch = append(chBatch, e)
				full := len(chBatch) >= chBatchSize
				chMu.Unlock()
				if full {
					flushCH()
				}
				if err := engine.ApplyEvent(ctx, e); err != nil {
					log.Printf("incremental apply: %v", err)
				}
				sentAtMu.Lock()
				sent, ok := sentAt[e.EventId]
				sentAtMu.Unlock()
				if ok {
					lagMu.Lock()
					lagSamplesMs = append(lagSamplesMs, float64(recvAt.Sub(sent).Milliseconds()))
					lagMu.Unlock()
				}
				atomic.AddInt64(&consumed, 1)
			case <-flushTicker.C:
				flushCH()
			case <-consumeCtx.Done():
				flushCH()
				return
			}
		}
	}()

	// --- producer: real edge (dedup) -> real Kafka, per-profile order preserved per worker ---
	producer := kafkaclient.NewStorefrontProducer(*broker, *topic)
	pipeline := eventingest.NewPipeline(eventingest.NewEventDedup(), producer)

	sendStart := time.Now()
	const sendWorkers = 8
	var sendWG sync.WaitGroup
	shardSize := (len(corpusProfiles) + sendWorkers - 1) / sendWorkers
	for wk := 0; wk < sendWorkers; wk++ {
		lo := wk * shardSize
		hi := lo + shardSize
		if hi > len(corpusProfiles) {
			hi = len(corpusProfiles)
		}
		if lo >= hi {
			continue
		}
		sendWG.Add(1)
		go func(shard []simstorefront.ProfileEvents) {
			defer sendWG.Done()
			for _, p := range shard {
				for _, e := range p.Events {
					sentAtMu.Lock()
					sentAt[e.EventId] = time.Now()
					sentAtMu.Unlock()
					if _, err := pipeline.Ingest(ctx, e); err != nil {
						log.Printf("produce: %v", err)
					}
				}
			}
		}(corpusProfiles[lo:hi])
	}
	sendWG.Wait()
	sendElapsed := time.Since(sendStart)
	sustainedRate := float64(totalEvents) / sendElapsed.Seconds()
	w("send phase: %d events sent in %s (%.0f events/sec offered)\n", totalEvents, sendElapsed, sustainedRate)

	// wait for the consumer to catch up
	deadline := time.Now().Add(2 * time.Minute)
	for atomic.LoadInt64(&consumed) < int64(totalEvents) && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	flushCH()
	w("consumed by segment engine: %d / %d\n", atomic.LoadInt64(&consumed), totalEvents)

	lagMu.Lock()
	samples := append([]float64{}, lagSamplesMs...)
	lagMu.Unlock()
	sort.Float64s(samples)
	p99 := 0.0
	if len(samples) > 0 {
		idx := int(0.99*float64(len(samples))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(samples) {
			idx = len(samples) - 1
		}
		p99 = samples[idx]
	}
	w("RESULT: event-to-segment p99 lag: %.1f ms (n=%d samples)\n", p99, len(samples))

	touched, err := engine.Finalize(ctx, simNow)
	if err != nil {
		log.Fatalf("finalize: %v", err)
	}
	w("finalize sweep: re-evaluated %d touched profiles as of simulated now (%s)\n", touched, simNow.UTC().Format(time.RFC3339))

	rows, rowsErr := ch.CountRows(ctx)
	if rowsErr != nil {
		w("clickhouse count rows error: %v\n", rowsErr)
	}
	distinctProfiles, distinctErr := ch.CountDistinctProfiles(ctx)
	if distinctErr != nil {
		w("clickhouse count distinct profiles error: %v\n", distinctErr)
	}
	w("clickhouse event store: %d rows, %d distinct profiles\n", rows, distinctProfiles)

	// --- recompute + diff over all 50 segments ---
	w("\n--- full recompute vs incremental diff, all %d segments ---\n", len(segments))
	exactSegments := 0
	totalMismatch := 0
	for _, seg := range segments {
		incr, err := segment.MembersOf(ctx, rdb, seg.ID)
		if err != nil {
			log.Fatalf("members of %s: %v", seg.ID, err)
		}
		rec, err := segment.Recompute(ctx, ch, seg, simNow)
		if err != nil {
			log.Fatalf("recompute %s: %v", seg.ID, err)
		}
		d := segment.DiffSets(seg.ID, incr, rec)
		if d.Exact() {
			exactSegments++
		} else {
			totalMismatch += d.OnlyIncremental + d.OnlyRecompute
			w("MISMATCH %-40s incremental=%d recompute=%d only_incremental=%d only_recompute=%d\n",
				seg.ID, d.IncrementalSize, d.RecomputeSize, d.OnlyIncremental, d.OnlyRecompute)
		}
	}
	w("RESULT: %d / %d segments matched the full recompute exactly (%d mismatched profile-segment pairs total)\n",
		exactSegments, len(segments), totalMismatch)

	// --- burst backlog drain ---
	w("\n--- burst backlog drain ---\n")
	burstRate := sustainedRate * *burstMultiplier
	burstCount := int(burstRate * float64(*burstSeconds))
	w("baseline sustained rate: %.0f/sec, burst target rate: %.0f/sec (%.0fx), burst duration: %ds, burst volume: %d events\n",
		sustainedRate, burstRate, *burstMultiplier, *burstSeconds, burstCount)

	burstStart := time.Now()
	var burstWG sync.WaitGroup
	perWorker := burstCount / sendWorkers
	for wk := 0; wk < sendWorkers; wk++ {
		burstWG.Add(1)
		go func(workerID int) {
			defer burstWG.Done()
			for i := 0; i < perWorker; i++ {
				e := &pb.StorefrontEvent{
					EventId:     fmt.Sprintf("burst-%d-%d", workerID, i),
					ProfileId:   fmt.Sprintf("profile-%08d", (workerID*perWorker+i)%(*profiles)),
					EventType:   "page_view",
					TimestampMs: time.Now().UnixMilli(),
					Attributes:  map[string]string{"category": "electronics"},
				}
				if _, err := pipeline.Ingest(ctx, e); err != nil {
					log.Printf("burst produce: %v", err)
				}
			}
		}(wk)
	}
	burstWG.Wait()
	burstProduceElapsed := time.Since(burstStart)
	w("burst produced in %s\n", burstProduceElapsed)

	drainStart := time.Now()
	drainDeadline := drainStart.Add(*burstDrainCap)
	var finalLag int64 = -1
	for time.Now().Before(drainDeadline) {
		offsets, err := kafkaclient.PartitionOffsets(ctx, *broker, *topic)
		if err != nil {
			log.Printf("partition offsets: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		cur := reader.CurrentOffsets()
		var lag int64
		for pid, bounds := range offsets {
			c := cur[pid]
			l := bounds.Last - c
			if l > 0 {
				lag += l
			}
		}
		finalLag = lag
		if lag <= 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	drainElapsed := time.Since(drainStart)
	w("RESULT: backlog drained (lag=%d) in %s (cap %s)\n", finalLag, drainElapsed, *burstDrainCap)

	cancelConsume()
	<-consumerDone
	reader.Close()
	producer.Close()
	rdb.Close()
	w("\ndone.\n")
}
