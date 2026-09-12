// cmd/deliveryscale is the two ClickHouse-scale claims' exact measurement:
// (1) rollup counts exact against an independent full recompute over 50M
// events, and (2) dashboard p95 latency dropping from a live raw-table
// aggregate to a pre-aggregated rollup-table read.
//
// This benchmark deliberately does not go through Kafka or the edge
// pipeline at all: it bulk-loads synthetic rows directly into ClickHouse's
// raw table. That is a scope choice, not an oversight, and is disclosed in
// the README's "what this is, and is not": the edge's own dedup-into-Kafka
// correctness at realistic volume is cmd/deliverygen's job, and the edge's
// own throughput ceiling is cmd/deliverythroughput's job; this benchmark's
// job is "once 50M rows of webhooks exist in ClickHouse, does the rollup
// table's accounting tie out to a from-scratch recompute, and is a
// dashboard query against the rollup table actually faster". Routing 50M
// rows through a courtesy-capped 6-goroutine Kafka producer first would
// only add unrelated Kafka-throughput noise to a ClickHouse-side question.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"device-telemetry-gateway/internal/deliverystats"

	pb "device-telemetry-gateway/proto"
)

func genBatch(rng *rand.Rand, batchStart, batchSize int, historyDays int, now time.Time) []*pb.DeliveryWebhook {
	out := make([]*pb.DeliveryWebhook, batchSize)
	for i := 0; i < batchSize; i++ {
		idx := batchStart + i
		ch := deliverystats.Channels[rng.Intn(len(deliverystats.Channels))]
		statuses := []string{"sent", "delivered", "delivered", "delivered", "delivered", "bounced", "failed"}
		status := statuses[rng.Intn(len(statuses))]
		daysAgo := rng.Float64() * float64(historyDays)
		ts := now.Add(-time.Duration(daysAgo * float64(24*time.Hour)))
		out[i] = &pb.DeliveryWebhook{
			WebhookId:   fmt.Sprintf("scale-%010d", idx),
			MessageId:   fmt.Sprintf("scale-msg-%010d", idx/2),
			Channel:     ch.Name,
			Status:      status,
			TimestampMs: ts.UnixMilli(),
			Provider:    ch.Provider,
		}
	}
	return out
}

func main() {
	totalEvents := flag.Int("events", 50_000_000, "total synthetic delivery webhooks to bulk-load")
	batchSize := flag.Int("batch-size", 50_000, "rows per ClickHouse insert batch")
	insertWorkers := flag.Int("insert-workers", 6, "concurrent ClickHouse insert workers (courtesy cap, see README)")
	historyDays := flag.Int("history-days", 30, "synthetic event history window in days")
	chAddr := flag.String("clickhouse", "127.0.0.1:8123", "ClickHouse HTTP address")
	chDatabase := flag.String("clickhouse-db", "deliverystats", "ClickHouse database")
	dashboardTrials := flag.Int("dashboard-trials", 25, "number of trials per query to compute p95 latency")
	dashboardWindowDays := flag.Float64("dashboard-window-days", 30, "trailing window the dashboard query covers")
	skipLoad := flag.Bool("skip-load", false, "skip the bulk load and reuse whatever is already in the table (for iterating on the dashboard-latency measurement only)")
	outFile := flag.String("out", "", "path to also write the full report to")
	flag.Parse()

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
	w("=== device-telemetry-gateway delivery-stats ClickHouse scale benchmark ===\n")
	w("target events: %d, batch size: %d, insert workers: %d (courtesy cap), history window: %d days\n",
		*totalEvents, *batchSize, *insertWorkers, *historyDays)

	ch := deliverystats.NewClickHouseStore(*chAddr, *chDatabase)
	if err := ch.EnsureSchema(ctx); err != nil {
		log.Fatalf("clickhouse ensure schema: %v", err)
	}

	now := time.Now()

	if !*skipLoad {
		if err := ch.TruncateAll(ctx); err != nil {
			log.Fatalf("clickhouse truncate: %v", err)
		}

		numBatches := (*totalEvents + *batchSize - 1) / *batchSize
		var nextBatch int64 = -1
		var inserted int64
		var wg sync.WaitGroup
		loadStart := time.Now()

		for wk := 0; wk < *insertWorkers; wk++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(workerID) + 1))
				for {
					b := atomic.AddInt64(&nextBatch, 1)
					if int(b) >= numBatches {
						return
					}
					start := int(b) * *batchSize
					size := *batchSize
					if start+size > *totalEvents {
						size = *totalEvents - start
					}
					if size <= 0 {
						continue
					}
					rows := genBatch(rng, start, size, *historyDays, now)
					if err := ch.InsertBatch(ctx, rows); err != nil {
						log.Printf("insert batch %d: %v", b, err)
						continue
					}
					n := atomic.AddInt64(&inserted, int64(size))
					if n%5_000_000 < int64(size) {
						w("  loaded %d / %d rows (%s elapsed)\n", n, *totalEvents, time.Since(loadStart).Round(time.Second))
					}
				}
			}(wk)
		}
		wg.Wait()
		loadElapsed := time.Since(loadStart)
		w("bulk load complete: %d rows in %s (%.0f rows/sec)\n", inserted, loadElapsed, float64(inserted)/loadElapsed.Seconds())

		rowCount, err := ch.CountRows(ctx)
		if err != nil {
			log.Fatalf("count rows: %v", err)
		}
		w("clickhouse delivery_events row count: %d\n", rowCount)

		refreshStart := time.Now()
		if err := ch.RefreshRollup(ctx); err != nil {
			log.Fatalf("refresh rollup: %v", err)
		}
		w("rollup refresh complete in %s\n", time.Since(refreshStart))

		rollupRows, err := ch.CountRollupRows(ctx)
		if err != nil {
			log.Fatalf("count rollup rows: %v", err)
		}
		w("clickhouse delivery_rollup_minute row count: %d (%.4fx the raw table)\n",
			rollupRows, float64(rollupRows)/float64(rowCount))
	} else {
		rowCount, err := ch.CountRows(ctx)
		if err != nil {
			log.Fatalf("count rows: %v", err)
		}
		w("skip-load set; reusing existing table, %d rows\n", rowCount)
	}

	// --- claim: counts exact against a recompute over 50M events ---
	w("\n--- rollup-vs-recompute exactness check ---\n")
	recompute, err := ch.RecomputeChannelStatusCounts(ctx)
	if err != nil {
		log.Fatalf("recompute: %v", err)
	}
	rollup, err := ch.RollupChannelStatusCounts(ctx)
	if err != nil {
		log.Fatalf("rollup counts: %v", err)
	}
	recomputeMap := map[string]int64{}
	for _, c := range recompute {
		recomputeMap[c.Channel+"|"+c.Status] = c.Count
	}
	rollupMap := map[string]int64{}
	for _, c := range rollup {
		rollupMap[c.Channel+"|"+c.Status] = c.Count
	}
	var keys []string
	for k := range recomputeMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	exactPairs, totalPairs := 0, 0
	var recomputeTotal, rollupTotal int64
	for _, k := range keys {
		totalPairs++
		rc, rl := recomputeMap[k], rollupMap[k]
		recomputeTotal += rc
		rollupTotal += rl
		if rc == rl {
			exactPairs++
		} else {
			w("MISMATCH %-30s recompute=%d rollup=%d\n", k, rc, rl)
		}
	}
	w("RESULT: %d / %d (channel,status) pairs match exactly between rollup and full recompute\n", exactPairs, totalPairs)
	w("RESULT: recompute total = %d, rollup total = %d, counts exact overall: %v\n",
		recomputeTotal, rollupTotal, recomputeTotal == rollupTotal)

	// --- claim: dashboard p95 latency, raw vs rollup ---
	w("\n--- dashboard query latency: raw table vs rollup table ---\n")
	windowStart := now.Add(-time.Duration(*dashboardWindowDays * float64(24*time.Hour)))
	w("trailing window: %.1f days, trials per query: %d\n", *dashboardWindowDays, *dashboardTrials)

	rawLatencies := make([]float64, 0, *dashboardTrials)
	for i := 0; i < *dashboardTrials; i++ {
		start := time.Now()
		if _, err := ch.DashboardRawQuery(ctx, windowStart); err != nil {
			log.Fatalf("dashboard raw query trial %d: %v", i, err)
		}
		rawLatencies = append(rawLatencies, float64(time.Since(start).Milliseconds()))
	}
	rollupLatencies := make([]float64, 0, *dashboardTrials)
	for i := 0; i < *dashboardTrials; i++ {
		start := time.Now()
		if _, err := ch.DashboardRollupQuery(ctx, windowStart); err != nil {
			log.Fatalf("dashboard rollup query trial %d: %v", i, err)
		}
		rollupLatencies = append(rollupLatencies, float64(time.Since(start).Milliseconds()))
	}

	rawP95 := percentile(rawLatencies, 0.95)
	rollupP95 := percentile(rollupLatencies, 0.95)
	w("raw-table query latency:    p95 = %.1f ms (min=%.1f max=%.1f, n=%d)\n",
		rawP95, min(rawLatencies), max(rawLatencies), len(rawLatencies))
	w("rollup-table query latency: p95 = %.1f ms (min=%.1f max=%.1f, n=%d)\n",
		rollupP95, min(rollupLatencies), max(rollupLatencies), len(rollupLatencies))
	if rollupP95 > 0 {
		w("RESULT: rollup speedup: %.1fx (raw p95 %.1f ms -> rollup p95 %.1f ms)\n", rawP95/rollupP95, rawP95, rollupP95)
	}
	w("done.\n")
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64{}, values...)
	sort.Float64s(sorted)
	idx := int(p*float64(len(sorted))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func min(values []float64) float64 {
	m := values[0]
	for _, v := range values {
		if v < m {
			m = v
		}
	}
	return m
}

func max(values []float64) float64 {
	m := values[0]
	for _, v := range values {
		if v > m {
			m = v
		}
	}
	return m
}
