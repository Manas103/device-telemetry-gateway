package deliverystats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	pb "device-telemetry-gateway/proto"
)

// ClickHouseStore is the delivery-stats event store: a thin client over
// ClickHouse's plain HTTP interface, the same hand-rolled-over-a-library
// choice internal/segment.ClickHouseStore already made in this repo, and for
// the same reason (see that type's doc comment): the only two operations
// this extension needs are "insert a batch of rows" and "run a read-only
// aggregate query and get rows back", both of which the HTTP interface does
// natively.
//
// This is a separate Go type from internal/segment.ClickHouseStore, not a
// shared one, on purpose: it points at its own database
// (database/deliverystats by default, see EnsureDatabase) so bulk-loading
// 50M rows for this extension's scale claims cannot collide with, or slow
// down, the segment-membership module's own `events` table in `default`,
// and a schema change made for one module can never silently change the
// other's query behavior.
type ClickHouseStore struct {
	baseURL  string
	database string
	client   *http.Client
}

func NewClickHouseStore(addr, database string) *ClickHouseStore {
	return &ClickHouseStore{
		baseURL:  fmt.Sprintf("http://%s/", addr),
		database: database,
		client:   &http.Client{Timeout: 300 * time.Second},
	}
}

func (c *ClickHouseStore) Database() string { return c.database }

// EnsureDatabase creates this store's database if absent.
func (c *ClickHouseStore) EnsureDatabase(ctx context.Context) error {
	_, err := c.raw(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.database))
	return err
}

// rawEventsDDL is the raw delivery-webhook log: one row per webhook. ORDER
// BY (channel, status, webhook_id) is a deliberate choice, not an oversight:
// it is the natural key for the write path (the edge already knows a
// webhook's channel and status when it lands), but it means a query that
// filters or buckets by timestamp gets no benefit from ClickHouse's sparse
// primary index on this table, forcing a full scan of the timestamp and
// webhook_id columns for every dashboard refresh. That is exactly the
// "why is this slow" mechanism the dashboard-latency claim below is built
// to demonstrate, and it is disclosed here rather than hidden behind a
// conveniently pre-sorted table. See README Findings and Architecture.
func (c *ClickHouseStore) rawEventsDDL() string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.delivery_events (
    webhook_id String,
    message_id String,
    channel LowCardinality(String),
    status LowCardinality(String),
    timestamp DateTime64(3),
    provider LowCardinality(String)
) ENGINE = MergeTree
ORDER BY (channel, status, webhook_id)
`, c.database)
}

// rollupDDL is the pre-aggregated dashboard table: one row per
// (minute, channel, status) with the count of webhooks landed in that
// bucket. It is orders of magnitude smaller than delivery_events (thousands
// of rows instead of tens of millions), which is the entire reason a
// dashboard query against it is fast: there is simply far less to scan, not
// a smarter query plan against the same data.
func (c *ClickHouseStore) rollupDDL() string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.delivery_rollup_minute (
    minute DateTime,
    channel LowCardinality(String),
    status LowCardinality(String),
    event_count UInt64
) ENGINE = SummingMergeTree(event_count)
ORDER BY (minute, channel, status)
`, c.database)
}

func (c *ClickHouseStore) EnsureSchema(ctx context.Context) error {
	if err := c.EnsureDatabase(ctx); err != nil {
		return err
	}
	if _, err := c.raw(ctx, c.rawEventsDDL()); err != nil {
		return err
	}
	if _, err := c.raw(ctx, c.rollupDDL()); err != nil {
		return err
	}
	return nil
}

// TruncateAll drops all rows from both tables, used to reset state between
// benchmark attempts so a re-run does not silently accumulate rows on top of
// a prior, possibly-aborted attempt.
func (c *ClickHouseStore) TruncateAll(ctx context.Context) error {
	if _, err := c.raw(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.delivery_events", c.database)); err != nil {
		return err
	}
	if _, err := c.raw(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.delivery_rollup_minute", c.database)); err != nil {
		return err
	}
	return nil
}

func (c *ClickHouseStore) raw(ctx context.Context, sqlText string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, strings.NewReader(sqlText))
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		snippet := sqlText
		if len(snippet) > 300 {
			snippet = snippet[:300] + "..."
		}
		return nil, fmt.Errorf("clickhouse http %d: %s (query: %s)", resp.StatusCode, string(body), snippet)
	}
	return body, nil
}

type chRow struct {
	WebhookID string `json:"webhook_id"`
	MessageID string `json:"message_id"`
	Channel   string `json:"channel"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
	Provider  string `json:"provider"`
}

// InsertBatch bulk-inserts a batch of webhooks using ClickHouse's
// JSONEachRow insert format, one HTTP request per batch.
func (c *ClickHouseStore) InsertBatch(ctx context.Context, webhooks []*pb.DeliveryWebhook) error {
	if len(webhooks) == 0 {
		return nil
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "INSERT INTO %s.delivery_events FORMAT JSONEachRow\n", c.database)
	enc := json.NewEncoder(&buf)
	for _, w := range webhooks {
		row := chRow{
			WebhookID: w.WebhookId,
			MessageID: w.MessageId,
			Channel:   w.Channel,
			Status:    w.Status,
			Timestamp: time.UnixMilli(w.TimestampMs).UTC().Format("2006-01-02 15:04:05.000"),
			Provider:  w.Provider,
		}
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	_, err := c.raw(ctx, buf.String())
	return err
}

// RefreshRollup rebuilds delivery_rollup_minute from a full scan of
// delivery_events. In this benchmark it is invoked as one batch job after a
// bulk load (or after a burst) completes; a production deployment would
// more likely run this incrementally (a materialized view attached to
// delivery_events, refreshed per-insert) or on a short fixed interval
// instead of via full rebuild, a simplification disclosed in Limitations.
func (c *ClickHouseStore) RefreshRollup(ctx context.Context) error {
	if _, err := c.raw(ctx, fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.delivery_rollup_minute", c.database)); err != nil {
		return err
	}
	q := fmt.Sprintf(`
INSERT INTO %s.delivery_rollup_minute
SELECT toStartOfMinute(timestamp) AS minute, channel, status, count() AS event_count
FROM %s.delivery_events
GROUP BY minute, channel, status
`, c.database, c.database)
	_, err := c.raw(ctx, q)
	return err
}

// ChannelStatusCount is one (channel, status) pair's total count, the unit
// the counts-exact-against-recompute claim compares.
type ChannelStatusCount struct {
	Channel string
	Status  string
	Count   int64
}

func (c *ClickHouseStore) queryChannelStatusCounts(ctx context.Context, query string) ([]ChannelStatusCount, error) {
	body, err := c.raw(ctx, query+"\nFORMAT JSONEachRow")
	if err != nil {
		return nil, err
	}
	var out []ChannelStatusCount
	dec := json.NewDecoder(bytes.NewReader(body))
	for dec.More() {
		var row struct {
			Channel string      `json:"channel"`
			Status  string      `json:"status"`
			Count   json.Number `json:"c"`
		}
		if err := dec.Decode(&row); err != nil {
			return nil, fmt.Errorf("decode row: %w", err)
		}
		n, err := row.Count.Int64()
		if err != nil {
			return nil, fmt.Errorf("parse count %q: %w", row.Count, err)
		}
		out = append(out, ChannelStatusCount{Channel: row.Channel, Status: row.Status, Count: n})
	}
	return out, nil
}

// RecomputeChannelStatusCounts answers "how many webhooks landed per
// (channel, status)" from zero, with one full-scan SQL aggregate directly
// against delivery_events. This is the independent oracle the rollup path
// is checked against; it shares no code with RollupChannelStatusCounts.
func (c *ClickHouseStore) RecomputeChannelStatusCounts(ctx context.Context) ([]ChannelStatusCount, error) {
	q := fmt.Sprintf(`SELECT channel, status, count() AS c FROM %s.delivery_events GROUP BY channel, status ORDER BY channel, status`, c.database)
	return c.queryChannelStatusCounts(ctx, q)
}

// RollupChannelStatusCounts answers the same question from the pre-
// aggregated rollup table instead of the raw log.
func (c *ClickHouseStore) RollupChannelStatusCounts(ctx context.Context) ([]ChannelStatusCount, error) {
	q := fmt.Sprintf(`SELECT channel, status, sum(event_count) AS c FROM %s.delivery_rollup_minute GROUP BY channel, status ORDER BY channel, status`, c.database)
	return c.queryChannelStatusCounts(ctx, q)
}

// DashboardRawQuery is the expensive, live-off-raw-data dashboard query: an
// exact distinct count of webhooks per (minute, channel, status) over the
// trailing window, computed fresh from delivery_events every time. uniqExact
// forces ClickHouse to read and hash every webhook_id in the window rather
// than just counting rows, which is the honest reason this query is
// expensive at 50M rows: it is not merely "no index", it is "an exact
// distinct aggregate over a String column with no pre-aggregation".
func (c *ClickHouseStore) DashboardRawQuery(ctx context.Context, windowStart time.Time) ([]byte, error) {
	q := fmt.Sprintf(`
SELECT toStartOfMinute(timestamp) AS minute, channel, status, uniqExact(webhook_id) AS distinct_count
FROM %s.delivery_events
WHERE timestamp >= '%s'
GROUP BY minute, channel, status
ORDER BY minute, channel, status
`, c.database, windowStart.UTC().Format("2006-01-02 15:04:05.000"))
	return c.raw(ctx, q+"\nFORMAT JSONEachRow")
}

// DashboardRollupQuery answers the identical dashboard question (counts per
// minute/channel/status over the trailing window) from the pre-aggregated
// rollup table instead.
func (c *ClickHouseStore) DashboardRollupQuery(ctx context.Context, windowStart time.Time) ([]byte, error) {
	q := fmt.Sprintf(`
SELECT minute, channel, status, sum(event_count) AS distinct_count
FROM %s.delivery_rollup_minute
WHERE minute >= '%s'
GROUP BY minute, channel, status
ORDER BY minute, channel, status
`, c.database, windowStart.UTC().Format("2006-01-02 15:04:05"))
	return c.raw(ctx, q+"\nFORMAT JSONEachRow")
}

// CountRows returns the total row count in delivery_events, used to report
// the actual scale a run reached.
func (c *ClickHouseStore) CountRows(ctx context.Context) (int64, error) {
	return c.scalarInt(ctx, fmt.Sprintf("SELECT count() AS c FROM %s.delivery_events", c.database))
}

// CountRollupRows returns the total row count in delivery_rollup_minute,
// the "how much smaller is the rollup table" number the README cites as the
// mechanical reason the rollup query is fast.
func (c *ClickHouseStore) CountRollupRows(ctx context.Context) (int64, error) {
	return c.scalarInt(ctx, fmt.Sprintf("SELECT count() AS c FROM %s.delivery_rollup_minute", c.database))
}

func (c *ClickHouseStore) scalarInt(ctx context.Context, query string) (int64, error) {
	body, err := c.raw(ctx, query+"\nFORMAT JSONEachRow")
	if err != nil {
		return 0, err
	}
	// See internal/segment.ClickHouseStore.scalarInt's comment: this
	// server build sends count()/sum() as a bare JSON number for these
	// scalar queries, so json.Number (whose Kind accepts either a quoted
	// string or a bare number) is used deliberately instead of string.
	var row struct {
		C json.Number `json:"c"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(body), &row); err != nil {
		return 0, fmt.Errorf("decode scalar: %w (body: %s)", err, body)
	}
	n, err := row.C.Int64()
	if err != nil {
		return 0, fmt.Errorf("parse scalar %q: %w", row.C, err)
	}
	return n, nil
}

// SummaryRow is one (channel, status) count pair as the dashboard API
// serves it.
type SummaryRow struct {
	Channel string `json:"channel"`
	Status  string `json:"status"`
	Count   int64  `json:"count"`
}

// Summary reads the current rollup table's totals per (channel, status),
// the exact query the dashboard API's /api/summary endpoint runs against
// live ClickHouse state (cmd/deliverygateway).
func (c *ClickHouseStore) Summary(ctx context.Context) ([]SummaryRow, error) {
	counts, err := c.RollupChannelStatusCounts(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]SummaryRow, 0, len(counts))
	for _, cnt := range counts {
		out = append(out, SummaryRow{Channel: cnt.Channel, Status: cnt.Status, Count: cnt.Count})
	}
	return out, nil
}
