package segment

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

// ClickHouseStore is the event store: a thin client over ClickHouse's plain
// HTTP interface (port 8123 by default). A hand-rolled HTTP client rather
// than the official driver is a deliberate choice for this project: the
// only two operations this benchmark needs are "insert a batch of rows" and
// "run a read-only aggregate query and get rows back", both of which the
// HTTP interface does natively by just POSTing SQL text as the request
// body, with no extra dependency beyond the standard library, in keeping
// with this repo's existing preference for hand-rolled infrastructure
// (Spool, Dedup) over pulling in a library for a narrow, well-understood
// need.
type ClickHouseStore struct {
	baseURL string
	client  *http.Client
}

func NewClickHouseStore(addr string) *ClickHouseStore {
	return &ClickHouseStore{
		baseURL: fmt.Sprintf("http://%s/", addr),
		client:  &http.Client{Timeout: 120 * time.Second},
	}
}

// EventsTableDDL is the event store's schema: one row per StorefrontEvent,
// ordered by (profile_id, timestamp) so a per-profile scan (which every
// recompute query below performs) is a contiguous read, not a scattered
// one.
const EventsTableDDL = `
CREATE TABLE IF NOT EXISTS events (
    event_id String,
    profile_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(3),
    attributes Map(String, String)
) ENGINE = MergeTree
ORDER BY (profile_id, timestamp)
`

func (c *ClickHouseStore) EnsureSchema(ctx context.Context) error {
	if _, err := c.raw(ctx, EventsTableDDL); err != nil {
		return err
	}
	return nil
}

// TruncateEvents drops all rows, used only to reset state between benchmark
// attempts so a re-run does not silently accumulate rows from a prior,
// possibly-aborted attempt on top of a fresh one.
func (c *ClickHouseStore) TruncateEvents(ctx context.Context) error {
	_, err := c.raw(ctx, "TRUNCATE TABLE IF EXISTS events")
	return err
}

// raw POSTs sqlText verbatim as the HTTP body, ClickHouse's own convention
// for its plain HTTP interface: the request body is the query (DDL, INSERT
// with inline data, or a SELECT with a trailing FORMAT clause), no query
// string parameter needed.
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

// chRow mirrors one StorefrontEvent for JSONEachRow insertion.
type chRow struct {
	EventID    string            `json:"event_id"`
	ProfileID  string            `json:"profile_id"`
	EventType  string            `json:"event_type"`
	Timestamp  string            `json:"timestamp"`
	Attributes map[string]string `json:"attributes"`
}

// InsertBatch bulk-inserts a batch of events using ClickHouse's
// JSONEachRow insert format, one HTTP request per batch. Batching (rather
// than one row per request) is what makes sustained-throughput ingestion
// into ClickHouse viable at all: ClickHouse is optimized for batch inserts
// of thousands of rows, not per-row round trips.
func (c *ClickHouseStore) InsertBatch(ctx context.Context, events []*pb.StorefrontEvent) error {
	if len(events) == 0 {
		return nil
	}
	var buf bytes.Buffer
	buf.WriteString("INSERT INTO events FORMAT JSONEachRow\n")
	enc := json.NewEncoder(&buf)
	for _, e := range events {
		row := chRow{
			EventID:    e.EventId,
			ProfileID:  e.ProfileId,
			EventType:  e.EventType,
			Timestamp:  time.UnixMilli(e.TimestampMs).UTC().Format("2006-01-02 15:04:05.000"),
			Attributes: e.Attributes,
		}
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	_, err := c.raw(ctx, buf.String())
	return err
}

// QueryProfileIDs runs a read-only SQL query expected to return exactly one
// column named profile_id, and returns the set of values. Used by
// recompute.go to run each segment's independent full-scan query.
func (c *ClickHouseStore) QueryProfileIDs(ctx context.Context, query string) (map[string]bool, error) {
	body, err := c.raw(ctx, query+"\nFORMAT JSONEachRow")
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool)
	dec := json.NewDecoder(bytes.NewReader(body))
	for dec.More() {
		var row struct {
			ProfileID string `json:"profile_id"`
		}
		if err := dec.Decode(&row); err != nil {
			return nil, fmt.Errorf("decode row: %w", err)
		}
		out[row.ProfileID] = true
	}
	return out, nil
}

// CountRows returns the total row count in the events table, used to report
// the actual scale a run reached.
func (c *ClickHouseStore) CountRows(ctx context.Context) (int64, error) {
	return c.scalarInt(ctx, "SELECT count() AS c FROM events")
}

// CountDistinctProfiles returns the number of distinct profile_id values in
// the events table, the "scale actually run at" number for the 1M-profile
// design claim.
func (c *ClickHouseStore) CountDistinctProfiles(ctx context.Context) (int64, error) {
	return c.scalarInt(ctx, "SELECT uniqExact(profile_id) AS c FROM events")
}

func (c *ClickHouseStore) scalarInt(ctx context.Context, query string) (int64, error) {
	body, err := c.raw(ctx, query+"\nFORMAT JSONEachRow")
	if err != nil {
		return 0, err
	}
	// ClickHouse's JSON/JSONEachRow output formats represent count()/uniqExact()
	// as a quoted string on some server builds and as a bare JSON number on
	// others (this project's own server sends the latter for these two
	// queries even though the row-level Reading/Event columns come back
	// quoted). json.Number's Kind is String, so decoding either a JSON string
	// or a JSON number literal into it succeeds without knowing in advance
	// which one the server will send. See README Findings.
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
