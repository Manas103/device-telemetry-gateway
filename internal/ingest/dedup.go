// Package ingest is the core of the gateway: deduplicate on (device_id,
// sequence), hand a Reading off to Kafka, and spill to disk rather than drop
// it when Kafka cannot take it right now.
package ingest

import "sync"

// Dedup tracks, per device, every sequence number already forwarded
// downstream. Telemetry can legitimately arrive out of order over a
// WebSocket connection (nothing here assumes readings arrive in sequence
// order), so unlike a monotonic replay counter this has to remember the
// full set of sequences seen, not just a high-water mark.
type Dedup struct {
	mu   sync.Mutex
	seen map[string]map[uint64]bool
}

func NewDedup() *Dedup {
	return &Dedup{seen: make(map[string]map[uint64]bool)}
}

// Admit reports whether (deviceID, sequence) has not been seen before, and
// records it if so. Safe for concurrent use across many device connections.
func (d *Dedup) Admit(deviceID string, sequence uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	set, ok := d.seen[deviceID]
	if !ok {
		set = make(map[uint64]bool)
		d.seen[deviceID] = set
	}
	if set[sequence] {
		return false
	}
	set[sequence] = true
	return true
}
