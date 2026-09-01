// Package ingest is the core of the gateway: deduplicate on (device_id,
// sequence), hand a Reading off to Kafka, and spill to disk rather than drop
// it when Kafka cannot take it right now.
package ingest

import "sync"

// MaxSequencesPerDevice bounds how many distinct sequence numbers Dedup
// remembers for one device. Set well above any legitimate reordering or
// duplication window this gateway is meant to tolerate (the replay
// benchmark's own out-of-order window is a handful of readings), so
// capping never changes correctness for any traffic this gateway is
// actually meant to handle; it only bounds memory for a device that has
// been sending for a long time. See cmd/soak and the README's Findings
// section for the unbounded-growth bug this constant fixes.
const MaxSequencesPerDevice = 4096

// deviceSeen is a bounded FIFO record of sequences admitted for one
// device: a set for O(1) membership, plus an insertion-order queue so the
// oldest entry can be evicted in O(1) once the set exceeds
// MaxSequencesPerDevice. FIFO rather than true LRU: it is the cheapest
// eviction policy that still bounds memory, and "duplicate arrives more
// than 4096 distinct sequences after its original" is far outside any
// duplication window this gateway is designed to dedup correctly, so the
// two are not in tension.
type deviceSeen struct {
	set   map[uint64]bool
	order []uint64
}

// Dedup tracks, per device, every sequence number already forwarded
// downstream, up to MaxSequencesPerDevice. Telemetry can legitimately
// arrive out of order over a WebSocket connection (nothing here assumes
// readings arrive in sequence order), so unlike a monotonic replay counter
// this has to remember a set of sequences seen, not just a high-water
// mark.
type Dedup struct {
	mu   sync.Mutex
	seen map[string]*deviceSeen
}

func NewDedup() *Dedup {
	return &Dedup{seen: make(map[string]*deviceSeen)}
}

// Admit reports whether (deviceID, sequence) has not been seen before, and
// records it if so. Safe for concurrent use across many device connections.
func (d *Dedup) Admit(deviceID string, sequence uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	ds, ok := d.seen[deviceID]
	if !ok {
		ds = &deviceSeen{set: make(map[uint64]bool)}
		d.seen[deviceID] = ds
	}
	if ds.set[sequence] {
		return false
	}
	ds.set[sequence] = true
	ds.order = append(ds.order, sequence)
	if len(ds.order) > MaxSequencesPerDevice {
		oldest := ds.order[0]
		ds.order = ds.order[1:]
		delete(ds.set, oldest)
	}
	return true
}

// EntryCount returns the total number of sequences currently remembered
// across every device, the direct measurement the soak benchmark and its
// regression test use to confirm memory stays bounded.
func (d *Dedup) EntryCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	total := 0
	for _, ds := range d.seen {
		total += len(ds.set)
	}
	return total
}

// DeviceCount returns how many distinct devices Dedup currently holds any
// state for.
func (d *Dedup) DeviceCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
