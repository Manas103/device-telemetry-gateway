// Package eventingest is the storefront-event edge: deduplicate on event_id
// and hand a StorefrontEvent off to Kafka. It shares no code with
// internal/ingest (the telemetry gateway's device+sequence dedup) because
// the two dedup keys have a genuinely different shape: telemetry has no
// single globally unique id per reading and instead relies on a compound
// (device_id, sequence) key, while a storefront event already carries one
// client-assigned event_id, the way a real event-tracking SDK (Segment,
// Amplitude, a first-party pixel) assigns a UUID per event before it is
// ever sent, so a retried send is trivially identifiable without a compound
// key at all.
package eventingest

import "sync"

// MaxEventIDs bounds how many distinct event ids EventDedup remembers in
// total (not per profile: unlike a device, a storefront profile has no
// natural cardinality limit worth tracking separately, and the dedup window
// that actually matters is "how far back could a retried send plausibly
// still arrive", not "how many events has this one profile ever sent").
// Sized well above the largest single burst this project's benchmarks
// offer, so capping never changes correctness for any traffic this edge is
// actually exercised with; it only bounds memory for a long-running edge.
const MaxEventIDs = 4_000_000

// EventDedup is a bounded, FIFO-evicting set of event ids already forwarded
// downstream. Safe for concurrent use.
type EventDedup struct {
	mu    sync.Mutex
	seen  map[string]struct{}
	order []string
}

func NewEventDedup() *EventDedup {
	return &EventDedup{seen: make(map[string]struct{}, 1024)}
}

// Admit reports whether eventID has not been seen before, and records it if
// so.
func (d *EventDedup) Admit(eventID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[eventID]; ok {
		return false
	}
	d.seen[eventID] = struct{}{}
	d.order = append(d.order, eventID)
	if len(d.order) > MaxEventIDs {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.seen, oldest)
	}
	return true
}

// EntryCount returns how many event ids are currently remembered.
func (d *EventDedup) EntryCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
