// Package deliverystats is the third extension's own edge: deduplicate
// simulated email/SMS/WhatsApp delivery webhooks on webhook_id and hand them
// off to Kafka, from which a consumer fans them out to ClickHouse (a raw
// event log plus a periodically refreshed rollup table) behind a small JSON
// API a React dashboard reads.
//
// This package deliberately does not import internal/eventingest, even
// though the dedup shape (a bounded, FIFO-evicting set of already-seen ids)
// is the same idea as EventDedup there. Two reasons: first, the task this
// extension was built to is explicit that this module's correctness must
// not be entangled with the storefront/segment module's state or package
// graph; second, and more concretely, a shared dedup type would mean a bug
// in that one shared implementation could silently pass both modules'
// differential tests at once, which is exactly the failure mode
// internal/segment's own doc comment on sharing definitions-not-evaluators
// already warns about. Independent, small, and duplicated in shape is the
// safer choice here, not an oversight.
package deliverystats

import "sync"

// MaxWebhookIDs bounds how many distinct webhook ids WebhookDedup remembers
// in total. Sized well above the largest single burst any benchmark in this
// extension offers through the live edge (the 50M-row ClickHouse scale
// benchmark inserts directly into ClickHouse and does not go through this
// dedup at all; see docs and the README's Architecture section for why
// those are deliberately different paths), so capping never changes
// correctness for traffic this edge is actually exercised with over Kafka.
const MaxWebhookIDs = 4_000_000

// WebhookDedup is a bounded, FIFO-evicting set of webhook ids already
// forwarded downstream. Safe for concurrent use.
type WebhookDedup struct {
	mu    sync.Mutex
	seen  map[string]struct{}
	order []string
}

func NewWebhookDedup() *WebhookDedup {
	return &WebhookDedup{seen: make(map[string]struct{}, 1024)}
}

// Admit reports whether webhookID has not been seen before, and records it
// if so.
func (d *WebhookDedup) Admit(webhookID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[webhookID]; ok {
		return false
	}
	d.seen[webhookID] = struct{}{}
	d.order = append(d.order, webhookID)
	if len(d.order) > MaxWebhookIDs {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.seen, oldest)
	}
	return true
}

// EntryCount returns how many webhook ids are currently remembered.
func (d *WebhookDedup) EntryCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
