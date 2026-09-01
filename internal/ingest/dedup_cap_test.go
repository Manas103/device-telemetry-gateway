package ingest

import "testing"

// TestDedupCacheStaysBounded is the regression test for the unbounded
// dedup cache the soak benchmark (cmd/soak) found: admit far more than
// MaxSequencesPerDevice distinct sequences for one device and assert the
// remembered set never exceeds the cap, rather than growing linearly with
// however many readings that device has ever sent.
func TestDedupCacheStaysBounded(t *testing.T) {
	d := NewDedup()
	const total = MaxSequencesPerDevice * 5

	for seq := uint64(0); seq < total; seq++ {
		if !d.Admit("dev-soak", seq) {
			t.Fatalf("sequence %d should be novel and admitted", seq)
		}
		if got := d.EntryCount(); got > MaxSequencesPerDevice {
			t.Fatalf("after admitting sequence %d: dedup holds %d entries, want <= %d",
				seq, got, MaxSequencesPerDevice)
		}
	}
	if got := d.EntryCount(); got != MaxSequencesPerDevice {
		t.Fatalf("final entry count = %d, want exactly %d (cap reached and held)", got, MaxSequencesPerDevice)
	}
}

// TestDedupCacheEvictionDoesNotReadmitRecentDuplicates confirms the cap's
// eviction policy does not undermine dedup for the traffic this gateway is
// actually meant to handle: a duplicate of a *recent* reading (well within
// the cap) must still be rejected after the cache has filled.
func TestDedupCacheEvictionDoesNotReadmitRecentDuplicates(t *testing.T) {
	d := NewDedup()
	for seq := uint64(0); seq < MaxSequencesPerDevice*2; seq++ {
		d.Admit("dev-soak", seq)
	}
	recentDuplicate := uint64(MaxSequencesPerDevice*2 - 1)
	if d.Admit("dev-soak", recentDuplicate) {
		t.Fatalf("a duplicate of the most recent sequence must still be rejected after the cache has filled")
	}
}

// TestDedupCacheIsPerDevice confirms the cap applies per device, not
// globally: a busy device filling its cap must not evict or affect a
// different device's entries.
func TestDedupCacheIsPerDevice(t *testing.T) {
	d := NewDedup()
	d.Admit("dev-quiet", 1)
	for seq := uint64(0); seq < MaxSequencesPerDevice*3; seq++ {
		d.Admit("dev-busy", seq)
	}
	if d.Admit("dev-quiet", 1) {
		t.Fatalf("dev-quiet's single entry must not have been evicted by dev-busy filling its own cap")
	}
	if d.DeviceCount() != 2 {
		t.Fatalf("expected 2 devices tracked, got %d", d.DeviceCount())
	}
}
