package segment

import "testing"

// TestGenerateTestSegmentsCountAndUniqueIDs pins the 50-segment claim: the
// exact count documented in GenerateTestSegments's comment, and no two
// segments sharing an ID (a duplicate ID would silently merge two segments
// in the Redis key space and in the recompute query set).
func TestGenerateTestSegmentsCountAndUniqueIDs(t *testing.T) {
	segments := GenerateTestSegments()
	if len(segments) != 50 {
		t.Fatalf("expected 50 test segments, got %d", len(segments))
	}
	seen := make(map[string]bool, len(segments))
	for _, s := range segments {
		if seen[s.ID] {
			t.Fatalf("duplicate segment ID: %s", s.ID)
		}
		seen[s.ID] = true
		if s.Family == "" {
			t.Fatalf("segment %s has no family", s.ID)
		}
	}
}

// TestDiffSetsExactMatch pins the zero-disagreement case the segmentbench
// benchmark reports as "50 / 50 segments matched exactly".
func TestDiffSetsExactMatch(t *testing.T) {
	a := map[string]bool{"p1": true, "p2": true}
	b := map[string]bool{"p1": true, "p2": true}
	d := DiffSets("seg", a, b)
	if !d.Exact() {
		t.Fatalf("expected exact match, got %+v", d)
	}
	if d.IncrementalSize != 2 || d.RecomputeSize != 2 {
		t.Fatalf("unexpected sizes: %+v", d)
	}
}

// TestDiffSetsDetectsDisagreementInBothDirections proves DiffSets actually
// catches a mismatch rather than only ever reporting Exact() == true; a
// diff helper that can't fail would make the benchmark's "50/50 exact"
// result meaningless.
func TestDiffSetsDetectsDisagreementInBothDirections(t *testing.T) {
	incrementalOnly := map[string]bool{"p1": true, "p2": true}
	recomputeOnly := map[string]bool{"p1": true, "p3": true}
	d := DiffSets("seg", incrementalOnly, recomputeOnly)
	if d.Exact() {
		t.Fatalf("expected a disagreement, DiffSets reported exact: %+v", d)
	}
	if d.OnlyIncremental != 1 {
		t.Fatalf("expected 1 profile only on the incremental side (p2), got %d", d.OnlyIncremental)
	}
	if d.OnlyRecompute != 1 {
		t.Fatalf("expected 1 profile only on the recompute side (p3), got %d", d.OnlyRecompute)
	}
}
