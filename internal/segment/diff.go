package segment

// SegmentDiff is one segment's comparison result between the incremental
// Redis membership set and the from-scratch ClickHouse recompute set.
type SegmentDiff struct {
	SegmentID       string
	IncrementalSize int
	RecomputeSize   int
	OnlyIncremental int
	OnlyRecompute   int
}

// Exact reports whether the two sides matched with zero disagreement.
func (d SegmentDiff) Exact() bool {
	return d.OnlyIncremental == 0 && d.OnlyRecompute == 0
}

// DiffSets compares two profile-id sets for one segment.
func DiffSets(segmentID string, incremental, recompute map[string]bool) SegmentDiff {
	onlyIncremental := 0
	for id := range incremental {
		if !recompute[id] {
			onlyIncremental++
		}
	}
	onlyRecompute := 0
	for id := range recompute {
		if !incremental[id] {
			onlyRecompute++
		}
	}
	return SegmentDiff{
		SegmentID:       segmentID,
		IncrementalSize: len(incremental),
		RecomputeSize:   len(recompute),
		OnlyIncremental: onlyIncremental,
		OnlyRecompute:   onlyRecompute,
	}
}
