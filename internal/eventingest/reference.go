package eventingest

import pb "device-telemetry-gateway/proto"

// ReferenceDedup is a deliberately naive, independently written
// deduplication used only to build ground truth in tests and the benchmark:
// given a raw, possibly duplicated delivery sequence, return one event per
// unique event_id, first occurrence wins. It shares no code with EventDedup
// or Pipeline, so a bug common to both of those could not also be present
// here by construction.
func ReferenceDedup(delivered []*pb.StorefrontEvent) []*pb.StorefrontEvent {
	seen := make(map[string]bool, len(delivered))
	out := make([]*pb.StorefrontEvent, 0, len(delivered))
	for _, e := range delivered {
		if seen[e.EventId] {
			continue
		}
		seen[e.EventId] = true
		out = append(out, e)
	}
	return out
}
