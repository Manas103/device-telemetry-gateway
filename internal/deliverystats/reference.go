package deliverystats

import pb "device-telemetry-gateway/proto"

// ReferenceDedup is a deliberately naive, independently written
// deduplication used only to build ground truth in tests and the dedup
// benchmark: given a raw, possibly-retried delivery sequence, return one
// webhook per unique webhook_id, first occurrence wins. It shares no code
// with WebhookDedup or Pipeline, so a bug common to both of those could not
// also be present here by construction.
func ReferenceDedup(delivered []*pb.DeliveryWebhook) []*pb.DeliveryWebhook {
	seen := make(map[string]bool, len(delivered))
	out := make([]*pb.DeliveryWebhook, 0, len(delivered))
	for _, w := range delivered {
		if seen[w.WebhookId] {
			continue
		}
		seen[w.WebhookId] = true
		out = append(out, w)
	}
	return out
}
