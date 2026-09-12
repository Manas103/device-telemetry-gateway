// Package simstorefront builds simulated storefront event corpora: profiles
// browsing, adding to cart, and purchasing, spread over a synthetic history
// window so day-windowed segment predicates (last 7/30/90 days) have real
// data to discriminate on.
package simstorefront

import (
	"fmt"
	"math/rand"
	"time"

	pb "device-telemetry-gateway/proto"
	"device-telemetry-gateway/internal/segment"
)

// ProfileEvents is one profile's own event history, in chronological order
// (a real user's own actions happen in real time order, even though many
// profiles' streams interleave in the corpus as a whole).
type ProfileEvents struct {
	ProfileID string
	Events    []*pb.StorefrontEvent
}

// Build generates numProfiles independent profiles, each with a random
// number of events (1..2*avgEvents-1, averaging avgEvents) spread uniformly
// over the last historyDays days up to now. Each profile's own event_ids
// are unique across the whole corpus.
func Build(numProfiles int, avgEvents int, historyDays int, now time.Time, seed int64) []ProfileEvents {
	rng := rand.New(rand.NewSource(seed))
	out := make([]ProfileEvents, numProfiles)

	for p := 0; p < numProfiles; p++ {
		profileID := fmt.Sprintf("profile-%08d", p)
		n := 1 + rng.Intn(2*avgEvents)
		events := make([]*pb.StorefrontEvent, 0, n)

		cartOpen := false
		for i := 0; i < n; i++ {
			daysAgo := rng.Float64() * float64(historyDays)
			ts := now.Add(-time.Duration(daysAgo * float64(24*time.Hour)))

			var eventType string
			r := rng.Float64()
			switch {
			case r < 0.55:
				eventType = "page_view"
			case r < 0.80:
				eventType = "add_to_cart"
				cartOpen = true
			case r < 0.92 && cartOpen:
				eventType = "purchase"
				cartOpen = false
			case r < 0.97 && cartOpen:
				eventType = "cart_abandon"
				cartOpen = false
			default:
				eventType = "page_view"
			}

			attrs := map[string]string{
				"category": segment.Categories[rng.Intn(len(segment.Categories))],
			}
			if eventType == "purchase" || eventType == "add_to_cart" {
				attrs["price"] = fmt.Sprintf("%.2f", 5+rng.Float64()*495)
				attrs["sku"] = fmt.Sprintf("sku-%05d", rng.Intn(50000))
			}

			events = append(events, &pb.StorefrontEvent{
				EventId:     fmt.Sprintf("evt-%08d-%04d", p, i),
				ProfileId:   profileID,
				EventType:   eventType,
				TimestampMs: ts.UnixMilli(),
				Attributes:  attrs,
			})
		}

		// Sort into this profile's own chronological order: a real
		// profile's actions are generated with random days-ago offsets
		// above, so without sorting a "later" event could have an
		// earlier timestamp than one generated before it.
		for i := 1; i < len(events); i++ {
			for j := i; j > 0 && events[j].TimestampMs < events[j-1].TimestampMs; j-- {
				events[j], events[j-1] = events[j-1], events[j]
			}
		}

		out[p] = ProfileEvents{ProfileID: profileID, Events: events}
	}
	return out
}

// Flatten concatenates every profile's events into one slice, in no
// particular cross-profile order (callers that care about send order sort
// or shuffle explicitly).
func Flatten(profiles []ProfileEvents) []*pb.StorefrontEvent {
	var out []*pb.StorefrontEvent
	for _, p := range profiles {
		out = append(out, p.Events...)
	}
	return out
}

// WithDuplicates returns a delivery-order copy of events where dupFraction
// of the delivered stream is duplicate traffic (an event resent with its
// original event_id, exactly the retried-send scenario the edge's dedup
// exists for), shuffled so delivery order is not simply "every event once,
// then all the dupes".
func WithDuplicates(events []*pb.StorefrontEvent, dupFraction float64, seed int64) []*pb.StorefrontEvent {
	rng := rand.New(rand.NewSource(seed))
	dupCount := int(float64(len(events)) * dupFraction / (1 - dupFraction))
	delivered := make([]*pb.StorefrontEvent, 0, len(events)+dupCount)
	delivered = append(delivered, events...)
	for i := 0; i < dupCount; i++ {
		delivered = append(delivered, events[rng.Intn(len(events))])
	}
	rng.Shuffle(len(delivered), func(i, j int) { delivered[i], delivered[j] = delivered[j], delivered[i] })
	return delivered
}
