// simwebhooks builds simulated provider delivery-webhook corpora: for each
// outbound message, a small realistic lifecycle of webhooks (a "sent"
// callback, then either "delivered" or "bounced"/"failed") spread across the
// three channels the resume claims (email, SMS, WhatsApp), each with its own
// simulated provider name, the way a real integration would route each
// channel through a different vendor.
package deliverystats

import (
	"fmt"
	"math/rand"
	"time"

	pb "device-telemetry-gateway/proto"
)

// Channels and their simulated provider, fixed so every benchmark in this
// extension exercises the same three-channel, three-provider shape the
// resume claims.
var Channels = []struct {
	Name     string
	Provider string
}{
	{"email", "sendgrid"},
	{"sms", "twilio"},
	{"whatsapp", "meta_whatsapp"},
}

// Build generates numMessages independent outbound messages, each producing
// 2 or 3 webhooks (sent, then delivered or bounced/failed, with a further
// small chance of a distinct final failed after an initial bounce) spread
// uniformly over the last historyMinutes minutes up to now, one channel per
// message chosen uniformly across the three. Returns the flat, unsorted list
// of webhooks (unsorted because a real multi-provider setup does not
// deliver webhooks to the receiver in any particular cross-message order).
func Build(numMessages int, historyMinutes int, now time.Time, seed int64) []*pb.DeliveryWebhook {
	rng := rand.New(rand.NewSource(seed))
	out := make([]*pb.DeliveryWebhook, 0, numMessages*2)

	for m := 0; m < numMessages; m++ {
		messageID := fmt.Sprintf("msg-%09d", m)
		ch := Channels[rng.Intn(len(Channels))]

		minutesAgo := rng.Float64() * float64(historyMinutes)
		sentAt := now.Add(-time.Duration(minutesAgo * float64(time.Minute)))

		wh := 0
		nextID := func() string {
			id := fmt.Sprintf("wh-%09d-%d", m, wh)
			wh++
			return id
		}

		out = append(out, &pb.DeliveryWebhook{
			WebhookId:   nextID(),
			MessageId:   messageID,
			Channel:     ch.Name,
			Status:      "sent",
			TimestampMs: sentAt.UnixMilli(),
			Provider:    ch.Provider,
		})

		outcome := rng.Float64()
		deliveredAt := sentAt.Add(time.Duration(1+rng.Intn(30)) * time.Second)
		switch {
		case outcome < 0.90:
			out = append(out, &pb.DeliveryWebhook{
				WebhookId:   nextID(),
				MessageId:   messageID,
				Channel:     ch.Name,
				Status:      "delivered",
				TimestampMs: deliveredAt.UnixMilli(),
				Provider:    ch.Provider,
			})
		case outcome < 0.97:
			out = append(out, &pb.DeliveryWebhook{
				WebhookId:   nextID(),
				MessageId:   messageID,
				Channel:     ch.Name,
				Status:      "bounced",
				TimestampMs: deliveredAt.UnixMilli(),
				Provider:    ch.Provider,
			})
		default:
			out = append(out, &pb.DeliveryWebhook{
				WebhookId:   nextID(),
				MessageId:   messageID,
				Channel:     ch.Name,
				Status:      "failed",
				TimestampMs: deliveredAt.UnixMilli(),
				Provider:    ch.Provider,
			})
		}
	}
	return out
}

// WithRetries returns a delivery-order copy of webhooks where dupFraction of
// the delivered stream is a provider retry (the identical webhook resent
// with its original webhook_id, because the receiver's prior 2xx ack did not
// reach the provider, or the provider retries on a fixed schedule regardless
// of ack), shuffled so delivery order is not simply "every webhook once,
// then all the retries".
func WithRetries(webhooks []*pb.DeliveryWebhook, dupFraction float64, seed int64) []*pb.DeliveryWebhook {
	rng := rand.New(rand.NewSource(seed))
	dupCount := int(float64(len(webhooks)) * dupFraction / (1 - dupFraction))
	delivered := make([]*pb.DeliveryWebhook, 0, len(webhooks)+dupCount)
	delivered = append(delivered, webhooks...)
	for i := 0; i < dupCount; i++ {
		delivered = append(delivered, webhooks[rng.Intn(len(webhooks))])
	}
	rng.Shuffle(len(delivered), func(i, j int) { delivered[i], delivered[j] = delivered[j], delivered[i] })
	return delivered
}
