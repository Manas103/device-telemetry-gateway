package segment

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	pb "device-telemetry-gateway/proto"
)

// IncrementalEngine updates a profile's segment membership in Redis as its
// own events land, using only bounded, per-profile state (a stats hash, a
// per-category view-count hash, and a purchase ZSET trimmed to 90 days) so
// each update costs O(segments) Redis round trips, never a scan of that
// profile's full history and never a scan of any other profile. This is
// the "incremental" half of the design; recompute.go is the independent,
// from-scratch half used only to check this one is right.
//
// Time-only predicates (cart_abandon_days, churn_risk_days,
// recently_active_hours becoming FALSE as time passes with no new event)
// cannot become true purely from an event handler, because no event
// arrives at the moment a day boundary is crossed. This engine handles
// that the way a real segment platform does: EvaluateProfile always
// evaluates against an explicit `now` the caller supplies, and Finalize
// re-evaluates every touched profile once at the run's end using the
// corpus's own latest timestamp as `now`, which is what actually resolves
// every time-elapsed transition. Between events, membership can lag reality
// on these particular predicates by up to one Finalize interval; that is
// disclosed in the README, not hidden.
type IncrementalEngine struct {
	rdb      *redis.Client
	segments []Segment
}

func NewIncrementalEngine(rdb *redis.Client, segments []Segment) *IncrementalEngine {
	return &IncrementalEngine{rdb: rdb, segments: segments}
}

func statsKey(p string) string    { return "stats:" + p }
func catviewsKey(p string) string { return "catviews:" + p }
func purchKey(p string) string    { return "purch:" + p }
func segmentsKey(p string) string { return "segments:" + p }
func memberKey(id string) string  { return "segmember:" + id }

// ApplyEvent updates profile state for one event and re-evaluates that
// profile's full membership immediately, using the event's own timestamp
// as `now`.
func (e *IncrementalEngine) ApplyEvent(ctx context.Context, ev *pb.StorefrontEvent) error {
	p := ev.ProfileId
	ts := time.UnixMilli(ev.TimestampMs)

	pipe := e.rdb.Pipeline()
	pipe.SAdd(ctx, "touched_profiles", p)
	pipe.HIncrBy(ctx, statsKey(p), "total_events", 1)
	pipe.HSet(ctx, statsKey(p), "last_event_ts", ev.TimestampMs)

	switch ev.EventType {
	case "purchase":
		price, _ := strconv.ParseFloat(ev.Attributes["price"], 64)
		pipe.HIncrBy(ctx, statsKey(p), "purchase_count_total", 1)
		pipe.HSet(ctx, statsKey(p), "last_purchase_ts", ev.TimestampMs)
		pipe.ZAdd(ctx, purchKey(p), redis.Z{Score: float64(ts.Unix()), Member: fmt.Sprintf("%s|%.2f", ev.EventId, price)})
		pipe.ZRemRangeByScore(ctx, purchKey(p), "-inf", strconv.FormatInt(ts.Add(-90*24*time.Hour).Unix(), 10))
	case "add_to_cart":
		pipe.HSet(ctx, statsKey(p), "last_cartadd_ts", ev.TimestampMs)
	case "page_view":
		if cat := ev.Attributes["category"]; cat != "" {
			pipe.HIncrBy(ctx, catviewsKey(p), cat, 1)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("apply event stats: %w", err)
	}

	return e.reevaluate(ctx, p, ts)
}

// Finalize re-evaluates every profile that ever received an event, using
// asOf as `now`, resolving any purely time-elapsed transitions that no
// event triggered. See the type doc for why this pass exists.
func (e *IncrementalEngine) Finalize(ctx context.Context, asOf time.Time) (int, error) {
	profiles, err := e.rdb.SMembers(ctx, "touched_profiles").Result()
	if err != nil {
		return 0, err
	}
	for _, p := range profiles {
		if err := e.reevaluate(ctx, p, asOf); err != nil {
			return 0, err
		}
	}
	return len(profiles), nil
}

func (e *IncrementalEngine) reevaluate(ctx context.Context, profileID string, now time.Time) error {
	matching, err := e.EvaluateProfile(ctx, profileID, now)
	if err != nil {
		return err
	}
	want := make(map[string]bool, len(matching))
	for _, id := range matching {
		want[id] = true
	}

	have, err := e.rdb.SMembers(ctx, segmentsKey(profileID)).Result()
	if err != nil {
		return err
	}
	haveSet := make(map[string]bool, len(have))
	for _, id := range have {
		haveSet[id] = true
	}

	pipe := e.rdb.Pipeline()
	changed := false
	for id := range want {
		if !haveSet[id] {
			pipe.SAdd(ctx, segmentsKey(profileID), id)
			pipe.SAdd(ctx, memberKey(id), profileID)
			changed = true
		}
	}
	for id := range haveSet {
		if !want[id] {
			pipe.SRem(ctx, segmentsKey(profileID), id)
			pipe.SRem(ctx, memberKey(id), profileID)
			changed = true
		}
	}
	if changed {
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("write membership delta: %w", err)
		}
	}
	return nil
}

// EvaluateProfile computes, from this profile's bounded incremental state
// only, which of e.segments it currently belongs to as of `now`.
func (e *IncrementalEngine) EvaluateProfile(ctx context.Context, profileID string, now time.Time) ([]string, error) {
	stats, err := e.rdb.HGetAll(ctx, statsKey(profileID)).Result()
	if err != nil {
		return nil, err
	}
	if len(stats) == 0 {
		return nil, nil
	}
	catviews, err := e.rdb.HGetAll(ctx, catviewsKey(profileID)).Result()
	if err != nil {
		return nil, err
	}

	totalEvents, _ := strconv.ParseInt(stats["total_events"], 10, 64)
	purchaseCountTotal, _ := strconv.ParseInt(stats["purchase_count_total"], 10, 64)
	lastEventTs, _ := strconv.ParseInt(stats["last_event_ts"], 10, 64)
	lastCartAddTs, _ := strconv.ParseInt(stats["last_cartadd_ts"], 10, 64)
	lastPurchaseTs, _ := strconv.ParseInt(stats["last_purchase_ts"], 10, 64)

	distinctCategories := len(catviews)

	// Every purchase_count_min and purchase_value_min segment needs a
	// ZSET read against this same profile's purchases; batching them into
	// one pipelined round trip (rather than one round trip per segment)
	// is what keeps a single event's re-evaluation cost from scaling
	// linearly with segment count in round trips, even though it still
	// scales linearly in command count.
	pipe := e.rdb.Pipeline()
	countCmds := make(map[string]*redis.IntCmd, 12)
	rangeCmds := make(map[string]*redis.StringSliceCmd, 6)
	for _, seg := range e.segments {
		windowStart := now.Add(-time.Duration(seg.WindowDays) * 24 * time.Hour).Unix()
		switch seg.Family {
		case FamilyPurchaseCountMin:
			countCmds[seg.ID] = pipe.ZCount(ctx, purchKey(profileID), strconv.FormatInt(windowStart, 10), strconv.FormatInt(now.Unix(), 10))
		case FamilyPurchaseValueMin:
			rangeCmds[seg.ID] = pipe.ZRangeByScore(ctx, purchKey(profileID), &redis.ZRangeBy{
				Min: strconv.FormatInt(windowStart, 10), Max: strconv.FormatInt(now.Unix(), 10),
			})
		}
	}
	if len(countCmds) > 0 || len(rangeCmds) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("batched purchase window reads: %w", err)
		}
	}

	var matching []string
	for _, seg := range e.segments {
		ok := false
		switch seg.Family {
		case FamilyPurchaseCountMin:
			count, err := countCmds[seg.ID].Result()
			if err != nil {
				return nil, err
			}
			ok = count >= int64(seg.MinCount)
		case FamilyPurchaseValueMin:
			members, err := rangeCmds[seg.ID].Result()
			if err != nil {
				return nil, err
			}
			var sum float64
			for _, m := range members {
				parts := strings.SplitN(m, "|", 2)
				if len(parts) == 2 {
					v, _ := strconv.ParseFloat(parts[1], 64)
					sum += v
				}
			}
			ok = sum >= seg.MinValue
		case FamilyCartAbandonDays:
			ok = lastCartAddTs > 0 && lastCartAddTs > lastPurchaseTs &&
				now.Sub(time.UnixMilli(lastCartAddTs)) >= time.Duration(seg.MinDays)*24*time.Hour
		case FamilyCategoryViewMin:
			c, _ := strconv.ParseInt(catviews[seg.Category], 10, 64)
			ok = c >= int64(seg.MinCount)
		case FamilyRecentlyActiveHours:
			ok = lastEventTs > 0 && now.Sub(time.UnixMilli(lastEventTs)) <= time.Duration(seg.MinHours)*time.Hour
		case FamilyNeverPurchased:
			ok = totalEvents > 0 && purchaseCountTotal == 0
		case FamilyChurnRiskDays:
			ok = purchaseCountTotal > 0 && lastEventTs > 0 &&
				now.Sub(time.UnixMilli(lastEventTs)) >= time.Duration(seg.MinDays)*24*time.Hour
		case FamilyMultiCategoryMin:
			ok = distinctCategories >= seg.MinDistinct
		}
		if ok {
			matching = append(matching, seg.ID)
		}
	}
	return matching, nil
}

// MembersOf returns the current incremental membership set for one segment,
// used by diff.go to compare against the recompute path.
func MembersOf(ctx context.Context, rdb *redis.Client, segmentID string) (map[string]bool, error) {
	members, err := rdb.SMembers(ctx, memberKey(segmentID)).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(members))
	for _, m := range members {
		out[m] = true
	}
	return out, nil
}
