package segment

import (
	"context"
	"fmt"
	"time"
)

// Recompute answers "which profiles belong to this segment" from zero, by
// running one SQL aggregate query directly against the full ClickHouse
// event log. It shares no code with IncrementalEngine: the two are meant to
// be wrong independently. The definitions (Segment values) are the only
// thing shared, because a diff is only meaningful if both sides are
// answering the same question.
func Recompute(ctx context.Context, ch *ClickHouseStore, seg Segment, asOf time.Time) (map[string]bool, error) {
	asOfStr := asOf.UTC().Format("2006-01-02 15:04:05.000")
	var q string
	switch seg.Family {
	case FamilyPurchaseCountMin:
		q = fmt.Sprintf(`SELECT profile_id FROM events
WHERE event_type = 'purchase' AND timestamp <= '%s' AND timestamp >= '%s' - INTERVAL %d DAY
GROUP BY profile_id HAVING count() >= %d`, asOfStr, asOfStr, seg.WindowDays, seg.MinCount)
	case FamilyPurchaseValueMin:
		q = fmt.Sprintf(`SELECT profile_id FROM events
WHERE event_type = 'purchase' AND timestamp <= '%s' AND timestamp >= '%s' - INTERVAL %d DAY
GROUP BY profile_id HAVING sum(toFloat64OrZero(attributes['price'])) >= %f`, asOfStr, asOfStr, seg.WindowDays, seg.MinValue)
	case FamilyCartAbandonDays:
		q = fmt.Sprintf(`SELECT profile_id FROM events
WHERE timestamp <= '%s'
GROUP BY profile_id
HAVING max(if(event_type = 'add_to_cart', timestamp, toDateTime64(0,3))) >
       max(if(event_type = 'purchase', timestamp, toDateTime64(0,3)))
   AND dateDiff('second', max(if(event_type = 'add_to_cart', timestamp, toDateTime64(0,3))), toDateTime64('%s',3)) >= %d`,
			asOfStr, asOfStr, seg.MinDays*86400)
	case FamilyCategoryViewMin:
		q = fmt.Sprintf(`SELECT profile_id FROM events
WHERE event_type = 'page_view' AND attributes['category'] = '%s' AND timestamp <= '%s'
GROUP BY profile_id HAVING count() >= %d`, seg.Category, asOfStr, seg.MinCount)
	case FamilyRecentlyActiveHours:
		q = fmt.Sprintf(`SELECT profile_id FROM events
WHERE timestamp <= '%s' AND timestamp >= '%s' - INTERVAL %d HOUR
GROUP BY profile_id`, asOfStr, asOfStr, seg.MinHours)
	case FamilyNeverPurchased:
		q = fmt.Sprintf(`SELECT profile_id FROM events
WHERE timestamp <= '%s'
GROUP BY profile_id HAVING sum(event_type = 'purchase') = 0`, asOfStr)
	case FamilyChurnRiskDays:
		q = fmt.Sprintf(`SELECT profile_id FROM events
WHERE timestamp <= '%s'
GROUP BY profile_id
HAVING sum(event_type = 'purchase') >= 1
   AND dateDiff('second', max(timestamp), toDateTime64('%s',3)) >= %d`, asOfStr, asOfStr, seg.MinDays*86400)
	case FamilyMultiCategoryMin:
		q = fmt.Sprintf(`SELECT profile_id FROM events
WHERE event_type = 'page_view' AND timestamp <= '%s'
GROUP BY profile_id HAVING uniqExact(attributes['category']) >= %d`, asOfStr, seg.MinDistinct)
	default:
		return nil, fmt.Errorf("unknown family %q", seg.Family)
	}
	return ch.QueryProfileIDs(ctx, q)
}
