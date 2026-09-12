// Package segment defines the ~50 test segments this project's incremental
// Redis path and from-scratch ClickHouse recompute path are both evaluated
// against, and both implementations of "what a profile currently belongs
// to": an incremental engine that updates a profile's membership as its own
// events land, and a recompute engine that answers the same question from
// zero by scanning the full ClickHouse event log with one SQL query per
// segment. Segment (this file) is the only thing the two paths share: the
// segment definitions are data, not code, so sharing them is what makes a
// diff between the two paths meaningful (both must be answering the exact
// same predicate) without sharing any evaluation logic (see incremental.go
// and recompute.go, which import this file but not each other).
package segment

import "fmt"

// Family names one of the eight predicate shapes every one of the 50
// segments is an instance of.
type Family string

const (
	FamilyPurchaseCountMin    Family = "purchase_count_min"
	FamilyCartAbandonDays     Family = "cart_abandon_days"
	FamilyCategoryViewMin     Family = "category_view_min"
	FamilyPurchaseValueMin    Family = "purchase_value_min"
	FamilyRecentlyActiveHours Family = "recently_active_hours"
	FamilyNeverPurchased      Family = "never_purchased"
	FamilyChurnRiskDays       Family = "churn_risk_days"
	FamilyMultiCategoryMin    Family = "multi_category_shopper_min"
)

// Segment is one parameterized instance of a Family. Not every field
// applies to every family; each family reads only the fields it needs
// (see the switch in incremental.go's Evaluate and recompute.go's query
// builder, which must both be kept in sync with this list by construction:
// GenerateTestSegments is the single source of the 50 segments, and both
// evaluators range over its output rather than hand-maintaining their own
// copy).
type Segment struct {
	ID           string
	Family       Family
	MinCount     int     // purchase_count_min, category_view_min
	WindowDays   int     // purchase_count_min, purchase_value_min
	Category     string  // category_view_min
	MinValue     float64 // purchase_value_min
	MinHours     int     // recently_active_hours
	MinDays      int     // cart_abandon_days, churn_risk_days
	MinDistinct  int     // multi_category_shopper_min
}

// Categories is the fixed set of storefront product categories this
// project's simulated corpus and segment predicates both use.
var Categories = []string{
	"electronics", "apparel", "home", "beauty",
	"sports", "books", "toys", "grocery",
}

// GenerateTestSegments returns the 50 test segments used across every
// benchmark in this project: 12 purchase_count_min, 5 cart_abandon_days, 16
// category_view_min, 6 purchase_value_min, 4 recently_active_hours, 1
// never_purchased, 3 churn_risk_days, 3 multi_category_shopper_min. The
// count and the exact parameter grid are deliberately spelled out here
// (not derived from a config file) so the 50-segment claim is verifiable
// by reading one function.
func GenerateTestSegments() []Segment {
	var out []Segment

	for _, n := range []int{1, 2, 3, 5} {
		for _, w := range []int{7, 30, 90} {
			out = append(out, Segment{
				ID:         fmt.Sprintf("purchase_count_min_%d_%dd", n, w),
				Family:     FamilyPurchaseCountMin,
				MinCount:   n,
				WindowDays: w,
			})
		}
	}

	for _, d := range []int{1, 3, 7, 14, 30} {
		out = append(out, Segment{
			ID:      fmt.Sprintf("cart_abandon_%dd", d),
			Family:  FamilyCartAbandonDays,
			MinDays: d,
		})
	}

	for _, c := range Categories {
		for _, n := range []int{3, 5} {
			out = append(out, Segment{
				ID:       fmt.Sprintf("category_view_min_%s_%d", c, n),
				Family:   FamilyCategoryViewMin,
				Category: c,
				MinCount: n,
			})
		}
	}

	for _, t := range []float64{100, 250, 500} {
		for _, w := range []int{30, 90} {
			out = append(out, Segment{
				ID:         fmt.Sprintf("purchase_value_min_%.0f_%dd", t, w),
				Family:     FamilyPurchaseValueMin,
				MinValue:   t,
				WindowDays: w,
			})
		}
	}

	for _, h := range []int{1, 6, 24, 72} {
		out = append(out, Segment{
			ID:       fmt.Sprintf("recently_active_%dh", h),
			Family:   FamilyRecentlyActiveHours,
			MinHours: h,
		})
	}

	out = append(out, Segment{ID: "never_purchased", Family: FamilyNeverPurchased})

	for _, d := range []int{14, 30, 60} {
		out = append(out, Segment{
			ID:      fmt.Sprintf("churn_risk_%dd", d),
			Family:  FamilyChurnRiskDays,
			MinDays: d,
		})
	}

	for _, k := range []int{2, 3, 4} {
		out = append(out, Segment{
			ID:          fmt.Sprintf("multi_category_shopper_min_%d", k),
			Family:      FamilyMultiCategoryMin,
			MinDistinct: k,
		})
	}

	return out
}
