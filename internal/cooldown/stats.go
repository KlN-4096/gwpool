// Package cooldown 聚合有界的消费端自报样本；推荐参与消费端决策，不改池子的路由票或满血判定。
package cooldown

import (
	"fmt"
	"sort"
	"time"

	"gwpool/internal/types"
)

const (
	MaxReports         = 8192
	MaxReporterReports = 1024
	Retention          = 7 * 24 * time.Hour
	MaxSeconds         = 24 * 60 * 60
	LegacyMaxSeconds   = 10 * 60 * 60
	MinSeconds         = 60 * 60
	MaxElapsed         = 7 * 24 * 60 * 60
	MinOwnSuccesses    = 2
	MinPoolReporters   = 3
	GraceSeconds       = 60
	StableMinSamples   = 6
	StableSuccesses    = 3
	StableSpan         = 24 * time.Hour
	ReportInterval     = 6 * time.Hour
	ReportPolicyTTL    = 24 * time.Hour
)

var steps = []int{3600, 7200, 14400, 21600, 28800, 36000, 43200, 57600, 72000, 86400}

func next(seconds int) int {
	for _, step := range steps {
		if step > seconds {
			return step
		}
	}
	return MaxSeconds + 1 // 超出上限仍失败，不应推荐上限本身。
}

func successBucket(report types.CooldownReport) int {
	// 稀疏流量只证明再次请求时已恢复，不能作为计划档或更长档的成功证据。
	// 原报告和成功计数仍保留；这里只过滤推荐与稳定上报所用的证据。
	if report.ElapsedSeconds > report.WindowSeconds+GraceSeconds {
		return 0
	}
	for _, step := range steps {
		if report.WindowSeconds <= step+GraceSeconds {
			return step
		}
	}
	return 0 // 计划档超出推荐范围。
}

func AccountKey(reporterID int64, tag string) string {
	return fmt.Sprintf("%d:%s", reporterID, tag)
}

type accountBucket struct {
	stats     types.CooldownAccountStats
	successes []int
	reports   []types.CooldownReport
}

// Summarize 每次在一个快照上聚合。个人至少两次成功，池子推荐至少三个独立 consumer。
// 采用成功档位的上四分位数，并避开已明确失败的较短档；它不是对恢复概率的保证。
func Summarize(reports []types.CooldownReport, now time.Time) []types.CooldownStats {
	return SummarizeWithin(reports, now, MaxSeconds)
}

// Old clients negotiate no larger than their existing ten-hour protocol.
// Recompute evidence under their ceiling instead of clamping a larger recommendation.
func SummarizeWithin(reports []types.CooldownReport, now time.Time, ceiling int) []types.CooldownStats {
	ceiling = min(MaxSeconds, max(MinSeconds, ceiling))
	groups := map[string]map[string]*accountBucket{}
	for _, report := range reports {
		if report.EventAt().Before(now.Add(-Retention)) || report.EventAt().After(now.Add(time.Minute)) {
			continue
		}
		if groups[report.Gateway] == nil {
			groups[report.Gateway] = map[string]*accountBucket{}
		}
		key := AccountKey(report.ReporterID, report.AccountTag)
		b := groups[report.Gateway][key]
		if b == nil {
			b = &accountBucket{stats: types.CooldownAccountStats{ReporterID: report.ReporterID, AccountTag: report.AccountTag}}
			groups[report.Gateway][key] = b
		}
		b.stats.Samples++
		b.reports = append(b.reports, report)
		if report.Result == "full" {
			b.stats.Successes++
			if seconds := successBucket(report); seconds > 0 && seconds <= ceiling {
				b.successes = append(b.successes, seconds)
			}
		} else {
			b.stats.Failures++
			b.stats.FailureFloorSeconds = max(b.stats.FailureFloorSeconds, next(max(report.WindowSeconds, report.ElapsedSeconds)))
		}
	}
	var out []types.CooldownStats
	for gateway, accounts := range groups {
		it := types.CooldownStats{Gateway: gateway}
		var recommendations []int
		reporterRecommendations := map[int64]int{}
		evidence := 0
		for _, b := range accounts {
			it.Samples += b.stats.Samples
			it.Successes += b.stats.Successes
			it.Failures += b.stats.Failures
			if b.stats.FailureFloorSeconds > ceiling {
				b.stats.RecommendationReason = "failure_above_limit"
			} else if len(b.successes) < MinOwnSuccesses {
				b.stats.RecommendationReason = "insufficient_successes"
			}
			if len(b.successes) >= MinOwnSuccesses && b.stats.FailureFloorSeconds <= ceiling {
				sort.Ints(b.successes)
				seconds := max(b.successes[(3*len(b.successes)-1)/4], b.stats.FailureFloorSeconds)
				b.stats.Recommendation = types.CooldownRecommendation{Seconds: seconds, Samples: b.stats.Samples, Source: "account"}
				applyStableReporting(&b.stats.Recommendation, b.reports, now)
				recommendations = append(recommendations, seconds)
				// 每个 consumer 只占一票，不能靠伪造更多匿名账号增加权重。
				reporterRecommendations[b.stats.ReporterID] = max(reporterRecommendations[b.stats.ReporterID], seconds)
				evidence += b.stats.Samples
			}
			it.Accounts = append(it.Accounts, b.stats)
		}
		sort.Slice(it.Accounts, func(i, j int) bool {
			return AccountKey(it.Accounts[i].ReporterID, it.Accounts[i].AccountTag) <
				AccountKey(it.Accounts[j].ReporterID, it.Accounts[j].AccountTag)
		})
		sort.Ints(recommendations)
		it.AccountDifferences = len(recommendations) >= 2 && recommendations[0] != recommendations[len(recommendations)-1]
		if len(reporterRecommendations) >= MinPoolReporters {
			poolValues := make([]int, 0, len(reporterRecommendations))
			for _, seconds := range reporterRecommendations {
				poolValues = append(poolValues, seconds)
			}
			sort.Ints(poolValues)
			it.Recommendation = types.CooldownRecommendation{
				Seconds: poolValues[(3*len(poolValues)-1)/4], Samples: evidence, Source: "pool",
			}
		}
		out = append(out, it)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Gateway < out[j].Gateway })
	return out
}

// Cadence is independent from scheduling confidence. Only a personal, recent,
// consecutive success streak supports delaying reports; failures never get hidden
// behind an older "last three successes" streak. Use event time, not batch arrival.
func applyStableReporting(rec *types.CooldownRecommendation, reports []types.CooldownReport, now time.Time) {
	if len(reports) < StableMinSamples {
		return
	}
	sort.SliceStable(reports, func(i, j int) bool {
		if reports[i].EventAt().Equal(reports[j].EventAt()) {
			return reports[i].Result == "full" && reports[j].Result != "full"
		}
		return reports[i].EventAt().Before(reports[j].EventAt())
	})
	oldest, latest := reports[0].EventAt(), reports[len(reports)-1].EventAt()
	if latest.Sub(oldest) < StableSpan || now.Sub(latest) > ReportPolicyTTL {
		return
	}
	for _, report := range reports[len(reports)-StableSuccesses:] {
		if report.Result != "full" || successBucket(report) != rec.Seconds {
			return
		}
	}
	rec.ReportIntervalSeconds = int(ReportInterval.Seconds())
	rec.ReportPolicyExpiresAt = now.Add(ReportPolicyTTL)
	if expires := oldest.Add(Retention); expires.Before(rec.ReportPolicyExpiresAt) {
		rec.ReportPolicyExpiresAt = expires
	}
}

func ForAccount(stats types.CooldownStats, reporterID int64, tag string) types.CooldownRecommendation {
	for _, a := range stats.Accounts {
		if a.ReporterID == reporterID && a.AccountTag == tag {
			if a.Recommendation.Seconds > 0 {
				return a.Recommendation
			}
			if stats.Recommendation.Seconds < a.FailureFloorSeconds {
				return types.CooldownRecommendation{}
			}
		}
	}
	return stats.Recommendation
}
