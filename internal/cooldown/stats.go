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
	MaxSeconds         = 10 * 60 * 60
	MinSeconds         = 60 * 60
	MaxElapsed         = 7 * 24 * 60 * 60
	MinOwnSuccesses    = 2
	MinPoolReporters   = 3
	GraceSeconds       = 60
)

var steps = []int{3600, 7200, 14400, 21600, 28800, 36000}

func next(seconds int) int {
	for _, step := range steps {
		if step > seconds {
			return step
		}
	}
	return MaxSeconds + 1 // 超出上限仍失败，不应推荐上限本身。
}

func successBucket(report types.CooldownReport) int {
	observed := report.ElapsedSeconds
	if report.ElapsedSeconds <= report.WindowSeconds+GraceSeconds {
		observed = report.WindowSeconds
	}
	for _, step := range steps {
		if observed <= step+GraceSeconds {
			return step
		}
	}
	return 0 // 等过 10 小时才恢复，不能证明 10 小时以内恢复。
}

func AccountKey(reporterID int64, tag string) string {
	return fmt.Sprintf("%d:%s", reporterID, tag)
}

type accountBucket struct {
	stats     types.CooldownAccountStats
	successes []int
}

// Summarize 每次在一个快照上聚合。个人至少两次成功，池子推荐至少三个独立 consumer。
// 采用成功档位的上四分位数，并避开已明确失败的较短档；它不是对恢复概率的保证。
func Summarize(reports []types.CooldownReport, now time.Time) []types.CooldownStats {
	groups := map[string]map[string]*accountBucket{}
	for _, report := range reports {
		if report.ReceivedAt.Before(now.Add(-Retention)) || report.ReceivedAt.After(now.Add(time.Minute)) {
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
		if report.Result == "full" {
			b.stats.Successes++
			if seconds := successBucket(report); seconds > 0 {
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
			if len(b.successes) >= MinOwnSuccesses && b.stats.FailureFloorSeconds <= MaxSeconds {
				sort.Ints(b.successes)
				seconds := max(b.successes[(3*len(b.successes)-1)/4], b.stats.FailureFloorSeconds)
				b.stats.Recommendation = types.CooldownRecommendation{Seconds: seconds, Samples: b.stats.Samples, Source: "account"}
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
