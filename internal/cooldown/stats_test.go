package cooldown

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestStableCooldownEvidenceAllowsSixHourReportingButNewFailureRevokesIt(t *testing.T) {
	now := time.Now().UTC()
	var reports []types.CooldownReport
	for i := range 6 {
		reports = append(reports, types.CooldownReport{ID: fmt.Sprint(i), ReporterID: 1, AccountTag: "a",
			Gateway: "g", WindowSeconds: 7200, ElapsedSeconds: 7200, Result: "full",
			ReceivedAt: now.Add(time.Duration(i-5) * 6 * time.Hour)})
	}
	interval := func(reports []types.CooldownReport) float64 {
		t.Helper()
		rec := ForAccount(Summarize(reports, now)[0], 1, "a")
		raw, _ := json.Marshal(rec)
		var fields map[string]any
		_ = json.Unmarshal(raw, &fields)
		value, _ := fields["report_interval_seconds"].(float64)
		return value
	}
	if got := interval(reports); got != 21600 {
		t.Fatalf("stable samples must permit six-hour reporting; got %v", got)
	}
	if got := interval(reports[1:]); got != 0 {
		t.Fatal("five samples cannot enable batching")
	}
	delayed := append([]types.CooldownReport(nil), reports...)
	for i := range delayed {
		delayed[i].ObservedAt, delayed[i].ReceivedAt = delayed[i].ReceivedAt, now
	}
	if got := interval(delayed); got != 21600 {
		t.Fatal("batch receipt collapsed the original observation span")
	}
	for i := range delayed {
		delayed[i].ObservedAt = now.Add(time.Duration(i-5) * time.Hour)
	}
	if got := interval(delayed); got != 0 {
		t.Fatal("a five-hour observation span cannot enable stable reporting")
	}
	reports = append(reports, types.CooldownReport{ID: "failure", ReporterID: 1, AccountTag: "a",
		Gateway: "g", WindowSeconds: 3600, ElapsedSeconds: 3600, Result: "degraded", ReceivedAt: now})
	if got := interval(reports); got != 0 {
		t.Fatal("a new failure must revoke stable reporting despite three older successes")
	}
}

func TestRecommendationsRequireEvidenceAndCompareAccounts(t *testing.T) {
	now := time.Now()
	var reports []types.CooldownReport
	for account, seconds := range []int{3600, 7200, 14400} {
		for range 2 {
			reports = append(reports, types.CooldownReport{ReporterID: int64(account), AccountTag: fmt.Sprint(account),
				Gateway: "unified-142", WindowSeconds: seconds, ElapsedSeconds: seconds,
				Result: "full", ReceivedAt: now})
		}
	}
	stats := Summarize(reports[:1], now)
	if stats[0].Recommendation.Seconds != 0 || ForAccount(stats[0], 0, "0").Seconds != 0 {
		t.Fatal("一个样本不能成为自动推荐")
	}
	stats = Summarize(reports[:4], now)
	if stats[0].Recommendation.Seconds != 0 || !stats[0].AccountDifferences {
		t.Fatal("两账号只能报告观测差异，不能给池子级推荐")
	}
	stats = Summarize(reports, now)
	if stats[0].Recommendation.Seconds != 14400 || stats[0].Recommendation.Samples != 6 {
		t.Fatalf("三个账号的参考值错误：%+v", stats[0].Recommendation)
	}
	if own := ForAccount(stats[0], 1, "1"); own.Seconds != 7200 || own.Source != "account" {
		t.Fatalf("个人证据应优先：%+v", own)
	}
}

func TestRecommendationCannotUndercutActualFailureOrTrustOneReporter(t *testing.T) {
	now := time.Now()
	var reports []types.CooldownReport
	for _, tag := range []string{"a", "b", "c"} {
		for range 2 {
			reports = append(reports, types.CooldownReport{ReporterID: 1, AccountTag: tag,
				Gateway: "g", WindowSeconds: 3600, ElapsedSeconds: 3600, Result: "full", ReceivedAt: now})
		}
	}
	if got := Summarize(reports, now)[0].Recommendation.Seconds; got != 0 {
		t.Fatalf("同一 consumer 伪造标签不能形成全池推荐：%d", got)
	}
	reports = append(reports, types.CooldownReport{ReporterID: 1, AccountTag: "a",
		Gateway: "g", WindowSeconds: 3600, ElapsedSeconds: 10800, Result: "degraded", ReceivedAt: now})
	if got := ForAccount(Summarize(reports, now)[0], 1, "a").Seconds; got != 14400 {
		t.Fatalf("实际静置3h仍失败，推荐应严格大于3h：%d", got)
	}
	reports[len(reports)-1].ElapsedSeconds = MaxSeconds
	stats := Summarize(reports, now)[0]
	stats.Recommendation = types.CooldownRecommendation{Seconds: 3600, Samples: 6, Source: "pool"}
	if got := ForAccount(stats, 1, "a").Seconds; got != 0 {
		t.Fatalf("最高档仍失败不能给更短推荐，也不能回退全池建议：%d", got)
	}
}

func TestCooldownExtendedStepsAndLegacyEvidenceCeiling(t *testing.T) {
	for _, test := range []struct{ from, want int }{
		{36000, 43200}, {43200, 57600}, {57600, 72000}, {72000, 86400}, {86400, 86401},
	} {
		if got := next(test.from); got != test.want {
			t.Fatalf("next(%d)=%d want %d", test.from, got, test.want)
		}
	}
	now := time.Now()
	var reports []types.CooldownReport
	for _, seconds := range []int{7200, 7200, 86400, 86400} {
		reports = append(reports, types.CooldownReport{Gateway: "g", AccountTag: "a",
			WindowSeconds: seconds, ElapsedSeconds: seconds, Result: "full", ReceivedAt: now})
	}
	if got := ForAccount(Summarize(reports, now)[0], 0, "a").Seconds; got != 86400 {
		t.Fatalf("new client did not receive 24h evidence: %d", got)
	}
	if got := ForAccount(SummarizeWithin(reports, now, LegacyMaxSeconds)[0], 0, "a").Seconds; got != 7200 {
		t.Fatalf("legacy evidence was clamped or thrown away: %d", got)
	}
	reports = append(reports, types.CooldownReport{Gateway: "g", AccountTag: "a",
		WindowSeconds: 86400, ElapsedSeconds: 86400, Result: "degraded", ReceivedAt: now})
	stats := Summarize(reports, now)[0]
	if stats.Accounts[0].RecommendationReason != "failure_above_limit" ||
		ForAccount(stats, 0, "a").Seconds != 0 {
		t.Fatal("at-cap failure must be visible, not labeled insufficient data")
	}
}

func TestRecommendationUsesActualIdleTimeAndFailureFloor(t *testing.T) {
	now := time.Now()
	reports := []types.CooldownReport{
		{AccountTag: "a", Gateway: "g", WindowSeconds: 3600, ElapsedSeconds: 9000, Result: "full", ReceivedAt: now},
		{AccountTag: "a", Gateway: "g", WindowSeconds: 3600, ElapsedSeconds: 9000, Result: "full", ReceivedAt: now},
		{AccountTag: "a", Gateway: "g", WindowSeconds: 14400, ElapsedSeconds: 14400, Result: "degraded", ReceivedAt: now},
	}
	stats := Summarize(reports, now)
	if got := ForAccount(stats[0], 0, "a"); got.Seconds != 21600 {
		t.Fatalf("不能将实际2.5h才成功记为1h，且需越过失败的4h档：%+v", got)
	}
	if len(Summarize(reports, now.Add(Retention+time.Hour))) != 0 {
		t.Fatal("过期样本仍参与推荐")
	}
}
