package cooldown

import (
	"fmt"
	"testing"
	"time"

	"gwpool/internal/types"
)

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
		t.Fatalf("10h仍失败不能给<=10h推荐，也不能回退全池建议：%d", got)
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
