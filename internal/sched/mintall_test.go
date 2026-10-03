package sched

import (
	"context"
	"errors"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

func TestMintAllGlobalExpansionDoesNotExpandDefaultScope(t *testing.T) {
	q, _, _, _ := mintAllFixture()
	cfg, err := config.GlobalCountryRegions(config.Defaults())
	if err != nil {
		t.Fatal(err)
	}
	q.pool.opt.Live = config.NewLive("", cfg)
	stat := q.Enqueue()
	if stat.Total != 18 {
		t.Fatalf("default action expanded from 9 exits to all countries: %d combinations", stat.Total)
	}
}

func TestMintAllSkipsAccountTotalAtEnqueue(t *testing.T) {
	q, _, mn, _ := mintAllFixture()
	mn.rateLimit = 4
	mn.ratePerAcct = map[string]map[string]int{"acct-1": {"other-history": 36}}
	stat := q.Enqueue()
	if stat.Queued != 9 {
		t.Fatalf("full account total still entered queue: %d", stat.Queued)
	}
}

func TestMintAllPreviewCapsProjectedTotalAndExpiresPending(t *testing.T) {
	q, _, mn, clk := mintAllFixture()
	cfg, err := config.GlobalCountryRegions(config.Defaults())
	if err != nil {
		t.Fatal(err)
	}
	q.pool.opt.Live = config.NewLive("", cfg)
	mn.rateLimit = 4
	var keys []string
	for _, region := range cfg.Regions {
		keys = append(keys, region.Key)
	}
	preview := q.Preview(keys...)
	if preview.Total != 500 || preview.Queued != 72 || q.Status().Active || len(mn.mints) != 0 {
		t.Fatalf("preview must cap each account at 36 with zero side effects: %+v", preview)
	}
	q.Enqueue(keys...)
	clk.add(mintAllBatchLifetime)
	_, ready, wait := q.next()
	if ready || wait != 0 || q.Status().Active || q.Status().Queued != 0 || len(mn.mints) != 0 {
		t.Fatal("expired batch must not trickle into later budgets")
	}
}

func TestMintAllConfirmDoesNotExpandAfterBudgetExpiry(t *testing.T) {
	q, _, mn, _ := mintAllFixture()
	mn.rateLimit = 4
	mn.ratePerAcct = map[string]map[string]int{"acct-1": {"other": 36}}
	preview := q.Preview()
	if preview.Queued != 9 {
		t.Fatal("fixture must initially have only one eligible account")
	}
	delete(mn.ratePerAcct, "acct-1") // old requests expired before confirmation
	next, accepted := q.Confirm(preview.Total, preview.Queued)
	if accepted || next.Queued != 18 || q.Status().Active {
		t.Fatal("increased capacity must require a new confirmation")
	}
	stat, accepted := q.Confirm(next.Total, next.Queued)
	if !accepted || stat.Queued != 18 {
		t.Fatal("confirmed plan was not committed")
	}
}

func mintAllFixture() (*MintAllQueue, *fakeStore, *fakeMinter, *clock) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	landEachRegionSomewhereNew(mn)
	s := New(st, mn, Options{Now: clk.now, MintGap: -1, RenewGap: -1})
	return NewMintAllQueue(s), st, mn, clk
}

func finishMintAll(t *testing.T, q *MintAllQueue) MintAllStatus {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); q.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("全量打票 worker 未停止")
		}
	}()
	deadline := time.After(2 * time.Second)
	for {
		status := q.Status()
		if !status.Active {
			return status
		}
		select {
		case <-deadline:
			t.Fatal("离线全量队列没有完成")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestMintAllEnqueuesEachEligibleCombinationOnce(t *testing.T) {
	q, st, mn, _ := mintAllFixture()
	addAccount(st, 3, "acct-3")
	st.accounts[2].Enabled = false
	first := q.Enqueue()
	if first.Total != 18 || first.Queued != 18 || !first.Active {
		t.Fatalf("应排两个账号各九个区域：%+v", first)
	}
	second := q.Enqueue()
	if second.Total != first.Total || second.Queued != first.Queued {
		t.Fatal("重复点击叠加了整批任务")
	}
	// 入队不执行，HTTP 线程不应碰上游。
	if len(mn.mintedGateways()) != 0 {
		t.Fatal("入队时执行了铸票")
	}
	final := finishMintAll(t, q)
	if final.Succeeded != 18 || final.Failed != 0 || final.Skipped != 0 ||
		final.Running != 0 || final.Queued != 0 || final.Finished.IsZero() {
		t.Fatalf("完成统计错误：%+v", final)
	}
	seen := map[string]bool{}
	for _, m := range mn.mints {
		key := comboKey(m.AccountID, m.RegionKey)
		if seen[key] {
			t.Fatalf("同一个组合打了两次：%s", key)
		}
		seen[key] = true
	}
	if len(seen) != 18 {
		t.Fatalf("没有遍历所有组合：%d", len(seen))
	}
	if mn.peakInflight() != 1 {
		t.Fatalf("手动队列并发不应增加：%d", mn.peakInflight())
	}
}

func TestMintAllSkipsFullQuotasAndRechecksQueuedAccounts(t *testing.T) {
	q, st, mn, _ := mintAllFixture()
	mn.rateLimit = 4
	mn.ratePerAcct = map[string]map[string]int{"acct-1": {"us-east": 4}}
	first := q.Enqueue()
	if first.Skipped != 1 || first.Queued != 17 || first.SkipReasons["区域配额已满"] != 1 {
		t.Fatalf("入队没有跳过已满配额：%+v", first)
	}
	if len(first.Recent) != 1 || first.Recent[0].AccountID != 1 || first.Recent[0].Region != "us-east" {
		t.Fatalf("入队跳过的账号/区域不可见：%+v", first.Recent)
	}
	// 入队后停掉一个号；另一个号在出队前又用满一格。
	st.accounts[0].Enabled = false
	mn.ratePerAcct["acct-2"] = map[string]int{"east-asia": 4}
	final := finishMintAll(t, q)
	if final.Succeeded != 8 || final.Skipped != 10 || final.Failed != 0 {
		t.Fatalf("没有按执行时状态重新检查：%+v", final)
	}
	for _, m := range mn.mints {
		if m.AccountID != 2 || m.RegionKey == "JP" {
			t.Fatal("停用/配额满的组合仍发送了请求")
		}
	}
}

func TestMintAllClassifiesMissingExitAtExecution(t *testing.T) {
	q, _, mn, _ := mintAllFixture()
	mn.mintErr = types.ErrNoExit
	q.Enqueue()
	final := finishMintAll(t, q)
	if final.Skipped != 18 || final.Failed != 0 || final.SkipReasons["无可用出口"] != 18 {
		t.Fatalf("执行期无出口应明确跳过：%+v", final)
	}
}

func TestMintAllDoesNotSleepForSkippedJobs(t *testing.T) {
	q, st, _, _ := mintAllFixture()
	q.pool.opt.MintGap = time.Hour
	q.Enqueue()
	st.accounts[0].Enabled, st.accounts[1].Enabled = false, false
	final := finishMintAll(t, q)
	if final.Skipped != 18 {
		t.Fatalf("未发请求的跳过任务不应逐项等待 MintGap：%+v", final)
	}
}

func TestMintAllSharesBackgroundBudget(t *testing.T) {
	q, _, mn, _ := mintAllFixture()
	q.Enqueue()
	job, ready, _ := q.next()
	if !ready {
		t.Fatal("没有可执行任务")
	}
	q.pool.backgroundMu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		q.execute(context.Background(), job)
	}()
	select {
	case <-done:
		q.pool.backgroundMu.Unlock()
		t.Fatal("自动后台持锁时手动队列仍然发出了请求")
	case <-time.After(20 * time.Millisecond):
	}
	q.pool.backgroundMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("后台释放预算后队列没有恢复")
	}
	if len(mn.mintedGateways()) != 1 {
		t.Fatal("后台释放预算后未执行唯一任务")
	}
}

func TestWarmOnceSharesBackgroundBudget(t *testing.T) {
	q, _, _, _ := mintAllFixture()
	q.pool.backgroundMu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		q.pool.warmOnce(context.Background())
	}()
	select {
	case <-done:
		q.pool.backgroundMu.Unlock()
		t.Fatal("自动后台绕过了全量队列持有的预算锁")
	case <-time.After(20 * time.Millisecond):
	}
	q.pool.backgroundMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("释放后台预算后自动巡检未结束")
	}
}

func TestMintAllKeepsBoundedProblemDetailsAfterSuccesses(t *testing.T) {
	q, _, mn, _ := mintAllFixture()
	mn.rateLimit = 1
	mn.ratePerAcct = map[string]map[string]int{"acct-1": {"us-east": 1}}
	q.Enqueue()
	for range mintAllRecent + 1 {
		q.recordResult(MintAllResult{Status: "succeeded"})
	}
	status := q.Status()
	if len(status.Problems) != 1 || status.Problems[0].AccountID != 1 || status.Problems[0].Region != "us-east" {
		t.Fatal("后续成功挤掉了入队时的异常组合")
	}
	status.Problems[0].Reason = "mutated"
	if q.Status().Problems[0].Reason == "mutated" {
		t.Fatal("异常详情不是独立副本")
	}
	for range mintAllProblems + 1 {
		q.recordResult(MintAllResult{Status: "skipped", Reason: "测试"})
	}
	if len(q.Status().Problems) != mintAllProblems {
		t.Fatal("异常详情超过了有界上限")
	}
}

func TestMintAllUsesDynamicExitsEvenWhenSeedsExist(t *testing.T) {
	q, st, mn, clk := mintAllFixture()
	duePair(st, clk, "unified-seed", 1, "east-asia")
	q.Enqueue()
	final := finishMintAll(t, q)
	if final.Succeeded != 18 {
		t.Fatalf("全量铸票未完成：%+v", final)
	}
	seeded, bare := 0, 0
	for _, m := range mn.mints {
		if m.Err == "" {
			bare++
		} else {
			seeded++
		}
	}
	if bare != 18 || seeded != 0 || len(mn.seedShots()) != 0 {
		t.Fatalf("全量新铸混入了静态续票出口：seeded=%d bare=%d", seeded, bare)
	}
}

func TestMintAllWaitsForShortAccountGapWithoutDroppingJobs(t *testing.T) {
	q, _, _, clk := mintAllFixture()
	q.pool.opt.RenewGap = 30 * time.Second
	q.pool.lastMintedAt[1] = clk.now()
	q.Enqueue()
	job, ready, _ := q.next()
	if !ready || job.accountID != 2 {
		t.Fatal("应先挑没有间隔限制的另一个号")
	}
	result, retry := q.execute(context.Background(), job)
	q.note(job, result, retry)
	if retry {
		t.Fatal("未受限的号不该延期")
	}
	_, ready, delay := q.next()
	if ready || delay != 30*time.Second || q.Status().Queued != 17 {
		t.Fatalf("账号间隔应等待而不是丢任务：ready=%v delay=%s", ready, delay)
	}
	clk.add(delay)
	job, ready, _ = q.next()
	if !ready || job.accountID != 1 {
		t.Fatal("间隔结束后原任务没有恢复")
	}
}

func TestMintAllReportsFailuresWithoutRawUpstreamErrors(t *testing.T) {
	q, _, mn, _ := mintAllFixture()
	mn.mintErr = errors.New("private-upstream-details")
	q.Enqueue()
	final := finishMintAll(t, q)
	if final.Failed != 18 || final.Succeeded != 0 {
		t.Fatalf("把失败算成了成功：%+v", final)
	}
	for _, row := range final.Recent {
		if row.Reason != "铸票失败（查看服务日志）" {
			t.Fatalf("结果里不应带原始上游错误：%s", row.Reason)
		}
	}
	final.Recent[0].Reason = "mutated"
	if q.Status().Recent[0].Reason == "mutated" {
		t.Fatal("状态接口泄漏了可修改的内部切片")
	}
}

func TestMintAllCancellationDoesNotSendPendingRequests(t *testing.T) {
	q, _, mn, _ := mintAllFixture()
	q.Enqueue()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q.Run(ctx)
	status := q.Status()
	if status.Active || status.Queued != 0 || status.Skipped != 18 ||
		len(mn.mintedGateways()) != 0 {
		t.Fatalf("取消后任务没有结束或仍发送请求：%+v", status)
	}
}
