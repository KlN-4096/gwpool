package sched

import (
	"context"
	"errors"
	"testing"
	"time"

	"gwpool/internal/types"
)

// 上游 429（free 额度耗尽、或打太猛）原来只给**槽位**记一次指数退避，10s 起、
// 封顶 60s（backoffBase / backoffMax），而且 429 不在 RejectedByUpstream 的
// {400,401,403} 里 ⇒ 账号既不停用也没有账号级退避。
//
// 后果：额度窗口是小时级的，池子却按「每槽位每分钟重试一次」的节奏对着一个
// 彻底打不动的号继续打，上限只被速率闸挡着（6 发/区/时 × 9 区 = 54 发/时）。
// 那 54 发不只是浪费，它们**吃掉的正是这个号本来能干正经铸票的配额**。
//
// 改成：上游 429 ⇒ 按住整个账号，默认一小时，优先听上游的 Retry-After。
func throttleFixture(t *testing.T) (*Scheduler, *fakeStore, *fakeMinter, *clock) {
	t.Helper()
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	st.accounts = []types.Account{
		{ID: 1, Label: "free-1", Enabled: true,
			Creds: types.Creds{AccessToken: "secret-1", AccountID: "acct-1"}},
		{ID: 2, Label: "free-2", Enabled: true,
			Creds: types.Creds{AccessToken: "secret-2", AccountID: "acct-2"}},
	}
	seedGateway(st, mn, "unified-1", 0, 1)
	landBare(mn, "unified-1")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(nil)})
	return s, st, mn, clk
}

func mintProbe(t *testing.T, s *Scheduler, a types.Account, sid string) error {
	t.Helper()
	_, err := s.mintOnce(context.Background(), a, types.Regions[0].Exit, sid, types.Pair{}, "", "unified-1")
	return err
}

func TestUpstream429HoldsWholeAccount(t *testing.T) {
	s, st, mn, clk := throttleFixture(t)
	acct := st.accounts[0]
	mn.mintStatus = 429
	mn.mintBody = `{"detail":"You've hit your usage limit."}`

	if err := mintProbe(t, s, acct, "sid1"); err == nil {
		t.Fatal("上游 429 该是失败")
	}
	if n := len(mn.mints); n != 1 {
		t.Fatalf("第一发要真打出去（我们是从它的 429 才知道该按住），实际 %d 发", n)
	}

	// 第二发：一发上游都不许再打。
	err := mintProbe(t, s, acct, "sid2")
	if !errors.Is(err, types.ErrRateLimited) {
		t.Fatalf("账号级退避要复用 ErrRateLimited（六个调用点对「铸不动且没出站」的处理已经全对），实际 %v", err)
	}
	if n := len(mn.mints); n != 1 {
		t.Fatalf("被按住的号不许再碰上游，实际打了 %d 发", n)
	}
	// 按住期间不许给槽位记退避、也不许多记触碰：那一发根本没出站。
	if !nothingWentOut(err) {
		t.Error("没出站的失败不该惩罚槽位（见 nothingWentOut 的调用方）")
	}

	// 默认按一小时；到点自动恢复，不需要人去点。
	clk.add(59 * time.Minute)
	if err := mintProbe(t, s, acct, "sid3"); !errors.Is(err, types.ErrRateLimited) {
		t.Fatalf("59 分钟还没到点，实际 %v", err)
	}
	if n := len(mn.mints); n != 1 {
		t.Fatalf("还在按住期内就不许打，实际 %d 发", n)
	}

	clk.add(2 * time.Minute)
	mn.mintStatus = 0 // 额度窗口过去了
	if err := mintProbe(t, s, acct, "sid4"); err != nil {
		t.Fatalf("到点后该照常铸，实际 %v", err)
	}
	if n := len(mn.mints); n != 2 {
		t.Fatalf("到点后该打出第二发，实际 %d 发", n)
	}
}

// 上游给了 Retry-After 就听它的：429 不听 Retry-After 正是被封的典型走法。
// 但要钳上限，否则上游报一个离谱的值就把这个号永久饿死（同 sub2api 的 MaxRetryAfter）。
func TestUpstream429HonorsRetryAfter(t *testing.T) {
	cases := []struct {
		name       string
		retryAfter time.Duration
		stillHeld  time.Duration // 这个时刻**仍然**该被按住
		freedAt    time.Duration // 这个时刻该放开
	}{
		{"听上游的短值", 5 * time.Minute, 4 * time.Minute, 6 * time.Minute},
		{"没给就用默认一小时", 0, 59 * time.Minute, 61 * time.Minute},
		{"离谱的值要钳住", 72 * time.Hour, acctHoldMax - time.Minute, acctHoldMax + time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, st, mn, clk := throttleFixture(t)
			acct := st.accounts[0]
			mn.mintStatus = 429
			mn.mintRetryAfter = tc.retryAfter

			if err := mintProbe(t, s, acct, "s0"); err == nil {
				t.Fatal("上游 429 该是失败")
			}
			clk.add(tc.stillHeld)
			if left := s.accountHeldFor(acct.ID); left <= 0 {
				t.Errorf("过了 %s 还该被按住，实际已放开", tc.stillHeld)
			}
			clk.add(tc.freedAt - tc.stillHeld)
			if left := s.accountHeldFor(acct.ID); left > 0 {
				t.Errorf("过了 %s 该放开了，实际还要等 %s", tc.freedAt, left)
			}
		})
	}
}

// 几个槽位并发撞 429 时，先记下的长退避不许被后来的短值改短。
func TestUpstream429HoldTakesTheLaterDeadline(t *testing.T) {
	s, st, _, _ := throttleFixture(t)
	acct := st.accounts[0]
	s.holdAccount(acct, time.Hour, "先来的长退避")
	s.holdAccount(acct, time.Minute, "后来的短退避")
	if left := s.accountHeldFor(acct.ID); left < 50*time.Minute {
		t.Errorf("该保留更晚那个到点，实际只剩 %s", left)
	}
}

// 被按住的号不该再被挑去铸票：挑了它只会在 mintOnce 的守卫上原地撞回来，
// 而要票的人已经等了一个往返。池子里还有别人时要挑别人。
func TestUpstream429SkipsHeldAccountWhenPickingMinter(t *testing.T) {
	s, st, mn, _ := throttleFixture(t)
	mn.mintStatus = 429
	if err := mintProbe(t, s, st.accounts[0], "s0"); err == nil {
		t.Fatal("上游 429 该是失败")
	}

	// exclude = 0（谁都不排除）⇒ 两个号都是候选，但 1 号被按住了。
	got, _, ok := s.minterFor(types.Exit{}, 0, 0)
	if !ok {
		t.Fatal("还有一个号没被按住，该挑得出人")
	}
	if got.ID != 2 {
		t.Errorf("不许再挑被上游按住的那个号，实际挑了 %d", got.ID)
	}

	// 两个都被按住 ⇒ 老实认挑不出来，别硬塞一个回去。
	if err := mintProbe(t, s, st.accounts[1], "s1"); err == nil {
		t.Fatal("上游 429 该是失败")
	}
	if _, _, ok := s.minterFor(types.Exit{}, 0, 0); ok {
		t.Error("全员被按住就该挑不出人")
	}
}
