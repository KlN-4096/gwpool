package sched

import (
	"context"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

// renewOn 把续票开关打开。
func renewOn(s *Scheduler) {
	c := s.opt.Live.Get()
	c.RenewPairs = true
	s.opt.Live = config.NewLive("", c)
}

// duePair 造一张「该续了」的票：路由 5 分钟后到点（低于 renewPairLead），
// 而 `__cflb` 还长 —— 它既是续票的目标，也是自己大区里的捐赠者。
//
// 两个死期**必须分开给**：实现只认同大区还活着的 `__cflb`，而不是这张票自己带的那张
// （第一版送自己那张，线上 12 发漂 10 发，见 pairsDueForRenew 的注释）。
// 两个都写成 5 分钟的话这些用例全都测不到东西。
func duePair(st *fakeStore, clk *clock, gw string, mintedBy int64, region string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.gws[gw] = types.Gateway{Name: gw, ReachableExits: []types.Exit{{Region: "BR"}}}
	st.pairs[gw] = types.Pair{Gateway: gw, Cookie: cookiePrefix + gw,
		ExpiresAt:     clk.now().Add(5 * time.Minute),
		CflbExpiresAt: clk.now().Add(50 * time.Minute),
		MintedBy:      mintedBy, RegionKey: region}
}

// 续票必须拿**铸出这张票的那个号**去打。
//
// 网关 =(大区 × 账号)，而 `__cflb` 只钉大区 ⇒ 换个号去打，落点是那个号在这个大区的网关，
// 不是手上这一张。2026-10-03 删掉的那版续期正是栽在这儿（它挑「本小时打得最少的号」，
// 实测 51 发漂 44 发）。这条钉住「别再挑别人」。
func TestRenewUsesThePairsOwnMinter(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	landBare(mn, "unified-9")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})
	renewOn(s)
	duePair(st, clk, "unified-9", 2, "south-america") // 2 号铸出来的

	s.renewExpiring(context.Background())

	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("该恰好续一发，实到 %d 发", n)
	}
	if got := mn.lastAccount(); got != 2 {
		t.Fatalf("续票用了 %d 号，必须用铸出这张票的 2 号", got)
	}
}

// 来源号停用/删了就续不了：换个号去打落的是别的网关，等于白烧一发还覆盖掉这一张。
func TestRenewSkipsPairsWhoseMinterIsGone(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	landBare(mn, "unified-9")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})
	renewOn(s)
	duePair(st, clk, "unified-9", 77, "south-america") // 77 号不存在

	s.renewExpiring(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("来源号没了还续了 %d 发", n)
	}
}

// 送出去的 `__cflb` 必须是**同大区还活着的**那张，不是这张票自己带的那张。
//
// 线上现象（12 发漂 10 发）的根因：ExpiresAt = min(route, cflb)，而 106 张票里 route
// 和 cflb 的死期逐秒相等 ⇒「快到点」就等于「cflb 快死了」，送一张死 cflb 钉不住大区、
// 落点退回随机。借不到活的捐赠者就**一发都不打**，烧的是真实上游额度。
func TestRenewNeedsALivingCflbDonor(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	landBare(mn, "unified-9")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})
	renewOn(s)
	duePair(st, clk, "unified-9", 2, "south-america")
	// 把 cflb 的死期压到和路由同一刻（线上那 106 张的样子）⇒ 整个大区没有活捐赠者。
	st.mu.Lock()
	p := st.pairs["unified-9"]
	p.CflbExpiresAt = p.ExpiresAt
	st.pairs["unified-9"] = p
	st.mu.Unlock()

	s.renewExpiring(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("没有活捐赠者还打了 %d 发（必漂，纯烧额度）", n)
	}

	// 同大区来一张 cflb 还长的票：捐赠者有了，这一发该打出去。
	duePair(st, clk, "unified-8", 1, "south-america")
	s.renewExpiring(context.Background())
	if n := len(mn.mintedGateways()); n == 0 {
		t.Fatal("有活捐赠者却一发都没打")
	}
}

// 大区为空的老记录续不了：借不到捐赠者（b6c1987 之前发现铸票留下的那批）。
func TestRenewSkipsPairsWithNoRegion(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	landBare(mn, "unified-9")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})
	renewOn(s)
	duePair(st, clk, "unified-9", 2, "")

	s.renewExpiring(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("没大区的老记录续了 %d 发", n)
	}
}

// 排序：难摸到的先续。一个网关只有少数几个号能落上去，票一过期就等于丢了。
func TestRenewOrdersRareGatewaysFirst(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})
	renewOn(s)

	duePair(st, clk, "unified-rare", 1, "south-america")
	duePair(st, clk, "unified-common", 1, "us-east")
	// common 三个号落到过，rare 只有一个。
	if err := st.NoteRegionMint(1, "south-america", "unified-rare", clk.now()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2, 3} {
		if err := st.NoteRegionMint(id, "us-east", "unified-common", clk.now()); err != nil {
			t.Fatal(err)
		}
	}

	due, _ := s.pairsDueForRenew()
	if len(due) != 2 {
		t.Fatalf("该有两张等着续，实到 %d", len(due))
	}
	if due[0].gateway != "unified-rare" {
		t.Fatalf("难摸到的该排第一，实到 %s（reach=%d）", due[0].gateway, due[0].reach)
	}
	if due[0].reach >= due[1].reach {
		t.Fatalf("reach 没算对: rare=%d common=%d", due[0].reach, due[1].reach)
	}
}

// 开关默认关：它会在没人要票的时候自己花上游额度。
func TestRenewIsOffByDefault(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	landBare(mn, "unified-9")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})
	duePair(st, clk, "unified-9", 1, "south-america")

	s.renewExpiring(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("开关默认关，却打了 %d 发", n)
	}
}

// 寿命还够的不续：提前续等于把还能用的寿命烧掉，还白花一发上游。
func TestRenewLeavesHealthyPairsAlone(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	landBare(mn, "unified-9")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})
	renewOn(s)
	duePair(st, clk, "unified-9", 1, "south-america")
	st.mu.Lock()
	p := st.pairs["unified-9"]
	p.ExpiresAt = clk.now().Add(renewPairLead + time.Minute)
	st.pairs["unified-9"] = p
	st.mu.Unlock()

	s.renewExpiring(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("寿命还够却续了 %d 发", n)
	}
}

// 没配种子出口就一发都不续：续票必须只送 `__cflb`，而摘 `__oailb` 只发生在种子那条路上。
func TestRenewRequiresASeedExit(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	mn.seedSIDErr = types.ErrNoExit
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	landBare(mn, "unified-9")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})
	renewOn(s)
	duePair(st, clk, "unified-9", 2, "south-america")

	s.renewExpiring(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("没有种子出口却打了 %d 发（送两件上游一个 cookie 都不回）", n)
	}
}
