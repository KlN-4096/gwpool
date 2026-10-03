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

	st.gws["unified-9"] = types.Gateway{Name: "unified-9",
		ReachableExits: []types.Exit{{Region: "BR"}}}
	// 2 号铸出来的票，还剩 5 分钟（低于 renewPairLead）。
	st.pairs["unified-9"] = types.Pair{Gateway: "unified-9", Cookie: "__cflb=lb-9",
		MintedBy: 2, ExpiresAt: clk.now().Add(5 * time.Minute)}

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

	st.gws["unified-9"] = types.Gateway{Name: "unified-9",
		ReachableExits: []types.Exit{{Region: "BR"}}}
	st.pairs["unified-9"] = types.Pair{Gateway: "unified-9", Cookie: "__cflb=lb-9",
		MintedBy: 77, ExpiresAt: clk.now().Add(5 * time.Minute)} // 77 号不存在

	s.renewExpiring(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("来源号没了还续了 %d 发", n)
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

	for _, n := range []string{"unified-rare", "unified-common"} {
		st.gws[n] = types.Gateway{Name: n, ReachableExits: []types.Exit{{Region: "BR"}}}
		st.pairs[n] = types.Pair{Gateway: n, Cookie: "__cflb=lb", MintedBy: 1,
			ExpiresAt: clk.now().Add(5 * time.Minute)}
	}
	// common 三个号落到过，rare 只有一个。
	if err := st.NoteRegionMint(1, "south-america", "unified-rare", clk.now()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2, 3} {
		if err := st.NoteRegionMint(id, "us-east", "unified-common", clk.now()); err != nil {
			t.Fatal(err)
		}
	}

	due := s.pairsDueForRenew()
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

	st.gws["unified-9"] = types.Gateway{Name: "unified-9",
		ReachableExits: []types.Exit{{Region: "BR"}}}
	st.pairs["unified-9"] = types.Pair{Gateway: "unified-9", Cookie: "__cflb=lb-9",
		MintedBy: 1, ExpiresAt: clk.now().Add(5 * time.Minute)}

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

	st.gws["unified-9"] = types.Gateway{Name: "unified-9",
		ReachableExits: []types.Exit{{Region: "BR"}}}
	st.pairs["unified-9"] = types.Pair{Gateway: "unified-9", Cookie: "__cflb=lb-9",
		MintedBy: 1, ExpiresAt: clk.now().Add(renewPairLead + time.Minute)}

	s.renewExpiring(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("寿命还够却续了 %d 发", n)
	}
}
