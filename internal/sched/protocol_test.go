package sched

// 这一份盯的是 2026-10-02 那轮协议改动和同批修复：排除名单、min_remaining、
// 归还、窗口锚点、已烧槽位的闸。用例一律用 fake，绝不打真实上游（SPEC 红线 8）。

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

// noVerifyRig 搭一个「两个开关都按默认关着」的池子：生产走的就是这条路
// （verify_full_at_deliver 默认关），而归还和 valid_for_s 的算法都在这条路上。
func noVerifyRig(t *testing.T) (*Scheduler, *fakeStore, *fakeMinter, *clock) {
	t.Helper()
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	for _, gw := range []string{"unified-1", "unified-2", "unified-3"} {
		region := strings.ToUpper(strings.TrimPrefix(gw, "unified-"))
		st.gws[gw] = types.Gateway{Name: gw, FirstSeenBy: 1, ReachableExits: []types.Exit{{Region: region}}}
		mn.land[region] = gw
		mn.full[gw] = true
	}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(nil)})
	return s, st, mn, clk
}

// spreadTouches 把三个网关的触碰时间摊开，让「歇得最久」有确定答案：unified-2 最久。
func spreadTouches(st *fakeStore, clk *clock) {
	now := clk.now()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1", LastTouch: now.Add(-time.Hour)}
	st.slots[slotKey(1, "unified-2")] = types.Slot{AccountID: 1, Gateway: "unified-2", LastTouch: now.Add(-3 * time.Hour)}
	st.slots[slotKey(1, "unified-3")] = types.Slot{AccountID: 1, Gateway: "unified-3", LastTouch: now.Add(-2 * time.Hour)}
}

func putPair(st *fakeStore, gateway, cookie string, expires time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.pairs[gateway] = types.Pair{Gateway: gateway, Cookie: cookie, ExpiresAt: expires, MintedBy: 2}
}

// exclude 在「有活 pair」的硬筛**之前**生效：调用方那本账比池子的准
// （池子只认它自己发过的票），挑完再过滤等于白跑一遍筛选。
func TestExcludeSkipsGatewayBeforeSelection(t *testing.T) {
	s, st, mn, clk := noVerifyRig(t)
	seedPairs(st, clk)
	spreadTouches(st, clk)

	d, err := s.Deliver(context.Background(), 1, Request{Exclude: map[string]bool{"unified-2": true}})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if d.Gateway != "unified-3" {
		t.Fatalf("交付了 %s，排掉 unified-2 之后该轮到 unified-3", d.Gateway)
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("排除名单把请求推进了铸票路径，白打了 %d 发", n)
	}
}

// exclude_versions 排的是**那张票**，不是那个落点：同一个网关换一张新 pair 之后还能用。
func TestExcludeVersionsSkipsThatTicket(t *testing.T) {
	s, st, mn, clk := noVerifyRig(t)
	seedPairs(st, clk)
	spreadTouches(st, clk)
	p, _ := st.Pair("unified-2")
	stale := types.CookieVersion(p.Cookie)

	d, err := s.Deliver(context.Background(), 1, Request{ExcludeVersions: map[string]bool{stale: true}})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if d.Gateway != "unified-3" {
		t.Fatalf("交付了 %s，unified-2 那张票被排掉了", d.Gateway)
	}

	// 同一个网关换了票之后照样能用：排的是票，不是网关。
	putPair(st, "unified-2", cookiePrefix+"unified-2-v2", clk.now().Add(types.PairLifetime))
	clk.add(types.DeliverTTL + time.Second)
	d2, err := s.Deliver(context.Background(), 1, Request{ExcludeVersions: map[string]bool{stale: true}})
	if err != nil || d2.Gateway != "unified-2" {
		t.Fatalf("换了票之后还被排除: gateway=%s err=%v", d2.Gateway, err)
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("白打了 %d 发", n)
	}
}

// min_remaining 只压低**这一次**请求的交付门槛，而 valid_for_s 必须跟着取小：
// 不取小就会把一张只剩 40 秒的票标成 valid_for_s=150 发出去。
func TestMinRemainingLowersFloorAndCapsValidFor(t *testing.T) {
	// 两个断言各起一个池子：一次失败的交付会给槽位记触碰和退避，
	// 同一个池子里接着问第二遍问的就不是同一件事了。
	staleRig := func() (*Scheduler, *fakeStore, *fakeMinter, *clock) {
		s, st, mn, clk := noVerifyRig(t)
		delete(st.gws, "unified-2")
		delete(st.gws, "unified-3")
		mn.mintErr = errors.New("不许铸：这个用例要的就是「只剩一张残票」")
		putPair(st, "unified-1", cookiePrefix+"unified-1", clk.now().Add(40*time.Second))
		return s, st, mn, clk
	}

	// 默认门槛 300 s：这张残票不参与交付。
	strict, _, _, _ := staleRig()
	if _, err := strict.Deliver(context.Background(), 1, Request{}); err == nil {
		t.Fatal("剩 40 秒的票按默认门槛就发出去了")
	}

	// 调用方说它只要 30 秒：收下，但有效期只能报 40 秒。
	s, st, mn, clk := staleRig()
	d, err := s.Deliver(context.Background(), 1, Request{MinRemaining: 30 * time.Second})
	if err != nil {
		t.Fatalf("min_remaining=30s 还是发不出来: %v", err)
	}
	if d.ValidForS != 40 {
		t.Fatalf("valid_for_s=%d，应取 pair 剩余 40（窗口 150 更长，不能拿它报）", d.ValidForS)
	}
	// expires_at 和 valid_for_s 讲的是同一件事，必须给同一个数：
	// 两个字段各算一次就会出现「按哪个算都可能超出真实可用期」的陷阱。
	p, _ := st.Pair("unified-1")
	if !d.ExpiresAt.Equal(p.ExpiresAt) {
		t.Fatalf("expires_at=%v，应是那张 pair 的硬过期时刻 %v（窗口末尾更晚）",
			d.ExpiresAt, p.ExpiresAt)
	}
	if got := int(d.ExpiresAt.Sub(clk.now()) / time.Second); got != d.ValidForS {
		t.Fatalf("两个字段对不上: expires_at 还有 %d 秒，valid_for_s=%d", got, d.ValidForS)
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("有票可用却铸了 %d 发", n)
	}
	// 门槛是**这一次**的事：池子自己的健康读数不跟着变。
	if s.FreeGateways() != 0 {
		t.Fatal("min_remaining 改写了池子的空闲读数，那个数是给匿名门槛用的")
	}
}

// 全在冷却里要说清「最早什么时候回来」：冷却是**小时**量级，
// 让客户端按默认节奏重试等于白打几百发。
func TestAllCoolingCarriesWait(t *testing.T) {
	s, st, _, clk := noVerifyRig(t)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	seedPairs(st, clk)

	if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	clk.add(types.DeliverTTL + time.Second)
	_, err := s.Deliver(context.Background(), 1, Request{})
	var cool *CoolingError
	if !errors.As(err, &cool) {
		t.Fatalf("err=%v，应是 *CoolingError", err)
	}
	// 调用方按 ErrNoFullSlot 分流的那条路不能变（503 + 挂发现扫描）。
	if !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("CoolingError 不再是 ErrNoFullSlot，调用方的分流会全错: %v", err)
	}
	want := types.SlotCooldown - types.DeliverTTL - time.Second
	if cool.Wait != want {
		t.Fatalf("Wait=%v，应为 %v", cool.Wait, want)
	}
}

// B-1：失败过一次的槽位，退避到点之后仍然在它自己的满血窗口里 ⇒ 不许交付。
//
// 失败是**同一个事件写两个字段**：mintOnce 按保守方向记 LastTouch，调用方紧接着
// 记退避。而 backoffMax 只有 1 分钟、满血窗口 183 秒 —— 退避管不到窗口尾部那段，
// 那段里槽位正在烧，发出去就是降智的。
func TestBurnedSlotStaysBlockedAfterBackoffExpires(t *testing.T) {
	// 单账号模式：铸票者就是消费者本人，于是失败那一发的触碰和退避落在**同一个**
	// 槽位上——这正是旧豁免条件（BackoffUntil 非零就放行）打开的那个洞。
	// 池子模式也到得了这里（别的号全被限流时 mintPair 会回落到消费者自己）。
	s, st, mn, clk := rig(t, types.ModeSingle)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	mn.mintErr = errors.New("铸票超时（请求可能已经出站，窗口已经烧了）")

	if _, err := s.Deliver(context.Background(), 1, Request{}); err == nil {
		t.Fatal("铸票失败还交付了")
	}
	sl, _ := st.Slot(1, "unified-1")
	if sl.LastTouch.IsZero() || sl.BackoffUntil.IsZero() {
		t.Fatalf("失败那一发没按保守方向记触碰/退避: %+v", sl)
	}

	// 退避到点，但窗口还剩一大截：这正是旧代码 fail-open 的那段。
	clk.add(backoffMax + time.Second)
	mn.mintErr = nil
	putPair(st, "unified-1", cookiePrefix+"unified-1", clk.now().Add(types.PairLifetime))
	if _, err := s.Deliver(context.Background(), 1, Request{}); err == nil {
		t.Fatal("退避到点、窗口还在烧，槽位就被当成首次接触发出去了")
	}

	// 窗口过完就该恢复（冷却那条闸不管它：从没真的交付过票）。
	clk.add(types.FullWindow)
	if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
		t.Fatalf("窗口过完还发不出来: %v", err)
	}
}

// B-2：窗口内重复请求的有效期必须按**窗口起算时刻**算，不是按「交付返回的时刻」。
//
// 两者差一个铸票往返（仓库自己的读数是 20~90 s）。按后者算的话，客户端在
// start+140s 重连会被告知还剩 70 秒，而真实窗口只剩 10 秒 —— 而且池子把一张
// 窗口已关的 pair 原样发了出去。
func TestRedeliverUsesWindowAnchorNotReturnTime(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	seedPairs(st, clk)
	mn.mintCost = 60 * time.Second // 铸票那一发的真实往返

	start := clk.now()
	first := mustDeliver(t, s, 1, "")
	if !first.ExpiresAt.Equal(start.Add(types.DeliverTTL)) {
		t.Fatalf("首次交付的窗口没从首次接触起算: %v（start=%v）", first.ExpiresAt, start)
	}
	// 推到 start+140s：窗口只剩 10 秒。
	clk.add(start.Add(140 * time.Second).Sub(clk.now()))
	again := mustDeliver(t, s, 1, "")
	if again.ValidForS > 12 {
		t.Fatalf("valid_for_s=%d，真实窗口只剩 10 秒（70 就是把锚点往后挪了一个往返）", again.ValidForS)
	}
	if !again.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("重复请求把窗口往后推了：%v → %v", first.ExpiresAt, again.ExpiresAt)
	}
}

// 归还：票取了但一发没打，槽位要还原成**交付之前**那两个时刻，而不是清零。
func TestReleaseRestoresPreviousTimes(t *testing.T) {
	s, st, mn, clk := noVerifyRig(t)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	seedPairs(st, clk)
	// 这个槽位有真实历史：五小时前碰过、也交付过（都在冷却之外）。
	old := clk.now().Add(-5 * time.Hour)
	st.mu.Lock()
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1",
		LastTouch: old, LastDeliver: old}
	st.mu.Unlock()

	d, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	version := types.CookieVersion(d.Cookie)

	// 票对不上就不受理：他手上那张可能早被续掉了，还原等于凭空多给一个槽位。
	if _, err := s.Release(1, "deadbeefdead"); !errors.Is(err, ErrNoRelease) {
		t.Fatalf("版本对不上却受理了: %v", err)
	}
	n, err := s.Release(1, version)
	if err != nil || n != 1 {
		t.Fatalf("Release: n=%d err=%v", n, err)
	}
	sl, _ := st.Slot(1, "unified-1")
	if !sl.LastDeliver.Equal(old) || !sl.LastTouch.Equal(old) {
		t.Fatalf("没还原成交付前那两个时刻（清零会把真实历史抹成「从没碰过」）: %+v", sl)
	}
	// 已经还过了就不能再还一遍：否则一次交付能换回好几个槽位。
	if _, err := s.Release(1, version); !errors.Is(err, ErrNoRelease) {
		t.Fatalf("同一张票还了两次: %v", err)
	}
	// 还回来了就该能立刻再取，而且一发上游都不用打。
	if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
		t.Fatalf("槽位还回来了却取不到票: %v", err)
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("这一轮不该打上游，却打了 %d 发", n)
	}
}

// 开着交付前验证时一律不受理归还：那条路池子真用消费者的凭据打了两发，
// 槽位是**实烧**的，还了就是对池子说谎（冷却会从几小时缩成一个满血窗口）。
func TestReleaseRefusedWhenVerifyIsOn(t *testing.T) {
	// rig 把 verify_full_at_deliver 开着。
	s, st, _, clk := rig(t, types.ModeSingle)
	seedPairs(st, clk)
	d, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if !d.VerifiedFull {
		t.Fatal("这个用例的前提是走了验证那条路")
	}
	if _, err := s.Release(1, types.CookieVersion(d.Cookie)); !errors.Is(err, ErrNoRelease) {
		t.Fatalf("实烧的槽位被还了: %v", err)
	}
	// 账本一个字都不许动。
	sl, _ := st.Slot(1, d.Gateway)
	if sl.LastDeliver.IsZero() || sl.LastTouch.IsZero() {
		t.Fatalf("槽位被动过了: %+v", sl)
	}
}

// 过了受理窗口就不还：那之后他很可能已经用过了，而「我没用过」池子无从验证。
func TestReleaseRefusedAfterWindow(t *testing.T) {
	s, st, _, clk := noVerifyRig(t)
	seedPairs(st, clk)
	d, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	clk.add(types.DeliverTTL + time.Second)
	if _, err := s.Release(1, types.CookieVersion(d.Cookie)); !errors.Is(err, ErrNoRelease) {
		t.Fatalf("过了 %v 还受理归还: %v", types.DeliverTTL, err)
	}
}

// N-5：不验证的那条交付路不许把 degraded 洗成 unknown。
// 那是花一发 state-echo 换来的唯一读数，而这一发什么都没验。
func TestUnverifiedDeliveryKeepsDegradedVerdict(t *testing.T) {
	s, st, _, clk := noVerifyRig(t)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	seedPairs(st, clk)
	st.mu.Lock()
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1",
		LastVerdict: types.VerdictDegraded}
	st.mu.Unlock()

	if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if sl, _ := st.Slot(1, "unified-1"); sl.LastVerdict != types.VerdictDegraded {
		t.Fatalf("判定被洗成 %q 了", sl.LastVerdict)
	}
}

// A-11：换票（force）时的失败分类。
//
// 两种失败要分开，因为**只有后者等得起**：
//   - 排掉冷却和 holding 之后候选为空 ⇒ all_cooling（几小时，不可等待）
//   - 候选非空但一张活票都没有 ⇒ no_live_pair（可等待，等到票那次重试一发不打）
//
// 归错了的后果是 wait 在**每一次轮换上都失效**，而轮换恰好是最想等一下现铸的时刻。
func TestForceFailureClassification(t *testing.T) {
	t.Run("候选非空但没有活票", func(t *testing.T) {
		s, st, mn, _ := noVerifyRig(t)
		mn.mintErr = errors.New("现铸也没成")
		// 一张 pair 都没铺：三个网关都没碰过 ⇒ 候选非空、都不在冷却里。
		_, err := s.Deliver(context.Background(), 1, Request{Force: true})
		if !errors.Is(err, ErrNoLivePair) {
			t.Fatalf("err=%v，应是 ErrNoLivePair（等得起那一格）", err)
		}
		var cool *CoolingError
		if errors.As(err, &cool) {
			t.Fatalf("归成了「全在冷却里」，wait 在每次轮换上都会失效: %v", err)
		}
		if len(st.Gateways()) == 0 {
			t.Fatal("这个用例的前提是池子里有网关")
		}
	})

	t.Run("手上那张是唯一候选", func(t *testing.T) {
		s, st, _, clk := noVerifyRig(t)
		delete(st.gws, "unified-2")
		delete(st.gws, "unified-3")
		seedPairs(st, clk)
		if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
		// 立刻 force：唯一的网关正是他手上那张 ⇒ 换不出来，而且等几小时也没用。
		_, err := s.Deliver(context.Background(), 1, Request{Force: true})
		var cool *CoolingError
		if !errors.As(err, &cool) {
			t.Fatalf("err=%v，应是 *CoolingError", err)
		}
		if cool.Wait <= 0 {
			t.Fatalf("没说要等多久: %+v", cool)
		}
	})
}

// A-12：点名的网关同时又在排除名单里 ⇒ 按排除办（拒）。
//
// exclude 编码的是「我烧过这个」，照它办永远不会发出一张坏票；反过来让 gateway
// 压过 exclude 就可能把客户端自己说烧过的落点发回去。
func TestExcludeBeatsGatewayHint(t *testing.T) {
	s, st, mn, clk := noVerifyRig(t)
	seedPairs(st, clk)
	_, err := s.Deliver(context.Background(), 1, Request{
		Gateway: "unified-1", Exclude: map[string]bool{"unified-1": true}})
	if err == nil {
		t.Fatal("点名的网关在排除名单里，却照样发了出来")
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("自相矛盾的请求还打了 %d 发上游", n)
	}
}

// warmRig 搭一个「续期开着、只有快到点的票」的保温场景。
//
// 票剩 100 秒：低于交付门槛 ⇒ livePairs()=0，也低于续期线 ⇒ 每个网关都该被续一次。
func warmRig(t *testing.T, gateways ...string) (*Scheduler, *fakeStore, *fakeMinter, *clock) {
	t.Helper()
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	for _, gw := range gateways {
		region := strings.ToUpper(strings.TrimPrefix(gw, "unified-"))
		st.gws[gw] = types.Gateway{Name: gw, FirstSeenBy: 1, ReachableExits: []types.Exit{{Region: region}}}
		mn.land[region] = gw
		mn.full[gw] = true
		st.pairs[gw] = types.Pair{Gateway: gw, Cookie: cookiePrefix + gw,
			ExpiresAt: clk.now().Add(100 * time.Second), MintedBy: 2}
	}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.RenewPairs = true })})
	s.markDemand()
	return s, st, mn, clk
}

// N-3：保温的「深度」计数必须按实测重数，不能每续成一发就 +1。
//
// 上游接受了一张还有效的 pair 时不下发新 cookie（SPEC 第 2 节），那张票的 ExpiresAt
// 一个字没变 ⇒ 无条件自增会让计数器虚高、本轮提前收手，而深度其实没涨。
func TestRenewCountsLivePairsNotSuccesses(t *testing.T) {
	s, _, mn, _ := warmRig(t, "unified-1", "unified-2", "unified-3")
	mn.noNewPair = true // 带着旧 pair 打进去，上游接受、不换 cookie

	s.warmOnce(context.Background())
	// 目标深度 2、实测深度恒为 0 ⇒ 三个网关都该被试过一遍，而不是续成两发就收手。
	if n := len(mn.mintedGateways()); n != 3 {
		t.Fatalf("只续了 %d 发：计数器把「没换成新票」当成深度涨了", n)
	}
}

// N-3（后半）：保温的续期也要拿网关锁，否则它和交付路径的现铸能同时为一个网关
// 各打一发，而一张 pair 本来就能同时服务多个消费者 —— 第二发纯属白烧一个槽位。
func TestRenewTakesGatewayLock(t *testing.T) {
	s, _, mn, _ := warmRig(t, "unified-1")
	release := s.lockGateway("unified-1")

	done := make(chan struct{})
	go func() {
		s.warmOnce(context.Background())
		close(done)
	}()
	select {
	case <-done:
		t.Fatalf("锁着这个网关还续了 %d 发", len(mn.mintedGateways()))
	case <-time.After(100 * time.Millisecond):
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("锁着这个网关还续了 %d 发", n)
	}
	release()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("锁放了还卡着")
	}
	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("锁放了之后续了 %d 发，应为 1", n)
	}
}

// S-4：被自动停用的托管号的交付历史不许解开预铸的成本闸。
//
// 「有外部消费者在取票就不算白烧」这条豁免要按 types.AnonPrefix 认人。按
// 「在不在 usable() 里」认的话，一个 400/401/403 停用掉的号也会被当成外部消费者
// （mintOnce / scan 一发就能停用一个号）⇒ 只要有任一个号在近 4 小时内交付过之后
// 被停用，这条闸就对整池永久失效。
func TestDisabledAccountDoesNotReopenPremintGate(t *testing.T) {
	s, st, _, clk := noVerifyRig(t)
	for _, id := range []int64{1, 2} {
		for gw := range st.gws {
			st.mu.Lock()
			st.slots[slotKey(id, gw)] = types.Slot{AccountID: id, Gateway: gw, LastDeliver: clk.now()}
			st.mu.Unlock()
		}
	}
	if !s.premintWasted() {
		t.Fatal("全员在全部网关上都在冷却里，这一轮本该不预铸")
	}
	// 2 号被上游否决、自动停用：它的历史还在，但它不会再来取票。
	if err := st.DisableAccount(2, "铸票 HTTP 403"); err != nil {
		t.Fatal(err)
	}
	if !s.premintWasted() {
		t.Fatal("一个被停用的号就把成本闸解开了：那不是外部消费者")
	}
	// 真正的外部消费者（匿名占位行）照旧解开它。
	st.mu.Lock()
	st.accounts = append(st.accounts, types.Account{ID: 99, Enabled: true,
		Creds: types.Creds{AccountID: types.AnonPrefix + "outsider"}})
	st.slots[slotKey(99, "unified-1")] = types.Slot{AccountID: 99, Gateway: "unified-1",
		LastDeliver: clk.now()}
	st.mu.Unlock()
	if s.premintWasted() {
		t.Fatal("有外部消费者在取票却停了预铸")
	}
}

// S-1：对外开放的开关和门槛必须**现读配置**。
// 把门槛调紧是运营方最可能的紧急动作，而那是匿名这条路唯一的让路机制。
func TestPublicReadsLiveConfig(t *testing.T) {
	s, st, _, clk := noVerifyRig(t)
	seedPairs(st, clk)
	if ps := s.Public(); ps.Enabled || ps.Open {
		t.Fatalf("默认该是关的: %+v", ps)
	}
	c := s.opt.Live.Get()
	c.PublicWhenSurplus.Enabled = true
	c.PublicWhenSurplus.MinFreeGateways = 3
	s.opt.Live = config.NewLive("", c)
	if ps := s.Public(); !ps.Open || ps.Threshold != 3 {
		t.Fatalf("改完配置没立即生效: %+v", ps)
	}
	// 门槛调紧应当立刻关门。
	c.PublicWhenSurplus.MinFreeGateways = 99
	s.opt.Live = config.NewLive("", c)
	if ps := s.Public(); ps.Open || ps.Threshold != 99 {
		t.Fatalf("门槛调紧没生效: %+v", ps)
	}
}
