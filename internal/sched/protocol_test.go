package sched

// 这一份盯的是 2026-10-02 那轮协议改动和同批修复：排除名单、min_remaining、
// 归还、窗口锚点、已烧槽位的闸。用例一律用 fake，绝不打真实上游（SPEC 红线 8）。

import (
	"context"
	"errors"
	"sync"
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
	for i, gw := range []string{"unified-1", "unified-2", "unified-3"} {
		seedGateway(st, mn, gw, i, 1)
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

// putPair 铺一张票。CflbExpiresAt 跟 ExpiresAt 一致——生产里 ExpiresAt 就是
// 两个组件取小的那个，而取小的几乎总是 __cflb（见 types.Pair）。
func putPair(st *fakeStore, gateway, cookie string, expires time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.pairs[gateway] = types.Pair{Gateway: gateway, Cookie: cookie,
		ExpiresAt: expires, CflbExpiresAt: expires, MintedBy: 2}
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

// DeliveredTo 是 /pair/renew 的归属闸，而那是那条路上**唯一挡得住人的**判据
// （交回来的 cookie 本体验不了签，见 mint.parseOailb）。
//
// 两件事必须同时成立，缺一条那道闸就形同虚设：
//   - 认**行**：别的号拿着同一个票号来，一律不认
//   - 窗口用 PairLifetime 而**不是** DeliverTTL（150 s）：续期发生在业务响应之后，
//     一条长流轻松超过 150 秒，按那个窗口算会把正常续期全拒掉
func TestDeliveredToAnswersOwnershipNotMerePossession(t *testing.T) {
	s, st, _, clk := noVerifyRig(t)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	seedPairs(st, clk)
	addAccount(st, 2, "acct-2")
	d, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	version := types.CookieVersion(d.Cookie)

	if !s.DeliveredTo(1, version) {
		t.Fatal("刚交付给 1 的票却不认")
	}
	// 另一个号：票号一样（谁都能自己取一张同网关的票看到它），但没交付给它。
	if s.DeliveredTo(2, version) {
		t.Fatal("别人的票也认——这道闸形同虚设，任何一把合法 key 都能换掉别人现役的 pair")
	}
	// 票号对不上不认：他手上那张可能早被换掉了。
	if s.DeliveredTo(1, "deadbeefdead") {
		t.Fatal("票号对不上也认")
	}
	// 窗口两侧。DeliverTTL 之后**仍然要认**（这正是不能照抄 /release 窗口的地方），
	// 而同一时刻 /release 已经拒了。
	clk.add(types.DeliverTTL + time.Minute)
	if !s.DeliveredTo(1, version) {
		t.Fatalf("交付 %v 之后就不认了：续期发生在业务响应之后，长流轻松超过这个数",
			types.DeliverTTL+time.Minute)
	}
	if _, err := s.Release(1, version); !errors.Is(err, ErrNoRelease) {
		t.Fatalf("同一时刻 /release 该已经拒了（两个窗口刻意不同）: %v", err)
	}
	// 过了一张票自己的寿命就不认了：那张票本来也活不到这时候。
	clk.add(types.PairLifetime)
	if s.DeliveredTo(1, version) {
		t.Fatalf("过了 %v 还认", types.PairLifetime)
	}
}

// 归属的判据必须是**交付出去那一张**，不是「池子现持那张」。
//
// 2026-10-02 复审抓到的 blocker：现持那张的号谁都查得到（自己取一张同网关的票就
// 知道），拿它当判据 ⇒ 一次交付能换到那张 pair 在整个窗口里被无限次改写（实测 5/5）。
// 这个用例把两者**错开**，所以两种实现的答案正好相反。
func TestDeliveredToFollowsTheTicketHandedOutNotThePoolsCurrent(t *testing.T) {
	s, st, _, clk := noVerifyRig(t)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	seedPairs(st, clk)
	d, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	handedOut := types.CookieVersion(d.Cookie)

	// 池子换了一张（续期、重铸都会走到这儿），交付记录不动。
	cur, _ := st.Pair(d.Gateway)
	cur.Cookie = cookiePrefix + d.Gateway + "-renewed"
	if err := st.PutPair(cur); err != nil {
		t.Fatal(err)
	}
	nowHeld := types.CookieVersion(cur.Cookie)
	if nowHeld == handedOut {
		t.Fatal("夹具没把两个号错开")
	}

	// 这一格是整条攻击链的闸，所以先查它：现持那张的号**任何人都查得到**。
	if s.DeliveredTo(1, nowHeld) {
		t.Fatal("认了「池子现持那张」的号 ⇒ 一次交付能换到无限次改写")
	}
	if !s.DeliveredTo(1, handedOut) {
		t.Fatal("不认交付出去那一张——那消费端换了票之后就续不了了")
	}
}

// 窗口内重复请求（redeliver）**不动**交付记录，票号也不许跟着变。
//
// 它刻意不记一次新交付（锚点要留在首次接触那一刻），所以票号也该原样留着。
// 跟着变的话每一次重连都等于发一张新的续期许可，一次交付的改写次数又回到无限。
func TestRedeliverDoesNotReissueTheRenewPermit(t *testing.T) {
	s, st, _, clk := noVerifyRig(t)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	seedPairs(st, clk)
	d, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	before, _ := st.Slot(1, d.Gateway)
	// 池子换了一张，然后窗口内重连：答回来的是**新**那张（路由一样），
	// 但交付记录里的票号得还是老那张。
	cur, _ := st.Pair(d.Gateway)
	cur.Cookie = cookiePrefix + d.Gateway + "-renewed"
	if err := st.PutPair(cur); err != nil {
		t.Fatal(err)
	}
	again, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("窗口内重连取不到票: %v", err)
	}
	if again.Cookie != cur.Cookie {
		t.Fatalf("重连答回的不是池子现持那张: %q", again.Cookie)
	}
	after, _ := st.Slot(1, d.Gateway)
	if after.LastDeliverVersion != before.LastDeliverVersion {
		t.Fatalf("重连把许可换了一张: %q → %q", before.LastDeliverVersion, after.LastDeliverVersion)
	}
	if !after.LastDeliver.Equal(before.LastDeliver) {
		t.Fatalf("重连挪了锚点: %v → %v", before.LastDeliver, after.LastDeliver)
	}
}

// 归还要把票号**一起**还原。
//
// 留着这次交付的号，还原完它就指向一张已经不属于任何人的票，而 /pair/renew 的
// 归属闸正按它算 ⇒ 一个已经还掉的槽位还留着一张续期许可。
func TestReleaseRestoresTheRenewPermitToo(t *testing.T) {
	s, st, _, clk := noVerifyRig(t)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	seedPairs(st, clk)
	// 这个槽位有真实历史：五小时前交付过另一张票。
	old := clk.now().Add(-5 * time.Hour)
	st.mu.Lock()
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1",
		LastTouch: old, LastDeliver: old, LastDeliverVersion: "aaaaaaaaaaaa"}
	st.mu.Unlock()

	d, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	version := types.CookieVersion(d.Cookie)
	if !s.DeliveredTo(1, version) {
		t.Fatal("刚交付出去的票不认")
	}
	if _, err := s.Release(1, version); err != nil {
		t.Fatalf("Release: %v", err)
	}
	// 还掉的槽位不该再留着这张票的续期许可。
	if s.DeliveredTo(1, version) {
		t.Fatal("槽位还掉了，续期许可还留着")
	}
	sl, _ := st.Slot(1, "unified-1")
	if sl.LastDeliverVersion != "aaaaaaaaaaaa" {
		t.Fatalf("票号没还原成交付之前那张: %q", sl.LastDeliverVersion)
	}
	if sl.PrevDeliverVersion != "" {
		t.Fatalf("还原点没清掉（第二次 release 会把槽位还到更早）: %q", sl.PrevDeliverVersion)
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
	for i, gw := range gateways {
		seedGateway(st, mn, gw, i, 1)
		mn.full[gw] = true
		exp := clk.now().Add(100 * time.Second)
		st.pairs[gw] = types.Pair{Gateway: gw, Cookie: cookiePrefix + gw,
			ExpiresAt: exp, CflbExpiresAt: exp, MintedBy: 2}
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
	// 门槛数的是**铸过票**的账号（provenAccounts，见 types.AccountsForContinuous），
	// 不是货架上的活 pair 数，也不是光上传没铸过的号。给每个号记一发成功铸票让它算数。
	for _, a := range st.Accounts() {
		if err := st.NoteRegionMint(a.ID, "us-east", "", clk.now()); err != nil {
			t.Fatal(err)
		}
	}
	if ps := s.Public(); ps.Enabled || ps.Open {
		t.Fatalf("默认该是关的: %+v", ps)
	}
	// 基准取「铸过票的号」，别写死——写死的话 rig 加一个号这条就假绿。
	have := s.provenAccounts()
	if have == 0 {
		t.Fatal("rig 里一个铸过票的账号都没有，这条测不出门槛")
	}
	c := s.opt.Live.Get()
	c.PublicWhenSurplus.Enabled = true
	c.PublicWhenSurplus.MinAccounts = have
	s.opt.Live = config.NewLive("", c)
	if ps := s.Public(); !ps.Open || ps.Threshold != have || ps.Accounts != have {
		t.Fatalf("改完配置没立即生效: %+v（有 %d 个号）", ps, have)
	}
	// 门槛调紧应当立刻关门。
	c.PublicWhenSurplus.MinAccounts = have + 1
	s.opt.Live = config.NewLive("", c)
	if ps := s.Public(); ps.Open || ps.Threshold != have+1 {
		t.Fatalf("门槛调紧没生效: %+v", ps)
	}
}

// F1：wait 的轮询判据和交付必须是同一把尺子——它看见的票得是**这个账号**真拿得到的。
//
// 「刚交付过的网关在冷却里、它那张 pair 还活着」是这个池子最常见的稳态
// （冷却 4 小时 ≫ pair 寿命 65 分钟），判据少看一条闸就会每次等待都重跑、
// 白铸一发（1 发请求 + 2 个槽位）。
func TestHasCandidateUsesTheSameJudgeAsDeliver(t *testing.T) {
	s, st, _, clk := noVerifyRig(t)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	putPair(st, "unified-1", cookiePrefix+"unified-1", clk.now().Add(types.PairLifetime))

	// 一侧：没碰过这个网关 ⇒ 这张现成票他拿得到。
	if !s.HasCandidate(1, Request{}) {
		t.Fatal("有现成票却说没有")
	}
	if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	// 窗口里他还能原样拿回手上那张（redeliver，一发上游都不打）⇒ 这算有候选。
	if !s.HasCandidate(1, Request{}) {
		t.Fatal("窗口内重连拿得回手上那张，判据却说没候选")
	}
	// 另一侧：窗口过了、冷却还在（4 小时 ≫ 150 秒，这是池子最常见的稳态）⇒
	// 票还活着，但对他而言没有候选。
	clk.add(types.DeliverTTL + time.Second)
	if s.HasCandidate(1, Request{}) {
		t.Fatal("槽位在冷却里、这张票他拿不到，判据却说有（每次等待都会白铸一发）")
	}
	// 冷却是 (账号 × 网关) 的事：同一张票对别的账号仍然算候选。
	if !s.HasCandidate(2, Request{}) {
		t.Fatal("另一个账号没碰过这个网关，该算有候选")
	}
	// 调用方自己说不要的也不算候选，否则等到的那一下重跑仍然是 503。
	p, _ := st.Pair("unified-1")
	for name, req := range map[string]Request{
		"exclude":          {Exclude: map[string]bool{"unified-1": true}},
		"exclude_versions": {ExcludeVersions: map[string]bool{types.CookieVersion(p.Cookie): true}},
		"min_remaining":    {MinRemaining: types.PairLifetime + time.Hour},
	} {
		if s.HasCandidate(2, req) {
			t.Fatalf("%s 在轮询判据里失效了", name)
		}
	}
}

// F3：等锁期间别人铸好的那张票，用之前必须**现读**一遍自己的闸。
//
// 池子模式下 minterFor 常常挑中另一个消费者，于是等到的票恰好是他自己刚铸的——
// 他对这个网关的满血窗口已经烧了几十秒，而铸票前的快照看不见这件事。
func TestWaitingForAnothersMintRechecksOwnWindow(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	mn.delay = 20 * time.Millisecond // 两路都在飞
	mn.mintCost = 60 * time.Second   // 一发 60 秒往返：窗口真的在烧
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	// 只有一个网关、一张票都没有 ⇒ 两个人必然挑到同一个，而铸票者只能是对方。
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	mn.land["US"] = "unified-1"
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(nil)})

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.Deliver(context.Background(), int64(i+1), Request{})
		}(i)
	}
	wg.Wait()
	got := 0
	for _, err := range errs {
		if err == nil {
			got++
		}
	}
	if got != 1 {
		t.Fatalf("成功了 %d 路：铸票那一发烧的正是另一个消费者的窗口，他不该拿到这张票（errs=%v）", got, errs)
	}
	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("铸了 %d 发，网关锁没拦住", n)
	}
}

// F5：挑中的网关在选中之后才跌破门槛时**不许顺手补一发**。
//
// 带进去的是一张「还有效、只是太短」的 pair ⇒ 上游接受它、不下发新 cookie
// （SPEC 第 2 节）⇒ finish 照样拒发，而请求和铸票者的槽位已经花掉了，
// 消费者还白得一次退避。铸票只归「一张活票都没有」那条回落路。
func TestTryGatewayNeverMints(t *testing.T) {
	s, st, mn, clk := noVerifyRig(t)
	gw := st.Gateways()["unified-1"]

	putPair(st, "unified-1", cookiePrefix+"unified-1", clk.now().Add(types.DeliverFloor-time.Second))
	if _, err := s.tryGateway(context.Background(), st.accounts[0], gw, Request{}); err == nil {
		t.Fatal("残票直接发出去了")
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("为一张残票铸了 %d 发（上游根本不会换它）", n)
	}
	// 另一侧：票够新鲜就照常交付，而且一发都不打。
	putPair(st, "unified-1", cookiePrefix+"unified-1", clk.now().Add(types.PairLifetime))
	if _, err := s.tryGateway(context.Background(), st.accounts[0], gw, Request{}); err != nil {
		t.Fatalf("够新鲜的票没发出去: %v", err)
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("有现成票还铸了 %d 发", n)
	}
}

// 洞 1：一张票能用多久按 **__cflb** 算，不按 __oailb 算。
//
// 两个 cookie 同一个响应下发，__cflb 早死 300 秒，而 DeliverFloor 恰好就是 300 秒
// ⇒ 按 __oailb 算的话，「刚好够交付」那张票的路由其实已经死了：亲和失效、上游按
// 出口地理重新分配，现场看到的就是「落点漂移：想要 unified-194，实到 unified-150」。
func TestPairLifeFollowsCflbNotOailb(t *testing.T) {
	s, st, mn, clk := noVerifyRig(t)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	// __oailb 还剩 301 秒（刚好够 DeliverFloor），而 __cflb 1 秒后就死。
	mn.pairLife = types.DeliverFloor + time.Second
	mn.cflbLife = time.Second

	if _, err := s.Deliver(context.Background(), 1, Request{}); err == nil {
		t.Fatal("路由已经死了的票被当成「刚好够交付」发了出去")
	}
	p, ok := st.Pair("unified-1")
	if !ok {
		t.Fatal("票没落盘")
	}
	if want := clk.now().Add(time.Second); !p.ExpiresAt.Equal(want) {
		t.Fatalf("ExpiresAt=%v，应按 __cflb 算成 %v", p.ExpiresAt, want)
	}
}

// 洞 2：续期只补发 __oailb 时，寿命必须还按**旧 __cflb** 算。
//
// 新 __oailb 的 65 分钟盖不住旧 __cflb——后者是 AEAD 密文、读不出时间，
// 只有账本上那个字段知道它什么时候死。盖掉的话虚报量无上限。
func TestRenewKeepsOldCflbDeadline(t *testing.T) {
	s, st, mn, clk := warmRig(t, "unified-1")
	dead := clk.now().Add(400 * time.Second) // 低于 RenewLead ⇒ 该续了
	putPair(st, "unified-1", cookiePrefix+"unified-1", dead)
	mn.onlyOailb = true // 上游只补发 __oailb，钉路由那张原样留着

	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("续了 %d 发，这个用例要的就是一发", n)
	}
	p, _ := st.Pair("unified-1")
	if !p.ExpiresAt.Equal(dead) {
		t.Fatalf("ExpiresAt=%v，只补发 __oailb 时该还是旧 __cflb 的 %v", p.ExpiresAt, dead)
	}
	if !p.CflbExpiresAt.Equal(dead) {
		t.Fatalf("CflbExpiresAt=%v，应为 %v", p.CflbExpiresAt, dead)
	}
}

// B-1：一把合法 key 替**别的**上游账号取票，那是鉴权过的调用方，
// 不该吃 public_when_surplus 这道闸。
//
// 门槛是给上传者留的余量（缺省 = 冷却 ÷ 单张可用时长 ≈ 96 个网关），而上传者本人
// 反而被挡了 ⇒ 默认配置下每一发都是 503；而那个码按契约说的是「**匿名**此刻不开放」，
// 对一个鉴权过的调用方是错的。
func TestCrossAccountIsNotGatedByPublicSwitch(t *testing.T) {
	s, st, _, clk := noVerifyRig(t)
	seedPairs(st, clk)
	if ps := s.Public(); ps.Open {
		t.Fatal("这个用例的前提是公开池关着（默认就是关的）")
	}

	// 鉴权过的那条路：照发。
	d, err := s.DeliverAnonymous(context.Background(), "acct-别人的号", Request{}, true)
	if err != nil {
		t.Fatalf("鉴权过的跨账号取票被公开池开关挡了: %v", err)
	}
	if d.Cookie == "" || d.VerifiedFull {
		t.Fatalf("交付内容不对（跨账号验不了满血）: %+v", d)
	}
	// 另一侧：真匿名照旧被挡（门槛是给上传者留的余量）。
	if _, err := s.DeliverAnonymous(context.Background(), "acct-路人", Request{}, false); !errors.Is(err, ErrPublicClosed) {
		t.Fatalf("匿名那条路该被挡: %v", err)
	}
}

// B-3：等别人代铸的那几十秒里，(消费者 × 落点) 可能被**并发的发现扫描**烧掉
// （scan 直接写 LastTouch，而且裸铸不受网关锁约束）。
//
// 这一发不是消费者自己打的 ⇒ 闸 2 必须现读账本，用铸票前的快照会漏掉它。
// 「自己铸票时要用快照」那条理由只在单账号模式成立。
func TestConcurrentTouchDuringAnothersMintBlocksDelivery(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	// 池子里只有这一个网关，裸铸从哪个区域出发都该落到它。只钉 "US" 不行：
	// 第一发成功铸票会给 us-east 记一笔，第二发就改从 us-west 出发了。
	landBare(mn, "unified-1")
	mn.full["unified-1"] = true
	// 铸票那一发拿到账号锁之后，模拟一次并发的发现扫描烧掉 (消费者 1 × unified-1)。
	// 用这个回调而不是真起 goroutine：时序得是确定的。
	st.afterLock = func(id int64) {
		if id == 2 { // 代铸的那个号
			_ = st.UpdateSlot(1, "unified-1", func(sl *types.Slot) { sl.LastTouch = clk.now() })
		}
	}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(nil)})

	if _, err := s.Deliver(context.Background(), 1, Request{}); err == nil {
		t.Fatal("铸票那几十秒里这个槽位已经被烧掉了，还是发了出去")
	}
	// 另一侧：没有那次并发触碰时照常交付（别把这条闸关成永久拦路）。
	st.afterLock = nil
	st.mu.Lock()
	delete(st.slots, slotKey(1, "unified-1"))
	delete(st.pairs, "unified-1")
	st.mu.Unlock()
	if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
		t.Fatalf("没有并发触碰时该照常交付: %v", err)
	}
}

// S-1：退避中的槽位不是候选，而「有票的网关全在退避里」要能回落到现铸。
//
// LongestRestedSlot 跳过正在退避的槽位，而 coolingSet 的闸 2 恰好对正在退避的槽位
// 豁免 ⇒ 只问 notCooling 的话，刚失败过的槽位会被当成候选（HasCandidate 说有、
// 重跑还是 503），而 Deliver 那半边会 503 且一发不打——可现铸本来能成。
func TestBackoffSlotIsNotACandidateAndFallsBackToMint(t *testing.T) {
	s, st, mn, clk := noVerifyRig(t)
	delete(st.gws, "unified-3")
	// unified-1 有票但槽位正在退避；unified-2 没票、可现铸。
	// 回落是从区域出发、不冲着候选网关去 ⇒ 落点显式钉在 unified-2 上。
	landBare(mn, "unified-2")
	putPair(st, "unified-1", cookiePrefix+"unified-1", clk.now().Add(types.PairLifetime))
	st.mu.Lock()
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1",
		BackoffUntil: clk.now().Add(30 * time.Second)}
	st.mu.Unlock()

	// 判据一侧：那张票他此刻拿不到（槽位在退避里）。
	if s.HasCandidate(1, Request{Gateway: "unified-1"}) {
		t.Fatal("退避中的槽位被当成候选了：等到的那一下重跑还是 503")
	}
	// 交付一侧：有票的那个在退避里 ⇒ 回落现铸，而不是 503 且一发不打。
	d, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("有票的网关全在退避里时该回落现铸: %v", err)
	}
	if d.Gateway != "unified-2" {
		t.Fatalf("交付了 %s，该是现铸出来的 unified-2", d.Gateway)
	}
	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("铸了 %d 发", n)
	}
}

// S-6：漂移之后 state-echo 要用**落到的那个**网关的出口。
//
// 拿「想去 A」的出口去验 B 大概率再漂一次，把一张好票判成失败。默认关着所以今天
// 不咬人，但它是定时炸弹。
func TestVerifyUsesLandedGatewayExit(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	// 裸铸落到 unified-1、被 drift 弹到 unified-77，两个网关各有自己的出口。
	//
	// 原来这里把目标出口写成**不带区域**的，为的是让 UpsertGateway 不把它并进
	// unified-77 的可达清单（它只合并带区域的出口），于是「用旧的」和「按落点重取」
	// 泾渭分明。按需铸票之后出口一律取自 types.Regions、必然带区域 ⇒ 那一发用过的出口
	// 一定会被并进落点的清单里，两个集合天生重叠，那个 A/B 构造再也造不出来。
	// 改成验**真正还能证的那条**：echo 的出口必须属于**落到的那个**网关的可达清单，
	// 而 unified-1 自己那条出口不在里面 —— 拿「想去哪」的出口去 echo 就会露出来。
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "R1"}}}
	st.gws["unified-77"] = types.Gateway{Name: "unified-77", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "R77"}}}
	landBare(mn, "unified-1")
	mn.drift = map[string]string{"unified-1": "unified-77"}
	mn.full["unified-77"] = true
	s := New(st, mn, Options{Mode: types.ModeSingle, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(verifyOn)})

	d, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if d.Gateway != "unified-77" || !d.VerifiedFull {
		t.Fatalf("交付不对: %+v", d)
	}
	landedExits := map[string]bool{}
	for _, e := range st.gws["unified-77"].ReachableExits {
		landedExits[e.Region] = true
	}
	if landedExits["R1"] {
		t.Fatal("夹具坏了：unified-1 的出口串进了落点的可达清单，这条用例就什么都没证")
	}
	mn.mu.Lock()
	defer mn.mu.Unlock()
	if len(mn.echoExits) != 1 || !landedExits[mn.echoExits[0]] {
		t.Fatalf("state-echo 用的出口是 %v，漂到 unified-77 之后只能用它自己可达的 %v",
			mn.echoExits, landedExits)
	}
}

// F8：窗口内重连那条路也得认 min_remaining——但只认**显式**给的那个。
//
// 默认门槛不能加在那儿：那会让重连掉进正常流程去烧下一个落点，而省掉那个槽位
// 正是 redeliver 存在的理由。
func TestRedeliverHonorsExplicitMinRemaining(t *testing.T) {
	s, st, _, clk := noVerifyRig(t)
	putPair(st, "unified-1", cookiePrefix+"unified-1", clk.now().Add(40*time.Second))
	st.mu.Lock()
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1",
		LastDeliver: clk.now()}
	st.mu.Unlock()

	// 一侧：没说门槛 ⇒ 照样拿回手上那张残票（默认门槛 300 秒在这条路上**不生效**）。
	d, ok := s.redeliver(1, Request{})
	if !ok || d.ValidForS != 40 {
		t.Fatalf("窗口内重连没原样答回去: ok=%v %+v", ok, d)
	}
	// 另一侧：显式说「至少 600 秒」⇒ 那张 40 秒的票不许发回去。
	if d, ok := s.redeliver(1, Request{MinRemaining: 10 * time.Minute}); ok {
		t.Fatalf("说了至少 600 秒，却拿回一张只剩 %d 秒的票", d.ValidForS)
	}
}
