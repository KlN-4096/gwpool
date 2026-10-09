package sched

// 这一份盯的是 2026-10-02 那轮协议改动和同批修复：排除名单、min_remaining、
// 归还、窗口锚点、已烧槽位的闸。用例一律用 fake，绝不打真实上游（SPEC 红线 8）。

import (
	"context"
	"errors"
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

	d, err := s.DeliverAnonymous(context.Background(), "member", Request{Gateway: "unified-3", Exclude: map[string]bool{"unified-2": true}}, true)
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

	_, err := s.DeliverAnonymous(context.Background(), "member", Request{Gateway: "unified-2", ExcludeVersions: map[string]bool{stale: true}}, true)
	if !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("排除点名票必须拒绝，不能擅自换网关: %v", err)
	}

	// 同一个网关换了票之后照样能用：排的是票，不是网关。
	putPair(st, "unified-2", cookiePrefix+"unified-2-v2", clk.now().Add(types.PairLifetime))
	clk.add(types.DeliverTTL + time.Second)
	d2, err := s.DeliverAnonymous(context.Background(), "member", Request{Gateway: "unified-2", ExcludeVersions: map[string]bool{stale: true}}, true)
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
	if _, err := strict.DeliverAnonymous(context.Background(), "member", Request{Gateway: "unified-1"}, true); err == nil {
		t.Fatal("剩 40 秒的票按默认门槛就发出去了")
	}

	// 调用方说它只要 30 秒：收下，但有效期只能报 40 秒。
	s, st, mn, clk := staleRig()
	d, err := s.DeliverAnonymous(context.Background(), "member", Request{Gateway: "unified-1", MinRemaining: 30 * time.Second}, true)
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

// A-12：点名的网关同时又在排除名单里 ⇒ 按排除办（拒）。
//
// exclude 编码的是「我烧过这个」，照它办永远不会发出一张坏票；反过来让 gateway
// 压过 exclude 就可能把客户端自己说烧过的落点发回去。
func TestExcludeBeatsGatewayHint(t *testing.T) {
	s, st, mn, clk := noVerifyRig(t)
	seedPairs(st, clk)
	_, err := s.DeliverAnonymous(context.Background(), "member", Request{
		Gateway: "unified-1", Exclude: map[string]bool{"unified-1": true}}, true)
	if err == nil {
		t.Fatal("点名的网关在排除名单里，却照样发了出来")
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("自相矛盾的请求还打了 %d 发上游", n)
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

// F5：挑中的网关在选中之后才跌破门槛时**不许顺手补一发**。
//
// 带进去的是一张「还有效、只是太短」的 pair ⇒ 上游接受它、不下发新 cookie
// （SPEC 第 2 节）⇒ finish 照样拒发，而请求和铸票者的槽位已经花掉了，
// 消费者还白得一次退避。铸票只归「一张活票都没有」那条回落路。
func TestTryGatewayNeverMints(t *testing.T) {
	s, st, mn, clk := noVerifyRig(t)

	putPair(st, "unified-1", cookiePrefix+"unified-1", clk.now().Add(types.DeliverFloor-time.Second))
	if _, err := s.DeliverAnonymous(context.Background(), "member", Request{Gateway: "unified-1"}, true); err == nil {
		t.Fatal("残票直接发出去了")
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("为一张残票铸了 %d 发（上游根本不会换它）", n)
	}
	// 另一侧：票够新鲜就照常交付，而且一发都不打。
	putPair(st, "unified-1", cookiePrefix+"unified-1", clk.now().Add(types.PairLifetime))
	if _, err := s.DeliverAnonymous(context.Background(), "member", Request{Gateway: "unified-1"}, true); err != nil {
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

	if _, err := backgroundMint(context.Background(), s, 1, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeliverAnonymous(context.Background(), "member", Request{Gateway: "unified-1"}, true); err == nil {
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
	d, err := s.DeliverAnonymous(context.Background(), "acct-别人的号", Request{Gateway: "unified-1"}, true)
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
