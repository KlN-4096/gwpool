package sched

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gwpool/internal/types"
)

// 测试一律用 fake，绝不打真实上游（SPEC 红线 8）。

// ---------- 可控时钟 ----------

type clock struct {
	mu sync.Mutex
	t  time.Time
}

// slotKey 是 fake 自己的槽位键，对齐真实 store 的 "<id>|<gateway>"。
func slotKey(accountID int64, gateway string) string {
	return strconv.FormatInt(accountID, 10) + "|" + gateway
}

func newClock() *clock { return &clock{t: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// ---------- fake store ----------

type fakeStore struct {
	mu       sync.Mutex
	clk      *clock
	accounts []types.Account
	gws      map[string]types.Gateway
	slots    map[string]types.Slot
	pairs    map[string]types.Pair
	samples  []types.Sample
	nextID   int64
	disabled []string // "id|原因"，按调用顺序

	lmu   sync.Mutex
	locks map[int64]*sync.Mutex
	// afterLock 在**拿到**账号锁之后回调，给死锁用例做确定性屏障用。
	afterLock func(int64)
}

func newStore(clk *clock) *fakeStore {
	return &fakeStore{
		clk:    clk,
		gws:    map[string]types.Gateway{},
		slots:  map[string]types.Slot{},
		pairs:  map[string]types.Pair{},
		locks:  map[int64]*sync.Mutex{},
		nextID: 100,
	}
}

func (f *fakeStore) Gateways() map[string]types.Gateway {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]types.Gateway, len(f.gws))
	for k, v := range f.gws {
		out[k] = v
	}
	return out
}

// 照 store.LongestRestedSlot 的语义实现：没记录的优先，退避中的跳过。
func (f *fakeStore) LongestRestedSlot(accountID int64, candidates []string) (types.Slot, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clk.now()
	names := append([]string(nil), candidates...)
	sort.Strings(names)
	var best types.Slot
	found := false
	for _, gw := range names {
		sl, ok := f.slots[slotKey(accountID, gw)]
		if !ok {
			sl = types.Slot{AccountID: accountID, Gateway: gw, LastVerdict: "unknown"}
		}
		if now.Before(sl.BackoffUntil) {
			continue
		}
		if !found || sl.LastTouch.Before(best.LastTouch) {
			best, found = sl, true
		}
	}
	return best, found
}

// Slot 只给用例自己断言用，不在 sched.Store 接口里。
func (f *fakeStore) Slot(accountID int64, gateway string) (types.Slot, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sl, ok := f.slots[slotKey(accountID, gateway)]
	return sl, ok
}

func (f *fakeStore) Slots() map[string]types.Slot {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]types.Slot, len(f.slots))
	for k, v := range f.slots {
		out[k] = v
	}
	return out
}

func (f *fakeStore) DeletePair(gateway string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.pairs, gateway)
	return nil
}

func (f *fakeStore) UpdateGateway(name string, fn func(*types.Gateway)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.gws[name]
	if !ok {
		return nil
	}
	fn(&g)
	f.gws[name] = g
	return nil
}

// UpdateSlot 照真实 store 的语义：读-改-写在同一把写锁里，不给并发丢更新的机会。
func (f *fakeStore) UpdateSlot(accountID int64, gateway string, fn func(*types.Slot)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := slotKey(accountID, gateway)
	sl := f.slots[k]
	fn(&sl)
	f.slots[k] = sl
	return nil
}

// LockAccount 是跨包唯一的单账号锁，真实实现在 store 上。
func (f *fakeStore) LockAccount(id int64) func() {
	f.lmu.Lock()
	l := f.locks[id]
	if l == nil {
		l = &sync.Mutex{}
		f.locks[id] = l
	}
	after := f.afterLock
	f.lmu.Unlock()
	l.Lock()
	if after != nil {
		after(id)
	}
	return l.Unlock
}

func (f *fakeStore) Pair(gateway string) (types.Pair, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.pairs[gateway]
	return p, ok
}

func (f *fakeStore) PutPair(p types.Pair) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pairs[p.Gateway] = p
	return nil
}

func (f *fakeStore) Accounts() []types.Account {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]types.Account(nil), f.accounts...)
}

func (f *fakeStore) AddAccount(a types.Account) (types.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	a.ID = f.nextID
	a.ViewToken = "tok" + a.Creds.AccountID
	f.accounts = append(f.accounts, a)
	return a, nil
}

func (f *fakeStore) DisableAccount(id int64, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disabled = append(f.disabled, fmt.Sprintf("%d|%s", id, reason))
	for i := range f.accounts {
		if f.accounts[i].ID == id {
			f.accounts[i].Enabled = false
			f.accounts[i].DisabledReason = reason
			return nil
		}
	}
	return fmt.Errorf("账号 %d 不存在", id)
}

func (f *fakeStore) AppendSample(sm types.Sample) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.samples = append(f.samples, sm)
	return nil
}

// ---------- fake minter ----------

const cookiePrefix = "__cflb=cf; __oailb="

type fakeMinter struct {
	mu    sync.Mutex
	clk   *clock
	land  map[string]string // exitKey → 落点网关
	full  map[string]bool   // 网关 → state-echo 判满血
	delay time.Duration

	noNewPair  bool // 模拟「上游接受了送进去的 pair，不下发新 cookie」
	mintErr    error
	mintStatus int           // 非 0 时顶替 200，用来模拟上游否决
	mintBody   string        // 上游那段 JSON
	mintCost   time.Duration // 一发铸票的往返耗时，推进假时钟

	sidErr    error
	sidSeq    int
	mints     []types.Sample // 只借这个结构记 (account, gateway) 调用流水
	echoes    int
	inflight  map[int64]int
	maxPerAcc int
}

func newMinter(clk *clock) *fakeMinter {
	return &fakeMinter{clk: clk, land: map[string]string{}, full: map[string]bool{}, inflight: map[int64]int{}}
}

func gwOf(cookie string) string {
	i := strings.Index(cookie, "__oailb=")
	if i < 0 {
		return ""
	}
	return cookie[i+len("__oailb="):]
}

// acctID 把凭据映射回账号序号：fake 里 AccountID 就写成 "acct-<n>"。
func acctID(c types.Creds) int64 {
	switch c.AccountID {
	case "acct-1":
		return 1
	case "acct-2":
		return 2
	}
	return 0
}

func (m *fakeMinter) enter(id int64) {
	m.mu.Lock()
	m.inflight[id]++
	if m.inflight[id] > m.maxPerAcc {
		m.maxPerAcc = m.inflight[id]
	}
	m.mu.Unlock()
}

func (m *fakeMinter) leave(id int64) {
	m.mu.Lock()
	m.inflight[id]--
	m.mu.Unlock()
}

func (m *fakeMinter) Mint(ctx context.Context, c types.Creds, e types.Exit, sid, sentPair string) (types.MintResult, error) {
	id := acctID(c)
	m.enter(id)
	defer m.leave(id)
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	if m.mintCost > 0 {
		m.clk.add(m.mintCost)
	}
	m.mu.Lock()
	gw := m.land[exitKey(e)]
	if sentPair != "" {
		gw = gwOf(sentPair) // 带 pair 就留在那个网关上
	}
	m.mints = append(m.mints, types.Sample{AccountID: id, Gateway: gw, RegionKey: exitKey(e)})
	err := m.mintErr
	noNew := m.noNewPair
	m.mu.Unlock()
	if err != nil {
		return types.MintResult{}, err
	}
	if st := m.status(); st != 0 && st != 200 {
		return types.MintResult{Status: st, Gateway: gw, ErrBody: m.body()}, nil
	}
	res := types.MintResult{Status: 200, Gateway: gw, TurnState: "ticket-" + gw + "-" + sid}
	if !(noNew && sentPair != "") {
		res.NewPair = cookiePrefix + gw
		res.PairExpires = m.clk.now().Add(types.PairLifetime)
	}
	return res, nil
}

func (m *fakeMinter) status() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mintStatus
}

func (m *fakeMinter) body() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mintBody
}

func (m *fakeMinter) VerifyFull(ctx context.Context, c types.Creds, e types.Exit, sid, pair, ticket string) (*types.Verdict, error) {
	id := acctID(c)
	m.enter(id)
	defer m.leave(id)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.echoes++
	gw := gwOf(pair)
	if !strings.Contains(ticket, gw) {
		return nil, errors.New("票和 pair 对不上：sid/pair 没有复用")
	}
	return &types.Verdict{Full: m.full[gw], Status: 200, Note: gw}, nil
}

// NewSID 照真实 mint 的语义：sid 由铸票方分配并绑死到一条出口上。
func (m *fakeMinter) NewSID(sticky bool) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sidErr != nil {
		return "", m.sidErr
	}
	if !sticky {
		return "", errors.New("sched 只该要粘性 sid")
	}
	m.sidSeq++
	return fmt.Sprintf("sid%05d", m.sidSeq), nil
}

func (m *fakeMinter) Geo(ctx context.Context, e types.Exit, sid string) (types.Observed, error) {
	return types.Observed{IP: "203.0.113.1", Country: e.Region, City: e.City}, nil
}

func (m *fakeMinter) mintedGateways() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.mints))
	for _, s := range m.mints {
		out = append(out, s.Gateway)
	}
	return out
}

// ---------- 场景搭建 ----------

// rig 搭一个三网关、单账号（既扫又用）的池子。
func rig(t *testing.T, mode types.Mode) (*Scheduler, *fakeStore, *fakeMinter, *clock) {
	t.Helper()
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	st.accounts = []types.Account{{
		ID: 1, Label: "pro1", Enabled: true,
		Creds: types.Creds{AccessToken: "secret", AccountID: "acct-1"},
	}}
	for _, gw := range []string{"unified-1", "unified-2", "unified-3"} {
		region := strings.ToUpper(strings.TrimPrefix(gw, "unified-"))
		st.gws[gw] = types.Gateway{Name: gw, FirstSeenBy: 1, ReachableExits: []types.Exit{{Region: region}}}
		mn.land[region] = gw
		mn.full[gw] = true
	}
	s := New(st, mn, Options{Mode: mode, Now: clk.now, MintGap: -1, RenewGap: -1})
	return s, st, mn, clk
}

// addAccount 补一个带凭据的号。保温至少要两个号（见 TestPoolModeNeedsTwoAccounts）。
func addAccount(st *fakeStore, id int64, upstream string) {
	st.accounts = append(st.accounts, types.Account{
		ID: id, Label: upstream, Enabled: true,
		Creds: types.Creds{AccessToken: "tok", AccountID: upstream},
	})
}

func mustDeliver(t *testing.T, s *Scheduler, acctID int64, hint string) types.Delivery {
	t.Helper()
	d, err := s.Deliver(context.Background(), acctID, hint, false)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	return d
}

// ---------- 用例 ----------

// 取的必须是歇得最久的那个槽位，而不是随便一个网关。
func TestDeliverPicksLongestRested(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	now := clk.now()
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1", LastTouch: now.Add(-time.Hour)}
	st.slots[slotKey(1, "unified-2")] = types.Slot{AccountID: 1, Gateway: "unified-2", LastTouch: now.Add(-3 * time.Hour)}
	st.slots[slotKey(1, "unified-3")] = types.Slot{AccountID: 1, Gateway: "unified-3", LastTouch: now.Add(-2 * time.Hour)}

	d := mustDeliver(t, s, 1, "")
	if d.Gateway != "unified-2" {
		t.Fatalf("交付了 %s，应该是歇了 3 小时的 unified-2", d.Gateway)
	}
	if !d.VerifiedFull || d.ValidForS != 150 {
		t.Fatalf("交付字段不对: %+v", d)
	}
	if got := d.ExpiresAt.Sub(now); got != types.DeliverTTL {
		t.Fatalf("有效期 %v，应为 %v", got, types.DeliverTTL)
	}
	// 单账号模式一个网关只打一发（那一发既建 pair 又是首次接触），加一发 echo。
	if got := mn.mintedGateways(); len(got) != 1 || got[0] != "unified-2" {
		t.Fatalf("铸票流水 %v，应只有一发打在 unified-2", got)
	}
	if mn.echoes != 1 {
		t.Fatalf("state-echo 打了 %d 发，应为 1（不能当持续闸门）", mn.echoes)
	}
	// 交付后该槽位已被烧过：下一次必须换成次久的那个。
	if d2 := mustDeliver(t, s, 1, ""); d2.Gateway != "unified-3" {
		t.Fatalf("第二次交付 %s，应轮到 unified-3", d2.Gateway)
	}
}

// 没有触碰记录的槽位（从没碰过）优先级最高。
func TestNeverTouchedWinsOverRested(t *testing.T) {
	s, st, _, clk := rig(t, types.ModeSingle)
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1", LastTouch: clk.now().Add(-99 * time.Hour)}
	delete(st.gws, "unified-3")
	// unified-2 没有任何记录 ⇒ 零值 LastTouch ⇒ 比歇了 99 小时的还靠前。
	if d := mustDeliver(t, s, 1, ""); d.Gateway != "unified-2" {
		t.Fatalf("交付 %s，从未触碰的 unified-2 该优先", d.Gateway)
	}
}

// 判降级就换下一个槽位，不交付降智的东西。
func TestDegradedFallsThrough(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	now := clk.now()
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1", LastTouch: now.Add(-time.Hour)}
	st.slots[slotKey(1, "unified-2")] = types.Slot{AccountID: 1, Gateway: "unified-2", LastTouch: now.Add(-3 * time.Hour)}
	st.slots[slotKey(1, "unified-3")] = types.Slot{AccountID: 1, Gateway: "unified-3", LastTouch: now.Add(-2 * time.Hour)}
	mn.full["unified-2"] = false

	d := mustDeliver(t, s, 1, "")
	if d.Gateway != "unified-3" {
		t.Fatalf("交付 %s，unified-2 降级后应换 unified-3", d.Gateway)
	}
	if sl, _ := st.Slot(1, "unified-2"); sl.LastVerdict != "degraded" || !sl.BackoffUntil.After(now) {
		t.Fatalf("降级槽位没被标记/退避: %+v", sl)
	}
	if sl, _ := st.Slot(1, "unified-3"); sl.LastVerdict != "full" {
		t.Fatalf("交付的槽位应记 full: %+v", sl)
	}
}

// 全部降级 ⇒ ErrNoFullSlot（HTTP 层映射成 503）。宁可 503 也不放降智。
func TestAllDegradedReturnsNoFullSlot(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModeSingle)
	for gw := range st.gws {
		mn.full[gw] = false
	}
	if _, err := s.Deliver(context.Background(), 1, "", false); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err = %v，应为 ErrNoFullSlot", err)
	}
	// 三个槽位都该试过一遍，都该进退避。
	if got := len(mn.mintedGateways()); got != 3 {
		t.Fatalf("试了 %d 个槽位，应为 3", got)
	}
	for gw := range st.gws {
		if sl, _ := st.Slot(1, gw); sl.BackoffUntil.IsZero() {
			t.Fatalf("%s 没进退避: %+v", gw, sl)
		}
	}
}

// 铸票本身失败（HTTP 非 200 之类）也要退避，不能快重试。
func TestMintFailureBacksOff(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModeSingle)
	mn.mintErr = errors.New("socks5 连不上")
	if _, err := s.Deliver(context.Background(), 1, "", false); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err = %v，应为 ErrNoFullSlot", err)
	}
	if mn.echoes != 0 {
		t.Fatalf("铸票都失败了还 echo 了 %d 发", mn.echoes)
	}
	if sl, _ := st.Slot(1, "unified-1"); sl.BackoffUntil.IsZero() {
		t.Fatal("铸票失败的槽位没进退避")
	}
}

// 硬失败的退避必须翻倍且封顶 20 分钟。
func TestBackoffDoublesAndCaps(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	mn.mintErr = errors.New("socks5 连不上")
	want := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
		16 * time.Minute, 20 * time.Minute, 20 * time.Minute}
	for i, w := range want {
		at := clk.now()
		if _, err := s.Deliver(context.Background(), 1, "unified-1", false); !errors.Is(err, ErrNoFullSlot) {
			t.Fatalf("第 %d 轮 err = %v", i, err)
		}
		sl, _ := st.Slot(1, "unified-1")
		if got := sl.BackoffUntil.Sub(at); got != w {
			t.Fatalf("第 %d 轮退避 %v，应为 %v", i, got, w)
		}
		clk.add(w + time.Second) // 等退避过期再来一轮
	}
	// 退避期内不该再选它：唯一候选在退避中 ⇒ 直接 ErrNoFullSlot，一发都不打。
	before := len(mn.mintedGateways())
	s.bumpBackoff(1, "unified-1")
	if _, err := s.Deliver(context.Background(), 1, "unified-1", false); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err = %v", err)
	}
	if after := len(mn.mintedGateways()); after != before {
		t.Fatalf("退避期内还打了 %d 发", after-before)
	}
	// 交付成功要把退避清零，否则一次误判会越积越长。
	mn.mintErr = nil
	clk.add(backoffMax + time.Second)
	mustDeliver(t, s, 1, "unified-1")
	if sl, _ := st.Slot(1, "unified-1"); !sl.BackoffUntil.IsZero() {
		t.Fatalf("成功后退避没清零: %+v", sl)
	}
}

// 单账号模式绝不保温：保温那一发恰好烧掉自己要用的窗口。
func TestSingleModeNeverWarms(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModeSingle)
	addAccount(st, 2, "acct-2")
	mustDeliver(t, s, 1, "") // 制造「有需求」
	st.pairs = map[string]types.Pair{}
	s.opt.WarmInterval = time.Millisecond

	done := make(chan struct{})
	go func() { s.Run(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("单账号模式的 Run 应立刻返回，它不该起保温循环")
	}
	before := len(mn.mintedGateways())
	time.Sleep(20 * time.Millisecond)
	if after := len(mn.mintedGateways()); after != before {
		t.Fatalf("单账号模式保温了 %d 发", after-before)
	}
	// 同一份配置换成池子模式就该续，说明上面不是因为别的原因没动静。
	s.opt.Mode = types.ModePool
	s.warmOnce(context.Background())
	if len(mn.mintedGateways()) == before {
		t.Fatal("池子模式也没保温，说明这个用例什么都没测到")
	}
}

// 无需求不扫不续：静默超过 IdleAfter 就让 pair 自然过期，成本归零。
func TestPoolModeWarmsOnlyOnDemand(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.opt.IdleAfter = 10 * time.Minute

	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("从没人要过就保温了 %d 发", n)
	}

	// 没有号碰过任何网关 ⇒ 目标深度就是 SpareTarget，三个网关里只铸够数的那几个。
	s.markDemand()
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != types.SpareTarget {
		t.Fatalf("有需求时保温了 %d 发，目标深度是 %d", n, types.SpareTarget)
	}

	// 够数了就不再打：pair 还新鲜，而且目标也没涨。
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != types.SpareTarget {
		t.Fatalf("够数了却又续了，共 %d 发", n)
	}

	// 需求停了：即便 pair 快过期也不续。
	clk.add(types.PairLifetime - types.RenewLead + time.Minute)
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != types.SpareTarget {
		t.Fatalf("静默期还在续，共 %d 发", n)
	}
	s.markDemand()
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 2*types.SpareTarget {
		t.Fatalf("需求回来后该把深度补回 %d，共 %d 发", types.SpareTarget, n)
	}
}

// 多账号模式：扫描账号保温，消费账号只打属于自己的那一发首次接触。
// 并且「上游没下发新 __oailb」不算失败（SPEC 红线 3）。
func TestPoolModeConsumerMintsOwnTicket(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	mn.noNewPair = true // 带 pair 打进去，上游接受、不换 cookie
	st.accounts = []types.Account{
		// 角色一律 both：产品规则是「上传了凭据的就能用池子」。
		// 谁保温谁消费由调用方传的 accountID 决定，不由 Role 决定。
		{ID: 1, Label: "甲", Enabled: true,
			Creds: types.Creds{AccessToken: "s", AccountID: "acct-1"}},
		{ID: 2, Label: "乙", Enabled: true,
			Creds: types.Creds{AccessToken: "c", AccountID: "acct-2"}},
	}
	st.gws["unified-9"] = types.Gateway{Name: "unified-9", FirstSeenBy: 1, ReachableExits: []types.Exit{{Region: "ES"}}}
	mn.land["ES"] = "unified-9"
	mn.full["unified-9"] = true
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})

	d := mustDeliver(t, s, 2, "")
	if d.Gateway != "unified-9" || d.Cookie != cookiePrefix+"unified-9" {
		t.Fatalf("交付内容不对: %+v", d)
	}
	// 第一发是扫描账号建 pair，第二发是消费账号的首次接触。
	mn.mu.Lock()
	calls := append([]types.Sample(nil), mn.mints...)
	mn.mu.Unlock()
	if len(calls) != 2 || calls[0].AccountID != 1 || calls[1].AccountID != 2 {
		t.Fatalf("铸票流水应为 [扫描账号, 消费账号]，实为 %+v", calls)
	}
	// 窗口烧在各自的槽位上，不要串账。
	if sl, ok := st.Slot(1, "unified-9"); !ok || sl.LastTouch.IsZero() {
		t.Fatalf("扫描账号的槽位没记触碰: %+v", sl)
	}
	if sl, _ := st.Slot(2, "unified-9"); sl.LastVerdict != "full" {
		t.Fatalf("消费账号的槽位应记 full: %+v", sl)
	}
	// pair 已经新鲜了，再要一次不该再劳烦另一个账号。
	mustDeliver(t, s, 2, "")
	mn.mu.Lock()
	n := len(mn.mints)
	last := mn.mints[len(mn.mints)-1].AccountID
	mn.mu.Unlock()
	if n != 3 || last != 2 {
		t.Fatalf("第二次交付只该由消费账号打一发，流水共 %d 发、最后一发账号 %d", n, last)
	}
}

// 单账号并发硬性 1：这是已知的封号主因，不能靠全局信号量糊过去。
func TestOneInFlightPerAccount(t *testing.T) {
	s, _, mn, _ := rig(t, types.ModeSingle)
	mn.delay = 5 * time.Millisecond
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = s.Deliver(context.Background(), 1, "", false)
		}()
	}
	wg.Wait()
	mn.mu.Lock()
	defer mn.mu.Unlock()
	if mn.maxPerAcc > 1 {
		t.Fatalf("同一账号最多并发 %d，必须是 1", mn.maxPerAcc)
	}
}

// 落点漂移（指定出口没把我们送到想去的网关）算这次失败，但新 pair 要留下，
// 触碰也要记在实际落点上。
func TestLandingDriftIsFailureButPairKept(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModeSingle)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	mn.land["1"] = "unified-77" // 想去 unified-1，实际落到 77

	if _, err := s.Deliver(context.Background(), 1, "", false); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err = %v，漂移该算失败", err)
	}
	if _, ok := st.Pair("unified-77"); !ok {
		t.Fatal("漂移拿到的新 pair 应该留在池子里，白来的供给别扔")
	}
	if sl, ok := st.Slot(1, "unified-77"); !ok || sl.LastTouch.IsZero() {
		t.Fatalf("触碰该记在实际落点上: %+v", sl)
	}
}

// 槽位是 (账号 × 网关)：给 A 算出来的不能发给 B。
func TestDeliverIsPerAccount(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	st.accounts = []types.Account{
		{ID: 1, Label: "A", Enabled: true,
			Creds: types.Creds{AccessToken: "a", AccountID: "acct-1"}},
		{ID: 2, Label: "B", Enabled: true,
			Creds: types.Creds{AccessToken: "b", AccountID: "acct-2"}},
	}
	for _, gw := range []string{"unified-1", "unified-2"} {
		region := strings.ToUpper(strings.TrimPrefix(gw, "unified-"))
		st.gws[gw] = types.Gateway{Name: gw, FirstSeenBy: 1, ReachableExits: []types.Exit{{Region: region}}}
		mn.land[region] = gw
		mn.full[gw] = true
	}
	// A 刚烧过 1，B 刚烧过 2 ⇒ 两个人该拿到相反的网关。
	now := clk.now()
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1", LastTouch: now}
	st.slots[slotKey(2, "unified-2")] = types.Slot{AccountID: 2, Gateway: "unified-2", LastTouch: now}
	s := New(st, mn, Options{Mode: types.ModeSingle, Now: clk.now, MintGap: -1, RenewGap: -1})

	if d := mustDeliver(t, s, 1, ""); d.Gateway != "unified-2" {
		t.Fatalf("A 拿到 %s，它刚烧过 unified-1，应给 unified-2", d.Gateway)
	}
	if d := mustDeliver(t, s, 2, ""); d.Gateway != "unified-1" {
		t.Fatalf("B 拿到 %s，它刚烧过 unified-2，应给 unified-1", d.Gateway)
	}
	// 各走各的账本，不串号。
	if sl, _ := st.Slot(1, "unified-2"); sl.LastVerdict != "full" {
		t.Fatalf("A 的槽位没记上: %+v", sl)
	}
	if sl, _ := st.Slot(2, "unified-1"); sl.LastVerdict != "full" {
		t.Fatalf("B 的槽位没记上: %+v", sl)
	}
}

// 剩余不足 DeliverFloor 的 pair 不能直接交付，要先续。
func TestStalePairIsRenewedBeforeDelivery(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	st.pairs["unified-1"] = types.Pair{
		Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(types.DeliverFloor - time.Minute), MintedBy: 1,
	}
	d := mustDeliver(t, s, 1, "")
	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("该现铸一发顶上，实际打了 %d 发", n)
	}
	p, _ := st.Pair("unified-1")
	if p.ExpiresAt.Sub(clk.now()) < types.DeliverFloor {
		t.Fatalf("续期后的 pair 剩余 %v，不够 DeliverFloor", p.ExpiresAt.Sub(clk.now()))
	}
	if d.Cookie != p.Cookie {
		t.Fatal("交付的应是续期后的那张 cookie")
	}
}

// 没有可用消费账号时要明确报错，不要拿空凭据去打上游。
// 不存在 / 已停用 / 没凭据的账号都不能交付：state-echo 要用它自己的凭据打一发。
func TestDeliverRejectsUnusableAccount(t *testing.T) {
	s, st, _, _ := rig(t, types.ModeSingle)
	st.accounts = append(st.accounts,
		types.Account{ID: 2, Label: "停用", Enabled: false,
			Creds: types.Creds{AccessToken: "x", AccountID: "acct-2"}},
		types.Account{ID: 3, Label: "只回报", Enabled: true,
			Creds: types.Creds{AccountID: "acct-3"}})
	for _, id := range []int64{0, 2, 3, 99} {
		if _, err := s.Deliver(context.Background(), id, "", false); err == nil {
			t.Fatalf("账号 %d 不该能交付", id)
		}
	}
}

// ---------- 审查回归用例 ----------

// B1：有效期必须从**首次接触**起算。铸票 + echo 的往返不能白算进满血窗口。
func TestExpiryRunsFromFirstContact(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	mn.mintCost = 60 * time.Second // 一发 60 s 往返，接近实测上限

	start := clk.now()
	d := mustDeliver(t, s, 1, "")
	if want := start.Add(types.DeliverTTL); !d.ExpiresAt.Equal(want) {
		t.Fatalf("ExpiresAt=%v，应为首次接触 %v + %v = %v", d.ExpiresAt, start, types.DeliverTTL, want)
	}
	// 往返已经吃掉 60 s，剩余只有 90 s，不能还写 150。
	if d.ValidForS != 90 {
		t.Fatalf("valid_for_s=%d，往返 60 s 后应为 90", d.ValidForS)
	}
	if left := d.ExpiresAt.Sub(clk.now()); left > types.FullWindow {
		t.Fatalf("交付期 %v 超出了满血窗口 %v", left, types.FullWindow)
	}
}

// B1 续：往返把窗口吃到不够用，这个槽位就该算失败，不许发一张马上过期的。
func TestExhaustedWindowIsNotDelivered(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModeSingle)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	mn.mintCost = 140 * time.Second // 剩 10 s，低于 minDeliverWindow

	if _, err := s.Deliver(context.Background(), 1, "", false); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v，窗口烧光了还交付", err)
	}
}

// B2：两个账号并发交付时互为铸票者，嵌套持锁就是 ABBA 死锁，必须都能返回。
func TestNoDeadlockBetweenAccounts(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	mn.delay = 10 * time.Millisecond
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	// 两个网关各有一张快过期的 pair，分别由对方铸的 ⇒ minterFor 会交叉选人：
	// A 的铸票者是 2、B 的铸票者是 1。
	for i, gw := range []string{"unified-1", "unified-2"} {
		region := strings.ToUpper(strings.TrimPrefix(gw, "unified-"))
		st.gws[gw] = types.Gateway{Name: gw, FirstSeenBy: int64(i + 1), ReachableExits: []types.Exit{{Region: region}}}
		mn.land[region] = gw
		mn.full[gw] = true
		st.pairs[gw] = types.Pair{Gateway: gw, Cookie: cookiePrefix + gw,
			ExpiresAt: clk.now().Add(time.Minute), MintedBy: int64(2 - i)}
	}
	// 确定性屏障：两个 goroutine 都拿到**第一把**锁之后才放行，
	// 嵌套持锁的实现到这一步必然互等。靠 sleep 撞时序是撞不出来的。
	barrier := make(chan struct{})
	var arrived int32
	st.afterLock = func(int64) {
		if k := atomic.AddInt32(&arrived, 1); k <= 2 {
			if k == 2 {
				close(barrier)
			}
			<-barrier
		}
	}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})

	done := make(chan error, 2)
	for _, id := range []int64{1, 2} {
		go func(id int64) {
			_, err := s.Deliver(context.Background(), id, "", false)
			done <- err
		}(id)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("并发交付死锁了：3 秒内没返回")
		}
	}
}

// B4：上游接受了旧 pair 就不下发新 cookie ⇒「续期」根本没发生，不许照样交付。
func TestUnrenewedStalePairIsNotDelivered(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	mn.noNewPair = true // 好 pair 不触发重新下发（SPEC 第 2 节）
	st.pairs["unified-1"] = types.Pair{
		Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(30 * time.Second), MintedBy: 1,
	}
	if _, err := s.Deliver(context.Background(), 1, "", false); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v，没续上的 pair 还是发出去了", err)
	}
}

// 一张 pair 同时发给两个账号是允许的，而且不该有任何「池子深度不足」的告警：
// 这条是 2026-10-01 实测的结论（两个号真并发带同一张票各打一发，4/4 都 200、
// 都没被换路由），详见 types.Pair 的注释。曾经的租约互斥就是按这个结论删掉的。
func TestPairIsSharedAcrossConsumers(t *testing.T) {
	s, st, _, _ := rig(t, types.ModeSingle)
	addAccount(st, 2, "acct-2")
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	var buf bytes.Buffer
	s.opt.Log = slog.New(slog.NewTextHandler(&buf, nil))

	a := mustDeliver(t, s, 1, "")
	b := mustDeliver(t, s, 2, "")
	if a.Gateway != "unified-1" || b.Gateway != "unified-1" {
		t.Fatalf("唯一的网关没能同时发给两个号: %s / %s", a.Gateway, b.Gateway)
	}
	if a.Cookie != b.Cookie {
		t.Fatalf("两个号拿到的不是同一张票: %q / %q", a.Cookie, b.Cookie)
	}
	// 同一个号再要一次也照样拿得到：没有任何东西在互斥。
	if again := mustDeliver(t, s, 1, ""); again.Gateway != "unified-1" {
		t.Fatalf("A 第二次拿到了 %s", again.Gateway)
	}
	if log := buf.String(); strings.Contains(log, "池子深度不足") {
		t.Fatalf("共享是正常状态，不该再告警: %s", log)
	}
}

// S1：调用方自己断了，不该把好槽位打进退避。
func TestClientCancelDoesNotBackoff(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModeSingle)
	mn.mintErr = context.Canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Deliver(ctx, 1, "", false); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v", err)
	}
	for gw := range st.gws {
		if sl, _ := st.Slot(1, gw); !sl.BackoffUntil.IsZero() {
			t.Fatalf("%s 因为调用方断连进了退避: %+v", gw, sl)
		}
	}
	// 而且断了就别再往下试其它槽位了，白烧供给。
	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("调用方已断还打了 %d 发", n)
	}
}

// S2：判降级只歇一个满血窗口，不进指数——那个判据有假阴。
func TestDegradedUsesShortHold(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	mn.full["unified-1"] = false
	for i := 0; i < 3; i++ {
		at := clk.now()
		if _, err := s.Deliver(context.Background(), 1, "unified-1", false); !errors.Is(err, ErrNoFullSlot) {
			t.Fatalf("第 %d 轮 err=%v", i, err)
		}
		sl, _ := st.Slot(1, "unified-1")
		if got := sl.BackoffUntil.Sub(at); got != types.FullWindow {
			t.Fatalf("第 %d 轮降级退避 %v，应恒为一个满血窗口 %v", i, got, types.FullWindow)
		}
		clk.add(types.FullWindow + time.Second)
	}
}

// S6：铸票超时也要记触碰——请求可能已经出站，窗口已经烧了。
func TestMintErrorStillRecordsTouch(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModeSingle)
	mn.mintErr = errors.New("i/o timeout")
	if _, err := s.Deliver(context.Background(), 1, "unified-1", false); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v", err)
	}
	sl, ok := st.Slot(1, "unified-1")
	if !ok || sl.LastTouch.IsZero() {
		t.Fatalf("超时没记触碰，下次会把烧过的槽位当新鲜的发出去: %+v", sl)
	}
}

// S9：池子模式只有一个号 = 红线 5。保温会烧掉它自己要用的窗口，所以不保温。
func TestPoolModeNeedsTwoAccounts(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModePool)
	var buf bytes.Buffer
	s.opt.Log = slog.New(slog.NewTextHandler(&buf, nil))
	s.markDemand()

	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("只有一个号却保温了 %d 发", n)
	}
	if !strings.Contains(buf.String(), "可用账号不足两个") {
		t.Fatalf("没留下为什么不保温的记录: %s", buf.String())
	}
	// 第二个号进来就该恢复保温。
	addAccount(st, 2, "acct-2")
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != types.SpareTarget {
		t.Fatalf("两个号了却保温了 %d 发，目标深度是 %d", n, types.SpareTarget)
	}
}

// ---------- 票充足时自动公开 ----------

// anonRig 搭一个有 n 张活 pair 的池子，门槛 min。
func anonRig(t *testing.T, n, min int) (*Scheduler, *fakeStore, *clock, *bytes.Buffer) {
	t.Helper()
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	for i := 0; i < n; i++ {
		gw := fmt.Sprintf("unified-%d", i+1)
		st.gws[gw] = types.Gateway{Name: gw, FirstSeenBy: 1, ReachableExits: []types.Exit{{Region: "US"}}}
		st.pairs[gw] = types.Pair{Gateway: gw, Cookie: cookiePrefix + gw,
			ExpiresAt: clk.now().Add(time.Hour), MintedBy: 1}
		mn.full[gw] = true
	}
	var buf bytes.Buffer
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		PublicWhenSurplus: true, MinFreeGateways: min,
		Log: slog.New(slog.NewTextHandler(&buf, nil))})
	return s, st, clk, &buf
}

// 空闲数达标就放行，并留下那行给运营方看的读数。
func TestAnonymousAllowedWhenSurplus(t *testing.T) {
	s, st, clk, buf := anonRig(t, 6, 6)
	if ps := s.Public(); !ps.Open || ps.Free != 6 {
		t.Fatalf("应开放: %+v", ps)
	}
	d, err := s.DeliverAnonymous(context.Background(), "acct-outsider", "", false)
	if err != nil {
		t.Fatalf("DeliverAnonymous: %v", err)
	}
	if d.Cookie == "" || d.ValidForS != 150 {
		t.Fatalf("交付内容不对: %+v", d)
	}
	// 没有他的凭据就做不了 state-echo，不能对外声称验过。
	if d.VerifiedFull {
		t.Fatal("匿名交付不该写 verified_full=true")
	}
	for _, want := range []string{"匿名交付", "gateway=" + d.Gateway, "free_gateways=6", "threshold=6"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("匿名交付日志缺了 %q: %s", want, buf.String())
		}
	}
	// 槽位按 (占位账号 × 网关) 记下了，而且占位账号没有凭据。
	var anon types.Account
	for _, a := range st.Accounts() {
		if a.Creds.AccountID == "anon:acct-outsider" {
			anon = a
		}
	}
	if anon.ID == 0 || anon.Creds.AccessToken != "" {
		t.Fatalf("占位账号不对: %+v", anon.Label)
	}
	if sl, _ := st.Slot(anon.ID, d.Gateway); sl.LastTouch.IsZero() {
		t.Fatal("匿名的槽位没记触碰，下次会把他烧过的网关再发给他")
	}
	// 他再要一次，应该换一个没烧过的网关。
	clk.add(types.DeliverTTL + time.Second)
	d2, err := s.DeliverAnonymous(context.Background(), "acct-outsider", "", false)
	if err != nil || d2.Gateway == d.Gateway {
		t.Fatalf("第二次又给了同一个网关 %s (err=%v)", d2.Gateway, err)
	}
}

// 空闲数不达标就拒，哪怕还剩好几张——上传者永远优先。
func TestAnonymousRefusedBelowThreshold(t *testing.T) {
	s, _, _, _ := anonRig(t, 5, 6)
	if ps := s.Public(); ps.Open {
		t.Fatalf("不该开放: %+v", ps)
	}
	if _, err := s.DeliverAnonymous(context.Background(), "x", "", false); !errors.Is(err, ErrPublicClosed) {
		t.Fatalf("err=%v，应为 ErrPublicClosed", err)
	}
	// 开关没开时，再富裕也不开放。
	off, _, _, _ := anonRig(t, 9, 1)
	off.opt.PublicWhenSurplus = false
	if ps := off.Public(); ps.Open || ps.Enabled {
		t.Fatalf("开关关着却开放了: %+v", ps)
	}
	if _, err := off.DeliverAnonymous(context.Background(), "x", "", false); !errors.Is(err, ErrPublicClosed) {
		t.Fatalf("err=%v", err)
	}
}

// 匿名只认「有活 pair」这一条，而门槛是它唯一的让路机制：
// 租约互斥按实测删掉之后（见 types.Pair），供给紧张时挡住匿名的就只剩 MinFreeGateways。
func TestAnonymousRespectsFreeThreshold(t *testing.T) {
	s, st, clk, _ := anonRig(t, 6, 1)
	// 让 5 张票剩余跌破 DeliverFloor，只剩 unified-6 还够交付。
	for i := 1; i <= 5; i++ {
		gw := fmt.Sprintf("unified-%d", i)
		p := st.pairs[gw]
		p.ExpiresAt = clk.now().Add(types.DeliverFloor - time.Minute)
		st.pairs[gw] = p
	}
	if free := s.FreeGateways(); free != 1 {
		t.Fatalf("空闲数算成了 %d，应为 1", free)
	}
	d, err := s.DeliverAnonymous(context.Background(), "x", "", false)
	if err != nil {
		t.Fatalf("DeliverAnonymous: %v", err)
	}
	if d.Gateway != "unified-6" {
		t.Fatalf("匿名拿到了票不够新鲜的 %s", d.Gateway)
	}
	// 最后一张也跌破门槛 ⇒ 空闲数 0 < 门槛 1 ⇒ 直接拒，哪怕池子里还有行。
	p := st.pairs["unified-6"]
	p.ExpiresAt = clk.now().Add(types.DeliverFloor - time.Minute)
	st.pairs["unified-6"] = p
	if _, err := s.DeliverAnonymous(context.Background(), "y", "", false); !errors.Is(err, ErrPublicClosed) {
		t.Fatalf("err=%v，空闲跌破门槛时应 ErrPublicClosed", err)
	}
}

// 匿名占了一张之后，上传者照样拿得到槽位。
//
// **刻意不要求换一张**：匿名和上传者拿到同一个网关是允许的（一张 pair 可以同时给几个号
// 用，见 types.Pair 的 2026-10-01 实测）。这里要守的是「上传者不会因为匿名先到而拿不到」。
func TestUploaderStillServedAfterAnonymous(t *testing.T) {
	s, st, _, _ := anonRig(t, 6, 1)
	addAccount(st, 2, "acct-2")
	if _, err := s.DeliverAnonymous(context.Background(), "x", "", false); err != nil {
		t.Fatalf("DeliverAnonymous: %v", err)
	}
	up, err := s.Deliver(context.Background(), 2, "", false)
	if err != nil {
		t.Fatalf("上传者被匿名挤掉了: %v", err)
	}
	if !up.VerifiedFull {
		t.Fatal("上传者那条路还是要验满血的")
	}
}

// 匿名报一个上传者的上游 ID，不能污染那个上传者的账本。
func TestAnonymousCannotPolluteUploaderLedger(t *testing.T) {
	s, st, _, _ := anonRig(t, 6, 1)
	if _, err := s.DeliverAnonymous(context.Background(), "acct-1", "", false); err != nil {
		t.Fatalf("DeliverAnonymous: %v", err)
	}
	for _, sl := range st.slots {
		if sl.AccountID == 1 {
			t.Fatalf("匿名把上传者 1 号的槽位写了: %+v", sl)
		}
	}
}

// 出口要不到就老实失败，别拿一个没绑定的 sid 去打——铸票和 echo 会落在不同 IP 上。
func TestNoStickyExitFails(t *testing.T) {
	s, _, mn, _ := rig(t, types.ModeSingle)
	mn.sidErr = errors.New("没有可用的粘性供应商")
	if _, err := s.Deliver(context.Background(), 1, "", false); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v", err)
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("没有粘性出口还打了 %d 发", n)
	}
}

// ---------- 强制刷新 ----------

// 普通路径发歇得最久的那张，不管他手上正拿着谁。
func TestNormalPathPicksLongestRested(t *testing.T) {
	s, st, _, clk := rig(t, types.ModeSingle)
	seedHeldGateway(st, clk)
	if d := mustDeliver(t, s, 1, ""); d.Gateway != "unified-2" {
		t.Fatalf("普通路径给了 %s，应是歇得最久的 unified-2", d.Gateway)
	}
}

// force=1 必须换一个网关：调用方就是因为手上那张不行了才来的。
// 「手上那张」靠槽位 LastTouch 认（租约字段已按 2026-10-01 的实测删掉，见 types.Pair）。
func TestForceSkipsHeldGateway(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	seedHeldGateway(st, clk)

	// 点名他正拿着的那个 + force ⇒ 没得换，只能 503。这一条只有 holding() 真生效才过。
	if _, err := s.Deliver(context.Background(), 1, "unified-1", true); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v，force 点名自己手上那张时应 ErrNoFullSlot", err)
	}
	echoesBefore := mn.echoes
	forced, err := s.Deliver(context.Background(), 1, "", true)
	if err != nil {
		t.Fatalf("force: %v", err)
	}
	if forced.Gateway != "unified-2" {
		t.Fatalf("force 给了 %s，应让开他手上的 unified-1、换到 unified-2", forced.Gateway)
	}
	// 换来的那张照样验过满血——force 不是「不验了」的意思。
	if !forced.VerifiedFull || mn.echoes != echoesBefore+1 {
		t.Fatalf("force 那次没走 state-echo: verified=%v echoes %d→%d",
			forced.VerifiedFull, echoesBefore, mn.echoes)
	}
}

// seedHeldGateway 让 1 号手上正拿着 unified-1（刚碰过），unified-2 则歇了三小时。
func seedHeldGateway(st *fakeStore, clk *clock) {
	now := clk.now()
	delete(st.gws, "unified-3")
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1", LastTouch: now}
	st.slots[slotKey(1, "unified-2")] = types.Slot{AccountID: 1, Gateway: "unified-2", LastTouch: now.Add(-3 * time.Hour)}
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: now.Add(time.Hour), MintedBy: 1}
}

// 没得换就 503：把原来那张再发一遍等于没换，客户端会死循环。
func TestForceWithNothingElseFails(t *testing.T) {
	s, st, _, _ := rig(t, types.ModeSingle)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")

	mustDeliver(t, s, 1, "")
	if _, err := s.Deliver(context.Background(), 1, "", true); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v，没得换时应 ErrNoFullSlot", err)
	}
}

// 匿名那条路也认 force。
func TestForceForAnonymous(t *testing.T) {
	s, _, _, _ := anonRig(t, 6, 1)
	first, err := s.DeliverAnonymous(context.Background(), "x", "", false)
	if err != nil {
		t.Fatalf("DeliverAnonymous: %v", err)
	}
	forced, err := s.DeliverAnonymous(context.Background(), "x", "", true)
	if err != nil {
		t.Fatalf("force: %v", err)
	}
	if forced.Gateway == first.Gateway {
		t.Fatalf("匿名 force 还是给了同一个网关 %s", forced.Gateway)
	}
	// 指名要自己正租着的那张 + force ⇒ 没得换，老实失败。
	if _, err := s.DeliverAnonymous(context.Background(), "x", forced.Gateway, true); err == nil {
		t.Fatal("指名自己租着的那张还 force，居然成功了")
	}
}

// ---------- 续期的选人与节流 ----------

// 续期专挑在该网关上**已经烧了**的账号：烧已烧的免费，烧好的要赔一个满血窗口。
func TestRenewPrefersBurnedAccount(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2") // full：最后才动
	addAccount(st, 3, "acct-3") // degraded：烧它免费
	addAccount(st, 4, "acct-4") // 没记录：次选
	gw := st.gws["unified-1"]
	now := clk.now()
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1", LastVerdict: "full", LastTouch: now}
	st.slots[slotKey(2, "unified-1")] = types.Slot{AccountID: 2, Gateway: "unified-1", LastVerdict: "full", LastTouch: now}
	st.slots[slotKey(3, "unified-1")] = types.Slot{AccountID: 3, Gateway: "unified-1", LastVerdict: "degraded", LastTouch: now}

	a, ok := s.minterFor(gw, 0, 0)
	if !ok || a.ID != 3 {
		t.Fatalf("挑了 %d，应挑账本上已降级的 3 号", a.ID)
	}
	// 没有已烧的，就挑没记录的，仍然不动「好」的。
	delete(st.slots, slotKey(3, "unified-1"))
	st.slots[slotKey(3, "unified-1")] = types.Slot{AccountID: 3, Gateway: "unified-1", LastVerdict: "full", LastTouch: now}
	if a, _ := s.minterFor(gw, 0, 0); a.ID != 4 {
		t.Fatalf("挑了 %d，应挑没有记录的 4 号", a.ID)
	}
	// 全是「好」的也得续：pair 死了损失更大，不能因为舍不得就不续。
	st.slots[slotKey(4, "unified-1")] = types.Slot{AccountID: 4, Gateway: "unified-1", LastVerdict: "full", LastTouch: now}
	if _, ok := s.minterFor(gw, 0, 0); !ok {
		t.Fatal("全是 full 就不续了，pair 会直接死掉")
	}
	_ = mn
}

// 同一档内挑最久没被用作铸票者的那个：别把续期全压在一个号上。
func TestRenewSpreadsAcrossAccounts(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	addAccount(st, 3, "acct-3")
	s.markDemand()
	// 目标深度 = 最忙的号碰过的网关数 + SpareTarget。让 1 号碰过一个，目标就是 3，
	// 三个网关各要一发，正好看得出摊不摊得开。
	st.slots[slotKey(1, "unified-3")] = types.Slot{AccountID: 1, Gateway: "unified-3",
		LastTouch: clk.now(), LastDeliver: clk.now()}

	// 三个网关、三个号：每个号各领一个，不该全压在一个号上。
	s.warmOnce(context.Background())
	mn.mu.Lock()
	used := map[int64]int{}
	for _, c := range mn.mints {
		used[c.AccountID]++
	}
	mn.mu.Unlock()
	if len(used) != 3 {
		t.Fatalf("三发续期只摊到 %d 个号上: %v", len(used), used)
	}
}

// 账号级节流：同一个号两次续期之间要隔够。
func TestRenewThrottlePerAccount(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.opt.RenewGap = 30 * time.Second
	s.markDemand()
	// 目标深度要够三个网关才看得出「第三个等下一轮」：让 1 号拿过一张票，目标 = 1+2。
	st.slots[slotKey(1, "unified-3")] = types.Slot{AccountID: 1, Gateway: "unified-3",
		LastTouch: clk.now(), LastDeliver: clk.now()}

	// 两个号、三个网关：节流下这一轮最多续两发，第三个网关要等下一轮。
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 2 {
		t.Fatalf("节流没生效，一轮续了 %d 发", n)
	}
	// 还没过节流窗口 ⇒ 一发都不该再续。
	clk.add(20 * time.Second)
	s.markDemand()
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 2 {
		t.Fatalf("节流窗口内又续了 %d 发", n-2)
	}
	// 过了就恢复。
	clk.add(15 * time.Second)
	s.markDemand()
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n <= 2 {
		t.Fatal("过了节流窗口还是不续")
	}
}

// slotOf 取一个槽位，取不到就直接 fail——断言里不该出现零值误判成「判定被清空」。
func slotOf(t *testing.T, st *fakeStore, acct int64, gw string) types.Slot {
	t.Helper()
	sl, ok := st.Slot(acct, gw)
	if !ok {
		t.Fatalf("槽位 %d|%s 不存在", acct, gw)
	}
	return sl
}

// 上游否决这个账号（400/401/403）就当场停用：保温/续期是个循环，
// 不停用它会一直按这个节奏白打下去。
func TestUpstreamRejectionDisablesAccount(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModeSingle)
	mn.mintStatus = 400
	mn.mintBody = `{"detail":"The 'gpt-6-astra' model is not supported when using Codex with a ChatGPT account."}`

	if _, err := s.Deliver(context.Background(), 1, "unified-1", false); err == nil {
		t.Fatal("上游回 400 居然交付成功了")
	}
	if len(st.disabled) != 1 {
		t.Fatalf("没停用账号: %v", st.disabled)
	}
	// 理由里必须带上上游原话，否则页面上只有一个「已停用」，没人知道为什么。
	if !strings.Contains(st.disabled[0], "not supported") {
		t.Fatalf("停用理由里没有上游原话: %q", st.disabled[0])
	}
	for _, a := range st.Accounts() {
		if a.ID == 1 && a.Enabled {
			t.Fatal("账本里还是启用状态")
		}
	}
}

// 429 是「现在不行」，不是「这个账号不行」。停用一个被限流的号
// 等于把一次限流变成永久损失。
func TestRateLimitDoesNotDisableAccount(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModeSingle)
	mn.mintStatus = 429

	if _, err := s.Deliver(context.Background(), 1, "unified-1", false); err == nil {
		t.Fatal("上游回 429 居然交付成功了")
	}
	if len(st.disabled) != 0 {
		t.Fatalf("429 把账号停用了: %v", st.disabled)
	}
}

// 一次交付不许把整池供给摸干净：每试一个网关都要真打一发上游验满血，
// 试过的槽位不管结果都烧掉了。试满上限就认 503，把剩下的留给下一个人。
func TestDeliverStopsAfterMaxTries(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModePool)
	// 六个网关，全都验不出满血（rig 自带三个，这里补到六个）。
	for i := 4; i <= 6; i++ {
		gw := fmt.Sprintf("unified-%d", i)
		region := fmt.Sprintf("R%d", i)
		st.gws[gw] = types.Gateway{Name: gw, FirstSeenBy: 1,
			ReachableExits: []types.Exit{{Region: region}}}
		mn.land[region] = gw
	}
	for gw := range st.gws {
		mn.full[gw] = false
	}
	s.opt.MaxTriesPerDeliver = 3

	if _, err := s.Deliver(context.Background(), 1, "", false); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v", err)
	}
	// 每试一个网关 = 一发铸票。试了 3 个就该停，不该把六个全摸一遍。
	if n := len(mn.mintedGateways()); n != 3 {
		t.Fatalf("试了 %d 个网关，上限是 3 —— 不设上限的话一个消费者会把整池摸空", n)
	}
}

// 门槛不是随手填的数：它是「一个账号能不停转」所需的网关数，
// = 冷却 ÷ 单张可用时长。写死一个小数字只会让池子在供给远不够时就对外开放。
func TestPublicThresholdIsDerivedFromCooldown(t *testing.T) {
	want := int(types.SlotCooldown / types.DeliverTTL)
	if got := types.GatewaysForContinuous(); got != want {
		t.Fatalf("门槛算成了 %d，按 %v ÷ %v 应是 %d", got, types.SlotCooldown, types.DeliverTTL, want)
	}
	// 量级检查：一张票只管几分钟而冷却是几小时，答案必然是几十上百，不可能是个位数。
	if n := types.GatewaysForContinuous(); n < 20 {
		t.Fatalf("门槛 %d 太小了：%v 的冷却除以 %v 的票长不该得出个位数",
			n, types.SlotCooldown, types.DeliverTTL)
	}
}

// 目标深度 = 最忙那个号**用过**的 pair 数 + SpareTarget，而且不能自激：
// 预铸那一发本身也会 touch 槽位，拿 LastTouch 当基数就会一路把所有网关铸满。
func TestPairTargetTracksBusiestConsumer(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.markDemand()

	if got := s.PairTarget(); got != types.SpareTarget {
		t.Fatalf("没人用过票时目标应是 %d，实为 %d", types.SpareTarget, got)
	}
	s.warmOnce(context.Background())
	first := len(mn.mintedGateways())
	if first != types.SpareTarget {
		t.Fatalf("第一轮铸了 %d 张，目标 %d", first, types.SpareTarget)
	}
	// 关键的一条：铸票写的是 LastTouch，目标**不许**因此上涨。
	if got := s.PairTarget(); got != types.SpareTarget {
		t.Fatalf("预铸把目标顶高到 %d，自激了", got)
	}

	// 1 号真的拿到过一张票，目标才该 +1。
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1",
		LastTouch: clk.now(), LastDeliver: clk.now()}
	if got, want := s.PairTarget(), types.SpareTarget+1; got != want {
		t.Fatalf("用掉一张后目标应是 %d，实为 %d", want, got)
	}
}

// 续期失败预算：同一张 pair 连败 RenewFailBudget 次就把它丢掉，下一轮改裸铸。
// **网关不许被拉黑**——续不上的是这张票，别的号随时可能在这个网关上铸出新的。
func TestDeadPairDroppedAfterRenewFailures(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.markDemand()

	// 先给每个网关一张快到期的票，这样保温走的是「续期」而不是裸铸。
	for name := range st.gws {
		st.pairs[name] = types.Pair{Gateway: name, Cookie: "__cflb=x; __oailb=y",
			ExpiresAt: clk.now().Add(types.RenewLead / 2)}
	}
	mn.mintErr = errors.New("上游不理")

	for i := 0; i < types.RenewFailBudget; i++ {
		s.warmOnce(context.Background())
		clk.add(2 * time.Minute)
		s.markDemand()
	}
	if n := len(st.pairs); n != 0 {
		t.Fatalf("连败 %d 轮后死票还留着 %d 张: %+v", types.RenewFailBudget, n, st.pairs)
	}
	// 预算用掉就归零，别让它一路累加到下一张票头上。
	for name, gw := range st.gws {
		if gw.RenewFails != 0 {
			t.Fatalf("%s 的失败计数没归零: %d", name, gw.RenewFails)
		}
	}
	// 网关本身照旧是候选：这里只要求它不被静默排除，交付成不成是满血判定的事。
	mn.mintErr = nil
	if _, err := s.Deliver(context.Background(), 1, "unified-1", false); err != nil {
		t.Fatalf("网关被拉黑了: %v", err)
	}
}

// 全员冷却就不预铸：两个号在三个网关上都刚取过票，铸出来的票发给谁都是降级的。
func TestNoPremintWhileEveryoneCooling(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	var buf bytes.Buffer
	s.opt.Log = slog.New(slog.NewTextHandler(&buf, nil))
	s.markDemand()
	for _, id := range []int64{1, 2} {
		for gw := range st.gws {
			st.slots[slotKey(id, gw)] = types.Slot{AccountID: id, Gateway: gw,
				LastDeliver: clk.now().Add(-types.SlotCooldown + time.Minute)}
		}
	}

	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("全员冷却却预铸了 %d 发", n)
	}
	if !strings.Contains(buf.String(), "都还在冷却") {
		t.Fatalf("没留下为什么不预铸的记录: %s", buf.String())
	}
	// 一个槽位出了冷却就该恢复预铸。
	st.slots[slotKey(2, "unified-2")] = types.Slot{AccountID: 2, Gateway: "unified-2",
		LastDeliver: clk.now().Add(-types.SlotCooldown)}
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n == 0 {
		t.Fatal("有号出了冷却却还是不预铸")
	}
}

// 失败的续期不该把预铸闸关掉：它写 LastTouch 不写 LastDeliver。
// 这是 TestDeadPairDroppedAfterRenewFailures 偶发失败的根因，单独钉一遍。
func TestFailedRenewDoesNotCloseThePremintGate(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.markDemand()
	mn.mintErr = errors.New("上游 500")

	s.warmOnce(context.Background())
	if s.premintWasted() {
		t.Fatal("一轮失败的续期就把预铸闸关了，死 pair 永远攒不满失败预算")
	}
}

// 外部消费者在取票时冷却闸不适用：pair 是纯路由，它在这些网关上多半还是首次接触。
func TestPremintKeepsServingExternalConsumers(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.markDemand()
	for _, id := range []int64{1, 2} {
		for gw := range st.gws {
			st.slots[slotKey(id, gw)] = types.Slot{AccountID: id, Gateway: gw,
				LastDeliver: clk.now()}
		}
	}
	// 99 号不在 usable() 里（没凭据的外部/匿名占位行），但它确实在取票。
	st.slots[slotKey(99, "unified-1")] = types.Slot{AccountID: 99, Gateway: "unified-1",
		LastDeliver: clk.now()}

	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n == 0 {
		t.Fatal("有外部消费者在取票却停了预铸")
	}
}

// 503 要把**真实原因**带出去：「没有验过满血的槽位可交付」会把「你的槽位都烧完了」
// 和「出口全熔断了、谁都铸不出票」说成同一件事，而这两者要做的事完全相反。
// 2026-10-01 现网就吃过这个：页面上只看到前者，真实原因（没有可用的出口代理）只在日志里。
func TestDeliverCarriesTheRealCause(t *testing.T) {
	s, _, mn, _ := rig(t, types.ModeSingle)
	mn.mintErr = errors.New("要不到粘性出口: 没有可用的出口代理（sticky=true，共 2 条）")

	_, err := s.Deliver(context.Background(), 1, "", false)
	if !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("调用方认不出该回 503: %v", err)
	}
	if !strings.Contains(err.Error(), "没有可用的出口代理") {
		t.Fatalf("真实原因丢了: %v", err)
	}
	if !errors.Is(err, mn.mintErr) {
		t.Fatalf("底层错误没串起来，上层没法按类型分流: %v", err)
	}
}

// 铸票和 state-echo 那两条路也要把原因落进样本：出口熔断那几分钟
// samples.jsonl 里只剩一串 status=0 的空行，事后什么都查不出来。
func TestFailedMintRecordsCauseInSample(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModeSingle)
	mn.mintErr = errors.New("经 novproxy 请求失败: socks connect refused")

	_, _ = s.Deliver(context.Background(), 1, "", false)
	var failed, withErr int
	for _, sm := range st.samples {
		if sm.Status == 200 {
			if sm.Err != "" {
				t.Fatalf("成功的样本也填了原因，账本分不清哪行真出过问题: %+v", sm)
			}
			continue
		}
		failed++
		if strings.Contains(sm.Err, "connect refused") {
			withErr++
		}
	}
	if failed == 0 {
		t.Fatal("一条失败样本都没落，交付路径没在记账")
	}
	if withErr == 0 {
		t.Fatalf("%d 条失败样本全是空原因", failed)
	}
}
