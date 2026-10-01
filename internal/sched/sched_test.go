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

	"gwpool/internal/config"
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
	mints    map[string]types.RegionMint // (账号 × 区域) 终身台账，键同 slotKey
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
		mints:  map[string]types.RegionMint{},
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

// UpsertGateway 照 store 的语义：落点入表，顺带把请求过的出口补进 ReachableExits。
func (f *fakeStore) UpsertGateway(name string, seenBy int64, requested types.Exit) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, had := f.gws[name]
	if !had {
		g = types.Gateway{Name: name, FirstSeenBy: seenBy, FirstSeenAt: f.clk.now()}
	}
	known := false
	for _, e := range g.ReachableExits {
		if e == requested {
			known = true
		}
	}
	if !known && requested.Region != "" {
		g.ReachableExits = append(g.ReachableExits, requested)
	}
	f.gws[name] = g
	return !had, nil
}

func (f *fakeStore) RegionMints() map[string]types.RegionMint {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]types.RegionMint, len(f.mints))
	for k, v := range f.mints {
		out[k] = v
	}
	return out
}

func (f *fakeStore) NoteRegionMint(accountID int64, regionKey string, at time.Time) error {
	if regionKey == "" {
		return nil // 照 store 的语义：画不到九宫格上的格子不记
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k := slotKey(accountID, regionKey)
	m := f.mints[k]
	m.AccountID, m.RegionKey = accountID, regionKey
	m.Count++
	m.LastAt = at
	f.mints[k] = m
	return nil
}

// UpdateSlot 照真实 store 的语义：读-改-写在同一把写锁里，不给并发丢更新的机会。
func (f *fakeStore) UpdateSlot(accountID int64, gateway string, fn func(*types.Slot)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := slotKey(accountID, gateway)
	sl := f.slots[k]
	fn(&sl)
	// 照真实 store：键是由参数算的，两个身份字段也由参数定，不让改写函数把行写到
	// 别人名下，也不会留下一行没有网关名的槽位（那行会被当候选挑中，而它谁也不是）。
	sl.AccountID, sl.Gateway = accountID, gateway
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

	// rateUsed / rateLimit 是速率闸的读数。默认 limit=0 = 「不设限」，
	// 于是不碰配额的用例不用管它（生产的 mint.perRegionHour 永远回正数）。
	// ratePerAcct 按上游账号 ID 覆盖 rateUsed，测「本小时总发数最少的优先」那条规则用。
	rateUsed    map[string]int
	ratePerAcct map[string]map[string]int
	rateLimit   int
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
	case "acct-3":
		return 3
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
	// 借 Err 这一列记「这一发带没带 pair 进去」：裸铸和续期的区别只在这个参数上，
	// 而「交付路径不许带着一张快到点的 pair 去续」正是要钉住的那条（S-2）。
	m.mints[len(m.mints)-1].Err = sentPair
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

// Rate 照真实 mint 的语义：按区域报本小时用量，外加每区上限。
func (m *fakeMinter) Rate(acct string) (map[string]int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	src := m.rateUsed
	if per, ok := m.ratePerAcct[acct]; ok {
		src = per
	}
	out := make(map[string]int, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out, m.rateLimit
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
	// 这组用例测的是**两个开关都打开**那条路：交付前验满血、保温走续期。
	// 两个开关默认都关（见 config.Config 的字段注释），默认那条路由
	// TestDeliverWithoutVerifyNeverEchoes / TestPremintBareDoesNotRenew 盯着。
	s := New(st, mn, Options{Mode: mode, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) {
			c.VerifyFullAtDeliver = true
			c.RenewPairs = true
		})})
	return s, st, mn, clk
}

// liveCfg 搭一份不落盘的活配置：config.NewLive 不要求有文件，改旋钮不用造配置文件。
func liveCfg(set func(*config.Config)) *config.Live {
	c := config.Defaults()
	if set != nil {
		set(&c)
	}
	return config.NewLive("", c)
}

// verifyOn / legacyOn 是 liveCfg 的两个常用改法。
// 两个开关都默认关，所以「测的是开着那条路」的用例要自己打开。
func verifyOn(c *config.Config) { c.VerifyFullAtDeliver = true }

// seedPairs 给每个已知网关一张新鲜的 pair。
//
// 交付**硬筛**掉没有活 pair 的网关（省一发请求加一个铸票者槽位），所以凡是要测
// 「在多个网关之间怎么挑」的用例都得先把票铺好；一张票都没有时走的是另一条路
// （回落只抽一次签，见 TestFreshMintDrawsOnlyOnce）。
func seedPairs(st *fakeStore, clk *clock) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for name := range st.gws {
		st.pairs[name] = types.Pair{Gateway: name, Cookie: cookiePrefix + name,
			ExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 1}
	}
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
	d, err := s.Deliver(context.Background(), acctID, Request{Gateway: hint})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	return d
}

// ---------- 用例 ----------

// 取的必须是歇得最久的那个槽位，而不是随便一个网关。
func TestDeliverPicksLongestRested(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	seedPairs(st, clk)
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
	// 交付后该槽位已被烧过：窗口过完之后必须换成次久的那个。
	// （窗口**之内**再问是原样再发同一张，见 TestRedeliverWithinWindow。）
	clk.add(types.DeliverTTL + time.Second)
	if d2 := mustDeliver(t, s, 1, ""); d2.Gateway != "unified-3" {
		t.Fatalf("第二次交付 %s，应轮到 unified-3", d2.Gateway)
	}
}

// 没有触碰记录的槽位（从没碰过）优先级最高。
func TestNeverTouchedWinsOverRested(t *testing.T) {
	s, st, _, clk := rig(t, types.ModeSingle)
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1", LastTouch: clk.now().Add(-99 * time.Hour)}
	delete(st.gws, "unified-3")
	seedPairs(st, clk)
	// unified-2 没有任何记录 ⇒ 零值 LastTouch ⇒ 比歇了 99 小时的还靠前。
	if d := mustDeliver(t, s, 1, ""); d.Gateway != "unified-2" {
		t.Fatalf("交付 %s，从未触碰的 unified-2 该优先", d.Gateway)
	}
}

// 判降级就换下一个槽位，不交付降智的东西。
func TestDegradedFallsThrough(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	seedPairs(st, clk)
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
	s, st, mn, clk := rig(t, types.ModeSingle)
	seedPairs(st, clk)
	// 默认只试 2 个（每试一个都要花一发上游），这里要看「三个都试过」所以放开。
	s.opt.MaxTriesPerDeliver = 3
	for gw := range st.gws {
		mn.full[gw] = false
	}
	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
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
	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err = %v，应为 ErrNoFullSlot", err)
	}
	if mn.echoes != 0 {
		t.Fatalf("铸票都失败了还 echo 了 %d 发", mn.echoes)
	}
	if sl, _ := st.Slot(1, "unified-1"); sl.BackoffUntil.IsZero() {
		t.Fatal("铸票失败的槽位没进退避")
	}
}

// 硬失败的退避必须翻倍且封顶在 backoffMax。
func TestBackoffDoublesAndCaps(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	mn.mintErr = errors.New("socks5 连不上")
	// 按常量算，别把数字写死：封顶值 2026-10-01 从 20 分钟压到 1 分钟，
	// 写死的话这个测试只会在改常量时红一次，证明不了「翻倍 + 封顶」这件事本身。
	want := []time.Duration{backoffBase, 2 * backoffBase, 4 * backoffBase,
		backoffMax, backoffMax}
	for i, w := range want {
		at := clk.now()
		if _, err := s.Deliver(context.Background(), 1, Request{Gateway: "unified-1"}); !errors.Is(err, ErrNoFullSlot) {
			t.Fatalf("第 %d 轮 err = %v", i, err)
		}
		sl, _ := st.Slot(1, "unified-1")
		if got := sl.BackoffUntil.Sub(at); got != w {
			t.Fatalf("第 %d 轮退避 %v，应为 %v", i, got, w)
		}
		// 等退避过期**再加一个满血窗口**：失败那一发按保守方向记了触碰
		// （mintOnce 读不出落点时按想去的地方记），而窗口闸会把这个槽位按住
		// types.FullWindow —— 不等它过去的话下一轮根本走不到铸票，
		// 退避也就不会翻倍（见 coolingSet 闸 2）。
		clk.add(types.FullWindow + w + time.Second)
	}
	// 退避期内不该再选它：唯一候选在退避中 ⇒ 直接 ErrNoFullSlot，一发都不打。
	before := len(mn.mintedGateways())
	s.bumpBackoff(1, "unified-1")
	if _, err := s.Deliver(context.Background(), 1, Request{Gateway: "unified-1"}); !errors.Is(err, ErrNoFullSlot) {
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
	// 按 DeliverFloor 算（不是 RenewLead）：livePairs 数的是「还够不够交付」的票。
	clk.add(types.PairLifetime - types.DeliverFloor + time.Minute)
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
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(verifyOn)})

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
	// 窗口内再要一次：原样再发同一张，**谁都不劳烦**（一发上游都不打）。
	again := mustDeliver(t, s, 2, "")
	if again.Gateway != d.Gateway || again.Cookie != d.Cookie {
		t.Fatalf("窗口内重复请求没拿到同一张: %+v", again)
	}
	mn.mu.Lock()
	n := len(mn.mints)
	mn.mu.Unlock()
	if n != 2 {
		t.Fatalf("窗口内重复请求又打了上游，流水共 %d 发", n)
	}
	// 窗口过完：这个 (账号 × 网关) 进冷却，而池子里只有这一个网关 ⇒ 老实 503。
	clk.add(types.DeliverTTL + time.Second)
	if _, err := s.Deliver(context.Background(), 2, Request{}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("窗口过完、唯一的网关在冷却里，却还发得出票: %v", err)
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
			_, _ = s.Deliver(context.Background(), 1, Request{})
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

	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
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
	s := New(st, mn, Options{Mode: types.ModeSingle, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(verifyOn)})

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

// 剩余不足 DeliverFloor 的 pair 不能直接交付，要**裸铸**一张顶上。
//
// 不是「续期」：带一张还有效的 pair 进去上游会接受它、不下发新 cookie（SPEC 第 2 节），
// 而走到这条路的前提恰好是「还有效但太短」⇒ 必然换不回新票（见 pairFor）。
func TestStalePairIsRemintedNotRenewed(t *testing.T) {
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
		if _, err := s.Deliver(context.Background(), id, Request{}); err == nil {
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

	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
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
	// 两个号各刚烧过一个网关 ⇒ 冷却过滤让他们挑到**不同**的网关。
	// 不铺这两行的话两个人会挑同一个，然后第二个卡在网关锁上根本走不到账号锁，
	// 屏障永远等不满——测不出 ABBA，只测出「网关锁确实是串行的」。
	now := clk.now()
	st.slots[slotKey(1, "unified-1")] = types.Slot{AccountID: 1, Gateway: "unified-1", LastTouch: now}
	st.slots[slotKey(2, "unified-2")] = types.Slot{AccountID: 2, Gateway: "unified-2", LastTouch: now}
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
	// 开着验证才有「铸票者锁 → 消费者锁」这两段，ABBA 才有得可死。
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(verifyOn)})

	done := make(chan error, 2)
	for _, id := range []int64{1, 2} {
		go func(id int64) {
			_, err := s.Deliver(context.Background(), id, Request{})
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

// S-2：交付的回落路径**不带**那张快到点的 pair 去续，改成裸铸。
//
// 为什么非这样不行：走到回落的前提是「所有票都低于交付门槛」，也就是剩余在
// 0~floor 之间、**仍然有效**——而有效的 pair 上游会接受且不下发新 cookie
// （SPEC 第 2 节），于是手里还是那张快到点的，finish 拒发 ⇒ 消费者 503，
// 而请求和铸票者的一个槽位已经花掉了，还顺手给消费者记一次不属于它的退避。
// 裸铸则一定拿回一张新的 65 分钟票（SPEC 第 6 节说的就是这种情形）。
func TestFallbackMintGoesBareNotRenew(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	mn.noNewPair = true // 好 pair 不触发重新下发（SPEC 第 2 节）
	st.pairs["unified-1"] = types.Pair{
		Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(30 * time.Second), MintedBy: 1,
	}
	d, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("回落该裸铸出一张新票，却 %v", err)
	}
	for _, m := range mn.mints {
		if m.Err != "" {
			t.Fatalf("回落那一发带了 pair 进去（%q）：带有效 pair 上游不下发新 cookie，必然续不上", m.Err)
		}
	}
	if left := st.pairs["unified-1"].ExpiresAt.Sub(clk.now()); left < types.DeliverFloor {
		t.Fatalf("裸铸之后 pair 只剩 %v，没拿回新的 65 分钟票", left)
	}
	if d.ValidForS <= 0 {
		t.Fatalf("交付的有效期不对: %+v", d)
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

	if _, err := s.Deliver(ctx, 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
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
		if _, err := s.Deliver(context.Background(), 1, Request{Gateway: "unified-1"}); !errors.Is(err, ErrNoFullSlot) {
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
	if _, err := s.Deliver(context.Background(), 1, Request{Gateway: "unified-1"}); !errors.Is(err, ErrNoFullSlot) {
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
	// 开关和门槛在**配置**里，不在 Options：Public() 现读它，于是管理页改完立即生效。
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) {
			c.VerifyFullAtDeliver = true
			c.PublicWhenSurplus.Enabled = true
			c.PublicWhenSurplus.MinFreeGateways = min
		}),
		Log: slog.New(slog.NewTextHandler(&buf, nil))})
	return s, st, clk, &buf
}

// 空闲数达标就放行，并留下那行给运营方看的读数。
func TestAnonymousAllowedWhenSurplus(t *testing.T) {
	s, st, clk, buf := anonRig(t, 6, 6)
	if ps := s.Public(); !ps.Open || ps.Free != 6 {
		t.Fatalf("应开放: %+v", ps)
	}
	d, err := s.DeliverAnonymous(context.Background(), "acct-outsider", Request{})
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
	d2, err := s.DeliverAnonymous(context.Background(), "acct-outsider", Request{})
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
	if _, err := s.DeliverAnonymous(context.Background(), "x", Request{}); !errors.Is(err, ErrPublicClosed) {
		t.Fatalf("err=%v，应为 ErrPublicClosed", err)
	}
	// 开关没开时，再富裕也不开放。改的是**活配置**：Public() 现读它。
	off, _, _, _ := anonRig(t, 9, 1)
	offCfg := off.opt.Live.Get()
	offCfg.PublicWhenSurplus.Enabled = false
	off.opt.Live = config.NewLive("", offCfg)
	if ps := off.Public(); ps.Open || ps.Enabled {
		t.Fatalf("开关关着却开放了: %+v", ps)
	}
	if _, err := off.DeliverAnonymous(context.Background(), "x", Request{}); !errors.Is(err, ErrPublicClosed) {
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
	d, err := s.DeliverAnonymous(context.Background(), "x", Request{})
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
	if _, err := s.DeliverAnonymous(context.Background(), "y", Request{}); !errors.Is(err, ErrPublicClosed) {
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
	if _, err := s.DeliverAnonymous(context.Background(), "x", Request{}); err != nil {
		t.Fatalf("DeliverAnonymous: %v", err)
	}
	up, err := s.Deliver(context.Background(), 2, Request{})
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
	if _, err := s.DeliverAnonymous(context.Background(), "acct-1", Request{}); err != nil {
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
	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
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
	if _, err := s.Deliver(context.Background(), 1, Request{Gateway: "unified-1", Force: true}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v，force 点名自己手上那张时应 ErrNoFullSlot", err)
	}
	echoesBefore := mn.echoes
	forced, err := s.Deliver(context.Background(), 1, Request{Force: true})
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
	if _, err := s.Deliver(context.Background(), 1, Request{Force: true}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v，没得换时应 ErrNoFullSlot", err)
	}
}

// 匿名那条路也认 force。
func TestForceForAnonymous(t *testing.T) {
	s, _, _, _ := anonRig(t, 6, 1)
	first, err := s.DeliverAnonymous(context.Background(), "x", Request{})
	if err != nil {
		t.Fatalf("DeliverAnonymous: %v", err)
	}
	forced, err := s.DeliverAnonymous(context.Background(), "x", Request{Force: true})
	if err != nil {
		t.Fatalf("force: %v", err)
	}
	if forced.Gateway == first.Gateway {
		t.Fatalf("匿名 force 还是给了同一个网关 %s", forced.Gateway)
	}
	// 指名要自己正租着的那张 + force ⇒ 没得换，老实失败。
	if _, err := s.DeliverAnonymous(context.Background(), "x", Request{Gateway: forced.Gateway, Force: true}); err == nil {
		t.Fatal("指名自己租着的那张还 force，居然成功了")
	}
}

// ---------- 铸票者的选人与节流 ----------

// 挑铸票者按 (账号 × 区域) 台账，**不再看「谁在这个网关上已经降智」**：
// 那条旧规则把铸票永久绑在最废的那几个号上，而池子缺的是没见过的落点。
func TestMinterPicksByRegionLedger(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	addAccount(st, 3, "acct-3")
	mn.rateLimit = 6

	// 规则 3（区域级）：同一个号里，优先它**终身没铸过**的格子——那是唯一可能出新落点的
	// 地方。给所有号的 us-east 记上终身铸票，清单第一格就该让位给 us-west。
	for _, id := range []int64{1, 2, 3} {
		if err := st.NoteRegionMint(id, "us-east", clk.now()); err != nil {
			t.Fatal(err)
		}
	}
	if _, r, ok := s.PickMinter(0); !ok || r.Key != "us-west" {
		t.Fatalf("us-east 终身铸过了还往那一格派：ok=%v region=%s", ok, r.Key)
	}

	// 规则 3（账号级）：1、2 号九个格子都铸过了，3 号还有空格 ⇒ 派 3 号，
	// 哪怕它刚被用过（「歇得最久」排在后面）。
	for _, id := range []int64{1, 2} {
		for _, r := range types.Regions {
			if err := st.NoteRegionMint(id, r.Key, clk.now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	s.mu.Lock()
	s.lastMintedAt[3] = clk.now()
	s.mu.Unlock()
	if a, _, ok := s.PickMinter(0); !ok || a.ID != 3 {
		t.Fatalf("1、2 号九格都铸过，该派还有空格的 3 号：ok=%v account=%d", ok, a.ID)
	}

	// 规则 2 压过规则 3：3 号本小时已经打了 4 发，1、2 号一发没打 ⇒ 换人。
	// 集中打一个号正是 2026-09-29 把通道打废的那个模式，所以这条优先。
	mn.ratePerAcct = map[string]map[string]int{
		"acct-3": {"us-east": 2, "us-west": 2},
	}
	if a, _, ok := s.PickMinter(0); !ok || a.ID == 3 {
		t.Fatalf("3 号本小时已打 4 发，不该还是它：ok=%v account=%d", ok, a.ID)
	}

	// exclude 必须生效：要票的那个号不给自己铸（那一发会烧掉它自己要用的首次接触）。
	for i := 0; i < 5; i++ {
		if got, _, _ := s.PickMinter(1); got.ID == 1 {
			t.Fatal("exclude 没生效，派了要票的人自己去铸")
		}
	}
}

// 九个区域本小时都打满的号直接跳过：不先问一声的话，那一发会一路走到速率闸才被拒。
func TestMinterSkipsAccountWithAllRegionsSpent(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	mn.rateLimit = 6
	mn.rateUsed = map[string]int{}
	for _, r := range types.Regions {
		mn.rateUsed[r.Key] = 6
	}
	if a, r, ok := s.PickMinter(0); ok {
		t.Fatalf("九个区域都打满了还派了账号 %d 去 %s", a.ID, r.Key)
	}
	// 腾出一个区域就该恢复。
	mn.rateUsed["oceania"] = 5
	a, r, ok := s.PickMinter(0)
	if !ok || r.Key != "oceania" {
		t.Fatalf("腾出 oceania 后该派它：ok=%v account=%d region=%s", ok, a.ID, r.Key)
	}
}

// 定向铸票（续期、点名网关）只问「它在那个区域还有配额吗」，区域由出口反查。
func TestMinterForTargetedExitUsesThatRegion(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	mn.rateLimit = 6
	mn.rateUsed = map[string]int{"east-asia": 6}

	jp := types.Exit{Region: "JP"} // = east-asia，配额打满了
	if a, _, ok := s.minterFor(jp, 0, 0); ok {
		t.Fatalf("east-asia 配额满了还派了账号 %d", a.ID)
	}
	// 别的区域照旧：定向到 AU 就该放行，而且返回的区域是**那个出口自己**。
	au := types.Exit{Region: "AU"}
	_, r, ok := s.minterFor(au, 0, 0)
	if !ok || r.Exit != au || r.Key != "oceania" {
		t.Fatalf("定向到 AU 没放行或区域对不上: ok=%v r=%+v", ok, r)
	}
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

	if _, err := s.Deliver(context.Background(), 1, Request{Gateway: "unified-1"}); err == nil {
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

	if _, err := s.Deliver(context.Background(), 1, Request{Gateway: "unified-1"}); err == nil {
		t.Fatal("上游回 429 居然交付成功了")
	}
	if len(st.disabled) != 0 {
		t.Fatalf("429 把账号停用了: %v", st.disabled)
	}
}

// 一次交付不许把整池供给摸干净：每试一个网关都要真打一发上游验满血，
// 试过的槽位不管结果都烧掉了。试满上限就认 503，把剩下的留给下一个人。
func TestDeliverStopsAfterMaxTries(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
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
	// 六个网关都有活票 ⇒ 走「一个个试」那条路；一张票都没有时只抽一次签，
	// 上限根本乘不上去（见 TestFreshMintDrawsOnlyOnce）。
	seedPairs(st, clk)
	s.opt.MaxTriesPerDeliver = 3

	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
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
	if got := types.GatewaysForContinuous(types.SlotCooldown); got != want {
		t.Fatalf("门槛算成了 %d，按 %v ÷ %v 应是 %d", got, types.SlotCooldown, types.DeliverTTL, want)
	}
	// 量级检查：一张票只管几分钟而冷却是几小时，答案必然是几十上百，不可能是个位数。
	if n := types.GatewaysForContinuous(types.SlotCooldown); n < 20 {
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
	// 剩余要压到 DeliverFloor 以下：livePairs 只数够交付的那些，不压就是「深度已达标」，
	// warmOnce 一发不打、失败预算永远攒不满。
	for name := range st.gws {
		st.pairs[name] = types.Pair{Gateway: name, Cookie: "__cflb=x; __oailb=y",
			ExpiresAt: clk.now().Add(types.DeliverFloor / 2)}
	}
	mn.mintErr = errors.New("上游不理")

	// 每轮推进一个满血窗口：失败的续期也会保守地记一次触碰（那一发可能已经出站），
	// 而触碰过的槽位在一个窗口内不参与挑选——不推够的话最后那次交付会被自己的
	// 保温触碰挡掉。
	for i := 0; i < types.RenewFailBudget; i++ {
		s.warmOnce(context.Background())
		clk.add(types.FullWindow + time.Second)
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
	if _, err := s.Deliver(context.Background(), 1, Request{Gateway: "unified-1"}); err != nil {
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
	// 99 号是匿名占位行（sched.anonAccount 建的那种：带 AnonPrefix、没凭据），
	// 它确实在取票。判据必须按**前缀**认它，不能按「在不在 usable() 里」——
	// 后者会把被自动停用的托管号也算成外部消费者，于是整条成本闸失效。
	st.accounts = append(st.accounts, types.Account{ID: 99, Label: "匿名 outsider",
		Enabled: true, Creds: types.Creds{AccountID: types.AnonPrefix + "outsider"}})
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

	_, err := s.Deliver(context.Background(), 1, Request{})
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

	_, _ = s.Deliver(context.Background(), 1, Request{})
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

// 出口没了是池子自己的基础设施故障，不该把槽位挨个打进退避：出口一恢复就该能用，
// 而不是让这个账号的每个槽位再瞎等一轮（2026-10-01 现网实测出的毛病）。
func TestExitOutageDoesNotBackoffSlots(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModeSingle)
	mn.mintErr = fmt.Errorf("要不到粘性出口: %w", types.ErrNoExit)

	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("该回 503: %v", err)
	}
	for key, sl := range st.slots {
		if !sl.BackoffUntil.IsZero() {
			t.Fatalf("出口故障却给槽位 %s 记了退避到 %v", key, sl.BackoffUntil)
		}
	}
	// 出口回来了就该立刻能交付，不用等退避。
	mn.mintErr = nil
	if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
		t.Fatalf("出口恢复后仍然交付不了: %v", err)
	}
}

// ---------- 交付前验满血（verify_full_at_deliver，默认关） ----------

// 默认不验：一发 echo 都不打，判定如实写 unknown（**不是** degraded——我们没验，
// 不是验坏了），对外也不声称 verified_full。
//
// 验证要用消费者自己的凭据先铸一张活票再 echo，那两发恰好烧掉它马上要用的那个窗口；
// 而它在一个没碰过的网关上本来就是满血。state-echo 有假阴无假阳，关掉只少一个读数。
func TestDeliverWithoutVerifyNeverEchoes(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	mn.land["US"] = "unified-1"
	mn.full["unified-1"] = false // 就算验也会判降智，但压根不该去验
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 1}
	s := New(st, mn, Options{Mode: types.ModeSingle, Now: clk.now, MintGap: -1, RenewGap: -1})

	d := mustDeliver(t, s, 1, "")
	if mn.echoes != 0 {
		t.Fatalf("关着验证还 echo 了 %d 发", mn.echoes)
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("有活 pair、又关着验证，还打了 %d 发上游", n)
	}
	if d.VerifiedFull {
		t.Fatal("没验还写 verified_full=true")
	}
	sl, _ := st.Slot(1, "unified-1")
	if sl.LastVerdict != types.VerdictUnknown {
		t.Fatalf("判定记成了 %q，没验就该是 unknown（不是 degraded）", sl.LastVerdict)
	}
	// 窗口照样要记：票交出去了窗口就算烧了，少记一次会让它被当成新鲜的再发一遍。
	if sl.LastTouch.IsZero() || sl.LastDeliver.IsZero() {
		t.Fatalf("交付没记触碰/交付时刻: %+v", sl)
	}
}

// 开了验证才回得出 degraded：errDegraded / degradedHold 只在这条路上可达。
func TestDegradedOnlyReachableWithVerifyOn(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle) // rig 把两个开关都开着
	seedPairs(st, clk)
	for gw := range st.gws {
		mn.full[gw] = false
	}
	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v", err)
	}
	degraded := 0
	for _, sl := range st.slots {
		if sl.LastVerdict == types.VerdictDegraded {
			degraded++
		}
	}
	if degraded == 0 {
		t.Fatal("开着验证、echo 全判降智，却一个 degraded 都没记上")
	}
}

// ---------- 150 s 窗口属于槽位，不是 pair 的互斥锁（第 3 条） ----------

// 窗口内重复请求：原样返回同一张，valid_for_s 是**剩余**窗口，一发上游都不打，
// 也不重置冷却、不烧新槽位。客户端的重连/重试全靠这条。
func TestRedeliverWithinWindow(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	seedPairs(st, clk)

	first := mustDeliver(t, s, 1, "")
	mintsBefore, echoesBefore := len(mn.mintedGateways()), mn.echoes

	clk.add(40 * time.Second)
	again := mustDeliver(t, s, 1, "")
	if again.Gateway != first.Gateway || again.Cookie != first.Cookie {
		t.Fatalf("窗口内重复请求换了票：%+v → %+v", first, again)
	}
	if want := int((types.DeliverTTL - 40*time.Second) / time.Second); again.ValidForS != want {
		t.Fatalf("valid_for_s=%d，应是剩余窗口 %d", again.ValidForS, want)
	}
	if !again.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("重复请求把窗口往后推了：%v → %v", first.ExpiresAt, again.ExpiresAt)
	}
	if len(mn.mintedGateways()) != mintsBefore || mn.echoes != echoesBefore {
		t.Fatalf("窗口内重复请求打了上游：铸票 %d→%d、echo %d→%d",
			mintsBefore, len(mn.mintedGateways()), echoesBefore, mn.echoes)
	}
	// 交付时刻没被改写 ⇒ 冷却照原样起算，不会被重试一路往后推。
	sl, _ := st.Slot(1, first.Gateway)
	if !sl.LastDeliver.Add(types.DeliverTTL).Equal(first.ExpiresAt) {
		t.Fatalf("交付时刻被重置了: %+v", sl)
	}
}

// 窗口过完 ⇒ 这个 (账号 × 网关) 进冷却，不再参与挑选。全烧完就 503，**不开后门**。
func TestWindowElapsedPutsSlotInCooldown(t *testing.T) {
	s, st, _, clk := rig(t, types.ModeSingle)
	delete(st.gws, "unified-2")
	delete(st.gws, "unified-3")
	seedPairs(st, clk)

	mustDeliver(t, s, 1, "")
	clk.add(types.DeliverTTL + time.Second)
	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("窗口过完、唯一的网关进冷却，还发得出来: %v", err)
	}
	// 歇满冷却就该恢复（这时候那张 pair 早过期了，走的是现铸）。
	clk.add(types.SlotCooldown)
	if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
		t.Fatalf("歇满 %v 还交付不了: %v", types.SlotCooldown, err)
	}
}

// 冷却对匿名/跨账号的占位行一视同仁：他们的交付也写 LastDeliver，
// 「这个上游账号在这个网关上烧过了」和他有没有上传凭据无关。
func TestCooldownAppliesToAnonymousToo(t *testing.T) {
	s, _, clk, _ := anonRig(t, 1, 1)
	if _, err := s.DeliverAnonymous(context.Background(), "outsider", Request{}); err != nil {
		t.Fatalf("DeliverAnonymous: %v", err)
	}
	clk.add(types.DeliverTTL + time.Second)
	if _, err := s.DeliverAnonymous(context.Background(), "outsider", Request{}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("匿名窗口过完还发得出来: %v", err)
	}
	// 换一个上游 ID 就是另一条账本，不受连坐。
	if _, err := s.DeliverAnonymous(context.Background(), "someone-else", Request{}); err != nil {
		t.Fatalf("另一个匿名上游被连坐了: %v", err)
	}
}

// ---------- 冷却时长可配（第 10 条） ----------

// 冷却是配置项，改完立即生效：读它的地方必须现读 Live，不能把常量编进去
// （用户实测 30 分钟到 3 小时都见过，4 小时只是保守的默认值）。
func TestSlotCooldownComesFromConfig(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	mn.land["US"] = "unified-1"
	mn.full["unified-1"] = true
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 1}
	s := New(st, mn, Options{Mode: types.ModeSingle, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.SlotCooldownMinutes = 10 })})

	mustDeliver(t, s, 1, "")
	clk.add(types.DeliverTTL + time.Second)
	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("窗口刚过完就又发了一张: %v", err)
	}
	// 配的 10 分钟远短于默认的 4 小时：到点就该恢复，证明读的是配置不是常量。
	clk.add(10 * time.Minute)
	if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
		t.Fatalf("配的是 10 分钟冷却，歇满了还交付不了: %v", err)
	}
}

// ---------- 一张活票都没有时只抽一次签（A 条） ----------

// 回落**只铸一发**，哪怕 MaxTriesPerDeliver 开到 5、哪怕抽到的落点正好在冷却里。
//
// 出口只能把落点「倾向」到某个网关、定不住 ⇒「先试 X 再试 Y」就是抽两次签、
// 花两发请求烧两个铸票者槽位，而两次的分布一模一样。
func TestFreshMintDrawsOnlyOnce(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.opt.MaxTriesPerDeliver = 5
	// 一张活 pair 都没有；1 号在 unified-3 上刚取过票（在冷却里），
	// 而这一发无论从哪个出口打都会落到 unified-3 ⇒ 抽到的签不可用。
	st.slots[slotKey(1, "unified-3")] = types.Slot{AccountID: 1, Gateway: "unified-3",
		LastDeliver: clk.now()}
	for k := range mn.land {
		mn.land[k] = "unified-3"
	}

	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v，抽到的落点在冷却里就该老实 503", err)
	}
	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("打了 %d 发上游，回落只该抽一次签（还剩两个候选网关没碰）", n)
	}
}

// 回落成功时也只花一发：默认不验满血，所以「现铸 + 交付」合起来就是一发请求。
func TestFreshMintSuccessCostsOneRequest(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	for _, gw := range []string{"unified-1", "unified-2", "unified-3"} {
		region := strings.ToUpper(strings.TrimPrefix(gw, "unified-"))
		st.gws[gw] = types.Gateway{Name: gw, FirstSeenBy: 1, ReachableExits: []types.Exit{{Region: region}}}
		mn.land[region] = gw
	}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})
	s.opt.MaxTriesPerDeliver = 5

	if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("冷池子交付花了 %d 发上游，应为 1", n)
	}
}

// 有活票的网关优先：硬筛之后根本不碰没票的那些，省一发请求外加一个铸票者槽位。
//
// 这条要防的是 store.LongestRestedSlot 的零值合成——它给没有记录的网关造一个零值
// Slot，于是「从没碰过」永远排在「碰过但有票」前面，冷启动的消费者几乎每次请求都要铸。
func TestDeliverPrefersGatewayWithLivePair(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	st.pairs["unified-2"] = types.Pair{Gateway: "unified-2", Cookie: cookiePrefix + "unified-2",
		ExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 2}

	d := mustDeliver(t, s, 1, "")
	if d.Gateway != "unified-2" {
		t.Fatalf("交付了 %s，只有 unified-2 有活票", d.Gateway)
	}
	// rig 开着验证，消费者自己那一发少不了；要守的是「没为补 pair 另派一个铸票者」。
	for _, sm := range mn.mints {
		if sm.AccountID == 2 {
			t.Fatalf("有活票还派了 2 号去补 pair: %+v", sm)
		}
	}
}

// 剩余跌破 DeliverFloor 的票不算「有活票」：硬筛和 livePairs 必须用同一把尺子，
// 否则会挑中一个马上要被重新路由的网关。
func TestStalePairDoesNotCountAsLive(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	for name := range st.gws {
		st.pairs[name] = types.Pair{Gateway: name, Cookie: cookiePrefix + name,
			ExpiresAt: clk.now().Add(types.DeliverFloor - time.Second), MintedBy: 2}
	}
	mustDeliver(t, s, 1, "")
	if n := len(mn.mintedGateways()); n == 0 {
		t.Fatal("票全跌破门槛了还当成活票直接发了出去")
	}
}

// ---------- 同一个网关一次只许有一发在铸（B 条） ----------

// 两路交付同时发现同一个网关没票：第二个必须等第一个铸完直接用，不许各铸一发。
//
// 账号锁拦不住这件事：minterFor 会给两个消费者挑**不同**的铸票者，于是两发都放出去了
// ——而一张 pair 本来就能同时服务多个消费者，第二发纯属白烧一个槽位加一发请求。
//
// 三个号、消费者是 2 和 3：铸票者必然是账号 1（minterFor 只排掉消费者自己，剩下按 ID）。
// 铸票者不能是其中一个消费者，否则等到票的那个人该被拒——那是另一条规则，由
// TestWaitingForAnothersMintRechecksOwnWindow 盯着。mintCost 非零，不然「等到的票
// 还剩多久」这件事恒为真，用例就什么都没证。
func TestConcurrentFetchMintsOncePerGateway(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	mn.delay = 30 * time.Millisecond
	mn.mintCost = 60 * time.Second
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	addAccount(st, 3, "acct-3")
	// 只有一个网关、一张票都没有 ⇒ 两个人必然挑到同一个。
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	mn.land["US"] = "unified-1"
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})

	var wg sync.WaitGroup
	out := make([]types.Delivery, 2)
	errs := make([]error, 2)
	for i := range out {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out[i], errs[i] = s.Deliver(context.Background(), int64(i+2), Request{})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("第 %d 路交付失败: %v", i+1, err)
		}
	}
	if out[0].Cookie != out[1].Cookie {
		t.Fatalf("两路拿到了不同的票：%q / %q", out[0].Cookie, out[1].Cookie)
	}
	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("同一个网关铸了 %d 发，网关锁没拦住", n)
	}
}

// ---------- 预铸（第 6、7 条与 D、F 条） ----------

// 第 7 条的复现用例：**没人要票就一发都不打**。
//
// 用户报「没人要的时候 pair 也在被续」。零需求跑满 5 轮巡检，假 Minter 必须 0 次调用。
func TestNoDemandMintsNothing(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	// 票全跌破门槛、深度明显不够——唯一拦着它的只有「没人要」这一条。
	for name := range st.gws {
		st.pairs[name] = types.Pair{Gateway: name, Cookie: cookiePrefix + name,
			ExpiresAt: clk.now().Add(time.Minute), MintedBy: 2}
	}
	for i := 0; i < 5; i++ {
		s.warmOnce(context.Background())
		clk.add(time.Minute)
	}
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("零需求跑了 5 轮巡检，却打了 %d 发上游", n)
	}
	if mn.echoes != 0 {
		t.Fatalf("零需求还 echo 了 %d 发", mn.echoes)
	}
}

// 默认（renew_pairs=false）预铸走裸铸：不带旧 pair 去续同一个网关。
//
// 续期每发只换来「同一个落点的新 cookie」，而池子缺的是落点不是票。
// 判据是落点：带旧 pair 打必然留在原网关，裸打才可能摸到新的。
func TestPremintBareDoesNotRenew(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	// 裸打会落到一个没见过的网关；带着旧 pair 打则留在 unified-1。
	for _, r := range types.Regions {
		mn.land[exitKey(r.Exit)] = "unified-9"
	}
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(time.Minute), MintedBy: 2}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})
	s.markDemand()

	s.warmOnce(context.Background())
	got := mn.mintedGateways()
	if len(got) == 0 {
		t.Fatal("有需求、深度不够，一发预铸都没打")
	}
	for _, gw := range got {
		if gw == "unified-1" {
			t.Fatalf("默认该裸铸，却带着旧 pair 去续了 unified-1: %v", got)
		}
	}
}

// renew_pairs 打开才回到「带旧 pair 续同一个网关」那条路。
func TestRenewPairsSwitchRestoresRenewal(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	for _, r := range types.Regions {
		mn.land[exitKey(r.Exit)] = "unified-9"
	}
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(time.Minute), MintedBy: 2}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.RenewPairs = true })})
	s.markDemand()

	s.warmOnce(context.Background())
	got := mn.mintedGateways()
	if len(got) == 0 || got[0] != "unified-1" {
		t.Fatalf("开了 renew_pairs 该续 unified-1，实际落点 %v", got)
	}
}

// D 条：连续两发裸铸都落回已有落点就收手——请求和槽位都花了，覆盖面没涨。
func TestPremintStopsAfterTwoWastedShots(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	// 九个区域全都落到同一个网关，而它已经有一张活票 ⇒ 每一发都是白烧。
	for _, r := range types.Regions {
		mn.land[exitKey(r.Exit)] = "unified-1"
	}
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 2}
	// 富余目标拉到 9，缺口远大于 2 ⇒「只打两发」只可能来自浪费预算。
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.SpareTarget = 9 })})
	s.markDemand()

	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 2 {
		t.Fatalf("裸铸打了 %d 发，连续两发白烧就该收手", n)
	}
}

// F 条：spare_target=0 就是「全按需，一发预铸都不打」。
func TestSpareTargetZeroDisablesPremint(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.opt.Live = liveCfg(func(c *config.Config) { c.SpareTarget = 0 })
	s.markDemand()

	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("spare_target=0 还预铸了 %d 发", n)
	}
}

// ---------- 一次交付最多试几个网关（E 条） ----------

// 上限从配置来，默认 2：每多试一个网关就多花一发上游。
func TestMaxTriesComesFromConfig(t *testing.T) {
	s, _, _, _ := rig(t, types.ModeSingle)
	if got := s.maxTries(); got != 2 {
		t.Fatalf("默认上限 %d，应为 2", got)
	}
	s.opt.Live = liveCfg(func(c *config.Config) { c.MaxTriesPerDeliver = 4 })
	if got := s.maxTries(); got != 4 {
		t.Fatalf("配置改成 4 了，还报 %d", got)
	}
}

// 整池同病的故障（出口全没了、速率闸）不换网关重试：换一个只是把同一句话重复一遍，
// 白烧 N 发请求。
func TestPoolWideFailureStopsTryingOtherGateways(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	seedPairs(st, clk)
	s.opt.MaxTriesPerDeliver = 5
	mn.mintErr = fmt.Errorf("要不到粘性出口: %w", types.ErrNoExit)

	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v", err)
	}
	if n := len(mn.mintedGateways()); n > 1 {
		t.Fatalf("出口全没了还试了 %d 个网关", n)
	}
}
