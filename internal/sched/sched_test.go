package sched

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
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

func (f *fakeStore) NoteRegionMint(accountID int64, regionKey, gateway string, at time.Time) error {
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
	if gateway != "" {
		m.Gateway = gateway // 照 store 的语义：读不出落点时保留上一次的，不清掉
	}
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
	mu   sync.Mutex
	clk  *clock
	land map[string]string // exitKey → 落点网关
	// landByAcct 是 (账号 × 大区) → 落点网关，key = comboKey(id, exitKey)。
	// 比 land 优先：真实机制里网关 = f(账号, 大区)，同一个出口不同账号落不同网关。
	landByAcct map[string]string
	full       map[string]bool // 网关 → state-echo 判满血
	delay      time.Duration

	noNewPair bool // 模拟「上游接受了送进去的 pair，不下发新 cookie」
	// drift 是「预期落点 → 实际落点」：出口只能把落点倾向到某个网关，定不住。
	drift map[string]string
	// pairLife / cflbLife 是两个组件各自的寿命，零值 = 生产常量。
	// 钉路由的是 __cflb（早死 300 秒），那条带子要能单独铺出来。
	pairLife time.Duration
	cflbLife time.Duration
	// onlyOailb = 这一发只补发 __oailb，钉路由那张原样留着。
	onlyOailb  bool
	mintErr    error
	mintStatus int           // 非 0 时顶替 200，用来模拟上游否决
	mintBody   string        // 上游那段 JSON
	mintCost   time.Duration // 一发铸票的往返耗时，推进假时钟
	// mintRetryAfter 是非 2xx 时上游 Retry-After 头折算出来的时长。
	// 零值 = 上游没给（429 的账号级退避该回落到默认时长）。
	mintRetryAfter time.Duration

	sidErr error
	// seedSIDErr 非 nil 时「没配种子出口」，预铸该回落到住宅（NewSID）。
	seedSIDErr error
	// seedSent 记每一发带种子铸票送进去的 cookie，key = sid。
	// 用它证明「有活种子票时走的是种子出口、而且把那张票送出去了」。
	seedSent    map[string]string
	sidSeq      int
	mints       []types.Sample // 只借这个结构记 (account, gateway) 调用流水
	echoes      int
	echoExits   []string // 每发 state-echo 用的出口区域键
	inflight    map[int64]int
	maxPerAcc   int
	allInflight int
	maxAll      int

	// rateUsed / rateLimit 是速率闸的读数。默认 limit=0 = 「不设限」，
	// 于是不碰配额的用例不用管它（生产的 mint.perRegionHour 永远回正数）。
	// ratePerAcct 按上游账号 ID 覆盖 rateUsed，测「本小时总发数最少的优先」那条规则用。
	rateUsed    map[string]int
	ratePerAcct map[string]map[string]int
	rateLimit   int

	// geoCountry 非空时盖掉**观测到的**出口国家，用来造「出口国家不符」：住宅代理
	// 库存不足时会回退到别的国家（1650 发里 23 次），而落点由真实出口 IP 决定。
	geoCountry string
}

func newMinter(clk *clock) *fakeMinter {
	return &fakeMinter{clk: clk, land: map[string]string{}, full: map[string]bool{}, inflight: map[int64]int{}}
}

// seedGateway 给网关铺一条出口、并让假 Minter 从那条出口落到它，返回那条出口。
//
// **出口必须取自 types.Regions**：按区域现铸（sched.mintByRegion）从那张表出发，
// 而以前各处自己拼的假区域码（"1"/"2"/"3"、"R4"）在 land 里查不到 ⇒ 现铸恒「没拿到落点」，
// 一切走回落铸票的用例都测不出真东西。idx 对区域数取模，所以网关再多也不会越界。
func seedGateway(st *fakeStore, mn *fakeMinter, gw string, idx int, firstSeenBy int64) types.Exit {
	e := types.Regions[idx%len(types.Regions)].Exit
	st.gws[gw] = types.Gateway{Name: gw, FirstSeenBy: firstSeenBy, ReachableExits: []types.Exit{e}}
	mn.land[exitKey(e)] = gw
	return e
}

// landBare 让**任何区域**出发的裸铸都落到 gw。
//
// 走回落铸票（sched.mintByRegion）的用例该用这个来钉落点，别靠 seedGateway 的 idx 顺序：
// 区域由铸票者的终身台账挑（bestRegion），空台账时才恰好是 types.Regions[0]，
// 台账上有一笔就换一个区域 —— 靠顺序钉落点的用例会在无关改动下莫名变红。
func landBare(mn *fakeMinter, gw string) {
	for _, r := range types.Regions {
		mn.land[exitKey(r.Exit)] = gw
	}
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
	// maxAll 是**跨账号**的同时在飞数，和 maxPerAcc 管的是两件事：后者守 SPEC 红线 2
	// （单账号并发 1），前者是预铸的 worker pool 到底有没有并行起来的唯一读数。
	m.allInflight++
	if m.allInflight > m.maxAll {
		m.maxAll = m.allInflight
	}
	m.mu.Unlock()
}

func (m *fakeMinter) leave(id int64) {
	m.mu.Lock()
	m.inflight[id]--
	m.allInflight--
	m.mu.Unlock()
}

// peakInflight 是整场跑下来最多几发铸票同时在飞。
func (m *fakeMinter) peakInflight() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxAll
}

func (m *fakeMinter) Mint(ctx context.Context, c types.Creds, e types.Exit, sid, sentPair string,
	sentCflbExp time.Time) (types.MintResult, error) {
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
	// 落点先看 (账号 × 大区)：这是真实机制（同账号同出口连铸 5 发落点逐字相同，
	// 同出口不同账号落不同网关）。没铺这一格的用例退回只按出口算，行为不变。
	if byAcct := m.landByAcct[comboKey(id, exitKey(e))]; byAcct != "" {
		gw = byAcct
	}
	seeded := strings.HasPrefix(sid, "seed")
	switch {
	case seeded:
		// 带**种子**票铸票：`__cflb` 只钉大区，具体网关仍由 (账号 × 大区) 决定，
		// 所以落点**不是**送进去那张票的网关。这一格就是整条种子路的全部要害——
		// 写成 gwOf(sentPair) 的话这个 fake 会把「摸到自己的新网关」变成
		// 「原地留在别人的网关上」，整类 bug 在套子里不可达。
		if m.seedSent == nil {
			m.seedSent = map[string]string{}
		}
		m.seedSent[sid] = sentPair
	case sentPair != "":
		gw = gwOf(sentPair) // 续期：带着自己那张 pair 回同一个网关
	}
	// 漂移：出口只能把落点「倾向」到某个网关，定不住。没有这个旋钮的话这个 fake
	// 结构性地永不漂移（两条路都写死「落点 = 预期落点」），于是整类漂移 bug
	// 在套子里不可达。
	if to := m.drift[gw]; to != "" {
		gw = to
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
		m.mu.Lock()
		ra := m.mintRetryAfter
		m.mu.Unlock()
		return types.MintResult{Status: st, Gateway: gw, ErrBody: m.body(), RetryAfter: ra}, nil
	}
	if noNew && sentPair != "" {
		// 上游接受了送进去的 pair、不换 cookie。**只有那张的 __cflb 可证还活着**
		// 才能把旧名字当落点（和真实实现同一条规矩，见 mint.Mint）：死了的话这一发
		// 其实是随机路由走的，落点读不出来 ⇒ Gateway 留空，调用方按失败走。
		if !sentCflbExp.After(m.clk.now()) {
			return types.MintResult{Status: 200}, nil
		}
		return types.MintResult{Status: 200, Gateway: gw, TurnState: "ticket-" + gw + "-" + sid}, nil
	}
	res := types.MintResult{Status: 200, Gateway: gw, TurnState: "ticket-" + gw + "-" + sid,
		NewPair: cookiePrefix + gw,
		// 这一格是 __oailb 的 exp，所以默认值用 OailbLifetime：比路由寿命长 300 秒，
		// 和线上一样。拿 PairLifetime 铺就把那条带子抹平了，而它正是出过 bug 的地方。
		PairExpires: m.clk.now().Add(m.lifetime(m.pairLife, types.OailbLifetime)),
	}
	// onlyOailb 模拟「上游只补发了 __oailb」：钉路由那张原样留在 cookie 里，
	// 响应里没有它的 Max-Age ⇒ CflbExpires 留零值，寿命得按账本上的旧值算。
	if !m.onlyOailb {
		res.CflbExpires = m.clk.now().Add(m.lifetime(m.cflbLife, types.PairLifetime))
	}
	return res, nil
}

// lifetime 取旋钮，没设就用生产常量。
func (m *fakeMinter) lifetime(set, def time.Duration) time.Duration {
	if set != 0 {
		return set
	}
	return def
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
	// 记下这一发用的出口：漂移之后必须按**落到的那个**网关重取出口，
	// 拿「想去 A」的出口去验 B 大概率再漂一次、把一张好票判成失败。
	m.echoExits = append(m.echoExits, exitKey(e))
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

// NewSeedSID 照真实 mint 的语义：绑在普通机房出口上的 sid，没配那条出口就报错。
// sid 前缀和 NewSID 分开，用例据此断言这一发走的是哪条出口。
func (m *fakeMinter) NewSeedSID() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seedSIDErr != nil {
		return "", m.seedSIDErr
	}
	m.sidSeq++
	return fmt.Sprintf("seed%05d", m.sidSeq), nil
}

func (m *fakeMinter) Geo(ctx context.Context, e types.Exit, sid string) (types.Observed, error) {
	m.mu.Lock()
	country := m.geoCountry
	m.mu.Unlock()
	if country == "" {
		country = e.Region
	}
	return types.Observed{IP: "203.0.113.1", Country: country, City: e.City}, nil
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

// seedShots 是每一发**带种子铸票**送进去的 cookie，key = sid。
// 空 map = 这一轮一发都没走种子出口。
func (m *fakeMinter) seedShots() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]string, len(m.seedSent))
	for k, v := range m.seedSent {
		out[k] = v
	}
	return out
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
	for i, gw := range []string{"unified-1", "unified-2", "unified-3"} {
		seedGateway(st, mn, gw, i, 1)
		mn.full[gw] = true
	}
	// 这组用例测的是**交付前验满血打开**那条路。它默认关（见 config.Config 的字段
	// 注释），默认那条路由 TestDeliverWithoutVerifyNeverEchoes 盯着。
	s := New(st, mn, Options{Mode: mode, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.VerifyFullAtDeliver = true })})
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
		// 寿命按 __cflb 算（它才钉路由、而且早死 300 秒）：按 __oailb 的 3900 铺场景
		// 等于铺一个生产永远不会出现的状态。
		exp := clk.now().Add(types.PairLifetime)
		st.pairs[name] = types.Pair{Gateway: name, Cookie: cookiePrefix + name,
			ExpiresAt: exp, CflbExpiresAt: exp, MintedBy: 1}
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

	// 没有号碰过任何网关 ⇒ 目标深度就是 SpareTarget，补到够数就停。
	// 断言量的是**达成的深度**不是发数：失手（落回已有活票的网关）会多花一发，
	// 那是 premintBare 的正常行为，写死发数会让这个用例变成在测别的东西。
	s.markDemand()
	s.warmOnce(context.Background())
	if n := s.FreeGateways(); n != types.SpareTarget {
		t.Fatalf("有需求时补到了 %d 张活票，目标深度是 %d（发数 %d）",
			n, types.SpareTarget, len(mn.mintedGateways()))
	}

	// 够数了就不再打：pair 还新鲜，而且目标也没涨。
	spent := len(mn.mintedGateways())
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != spent {
		t.Fatalf("够数了却又打了 %d 发", n-spent)
	}

	// 需求停了：即便 pair 快过期也一发不打。
	// 按 DeliverFloor 算：livePairs 数的是「还够不够交付」的票。
	clk.add(types.PairLifetime - types.DeliverFloor + time.Minute)
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != spent {
		t.Fatalf("静默期还打了 %d 发", n-spent)
	}
	s.markDemand()
	s.warmOnce(context.Background())
	if n := s.FreeGateways(); n != types.SpareTarget {
		t.Fatalf("需求回来后该把深度补回 %d，实际 %d 张活票", types.SpareTarget, n)
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
	landBare(mn, "unified-9") // 池子里只有这一个网关，裸铸从哪个区域出发都该落到它
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

// driftRig 搭一个「只有一个网关、出口会把我们漂到别处」的池子，验证默认关着
// （生产就是这样），所以漂移的后果是调度自己的决定，不是被验证挡下来的。
//
// 为什么要专门搭：原先那个用例发 Request{} ⇒ want 为空 ⇒ 漂移判据根本不执行，
// 三条断言全靠**验证把漂到的网关判成 degraded** 才绿，被测分支其实不可达。
func driftRig(t *testing.T, from, to string) (*Scheduler, *fakeStore, *fakeMinter, *clock) {
	t.Helper()
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	seedGateway(st, mn, from, 0, 1)
	// 裸铸也要落到 from（再被 drift 弹到 to）：回落铸票从区域出发，不冲着某个网关去。
	landBare(mn, from)
	mn.drift = map[string]string{from: to} // 想去 from，实到 to
	mn.full[from], mn.full[to] = true, true
	s := New(st, mn, Options{Mode: types.ModeSingle, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(nil)}) // 两个开关都按默认关着
	return s, st, mn, clk
}

// **点名了网关**时漂移算失败：他点名是因为他知道自己没烧过那个落点。
// 这一条才真正钉住 mintOnce 里的漂移判据（want 非空那一支）。
func TestNamedGatewayDriftIsFailure(t *testing.T) {
	s, st, _, _ := driftRig(t, "unified-1", "unified-77")

	_, err := s.Deliver(context.Background(), 1, Request{Gateway: "unified-1"})
	if !errors.Is(err, ErrMintFailed) || !strings.Contains(err.Error(), "落点漂移") {
		t.Fatalf("err = %v，点名网关时漂移该是 ErrMintFailed 且说清是漂移", err)
	}
	// 白来的供给别扔：pair 留在池里，触碰记在**实际**落点上。
	if _, ok := st.Pair("unified-77"); !ok {
		t.Fatal("漂移拿到的新 pair 该留在池子里")
	}
	if sl, ok := st.Slot(1, "unified-77"); !ok || sl.LastTouch.IsZero() {
		t.Fatalf("触碰该记在实际落点上: %+v", sl)
	}
}

// **没点名**时漂移是抽签的正常结果，照发漂到的那个（代码的约定就是这样：
// 出口只能把落点倾向到某个网关，定不住，所以 want 留空时不判漂移）。
func TestUnnamedDriftDeliversWhereItLanded(t *testing.T) {
	s, st, _, clk := driftRig(t, "unified-1", "unified-77")

	d, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("没点名网关时漂移不该算失败: %v", err)
	}
	if d.Gateway != "unified-77" {
		t.Fatalf("交付了 %s，抽签抽到的是 unified-77", d.Gateway)
	}
	// 账本两条：pair 落在实际落点上，交付时刻也记在那一行。
	if p, ok := st.Pair("unified-77"); !ok || p.Cookie != d.Cookie {
		t.Fatalf("pair 没落在实际落点上: %+v", p)
	}
	if sl, ok := st.Slot(1, "unified-77"); !ok || sl.LastDeliver.IsZero() {
		t.Fatalf("交付没记在实际落点的槽位上: %+v", sl)
	}
	if sl, ok := st.Slot(1, "unified-1"); ok && !sl.LastDeliver.IsZero() {
		t.Fatalf("交付记到了想去的那个网关上: %+v", sl)
	}
	_ = clk
}

// B-2：漂到调用方**明确排除**的网关上时不许交付。
//
// exclude 编码的是「我自己烧过这个」——池子账本上没有记录，两条冷却闸都看不见。
// 换票（force + exclude_versions 把有票的网关全排掉）正好最常落到这一格，
// 于是客户端拿回一张自己刚烧过的落点，而验证默认关 ⇒ 无从察觉。
func TestUnnamedDriftRespectsExclude(t *testing.T) {
	s, st, _, _ := driftRig(t, "unified-1", "unified-77")

	_, err := s.Deliver(context.Background(), 1, Request{Exclude: map[string]bool{"unified-77": true}})
	if !errors.Is(err, ErrNoLivePair) {
		t.Fatalf("err = %v，漂到被排除的网关上该拒发（而且归成等得起的那一格）", err)
	}
	// 票照旧留在池里：白来的供给别扔，只是不发给**他**。
	if _, ok := st.Pair("unified-77"); !ok {
		t.Fatal("拒发不等于扔掉这张票")
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
	for i, gw := range []string{"unified-1", "unified-2"} {
		seedGateway(st, mn, gw, i, 1)
		mn.full[gw] = true
	}
	// 两个网关都铺上活票：这一条测的是**挑选**按 (账号 × 网关) 走，不是铸票。
	// 不铺票会掉进回落铸票，而那条路的落点由区域定、根本不看候选集，测的就不是这件事了。
	seedPairs(st, clk)
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
	// 必须是**有 sentinel 的**那个错：没有它的话这件事掉进 no_live_pair，
	// 于是运营方逐个启号来验时拿到「稍后再试」+ 挂满 wait + 白排一次发现扫描，
	// 而真实原因是那个号本身（见 httpapi 的 classify）。
	for _, id := range []int64{0, 2, 3, 99} {
		_, err := s.Deliver(context.Background(), id, Request{})
		if !errors.Is(err, ErrAccountUnusable) {
			t.Fatalf("账号 %d: err=%v，应为 ErrAccountUnusable", id, err)
		}
	}
	// 停用理由要带出来：「手动暂停」和「凭据已死」要做的事完全不同。
	if err := s.st.DisableAccount(2, "铸票 HTTP 401：token_revoked"); err != nil {
		t.Fatal(err)
	}
	_, err := s.Deliver(context.Background(), 2, Request{})
	if err == nil || !strings.Contains(err.Error(), "token_revoked") {
		t.Fatalf("停用理由没带出来: %v", err)
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
		seedGateway(st, mn, gw, i, int64(i+1))
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
	// 票全低于交付门槛 ⇒ 两路都走回落铸票，而那条路按**区域**单飞：撞上同一个区域的那一路
	// 会被当场挡回去，于是它根本走不到账号锁、屏障等不满 —— 和上面「挑到不同网关」同一个
	// 道理，换成区域再说一遍。铸票者是交叉的（1 号的是 2、2 号的是 1，各自只剩对方可选），
	// 给 2 号的 us-east 记一笔 ⇒ 它改挑 us-west(→unified-2)，1 号仍挑 us-east(→unified-1)，
	// 正好各自落在**对方**烧过的那个上，两路都能走完 finish，ABBA 才真的有得可死。
	if err := st.NoteRegionMint(2, "us-east", "", now); err != nil {
		t.Fatal(err)
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
	// 第二个号进来就该恢复保温（量达成的深度，不量发数，见 TestPoolModeWarmsOnlyOnDemand）。
	addAccount(st, 2, "acct-2")
	s.warmOnce(context.Background())
	if n := s.FreeGateways(); n != types.SpareTarget {
		t.Fatalf("两个号了却只补到 %d 张活票，目标深度是 %d（发数 %d）",
			n, types.SpareTarget, len(mn.mintedGateways()))
	}
}

// ---------- 票充足时自动公开 ----------

// anonRig 搭一个有 n 张活 pair 的池子，门槛 min。
// anonRig 搭一个匿名交付的台子：n 个有活 pair 的网关、accts 个托管账号、门槛 min。
//
// **accts 和 min 是独立的两个数**：门槛数的是托管账号数，不是货架上的活 pair 数
// （见 types.AccountsForContinuous），所以「够不够格开放」只由 accts vs min 决定，
// 跟 n 无关。合成一个参数的话就测不出「票多但号少 ⇒ 仍然不开放」。
func anonRig(t *testing.T, n, accts, min int) (*Scheduler, *fakeStore, *clock, *bytes.Buffer) {
	t.Helper()
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	for i := 0; i < accts; i++ {
		id := int64(i + 1)
		addAccount(st, id, fmt.Sprintf("acct-%d", i+1))
		// 对外开放门槛数的是**铸过票**的账号（provenAccounts，防伪造 JWT 顶开），
		// 所以这里得给每个号记一发成功铸票，不然 Public() 永远算 0 个、测不出开放。
		if err := st.NoteRegionMint(id, "us-east", "", clk.now()); err != nil {
			t.Fatal(err)
		}
	}
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
			c.PublicWhenSurplus.MinAccounts = min
		}),
		Log: slog.New(slog.NewTextHandler(&buf, nil))})
	return s, st, clk, &buf
}

// 空闲数达标就放行，并留下那行给运营方看的读数。
func TestAnonymousAllowedWhenSurplus(t *testing.T) {
	s, st, clk, buf := anonRig(t, 6, 6, 6)
	if ps := s.Public(); !ps.Open || ps.Accounts != 6 {
		t.Fatalf("应开放: %+v", ps)
	}
	d, err := s.DeliverAnonymous(context.Background(), "acct-outsider", Request{}, false)
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
	for _, want := range []string{"无凭据交付", "authed=false", "gateway=" + d.Gateway, "free_gateways=6"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("无凭据交付日志缺了 %q: %s", want, buf.String())
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
	d2, err := s.DeliverAnonymous(context.Background(), "acct-outsider", Request{}, false)
	if err != nil || d2.Gateway == d.Gateway {
		t.Fatalf("第二次又给了同一个网关 %s (err=%v)", d2.Gateway, err)
	}
}

// 空闲数不达标就拒，哪怕还剩好几张——上传者永远优先。
func TestAnonymousRefusedBelowThreshold(t *testing.T) {
	s, _, _, _ := anonRig(t, 5, 1, 6)
	if ps := s.Public(); ps.Open {
		t.Fatalf("不该开放: %+v", ps)
	}
	if _, err := s.DeliverAnonymous(context.Background(), "x", Request{}, false); !errors.Is(err, ErrPublicClosed) {
		t.Fatalf("err=%v，应为 ErrPublicClosed", err)
	}
	// 开关没开时，再富裕也不开放。改的是**活配置**：Public() 现读它。
	off, _, _, _ := anonRig(t, 9, 1, 1)
	offCfg := off.opt.Live.Get()
	offCfg.PublicWhenSurplus.Enabled = false
	off.opt.Live = config.NewLive("", offCfg)
	if ps := off.Public(); ps.Open || ps.Enabled {
		t.Fatalf("开关关着却开放了: %+v", ps)
	}
	if _, err := off.DeliverAnonymous(context.Background(), "x", Request{}, false); !errors.Is(err, ErrPublicClosed) {
		t.Fatalf("err=%v", err)
	}
}

// mintedOf 从调度器身上取回那个假 Minter 的铸票记录。
// 比给 anonRig 再加一个返回值省事，而且想数铸票次数的只有这一处。
func mintedOf(s *Scheduler) []string { return s.mint.(*fakeMinter).mintedGateways() }

// 匿名只发**现成且够新鲜**的票，而且货架空了也绝不替它现铸。
//
// 这条以前断言「票全过期 ⇒ ErrPublicClosed」。门槛改成按托管账号数算之后那个前提没了
// （见 types.AccountsForContinuous）：库存不再是开放判据，所以票发完露出来的是
// 「没有可交付的槽位」而不是「不对外开放」。**真正要钉死的不变量是「不替匿名铸票」**
// ——铸票要烧某个上传者的满血窗口，为匿名花这个钱是这条路最容易悄悄退化掉的地方。
func TestAnonymousGetsOnlyFreshStockAndNeverMints(t *testing.T) {
	s, st, clk, _ := anonRig(t, 6, 1, 1)
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
	d, err := s.DeliverAnonymous(context.Background(), "x", Request{}, false)
	if err != nil {
		t.Fatalf("DeliverAnonymous: %v", err)
	}
	if d.Gateway != "unified-6" {
		t.Fatalf("匿名拿到了票不够新鲜的 %s", d.Gateway)
	}
	minted := len(mintedOf(s))
	// 最后一张也跌破 DeliverFloor ⇒ 货架空了。
	p := st.pairs["unified-6"]
	p.ExpiresAt = clk.now().Add(types.DeliverFloor - time.Minute)
	st.pairs["unified-6"] = p
	_, err = s.DeliverAnonymous(context.Background(), "y", Request{}, false)
	if err == nil {
		t.Fatal("货架空了还交付成功了")
	}
	// 开放门槛仍然达标（1 个号 ≥ 门槛 1），所以这里**不该**是 public_closed：
	// 拒绝的理由是没有现成票，不是「不对外开放」，两者要让调用方分得开。
	if errors.Is(err, ErrPublicClosed) {
		t.Fatalf("库存空了却报成不对外开放: %v", err)
	}
	if !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v，应为 ErrNoFullSlot", err)
	}
	// 最要紧的一条：一发都没替匿名铸。
	if now := len(mintedOf(s)); now != minted {
		t.Fatalf("替匿名铸了 %d 发票（烧的是上传者的窗口）", now-minted)
	}
}

// 匿名占了一张之后，上传者照样拿得到槽位。
//
// **刻意不要求换一张**：匿名和上传者拿到同一个网关是允许的（一张 pair 可以同时给几个号
// 用，见 types.Pair 的 2026-10-01 实测）。这里要守的是「上传者不会因为匿名先到而拿不到」。
func TestUploaderStillServedAfterAnonymous(t *testing.T) {
	s, st, _, _ := anonRig(t, 6, 1, 1)
	addAccount(st, 2, "acct-2")
	if _, err := s.DeliverAnonymous(context.Background(), "x", Request{}, false); err != nil {
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
	s, st, _, _ := anonRig(t, 6, 1, 1)
	if _, err := s.DeliverAnonymous(context.Background(), "acct-1", Request{}, false); err != nil {
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
	s, st, mn, clk := rig(t, types.ModeSingle)
	seedHeldGateway(st, mn, clk)
	if d := mustDeliver(t, s, 1, ""); d.Gateway != "unified-2" {
		t.Fatalf("普通路径给了 %s，应是歇得最久的 unified-2", d.Gateway)
	}
}

// force=1 必须换一个网关：调用方就是因为手上那张不行了才来的。
// 「手上那张」靠槽位 LastTouch 认（租约字段已按 2026-10-01 的实测删掉，见 types.Pair）。
func TestForceSkipsHeldGateway(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	seedHeldGateway(st, mn, clk)

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
func seedHeldGateway(st *fakeStore, mn *fakeMinter, clk *clock) {
	now := clk.now()
	delete(st.gws, "unified-3")
	// unified-2 没票 ⇒ 这两个用例都要走回落铸票，而回落是从区域出发、不冲着
	// 「歇得最久的那个候选」去的 ⇒ 落点得显式钉在 unified-2 上。
	// 用例仍然在验「别把他手上的 unified-1 发回去」。
	landBare(mn, "unified-2")
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
	s, _, _, _ := anonRig(t, 6, 1, 1)
	first, err := s.DeliverAnonymous(context.Background(), "x", Request{}, false)
	if err != nil {
		t.Fatalf("DeliverAnonymous: %v", err)
	}
	forced, err := s.DeliverAnonymous(context.Background(), "x", Request{Force: true}, false)
	if err != nil {
		t.Fatalf("force: %v", err)
	}
	if forced.Gateway == first.Gateway {
		t.Fatalf("匿名 force 还是给了同一个网关 %s", forced.Gateway)
	}
	// 指名要自己正租着的那张 + force ⇒ 没得换，老实失败。
	if _, err := s.DeliverAnonymous(context.Background(), "x", Request{Gateway: forced.Gateway, Force: true}, false); err == nil {
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
		if err := st.NoteRegionMint(id, "us-east", "", clk.now()); err != nil {
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
			if err := st.NoteRegionMint(id, r.Key, "", clk.now()); err != nil {
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

// bestRegion 的三档排序，整条钉一遍：没有活种子票 > 没摸过的大区 > 终身铸得最少。
//
// 要紧的是**第一档压过第二档**：丢了种子的大区哪怕已经铸过几十发，也排在一个终身
// 没摸过的大区前面。理由不对称——种子一掉，那个大区的所有账号都回落到住宅出口，
// 一张票就能把整个大区的便宜路径恢复；而「摸一个新落点」只多一个槽位。
// 另外两条用例各自只盖一档（TestMinterPicksByRegionLedger 盖第二三档、
// TestPremintRefillsSeedlessRegionsFirst 盖第一档但那里所有大区终身计数都是 0），
// 跨档那一格没人盯，而 bestRegion 里正是那个 `p && !bestPrefer` 分支。
func TestBestRegionPrefersSeedlessOverUnseen(t *testing.T) {
	seedless, unseen := types.Regions[0], types.Regions[1]
	prefer := func(key string) bool { return key == seedless.Key }
	// 第一档那个大区铸得最多，第二档那个终身没铸过 —— 排序只能靠档位分开它们。
	lifetime := map[string]int{seedless.Key: 99}
	for _, r := range types.Regions[2:] {
		lifetime[r.Key] = 50
	}

	got, count, preferred, ok := bestRegion(types.Exit{}, nil, 6, lifetime, nil, prefer)
	if !ok {
		t.Fatal("配额没打满，不该说挑不出来")
	}
	if got.Key != seedless.Key {
		t.Fatalf("挑了 %s，该挑丢了种子的 %s（它铸过 99 发也要先补）", got.Key, seedless.Key)
	}
	if !preferred || count != 99 {
		t.Fatalf("回报不对: preferred=%v count=%d", preferred, count)
	}

	// 种子都齐了（prefer 恒 false）⇒ 退到第二档：挑终身没铸过的那个。
	got, _, preferred, ok = bestRegion(types.Exit{}, nil, 6, lifetime, nil,
		func(string) bool { return false })
	if !ok || got.Key != unseen.Key || preferred {
		t.Fatalf("种子齐了该挑终身没铸过的 %s，得到 %s（preferred=%v, ok=%v）",
			unseen.Key, got.Key, preferred, ok)
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
	if a, _, ok := s.minterFor(jp, 0, 0, nil); ok {
		t.Fatalf("east-asia 配额满了还派了账号 %d", a.ID)
	}
	// 别的区域照旧：定向到 AU 就该放行，而且返回的区域是**那个出口自己**。
	au := types.Exit{Region: "AU"}
	_, r, ok := s.minterFor(au, 0, 0, nil)
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
		seedGateway(st, mn, fmt.Sprintf("unified-%d", i), i-1, 1)
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
	if n := s.FreeGateways(); n != types.SpareTarget {
		t.Fatalf("第一轮补到 %d 张活票，目标 %d（发数 %d）",
			n, types.SpareTarget, len(mn.mintedGateways()))
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

// 目标深度封顶在上游网关总数：最忙的号快用遍全部网关时，再加富余也铸不出来，
// 不封顶的话每轮保温都会为够不着的那几张白打。
func TestPairTargetCappedAtGatewayCeiling(t *testing.T) {
	s, st, _, clk := rig(t, types.ModePool)
	for i := 1; i < types.GatewayCeiling; i++ {
		gw := fmt.Sprintf("unified-%d", i)
		st.slots[slotKey(1, gw)] = types.Slot{AccountID: 1, Gateway: gw, LastTouch: clk.now(), LastDeliver: clk.now()}
	}
	if got := s.PairTarget(); got != types.GatewayCeiling {
		t.Fatalf("最忙的号用过 %d 张时目标是 %d，应封顶在 %d", types.GatewayCeiling-1, got, types.GatewayCeiling)
	}
}

// min_pairs 是**下界**：池子空闲（没人用过票）时目标不塌到 SpareTarget，而是托在这个数上。
// 和富余的区别就是这一格 —— 富余是相对量，跟着负载走。
func TestMinPairsFloorsTheTarget(t *testing.T) {
	s, st, _, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	c := s.opt.Live.Get()
	c.MinPairs = 12
	s.opt.Live = config.NewLive("", c)

	// 没人用过票 ⇒ busiest=0 ⇒ 相对目标是 SpareTarget，下界把它顶到 12。
	if got := s.PairTarget(); got != 12 {
		t.Fatalf("空闲时目标应被下界托到 12，实为 %d", got)
	}
	// 负载超过下界之后下界就不再起作用（取较大者，不是取下界）。
	for i := 1; i <= 20; i++ {
		gw := fmt.Sprintf("unified-%d", i)
		st.slots[slotKey(1, gw)] = types.Slot{AccountID: 1, Gateway: gw,
			LastTouch: clk.now(), LastDeliver: clk.now()}
	}
	if got, want := s.PairTarget(), 20+types.SpareTarget; got != want {
		t.Fatalf("负载高于下界时应按 busiest+富余 = %d，实为 %d", want, got)
	}
}

// 下界填得比网关总数还大就按上限算 —— **不能**走富余那条「超了就停止预铸」的路：
// 「至少 N 张」解释成「一张都不要」正好反过来。
func TestMinPairsOverCeilingCapsInsteadOfStopping(t *testing.T) {
	s, st, _, _ := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	c := s.opt.Live.Get()
	c.MinPairs = types.GatewayCeiling + 50
	s.opt.Live = config.NewLive("", c)
	if got := s.PairTarget(); got != types.GatewayCeiling {
		t.Fatalf("下界超过网关总数时应封顶在 %d，实为 %d", types.GatewayCeiling, got)
	}
}

// 富余显式关掉预铸时，下界不许把它打开：那是运营方的显式决定。
func TestMinPairsDoesNotReopenPremintWhenSpareStopsIt(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.markDemand()
	c := s.opt.Live.Get()
	c.SpareTarget = types.GatewayCeiling + 1
	c.MinPairs = 20
	s.opt.Live = config.NewLive("", c)
	if got := s.PairTarget(); got != 0 {
		t.Fatalf("富余关掉预铸时下界不该把目标顶回 %d", got)
	}
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("富余关掉预铸、下界却让它打了 %d 发", n)
	}
}

// 富余张数填超过网关总数上限 = 停止预铸（不报错）：目标归零，保温一发都不打。
func TestSpareOverCeilingStopsPremint(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.markDemand()
	c := s.opt.Live.Get()
	c.SpareTarget = types.GatewayCeiling + 1
	s.opt.Live = config.NewLive("", c)
	if got := s.PairTarget(); got != 0 {
		t.Fatalf("富余张数超过上限时目标应是 0（停止预铸），实为 %d", got)
	}
	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("富余张数超过上限还预铸了 %d 发", n)
	}
}

// **全员刚取过票也照样预铸。** 这里原先是三条用例，钉的是 premintWasted 那道闸
// （「全员在全部已知网关上都还在消费冷却里 ⇒ 这一轮别铸」）和它的两个豁免
// （失败的续期不算、有外部消费者在取票不算）。2026-10-03 消费冷却整条删掉之后
// 那个状态不存在了，闸和三条用例一起没了。
//
// 换成钉反面：铺出当年最硬的那个场景（两个号 × 三个网关全部刚取过票），预铸必须照打。
// 那道闸 24 小时内触发 279 次，是铸票慢的三个原因之一；它最可能被人「顺手修回去」。
func TestPremintRunsEvenWhenEveryoneJustFetched(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.markDemand()
	// LastDeliver 铺满、LastTouch 留空：这些是**消费**，不是池子自己打的那一发。
	for _, id := range []int64{1, 2} {
		for gw := range st.gws {
			st.slots[slotKey(id, gw)] = types.Slot{AccountID: id, Gateway: gw,
				LastDeliver: clk.now()}
		}
	}

	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n == 0 {
		t.Fatal("全员刚取过票就不预铸了：消费冷却那道闸被修回来了")
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
// 验证要用消费者自己的凭据先铸一张活票再 echo，那是 2 发上游请求，花在与铸票共用的
// 区域预算上；而它在一个没碰过的网关上本来就是满血。state-echo 有假阴无假阳，关掉只少一个
// 读数。（那两发**不**烧窗口 —— 2026-10-02 实测窗口内连打多发都是满血。）
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
		ExpiresAt: clk.now().Add(types.PairLifetime), CflbExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 1}
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
	s, _, clk, _ := anonRig(t, 1, 1, 1)
	if _, err := s.DeliverAnonymous(context.Background(), "outsider", Request{}, false); err != nil {
		t.Fatalf("DeliverAnonymous: %v", err)
	}
	clk.add(types.DeliverTTL + time.Second)
	if _, err := s.DeliverAnonymous(context.Background(), "outsider", Request{}, false); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("匿名窗口过完还发得出来: %v", err)
	}
	// 换一个上游 ID 就是另一条账本，不受连坐。
	if _, err := s.DeliverAnonymous(context.Background(), "someone-else", Request{}, false); err != nil {
		t.Fatalf("另一个匿名上游被连坐了: %v", err)
	}
}

// ---------- 满血窗口过完就能再发（第 10 条） ----------

// 交付完的槽位在**一个满血窗口**之内不再发第二张，过完就放行。
//
// 这条用例原名 TestSlotCooldownComesFromConfig，钉的是「slot_cooldown_minutes 要现读
// Live、别把 4 小时编进去」。2026-10-03 消费冷却那条闸整个删了（见 sched.burning），
// 那个配置项降级成读数与统计窗口、不再拦交付 ⇒ 这里配 10 分钟或 4 小时都一样，
// 真正管事的是 types.FullWindow。配置项照旧设上，就为了证明它**不**再影响交付。
func TestDeliverReopensAfterTheFullWindow(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	mn.land["US"] = "unified-1"
	mn.full["unified-1"] = true
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(types.PairLifetime), CflbExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 1}
	s := New(st, mn, Options{Mode: types.ModeSingle, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.SlotCooldownMinutes = 10 })})

	mustDeliver(t, s, 1, "")
	clk.add(types.DeliverTTL + time.Second)
	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("窗口刚过完就又发了一张: %v", err)
	}
	// 满血窗口一过就该恢复 —— 而且**早于**配置里那 10 分钟，证明拦它的不再是那个旋钮。
	clk.add(types.FullWindow)
	if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
		t.Fatalf("满血窗口过完了还交付不了: %v", err)
	}
}

// ---------- 一张活票都没有时只抽一次签（A 条） ----------

// 回落**只铸一发**，哪怕 MaxTriesPerDeliver 开到 5、哪怕抽到的落点正好不可用。
//
// 出口只能把落点「倾向」到某个网关、定不住 ⇒「先试 X 再试 Y」就是抽两次签、
// 花两发请求烧两个铸票者槽位，而两次的分布一模一样。
func TestFreshMintDrawsOnlyOnce(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.opt.MaxTriesPerDeliver = 5
	// 一张活 pair 都没有；池子刚拿 1 号的凭据在 unified-3 上打过一发（LastTouch），
	// 而这一发无论从哪个出口打都会落到 unified-3 ⇒ 抽到的签不可用。
	//
	// 铺的是 LastTouch 不是 LastDeliver：后者（「他取过票」）2026-10-03 起不再拦交付
	// ——消费归客户端账本管（见 sched.burning），拿它铺这个场景会让用例什么都没拦住。
	st.slots[slotKey(1, "unified-3")] = types.Slot{AccountID: 1, Gateway: "unified-3",
		LastTouch: clk.now()}
	for k := range mn.land {
		mn.land[k] = "unified-3"
	}

	if _, err := s.Deliver(context.Background(), 1, Request{}); !errors.Is(err, ErrNoFullSlot) {
		t.Fatalf("err=%v，抽到的落点正在烧窗口就该老实 503", err)
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
	for i, gw := range []string{"unified-1", "unified-2", "unified-3"} {
		seedGateway(st, mn, gw, i, 1)
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
		ExpiresAt: clk.now().Add(types.PairLifetime), CflbExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 2}

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

// ---------- 一个区域一次只许有一发裸铸（B 条） ----------

// 两路交付同时撞上冷池子：只许打出去**一发**，另一路当场被挡回去。
//
// 原来这一条写的是「按网关拦，第二路等第一路铸完直接用那张票」。按需铸票（mintByRegion）
// 之后没有网关可拦了——出手前根本不知道会落到哪 ⇒ 退化成按区域单飞。
// 而且只能是「占不住就走」，不能排队：两路的铸票者不是同一个号，排队等于让后一路在走到
// **账号锁**之前就停住，前一路正等着账号锁 ⇒ 真死锁（TestNoDeadlockBetweenAccounts
// 的屏障就是钉这件事，2026-10-02 拿全局锁实测 3 秒超时）。
//
// 要守的不变量没变：**不许为同一件事烧两个铸票者槽位**。槽位是池子最紧的资源，
// 而被挡回去的那一路重试时第一路的票已经在池里了，一发上游都不用打——下面最后一段
// 就是在证这件事。
//
// 三个号、消费者是 2 和 3，而 2 和 3 的本小时配额**打满**⇒ 两路的铸票者都只能是
// 账号 1，于是两路是同一个 (账号 × 大区)，正是 claimRegion 该拦的那一格。
//
// 配额不打满的那一版是条随调度顺序摆的用例：前一路一进 mint.spend 就把账号 1 在
// us-east 的发数抬成 1，后一路的 minterFor 按「本小时总发数最少的优先」就改挑账号 3
// ⇒ 两个不同的号走同一个大区 ⇒ 2026-10-03 收窄粒度之后这是**允许**的
// （见 TestConcurrentBareMintsShareARegionAcrossAccounts），于是它时红时绿。
//
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
	// 把两个消费者从铸票者候选里剔掉：九个大区全打满本小时配额。
	mn.rateLimit = 6
	full := map[string]int{}
	for _, r := range types.Regions {
		full[r.Key] = 6
	}
	mn.ratePerAcct = map[string]map[string]int{"acct-2": full, "acct-3": full}
	// 只有一个网关、一张票都没有 ⇒ 两个人必然撞上同一个区域。
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	landBare(mn, "unified-1")
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
	// 一发、而且只一发：这是这条用例的全部重点。
	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("两路裸铸打了 %d 发上游，单飞没拦住（烧的是两个铸票者槽位）", n)
	}
	// 一个成、一个被挡。谁成谁被挡是调度顺序的事，不钉。
	won, lost, blocked := 0, 0, int64(0)
	for i, err := range errs {
		switch {
		case err == nil:
			won++
			if out[i].Gateway != "unified-1" {
				t.Fatalf("第 %d 路落点不对: %+v", i+1, out[i])
			}
		case errors.Is(err, ErrNoLivePair):
			// 归成「等得起」那一格：调用方该重试，而不是当成永久 503
			lost, blocked = lost+1, int64(i+2)
		default:
			t.Fatalf("第 %d 路的错不该是这个: %v", i+1, err)
		}
	}
	if won != 1 || lost != 1 {
		t.Fatalf("成 %d 路、被挡 %d 路，应各一路", won, lost)
	}
	// 被挡那一路重试：池子里已经有票了 ⇒ 拿到同一张，而且**一发上游都不用打**。
	// 这是「挡回去」能成立的全部前提，不验这一条就等于把 503 当成了功能。
	retry, err := s.Deliver(context.Background(), blocked, Request{})
	if err != nil {
		t.Fatalf("被挡的 %d 号重试该直接捡到池里那张票: %v", blocked, err)
	}
	if retry.Gateway != "unified-1" {
		t.Fatalf("重试拿到 %s，池里只有 unified-1 那张", retry.Gateway)
	}
	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("重试又打了上游，共 %d 发", n)
	}
}

// claimRegion 的粒度就是 (账号 × 大区)，三条都要钉：同号同区拦、**换号不拦**、换区不拦。
//
// 中间那条是 2026-10-03 改的方向，也是唯一会被「顺手改回去」的一条：按大区整体拦
// 看着更安全，代价是并发被焊死在 9，而实测不支持那个安全（us-east 一个大区 253 发
// 摸到 36 个不同落点，105 个网关里 100 个被多个账号打到过 ⇒ 两个号走同一个大区
// 大概率是两个不同落点）。直接测这个方法而不是拿两路 Deliver 并发去测：后者的铸票者
// 由 minterFor 按本小时发数现挑，前一路一进速率闸就把读数改了，用例会随调度顺序摆。
func TestClaimRegionIsPerAccountAndRegion(t *testing.T) {
	clk := newClock()
	s := New(newStore(clk), newMinter(clk), Options{Mode: types.ModePool, Now: clk.now})

	release, ok := s.claimRegion(1, "us-east")
	if !ok {
		t.Fatal("头一发就占不住")
	}
	if _, ok := s.claimRegion(1, "us-east"); ok {
		t.Fatal("同一个号在同一个大区占住了两发：第二发落点和第一发逐字相同，白烧一个槽位")
	}
	other, ok := s.claimRegion(2, "us-east")
	if !ok {
		t.Fatal("换个号走同一个大区被拦了：这是并发被焊死在 9 的那个老毛病")
	}
	elsewhere, ok := s.claimRegion(1, "east-asia")
	if !ok {
		t.Fatal("同一个号换个大区被拦了")
	}
	other()
	elsewhere()

	// 放掉之后要能再占，否则一轮预铸之后这个组合就永久不可用了。
	release()
	if again, ok := s.claimRegion(1, "us-east"); !ok {
		t.Fatal("release 之后占不回来")
	} else {
		again()
	}
}

// pickExit 只许给九个大区的代表出口：它是唯一会产出「反查不到大区」的出口的地方，
// 而那个空串会一路带进速率闸的分桶键（acct|""，九个大区的定向铸票挤一个桶互相饿死）
// 和 (账号 × 大区) 台账。
func TestPickExitOnlyReturnsStandardRegionExits(t *testing.T) {
	wild := types.Exit{Region: "US", City: "Chicago"}
	if types.RegionKeyOf(wild) != "" {
		t.Fatalf("这条用例的前提坏了：%+v 居然反查到了大区", wild)
	}
	// 混着放：野出口在前，防「取第一条」式的实现碰巧蒙对。
	mixed := types.Gateway{Name: "unified-1", ReachableExits: []types.Exit{
		wild, {Region: "US", City: "Dallas"}, {Region: "JP"}}}
	for i := 0; i < 20; i++ { // pickExit 是随机取，多转几圈
		got, ok := pickExit(mixed)
		if !ok {
			t.Fatal("有一条 JP 代表出口，不该说没有可达出口")
		}
		if got != (types.Exit{Region: "JP"}) {
			t.Fatalf("挑到了反查不到大区的出口 %+v", got)
		}
	}
	// 一条标准出口都没有 ⇒ 认「没有已知可达出口」，而不是拿野出口去定向铸：
	// 那一发的配额会记进空串桶，落点也不会进任何一个大区的台账。
	if _, ok := pickExit(types.Gateway{Name: "unified-2",
		ReachableExits: []types.Exit{wild}}); ok {
		t.Fatal("只有野出口时仍然挑了一条出来")
	}
}

// gatewaysFrom 逐字比整个出口，不只比国家。
//
// 它只给「读不出落点、保守记触碰」那条路用，方向偏多记——但只比国家会多到离谱：
// {US,""} 会把只从 {US,"Seattle"} 摸到过的网关也按住 183 秒，而那两个出口的落点集
// 实测是 36 / 29、交集 27，多出来的那些从没在这个出口上被摸到过。
func TestGatewaysFromMatchesTheWholeExitNotJustTheCountry(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	east, west := types.Exit{Region: "US"}, types.Exit{Region: "US", City: "Seattle"}
	st.gws["only-east"] = types.Gateway{Name: "only-east", ReachableExits: []types.Exit{east}}
	st.gws["only-west"] = types.Gateway{Name: "only-west", ReachableExits: []types.Exit{west}}
	st.gws["both"] = types.Gateway{Name: "both", ReachableExits: []types.Exit{east, west}}
	st.gws["jp"] = types.Gateway{Name: "jp", ReachableExits: []types.Exit{{Region: "JP"}}}
	s := New(st, newMinter(clk), Options{Mode: types.ModePool, Now: clk.now})

	for _, tc := range []struct {
		name string
		exit types.Exit
		want []string
	}{
		{"us-east", east, []string{"both", "only-east"}},
		{"us-west", west, []string{"both", "only-west"}},
	} {
		got := s.gatewaysFrom(tc.exit)
		sort.Strings(got)
		if !slices.Equal(got, tc.want) {
			t.Fatalf("%s: 得到 %v，要 %v", tc.name, got, tc.want)
		}
	}
}

// 出口国家不符的那一发，两本按大区分格的账都不许记。
//
// 1650 发里 23 次（us-east 要美国，拿到 BR/AR/CL/CD/JM/MQ/MX）。落点是上游按真实
// 出口 IP 分的 ⇒ 这一发对「哪个大区能到哪个网关」什么都没证明：
//   - 记进 ReachableExits 会留一条假线索，pickExit 下次照它定向铸、必然漂移
//   - 记进 (账号 × 大区) 台账会把一个还没走通的大区标成走过，而 bestRegion 优先挑
//     「终身没铸过」的 ⇒ 从此不再去试它
//
// 网关本体要照记：它确实存在，而 Gateways() 是候选清单的唯一来源。
func TestMintDoesNotChargeTheRegionWhenTheExitCountryDisagrees(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	// 要巴西，住宅代理给了阿根廷。
	mn.geoCountry = "AR"
	landBare(mn, "unified-9")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})

	acct, _ := s.account(1)
	got, err := s.mintOnce(context.Background(), acct, types.Exit{Region: "BR"}, "sid-1",
		types.Pair{}, "", "")
	if err != nil {
		t.Fatalf("铸票本身不该失败（国家不符不是失败）: %v", err)
	}
	if got.pair.Gateway != "unified-9" {
		t.Fatalf("落点不对: %+v", got.pair)
	}
	if _, ok := st.gws["unified-9"]; !ok {
		t.Fatal("网关本体没记：Gateways() 是候选清单的唯一来源，漏了它这个落点就永远挑不中")
	}
	if ex := st.gws["unified-9"].ReachableExits; len(ex) != 0 {
		t.Fatalf("把假线索记进了 ReachableExits: %+v", ex)
	}
	if ms := st.RegionMints(); len(ms) != 0 {
		t.Fatalf("把这一发记进了大区台账: %+v", ms)
	}

	// 对照组：国家相符时两本账都要记，否则上面那条断言用「什么都不记」也能过。
	mn.geoCountry = ""
	if _, err := s.mintOnce(context.Background(), acct, types.Exit{Region: "BR"}, "sid-2",
		types.Pair{}, "", ""); err != nil {
		t.Fatalf("对照组铸票失败: %v", err)
	}
	if ex := st.gws["unified-9"].ReachableExits; len(ex) != 1 || ex[0] != (types.Exit{Region: "BR"}) {
		t.Fatalf("国家相符时该把出口记进 ReachableExits，得到 %+v", ex)
	}
	ms := st.RegionMints()
	if len(ms) != 1 {
		t.Fatalf("国家相符时该恰好记一笔台账，得到 %+v", ms)
	}
	for _, m := range ms {
		if m.RegionKey != "south-america" || m.Count != 1 || m.AccountID != 1 {
			t.Fatalf("台账那一笔不对: %+v", m)
		}
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

// 种子票：这个大区手上有活 __cflb 时，预铸走我们自己的普通机房出口、把那张票送出去，
// 而落点是**执行这一发的号自己**在那个大区的网关，不是种子那张票的网关。
//
// 这一条是整条种子路的要害。2026-10-03 实测：3 个 free 号带同一张 east-asia 票从同一个
// 机房出口打，分别落 unified-88 / 94 / 75（裸打基线 146 / 120 / 128）⇒ __cflb 只钉大区。
// 实现里摘 __oailb 的动作在 mint.Mint（见 TestSeedBoundSIDSendsOnlyTheCflb），
// sched 这一层送的是完整那张票。
func TestPremintUsesASeedTicketWhenTheRegionHasOne(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	us := types.Regions[0]
	// 两个号在 us-east 都落 unified-9（池子还没见过它）。
	mn.landByAcct = map[string]string{
		comboKey(1, exitKey(us.Exit)): "unified-9",
		comboKey(2, exitKey(us.Exit)): "unified-9",
	}
	// **每个大区都有一张活票**（= 都有种子）。这是池子热起来之后的常态，也是这个用例
	// 必须这样铺的原因：「丢了种子的大区优先」会让缺种子的大区排在前面，只给 us-east
	// 铺种子的话挑中的是别的大区，这个用例就测不到种子路了。
	for i, r := range types.Regions {
		gw := fmt.Sprintf("unified-s%d", i)
		st.gws[gw] = types.Gateway{Name: gw, FirstSeenBy: 1, ReachableExits: []types.Exit{r.Exit}}
		st.pairs[gw] = types.Pair{Gateway: gw, Cookie: cookiePrefix + gw,
			RegionKey: r.Key, ExpiresAt: clk.now().Add(types.PairLifetime),
			CflbExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 2}
	}
	// 目标 = 大区数 + 1、已有大区数张活票 ⇒ 缺口 1 发。
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.SpareTarget = len(types.Regions) + 1 })})
	s.markDemand()

	s.warmOnce(context.Background())
	if got := mn.mintedGateways(); len(got) != 1 || got[0] != "unified-9" {
		t.Fatalf("该落到这个号自己在 us-east 的网关 unified-9，实际 %v", got)
	}
	sent := mn.seedShots()
	if len(sent) != 1 {
		t.Fatalf("该走种子出口打一发，实际走了 %d 发", len(sent))
	}
	for sid, cookie := range sent {
		if !strings.HasPrefix(sid, "seed") {
			t.Fatalf("sid %q 不是种子出口的", sid)
		}
		if cookie != cookiePrefix+"unified-s0" {
			t.Fatalf("送出去的不是 us-east 那张种子票：%q", cookie)
		}
	}
}

// 快死的票不许当种子：只送 `__cflb` 时上游一般不补发新的，铸出来那张会原样留着种子
// 那张、寿命跟着它一起到点 ⇒ 种子剩两分钟就只能铸出一张剩两分钟的票，低于 DeliverFloor、
// 生下来就发不出去，还会被记成「补上了一格」。这种时候该回落住宅（拿一张满寿命的新 cflb），
// 而不是花一发上游换一张死胎。门槛见 seedCflbFloor。
func TestPremintSkipsASeedThatDiesTooSoon(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	us := types.Regions[0]
	mn.landByAcct = map[string]string{
		comboKey(1, exitKey(us.Exit)): "unified-9",
		comboKey(2, exitKey(us.Exit)): "unified-9",
	}
	// 每个大区都铺一张票，但 us-east 那张只剩 2 分钟（低于 seedCflbFloor）⇒ 它算
	// 「没有种子」，于是「丢了种子的大区优先」正好把这一发派到 us-east 上。
	for i, r := range types.Regions {
		gw := fmt.Sprintf("unified-s%d", i)
		left := types.PairLifetime
		if r.Key == us.Key {
			left = 2 * time.Minute
		}
		st.gws[gw] = types.Gateway{Name: gw, FirstSeenBy: 1, ReachableExits: []types.Exit{r.Exit}}
		st.pairs[gw] = types.Pair{Gateway: gw, Cookie: cookiePrefix + gw,
			RegionKey: r.Key, ExpiresAt: clk.now().Add(left),
			CflbExpiresAt: clk.now().Add(left), MintedBy: 2}
	}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.SpareTarget = len(types.Regions) + 1 })})
	s.markDemand()

	s.warmOnce(context.Background())
	if got := mn.mintedGateways(); len(got) == 0 {
		t.Fatal("该照常铸票，只是走住宅")
	}
	// 判据只盯那张快死的票有没有被送出去。本轮后面几发会用上刚铸出来的新票当种子，
	// 那是对的，不该一起拦。
	for sid, cookie := range mn.seedShots() {
		if cookie == cookiePrefix+"unified-s0" {
			t.Fatalf("把只剩 2 分钟的那张票当种子送出去了（sid %s）：铸出来的票会跟着它一起死",
				sid)
		}
	}
}

// 丢了种子票的大区优先补：它一掉，那个大区的所有账号都回落到住宅出口，而一张票就能
// 把整个大区的便宜路径恢复 ⇒ 收益高于普通的「去摸一个新落点」（后者只多一个槽位）。
func TestPremintRefillsSeedlessRegionsFirst(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	// 除了最后一个大区，其余都有活种子票。每个大区的落点各不相同。
	seedless := types.Regions[len(types.Regions)-1]
	for i, r := range types.Regions {
		gw := fmt.Sprintf("unified-s%d", i)
		st.gws[gw] = types.Gateway{Name: gw, FirstSeenBy: 1, ReachableExits: []types.Exit{r.Exit}}
		mn.land[exitKey(r.Exit)] = fmt.Sprintf("unified-n%d", i) // 裸铸会落到的新落点
		if r.Key == seedless.Key {
			continue
		}
		st.pairs[gw] = types.Pair{Gateway: gw, Cookie: cookiePrefix + gw,
			RegionKey: r.Key, ExpiresAt: clk.now().Add(types.PairLifetime),
			CflbExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 2}
	}
	// 缺口 1 发：那一发必须花在没有种子的那个大区上。
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.SpareTarget = len(types.Regions) })})
	s.markDemand()

	s.warmOnce(context.Background())
	want := fmt.Sprintf("unified-n%d", len(types.Regions)-1)
	if got := mn.mintedGateways(); len(got) != 1 || got[0] != want {
		t.Fatalf("该先补没种子的 %s（落点 %s），实际打了 %v", seedless.Key, want, got)
	}
	// 而且那一发只能走住宅：那个大区本来就没有种子票可送。
	if sent := mn.seedShots(); len(sent) != 0 {
		t.Fatalf("没种子的大区不该走种子出口，实际 %v", sent)
	}
}

// 这个大区没有活票就没有种子，照旧走住宅出口 —— 别把别的大区的票当种子送出去。
func TestPremintFallsBackToResidentialWhenTheRegionHasNoSeed(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	us, oc := types.Regions[0], types.Regions[6]
	mn.landByAcct = map[string]string{
		comboKey(1, exitKey(us.Exit)): "unified-9",
		comboKey(2, exitKey(us.Exit)): "unified-9",
	}
	// 唯一那张活票在 oceania，而预铸挑中的是 us-east（终身计数全 0 ⇒ 取第一个大区）。
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{oc.Exit}}
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		RegionKey: oc.Key, ExpiresAt: clk.now().Add(types.PairLifetime),
		CflbExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 2}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.SpareTarget = 2 })})
	s.markDemand()

	s.warmOnce(context.Background())
	if got := mn.mintedGateways(); len(got) != 1 || got[0] != "unified-9" {
		t.Fatalf("该照旧裸铸到 unified-9，实际 %v", got)
	}
	if sent := mn.seedShots(); len(sent) != 0 {
		t.Fatalf("us-east 没有种子票，不该走种子出口，实际 %v", sent)
	}
}

// 种子票的 __cflb 快到点了就不能用：路由要活过这一次往返。
func TestSeedPairIgnoresTicketsWhoseCflbIsAboutToDie(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	us := types.Regions[0]
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{us.Exit}}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now})

	base := types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		RegionKey: us.Key, ExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 2}
	for _, tc := range []struct {
		name string
		cflb time.Duration
		want bool
	}{
		{"还剩很久", types.PairLifetime, true},
		{"刚好够一次往返", seedCflbFloor + time.Second, true},
		{"只剩一点点", seedCflbFloor - time.Second, false},
		{"已经死了", -time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := base
			p.CflbExpiresAt = clk.now().Add(tc.cflb)
			st.pairs["unified-1"] = p
			if _, ok := s.seedPairFor(us.Key); ok != tc.want {
				t.Fatalf("cflb 还剩 %s，能当种子=%v，想要 %v", tc.cflb, ok, tc.want)
			}
		})
	}
}

// D 条：台账是空的时候预铸也得有封顶 —— 失手预算 = 缺口，总发数封顶 2n。
//
// 这是替掉旧「连续两发白烧就收手」的那条。旧规则数的是「随机撞上已有落点」，
// 新规则数的是「预测错」；而台账空的时候一个组合都预测不出来，于是候选集
// （账号数 × 9）会全部被当成值得一试 —— 没有这个预算就是一轮打上千发。
func TestPremintCapsWastedShotsAtTheGap(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	// 九个区域全都落到同一个网关，而它已经有一张活票 ⇒ 每一发都是失手。
	for _, r := range types.Regions {
		mn.land[exitKey(r.Exit)] = "unified-1"
	}
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(types.PairLifetime), CflbExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 2}
	// 富余目标 3 ⇒ 缺口 = 3 - 1 = 2（unified-1 那张活票算一格）⇒ 失手预算 2。
	// 候选组合有 2 × 9 = 18 个，所以「只打 2 发」只可能来自这个预算。
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.SpareTarget = 3 })})
	s.markDemand()

	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 2 {
		t.Fatalf("裸铸打了 %d 发，失手预算是缺口 2 发就该收手（候选组合 18 个）", n)
	}
}

// 台账知道这个组合上次落在哪、而那个落点还有活票 ⇒ 一发都不该打。
//
// 这是新规则真正省下来的那部分：旧版要**打完**才知道白打了。
func TestPremintSkipsCombosWhosePredictedGatewayStillHasAPair(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	for _, r := range types.Regions {
		mn.land[exitKey(r.Exit)] = "unified-1"
	}
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(types.PairLifetime), CflbExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 2}
	// 台账：两个号的九个大区全都落过 unified-1 ⇒ 全部可预测、全部该跳过。
	for _, id := range []int64{1, 2} {
		for _, r := range types.Regions {
			if err := st.NoteRegionMint(id, r.Key, "unified-1", clk.now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.SpareTarget = 9 })})
	s.markDemand()

	s.warmOnce(context.Background())
	if n := len(mn.mintedGateways()); n != 0 {
		t.Fatalf("预测落点全都还有活票，一发都不该打，实际打了 %d 发", n)
	}
}

// 台账说这个组合的落点已经没票了 ⇒ 正是该补的那一格，而且不算失手。
func TestPremintRefillsCombosWhosePairExpired(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	// 1 号在 us-east 落 unified-7，2 号在 us-east 落 unified-8（网关 = f(账号, 大区)）。
	mn.landByAcct = map[string]string{
		comboKey(1, exitKey(types.Regions[0].Exit)): "unified-7",
		comboKey(2, exitKey(types.Regions[0].Exit)): "unified-8",
	}
	for _, r := range types.Regions {
		mn.land[exitKey(r.Exit)] = "unified-1" // 其余大区都落回已有活票那个
	}
	st.gws["unified-1"] = types.Gateway{Name: "unified-1", FirstSeenBy: 1,
		ReachableExits: []types.Exit{{Region: "US"}}}
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(types.PairLifetime), CflbExpiresAt: clk.now().Add(types.PairLifetime), MintedBy: 2}
	// unified-7 / unified-8 在表上但**票已经过期**（没写 pairs）⇒ 这两格该补。
	for _, g := range []string{"unified-7", "unified-8"} {
		st.gws[g] = types.Gateway{Name: g, FirstSeenBy: 1, ReachableExits: []types.Exit{{Region: "US"}}}
	}
	for _, id := range []int64{1, 2} {
		for _, r := range types.Regions {
			gw := "unified-1"
			if r.Key == types.Regions[0].Key {
				gw = map[int64]string{1: "unified-7", 2: "unified-8"}[id]
			}
			if err := st.NoteRegionMint(id, r.Key, gw, clk.now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.SpareTarget = 9 })})
	s.markDemand()

	s.warmOnce(context.Background())
	got := mn.mintedGateways()
	sort.Strings(got)
	want := []string{"unified-7", "unified-8"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("该只补过期那两格 %v，实际打了 %v", want, got)
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

// 交付要如实报出**铸这张票的出口**属于哪个区域，而且和区域台账记的那一格是同一个口径。
//
// 消费端（sub2api）拿它把自己的落点按大区归档。网关 = (区域 × 账号)：同一个网关名在
// 不同账号眼里可能来自不同区域 ⇒ 事后反查不出来，只有铸票那一刻知道。口径和台账错开
// 一格的后果是消费端把落点记到别的大区，于是「这个号在哪个大区还有没烧过的落点」全错。
func TestDeliveryCarriesTheMintRegion(t *testing.T) {
	s, st, _, _ := driftRig(t, "unified-1", "unified-77")

	d, err := s.Deliver(context.Background(), 1, Request{})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	mints := st.RegionMints()
	if len(mints) != 1 {
		t.Fatalf("区域台账 %d 格，这一发现铸该恰好记一格: %+v", len(mints), mints)
	}
	var want string
	for _, m := range mints {
		want = m.RegionKey
	}
	if want == "" {
		t.Fatal("台账记了个空区域，这条用例什么都没验到")
	}
	if d.Region != want {
		t.Fatalf("交付报的区域是 %q，台账记的是 %q", d.Region, want)
	}
	if p, ok := st.Pair("unified-77"); !ok || p.RegionKey != want {
		t.Fatalf("pair 上存的区域是 %q，应为 %q", p.RegionKey, want)
	}
}

// 不是现铸那条路（直接发池子手上的票）也要报区域；读不出来时报空，不许拿别的顶上。
func TestDeliveryReportsThePairsRegion(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	// 两个开关都关着（默认）：这一发直接发池子手上那张票，不会重铸 —— 重铸会按**那一发的
	// 出口**重算区域，就验不到「交付报的是手上这张的区域」。
	for i, gw := range []string{"unified-1", "unified-2"} {
		seedGateway(st, mn, gw, i, 1)
	}
	seedPairs(st, clk)
	pr := st.pairs["unified-1"]
	pr.RegionKey = "europe"
	st.pairs["unified-1"] = pr
	s := New(st, mn, Options{Mode: types.ModeSingle, Now: clk.now, MintGap: -1, RenewGap: -1})

	if d := mustDeliver(t, s, 1, "unified-1"); d.Region != "europe" {
		t.Fatalf("交付报的区域是 %q，池子手上这张是从 europe 铸的", d.Region)
	}
	// 老 state.json 里的 pair 没这个字段 ⇒ 空。猜一个区域比不报更坏：
	// 消费端会据此认为某个大区已经烧过。
	if d := mustDeliver(t, s, 1, "unified-2"); d.Region != "" {
		t.Fatalf("老 pair 报了区域 %q，应为空", d.Region)
	}
}

// 九个大区各落一个没见过的网关 ⇒ 每一发都算补上一格，不会撞失手预算。
func landEachRegionSomewhereNew(mn *fakeMinter) {
	for i, r := range types.Regions {
		mn.land[exitKey(r.Exit)] = fmt.Sprintf("unified-%d", i+1)
	}
}

// 预铸并发起来之后，同一个号在一个 RenewGap 里只许被派一发。
//
// 登记（lastMintedAt）要是还留在 mintOnce 里面——那是一发上游**之后**才落笔的——
// 几个 worker 会在第一发落笔前全部选中这同一个号，而「同一个号两次铸票至少隔
// RenewGap」是防封节奏（见 Options.RenewGap），账号锁拦得住并发但拦不住节奏。
func TestPremintDoesNotDispatchOneAccountTwiceWithinTheRenewGap(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	addAccount(st, 1, "acct-1")
	landEachRegionSomewhereNew(mn)
	// 三个 worker、一个号、RenewGap 一小时：缺口 9 格，所以「只打一发」只可能来自节流。
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: time.Hour,
		Live: liveCfg(func(c *config.Config) { c.MintConcurrency = 3 })})

	s.premintBare(context.Background(), 9)
	if n := len(mn.mintedGateways()); n != 1 {
		t.Fatalf("同一个号在一个 RenewGap 里被派了 %d 发，只该有 1 发", n)
	}
	if mn.maxPerAcc > 1 {
		t.Fatalf("单账号同时在飞 %d 发，SPEC 红线 2 要求 1", mn.maxPerAcc)
	}
}

// worker pool 真的并行：三个号三个大区，铸票要同时在飞，不是一发接一发。
func TestPremintRunsDifferentAccountsInParallel(t *testing.T) {
	clk := newClock()
	st := newStore(clk)
	mn := newMinter(clk)
	// 让每一发在上游那儿待一会儿，不然串行实现也可能"看起来"重叠不了也测不出来。
	mn.delay = 30 * time.Millisecond
	for id := int64(1); id <= 3; id++ {
		addAccount(st, id, fmt.Sprintf("acct-%d", id))
	}
	landEachRegionSomewhereNew(mn)
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: time.Hour,
		Live: liveCfg(func(c *config.Config) { c.MintConcurrency = 3 })})

	s.premintBare(context.Background(), 9)
	if n := len(mn.mintedGateways()); n != 3 {
		t.Fatalf("三个号各该打一发（之后全被 RenewGap 节流），实际 %d 发", n)
	}
	if peak := mn.peakInflight(); peak < 2 {
		t.Fatalf("最大同时在飞 %d 发，worker pool 没并行起来", peak)
	}
}
