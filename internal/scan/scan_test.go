package scan

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gwpool/internal/mint"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

// 真实实现必须满足本包的 Minter，否则 fake 测过了生产也接不上。
var _ Minter = (*mint.Client)(nil)

// fakeMinter 按请求的 region 给落点和地理，不碰网络。
type fakeMinter struct {
	mu sync.Mutex

	gatewayFor map[string]string // 请求的国家 -> 落点
	observed   map[string]types.Observed
	failMint   map[string]error // 哪个国家铸票失败
	failGeo    map[string]error
	statusFor  map[string]int // 哪个国家回非 200

	sids      []string // 调用顺序记下来，验「同一发用同一个 sid」
	geoSIDs   []string
	nonSticky bool // NewSID(false) 时给非粘性出口
	stickyReq []bool
	inflight  int32
	maxPar    int32
	sidN      int
}

func newFake() *fakeMinter {
	return &fakeMinter{
		gatewayFor: map[string]string{},
		observed:   map[string]types.Observed{},
		failMint:   map[string]error{},
		failGeo:    map[string]error{},
		statusFor:  map[string]int{},
	}
}

func (f *fakeMinter) NewSID(sticky bool) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stickyReq = append(f.stickyReq, sticky)
	f.sidN++
	return "sid" + strconv.Itoa(f.sidN), nil
}

// Sticky 只在 scan 要求非粘性、而本 fake 被配成「给非粘性出口」时返回 false。
func (f *fakeMinter) Sticky(string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.nonSticky
}

func (f *fakeMinter) Geo(_ context.Context, e types.Exit, sid string) (types.Observed, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.geoSIDs = append(f.geoSIDs, sid)
	if err := f.failGeo[e.Region]; err != nil {
		return types.Observed{}, err
	}
	if o, ok := f.observed[e.Region]; ok {
		return o, nil
	}
	return types.Observed{IP: "10.0.0." + e.Region, Country: e.Region, City: e.City}, nil
}

func (f *fakeMinter) Mint(_ context.Context, _ types.Creds, e types.Exit, sid, sentPair string,
	_ time.Time) (types.MintResult, error) {
	n := atomic.AddInt32(&f.inflight, 1)
	defer atomic.AddInt32(&f.inflight, -1)
	for {
		old := atomic.LoadInt32(&f.maxPar)
		if n <= old || atomic.CompareAndSwapInt32(&f.maxPar, old, n) {
			break
		}
	}
	time.Sleep(time.Millisecond) // 给并发一个撞上的机会

	f.mu.Lock()
	defer f.mu.Unlock()
	f.sids = append(f.sids, sid)
	if sentPair != "" {
		return types.MintResult{}, errors.New("扫描必须裸打，不许带 pair")
	}
	if err := f.failMint[e.Region]; err != nil {
		return types.MintResult{Status: 429}, err
	}
	gw := f.gatewayFor[e.Region]
	if gw == "" {
		gw = "unified-" + e.Region
	}
	status := 200
	if s, ok := f.statusFor[e.Region]; ok {
		status = s
	}
	return types.MintResult{
		Status:      status,
		Gateway:     gw,
		NewPair:     "__cflb=c-" + gw + "; __oailb=o-" + gw,
		TurnState:   "ticket-" + gw,
		PairExpires: time.Now().Add(types.OailbLifetime).UTC(),
	}, nil
}

func (f *fakeMinter) VerifyFull(context.Context, types.Creds, types.Exit, string, string, string) (*types.Verdict, error) {
	return nil, errors.New("扫描不该验满血：验证动作本身会烧掉被测网关")
}

func newScanner(t *testing.T, f *fakeMinter) (*Scanner, *store.Store) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return &Scanner{Mint: f, Store: st, Backoff: time.Nanosecond, MaxTries: 2}, st
}

var acct = types.Account{ID: 1, Label: "pro1",
	Creds: types.Creds{AccessToken: "tok", AccountID: "acc"}}

// scanAll 把九个区域各打一发。**循环在调用方**：九区域首扫那条路 2026-10-01 删掉了
// （没需求也打、还顺手烧掉执行者在九个落点上的窗口），scan 只剩单区域入口。
func scanAll(t *testing.T, s *Scanner) []Landing {
	t.Helper()
	var out []Landing
	for _, r := range types.Regions {
		l, err := s.ScanRegion(context.Background(), acct, r)
		if err != nil {
			continue
		}
		out = append(out, l)
	}
	return out
}

func TestScanAllDistinct(t *testing.T) {
	f := newFake()
	s, st := newScanner(t, f)
	landed := scanAll(t, s)
	if len(landed) != len(types.Regions) {
		t.Fatalf("九个区域都该成功，成了 %d 个", len(landed))
	}
	// us-east 和 us-west 的 Exit.Region 都是 US，假 minter 给同一个落点 ⇒ 8 个
	fresh := 0
	for _, l := range landed {
		if l.IsNew {
			fresh++
		}
	}
	if fresh != 8 {
		t.Errorf("新网关 %d 个: %+v", fresh, landed)
	}
	rows, err := st.Samples()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(types.Regions) {
		t.Fatalf("样本该一区一行，得到 %d 行", len(rows))
	}
	if rows[0].RegionKey != types.Regions[0].Key || rows[0].Requested != types.Regions[0].Exit {
		t.Errorf("样本没带上区域信息: %+v", rows[0])
	}
	if len(st.Gateways()) != 8 {
		t.Errorf("网关表 %d 条", len(st.Gateways()))
	}
	// 台账要按区域各记一格：挑下一个铸票者就看它。
	mints := st.RegionMints()
	if len(mints) != len(types.Regions) {
		t.Fatalf("台账该九格，得到 %d 格: %+v", len(mints), mints)
	}
	m := mints[store.MintKey(acct.ID, "east-asia")]
	if m.Count != 1 || m.LastAt.IsZero() || m.RegionKey != "east-asia" || m.AccountID != acct.ID {
		t.Errorf("台账那一格不对: %+v", m)
	}
	// 每一发都是一次真实接触，槽位必须记上，否则调度会误判它歇着。
	if sl, ok := st.Slot(acct.ID, "unified-JP"); !ok || sl.LastTouch.IsZero() {
		t.Errorf("槽位没记: %+v", sl)
	} else if sl.LastVerdict != "unknown" {
		t.Errorf("扫描不验满血，判定该是 unknown，得到 %q", sl.LastVerdict)
	}
	if p, ok := st.Pair("unified-JP"); !ok || p.MintedBy != acct.ID || p.ExpiresAt.IsZero() {
		t.Errorf("pair 没入池: %+v", p)
	}
	// 同一发的 geo 和 mint 必须共用 sid，否则可能换 IP、换网关。
	if len(f.sids) != len(f.geoSIDs) {
		t.Fatalf("geo %d 发、mint %d 发", len(f.geoSIDs), len(f.sids))
	}
	for i := range f.sids {
		if f.sids[i] != f.geoSIDs[i] {
			t.Fatalf("第 %d 发 geo/mint 用了不同的 sid: %q vs %q", i, f.geoSIDs[i], f.sids[i])
		}
	}
	// 默认必须要粘性出口：非粘性配不出「Geo 和 Mint 同一个 IP」。
	for i, sticky := range f.stickyReq {
		if !sticky {
			t.Fatalf("第 %d 发没要求粘性出口", i)
		}
	}
}

// B5：可达出口记的必须是**请求过的**那个，不是观测到的地理。
// 观测到的城市是自由文本（"Frankfurt am Main"、"São Paulo"），回灌成代理参数
// 会认证失败或库存回退到别国 ⇒ 落到别的网关 ⇒ 被误报成「落点漂移」⇒ 永远续不上。
func TestScanRecordsRequestedExitForReplay(t *testing.T) {
	f := newFake()
	// 模拟实测过的那种回退：要 US:Seattle，拿到尼日利亚拉各斯的 IP。
	f.observed["US"] = types.Observed{IP: "102.0.0.1", Country: "NG", City: "Lagos", ASN: "AS1"}
	f.observed["BR"] = types.Observed{IP: "1.1.1.1", Country: "DE", City: "Frankfurt am Main"}
	s, st := newScanner(t, f)
	scanAll(t, s)

	for name, want := range map[string][]types.Exit{
		"unified-US": {{Region: "US"}, {Region: "US", City: "Seattle"}},
		"unified-BR": {{Region: "BR"}},
	} {
		got := st.Gateways()[name].ReachableExits
		if len(got) != len(want) {
			t.Fatalf("%s 的可达出口 %+v，要 %+v", name, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s 的第 %d 个可达出口是 %+v，要 %+v（必须能直接当代理参数用）",
					name, i, got[i], want[i])
			}
		}
	}

	// 观测到的地理没丢，它在样本里——Requested / Observed 两列都留着，事后能查回退。
	rows, _ := st.Samples()
	if rows[0].Observed.Country != "NG" || rows[0].Requested.Region != "US" {
		t.Errorf("样本该两列都留: %+v", rows[0])
	}
}

// 同一个网关被多个区域落到时，每个区域请求过的出口都要进 ReachableExits：
// 那张表的用途是「下次想回到这个网关该怎么请求」。
func TestRepeatLandingsAccumulateExits(t *testing.T) {
	f := newFake()
	for _, r := range types.Regions {
		f.gatewayFor[r.Exit.Region] = "unified-167" // 九个区域全落同一个
	}
	s, st := newScanner(t, f)
	scanAll(t, s)
	if len(st.Gateways()) != 1 {
		t.Errorf("网关表该只有 1 条，得到 %d", len(st.Gateways()))
	}
	// 九个区域、九种请求参数（us-east 和 us-west 国家相同但城市不同，算两种）。
	if exits := st.Gateways()["unified-167"].ReachableExits; len(exits) != len(types.Regions) {
		t.Errorf("每个区域请求过的出口都该记进可达出口，得到 %+v", exits)
	}
}

// N1：只要下发了 __oailb，429/403 也会给出落点。限流风暴里那不是
// 「这个账号能到这个网关」的证据，不许记进池子。
func TestScanIgnoresLandingFromNon200(t *testing.T) {
	f := newFake()
	f.statusFor["BR"] = 429
	f.statusFor["JP"] = 403
	s, st := newScanner(t, f)
	fails := 0
	for _, r := range types.Regions {
		if _, err := s.ScanRegion(context.Background(), acct, r); err != nil {
			fails++
		}
	}
	if _, ok := st.Gateways()["unified-BR"]; ok {
		t.Error("429 的落点进了网关表")
	}
	if _, ok := st.Gateways()["unified-JP"]; ok {
		t.Error("403 的落点进了网关表")
	}
	if _, ok := st.Pair("unified-BR"); ok {
		t.Error("429 的 pair 进了池子")
	}
	if _, ok := st.Slot(acct.ID, "unified-BR"); ok {
		t.Error("429 的槽位被记上了")
	}
	if fails != 2 {
		t.Errorf("两个区域该算失败，得到 %d", fails)
	}
	// 失败的那一发不进台账：记上去会把一个打不通的区域标成「走过了」，于是再也不去试。
	if n := len(st.RegionMints()); n != len(types.Regions)-2 {
		t.Errorf("台账该只有成功的 %d 格，得到 %d", len(types.Regions)-2, n)
	}
	// 但样本要留：那一发是真打出去的，状态码是后面排查供给的关键读数。
	rows, _ := st.Samples()
	var seen429 bool
	for _, r := range rows {
		if r.RegionKey == "south-america" && r.Status == 429 {
			seen429 = true
		}
	}
	if !seen429 {
		t.Error("失败那一发的样本没落盘")
	}
}

// 非粘性出口（webshare 这类）每连接换 IP，Geo 查的不是铸票那一发的 IP。
// 这种粗粒度扫描只拿落点，不记地理——宁可留空也不记一个假的。
func TestCoarseScanSkipsGeo(t *testing.T) {
	f := newFake()
	f.nonSticky = true
	s, st := newScanner(t, f)
	s.Coarse = true
	scanAll(t, s)
	if len(f.geoSIDs) != 0 {
		t.Errorf("非粘性出口不该查 geo，查了 %d 次", len(f.geoSIDs))
	}
	for _, sticky := range f.stickyReq {
		if sticky {
			t.Error("Coarse 模式该允许非粘性出口")
		}
	}
	rows, _ := st.Samples()
	if len(rows) == 0 {
		t.Fatal("样本没落")
	}
	for _, r := range rows {
		if r.Observed != (types.Observed{}) {
			t.Errorf("非粘性出口的样本不该带地理: %+v", r.Observed)
		}
		if r.Gateway == "" {
			t.Error("落点还是要拿到的——这是粗粒度扫描的全部价值")
		}
	}
	// 落点照样入池：webshare 的机房 IP 能摸到 novproxy 碰不到的网关。
	if len(st.Gateways()) == 0 {
		t.Error("粗粒度扫描发现的网关也要入池")
	}
}

// 失败的区域：样本仍要落盘，那一发是真打出去的。
func TestScanFailureStillRecordsSample(t *testing.T) {
	f := newFake()
	f.failMint["BR"] = errors.New("429 挡了")
	f.failGeo["AU"] = errors.New("ipinfo 超时")
	s, st := newScanner(t, f)
	fails := 0
	for _, r := range types.Regions {
		if _, err := s.ScanRegion(context.Background(), acct, r); err != nil {
			fails++
		}
	}
	if fails != 2 {
		t.Errorf("该有 2 个区域彻底失败，得到 %d", fails)
	}
	rows, _ := st.Samples()
	// MaxTries=2：BR 的两次重试都落了样本行；AU 连 geo 都没过，落不了行。
	var br int
	for _, r := range rows {
		if r.RegionKey == "south-america" {
			br++
			if r.Status != 429 || r.Gateway != "" {
				t.Errorf("失败样本该带状态码、落点为空: %+v", r)
			}
		}
		if r.RegionKey == "oceania" {
			t.Error("geo 都没拿到时没有可信地理，不该落样本")
		}
	}
	if br != 2 {
		t.Errorf("BR 该落 2 行（重试次数），得到 %d", br)
	}
}

// 红线 2：单账号并发硬性 1，而且锁必须是 store 上那一把（scan 和 sched 共用）。
func TestSingleAccountConcurrencyIsOne(t *testing.T) {
	f := newFake()
	s, st := newScanner(t, f)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, r := range types.Regions[:3] {
				if _, err := s.ScanRegion(context.Background(), acct, r); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	// 同时有一个「交付侧」也在抢同一个账号——它拿的是 store 上的同一把锁。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			unlock := st.LockAccount(acct.ID)
			atomic.AddInt32(&f.inflight, 1)
			time.Sleep(time.Millisecond)
			atomic.AddInt32(&f.inflight, -1)
			unlock()
		}
	}()
	wg.Wait()
	if got := atomic.LoadInt32(&f.maxPar); got != 1 {
		t.Fatalf("同一账号最大并发 %d，必须是 1（已知的封号主因）", got)
	}
}

// 不同账号之间不该互相排队：锁是 per-account 的，不是全局信号量。
func TestDifferentAccountsRunInParallel(t *testing.T) {
	f := newFake()
	s, _ := newScanner(t, f)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for id := int64(1); id <= 3; id++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			<-start
			a := acct
			a.ID = id
			for _, r := range types.Regions[:3] {
				if _, err := s.ScanRegion(context.Background(), a, r); err != nil {
					t.Error(err)
				}
			}
		}(id)
	}
	close(start)
	wg.Wait()
	if got := atomic.LoadInt32(&f.maxPar); got < 2 {
		t.Errorf("三个账号同时扫，最大并发只有 %d，锁串错了范围", got)
	}
}

func TestScanCanceled(t *testing.T) {
	f := newFake()
	s, _ := newScanner(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ScanRegion(ctx, acct, types.Regions[0]); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消该原样冒出来，得到 %v", err)
	}
}

func TestScannerNeedsDeps(t *testing.T) {
	if _, err := (&Scanner{}).ScanRegion(context.Background(), acct, types.Regions[0]); err == nil {
		t.Fatal("没配 Mint/Store 该报错")
	}
}

// 上游否决这个账号（400/401/403）就当场停用：再派它去别的区域只是把同一句话
// 再听八遍（2026-10-01 的二十七发就是这么浪费掉的）。
func TestRejectedAccountIsDisabledOnTheSpot(t *testing.T) {
	f := newFake()
	f.statusFor["US"] = 400
	s, st := newScanner(t, f)
	a, err := st.AddAccount(types.Account{Label: "free 号", Enabled: true,
		Creds: types.Creds{AccessToken: "tok", AccountID: "acc"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ScanRegion(context.Background(), a, types.Regions[0]); !errors.Is(err, ErrAccountRejected) {
		t.Fatalf("400 该认成「上游否决这个账号」，得到 %v", err)
	}
	// 只打一发就认命：MaxTries=2，但否决不重试。
	if len(f.sids) != 1 {
		t.Errorf("否决后还重试了，共打了 %d 发", len(f.sids))
	}
	got, _ := st.Account(a.ID)
	if got.Enabled || got.DisabledReason == "" {
		t.Fatalf("没停用或没记原因: enabled=%v reason=%q", got.Enabled, got.DisabledReason)
	}
}

// S-3：上游已经答过话就不重试，只有本地/传输层的失败才值得再打一发。
//
// 429 重试三轮正是把配额往最没用的地方花：每一发都过速率闸、都可能烧掉一个落点，
// 而上游正在限流时三发换回来的是同一句话。代理抽风则相反，重打一发有意义。
func TestRetryOnlyForTransportFailures(t *testing.T) {
	t.Run("上游答了 429 就不重试", func(t *testing.T) {
		f := newFake()
		f.statusFor["BR"] = 429
		s, _ := newScanner(t, f)
		if _, err := s.ScanRegion(context.Background(), acct, types.Region{
			Key: "south-america", Exit: types.Exit{Region: "BR"}}); err == nil {
			t.Fatal("429 该算失败")
		}
		if len(f.sids) != 1 {
			t.Fatalf("429 之后还重试了，共打了 %d 发", len(f.sids))
		}
	})

	t.Run("速率闸和出口故障也不重试", func(t *testing.T) {
		for _, sentinel := range []error{types.ErrRateLimited, types.ErrNoExit} {
			f := newFake()
			f.failMint["BR"] = fmt.Errorf("包一层: %w", sentinel)
			s, _ := newScanner(t, f)
			if _, err := s.ScanRegion(context.Background(), acct, types.Region{
				Key: "south-america", Exit: types.Exit{Region: "BR"}}); err == nil {
				t.Fatal("该算失败")
			}
			if len(f.sids) != 1 {
				t.Fatalf("%v 之后还重试了，共打了 %d 发", sentinel, len(f.sids))
			}
		}
	})

	t.Run("传输层失败照旧重试", func(t *testing.T) {
		f := newFake()
		f.failMint["BR"] = errors.New("socks5: connection reset by peer")
		s, _ := newScanner(t, f)
		if _, err := s.ScanRegion(context.Background(), acct, types.Region{
			Key: "south-america", Exit: types.Exit{Region: "BR"}}); err == nil {
			t.Fatal("该算失败")
		}
		if len(f.sids) != 2 {
			t.Fatalf("代理抽风该重试到 MaxTries=2，只打了 %d 发", len(f.sids))
		}
	})
}
