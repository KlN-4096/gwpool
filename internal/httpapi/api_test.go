package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"gwpool/internal/mint"
	"gwpool/internal/sched"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

const (
	secretToken = "sk-must-never-appear-in-any-page"
	// 两个 owner 的令牌。够长够像真的（真的是 32 个十六进制字符），
	// 而且互为「另一个人的」，用来钉死 /u/ 的鉴权边界。
	ownerTok1 = "0wn3r1aaaaaaaaaaaaaaaaaaaaaaaaaa"
	ownerTok2 = "0wn3r2bbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// fakeToken 拼一张形状对的 Codex access_token：三段、payload 里有声明、不验签。
// 中间那段才是 parseCreds 会读的，首尾两段只要是合法 base64url 就行。
func fakeToken(accountID, plan string, exp time.Time) string {
	payload, _ := json.Marshal(map[string]any{
		"exp": exp.Unix(),
		"https://api.openai.com/auth": map[string]string{
			"chatgpt_account_id": accountID,
			"chatgpt_plan_type":  plan,
		},
	})
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"RS256"}`)) + "." + enc(payload) + "." + enc([]byte("sig"))
}

const liveCookie = "__cflb=CFLB-SECRET; __oailb=OAILB-SECRET"

type fakePool struct {
	mode     types.Mode
	delivery types.Delivery
	err      error
	public   sched.PublicState
	target   int
	anon     []string
	// 按账号记流水：调度正确性全看池子有没有认出是谁在要。
	delivered []string
	// reqs 留住每次收到的请求参数：exclude / min_remaining 这些旋钮必须真的传下去，
	// 而「传没传到」在响应里看不出来。
	reqs []sched.Request
	// calls 数一共取了几次票：wait 那条路最要紧的保证是「等不到就别重跑」，
	// 重跑一次可能真打一发上游。
	calls int
	// hasCand 顶替 sched.HasCandidate。nil = 一张候选都没有。
	// 判据归调度（只有它看得见冷却），HTTP 那层照它的答案办——而「池子里有票」
	// 和「这个账号拿得到票」是两件事，这个 fake 必须能把它们分开。
	hasCand func(accountID int64, req sched.Request) bool

	released []string
	relN     int
	relErr   error

	// deliveredTo 顶替归属闸的判据，askedDelivered 留住它被问过的 (行|票号)——
	// 「问的是哪一行」正是这道闸的全部内容，光看状态码看不出来。
	deliveredTo    func(accountID int64, version string) bool
	askedDelivered []string
}

func (p *fakePool) Deliver(_ context.Context, accountID int64, req sched.Request) (types.Delivery, error) {
	p.calls++
	p.reqs = append(p.reqs, req)
	p.delivered = append(p.delivered, fmt.Sprintf("%d|%s|force=%v", accountID, req.Gateway, req.Force))
	return p.delivery, p.err
}

func (p *fakePool) DeliverAnonymous(_ context.Context, upstream string, req sched.Request, authed bool) (types.Delivery, error) {
	p.calls++
	p.reqs = append(p.reqs, req)
	p.anon = append(p.anon, fmt.Sprintf("%s|%s|force=%v|authed=%v", upstream, req.Gateway, req.Force, authed))
	// **这道闸真实实现里有，fake 少了它两轮审查都没抓到跨账号那条**：
	// HTTP 层所有跨账号用例都绿在「真实实现必然 503」的地方。
	// 鉴权过的调用方不吃这个门槛（门槛是给上传者留的余量，别把上传者本人挡在门外）。
	if !authed && !p.public.Open {
		return types.Delivery{}, sched.ErrPublicClosed
	}
	return p.delivery, p.err
}

func (p *fakePool) Release(accountID int64, version string) (int, error) {
	p.released = append(p.released, fmt.Sprintf("%d|%s", accountID, version))
	return p.relN, p.relErr
}

// DeliveredTo 顶替 sched 那份归属比对。nil = 谁都没拿到过任何票。
//
// 这个 fake 必须能分开「这张票是池子现持的」和「这张票交付给过**这一行**」：
// 两者混成一个，/pair/renew 的归属闸（整条路上唯一挡得住人的那道）就无从检验。
func (p *fakePool) DeliveredTo(accountID int64, version string) bool {
	p.askedDelivered = append(p.askedDelivered, fmt.Sprintf("%d|%s", accountID, version))
	if p.deliveredTo == nil {
		return false
	}
	return p.deliveredTo(accountID, version)
}

func (p *fakePool) HasCandidate(accountID int64, req sched.Request) bool {
	if p.hasCand == nil {
		return false
	}
	return p.hasCand(accountID, req)
}

func (p *fakePool) Public() sched.PublicState { return p.public }

func (p *fakePool) Mode() types.Mode { return p.mode }

func (p *fakePool) PairTarget() int { return p.target }

type fakeStore struct {
	accounts []types.Account
	owners   []types.Owner
	mints    map[string]types.RegionMint
	gws      map[string]types.Gateway
	pairs    map[string]types.Pair
	slots    map[string]types.Slot
	samples  []types.Sample
	added    []types.Account
	deleted  []int64
	// addedOwners 留住每一次 AddOwner：「贴了一份已经属于别人的凭据」那条路上**不许**
	// 凭空长出一个空 owner，而响应里看不出来。
	addedOwners []types.Owner
	// puts 留住每一次 PairPut：「受理但不落盘」那条路唯一的判据就是这里**没有**
	// 多出一行，光看响应（两边都是 200）区分不出来。
	puts   []types.Pair
	putErr error
}

func (f *fakeStore) Gateways() map[string]types.Gateway { return f.gws }

func (f *fakeStore) Pairs() map[string]types.Pair { return f.pairs }
func (f *fakeStore) Slots() map[string]types.Slot { return f.slots }
func (f *fakeStore) Accounts() []types.Account    { return f.accounts }

func (f *fakeStore) AccountByViewToken(tok string) (types.Account, bool) {
	for _, a := range f.accounts {
		if a.ViewToken != "" && a.ViewToken == tok {
			return a, true
		}
	}
	return types.Account{}, false
}

func (f *fakeStore) Owners() []types.Owner { return f.owners }

func (f *fakeStore) OwnerByToken(tok string) (types.Owner, bool) {
	for _, o := range f.owners {
		if tok != "" && o.Token == tok {
			return o, true
		}
	}
	return types.Owner{}, false
}

// AccountsByOwner 照真实 store：ownerID 为 0 时回 nil，不是「所有没归属的行」
// （0 是匿名占位行的常态值）。
func (f *fakeStore) AccountsByOwner(ownerID int64) []types.Account {
	if ownerID == 0 {
		return nil
	}
	var out []types.Account
	for _, a := range f.accounts {
		if a.OwnerID == ownerID {
			out = append(out, a)
		}
	}
	return out
}

func (f *fakeStore) AddOwner(label string) (types.Owner, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return types.Owner{}, err
	}
	o := types.Owner{ID: int64(len(f.owners) + 1), Token: base64.RawURLEncoding.EncodeToString(b),
		Label: label, CreatedAt: time.Now().UTC()}
	f.owners = append(f.owners, o)
	f.addedOwners = append(f.addedOwners, o)
	return o, nil
}

func (f *fakeStore) RegionMints() map[string]types.RegionMint { return f.mints }

// PruneEmptyOwners 丢掉名下零账号的 owner，照真 store 的行为。
func (f *fakeStore) PruneEmptyOwners() (int, error) {
	used := map[int64]bool{}
	for _, a := range f.accounts {
		if a.OwnerID != 0 {
			used[a.OwnerID] = true
		}
	}
	kept := f.owners[:0:0]
	for _, o := range f.owners {
		if used[o.ID] {
			kept = append(kept, o)
		}
	}
	n := len(f.owners) - len(kept)
	f.owners = kept
	return n, nil
}

// 照真实 store 的行为（store.AddAccount 的镜像，改那边这里要一起改）：分配 ID、
// ViewToken 为空时兜底生成，去重命中时**属于别人的原样返回一个字段都不碰**。
// 这条镜像是 httpapi 侧「不碰别人的 owner / 不交出别人的钥匙」全部断言的前提；
// 两份实现一旦漂移，那些断言会继续绿在一个生产里不存在的行为上（这就是审查里的
// S6）。所以 TestUploadWiredToRealStore 用真 store 又跑了一遍最关键的那条，兜住漂移。
func (f *fakeStore) AddAccount(a types.Account) (types.Account, error) {
	if up := a.Creds.AccountID; up != "" {
		for i, x := range f.accounts {
			if x.Creds.AccountID != up {
				continue
			}
			// 已经属于别人 ⇒ 原样返回，一个字段都不碰（见 store.AddAccount 的长注释）。
			if x.OwnerID != 0 && x.OwnerID != a.OwnerID {
				return x, nil
			}
			x.Creds.AccessToken = a.Creds.AccessToken
			if a.Creds.RefreshToken != "" {
				x.Creds.RefreshToken = a.Creds.RefreshToken
			}
			if a.PlanType != "" {
				x.PlanType = a.PlanType
			}
			if a.Label != "" {
				x.Label = a.Label
			}
			x.Enabled = true
			x.DisabledReason = ""
			if x.OwnerID == 0 {
				x.OwnerID = a.OwnerID
			}
			f.accounts[i] = x
			f.added = append(f.added, x)
			return x, nil
		}
	}
	a.ID = int64(len(f.accounts) + 1)
	if a.ViewToken == "" {
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			return types.Account{}, err
		}
		a.ViewToken = base64.RawURLEncoding.EncodeToString(b)
	}
	f.accounts = append(f.accounts, a)
	f.added = append(f.added, a)
	return a, nil
}

func (f *fakeStore) SetAccountEnabled(id int64, enabled bool) error {
	for i := range f.accounts {
		if f.accounts[i].ID == id {
			f.accounts[i].Enabled = enabled
			return nil
		}
	}
	return fmt.Errorf("账号 %d 不存在", id)
}

func (f *fakeStore) DeleteAccount(id int64) error {
	for i := range f.accounts {
		if f.accounts[i].ID == id {
			f.accounts = append(f.accounts[:i:i], f.accounts[i+1:]...)
			f.deleted = append(f.deleted, id)
			return nil
		}
	}
	return fmt.Errorf("账号 %d 不存在", id)
}

func (f *fakeStore) Samples() ([]types.Sample, error) { return f.samples, nil }

// PutPair 照真实 store：按网关覆盖。putErr 是「落盘失败」那条路的旋钮。
func (f *fakeStore) PutPair(p types.Pair) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.puts = append(f.puts, p)
	f.pairs[p.Gateway] = p
	return nil
}

type fakeScanner struct {
	discovery []int64
	// refuse 非空时模拟「没派出去」：挑不出人、配额打满、还在发现冷却里。
	refuse string
}

func (s *fakeScanner) EnqueueDiscovery(exclude int64) string {
	s.discovery = append(s.discovery, exclude)
	return s.refuse
}

func rig(t *testing.T, mode types.Mode) (*Server, http.Handler, *fakePool, *fakeStore, *fakeScanner) {
	t.Helper()
	now := time.Now().UTC()
	st := &fakeStore{
		accounts: []types.Account{
			{
				ID: 1, Label: "pro1", PlanType: "pro", Enabled: true,
				ViewToken: "vt-1", CreatedAt: now.Add(-time.Hour), OwnerID: 1,
				// refresh_token 也塞 secretToken 前缀：泄漏断言用 Contains 查它，
				// 顺手覆盖「refresh_token 不许出现在任何页面上」（审查 N8，原先只有
				// access_token 这一个哨兵，refresh 漏了也测不出来）。
				Creds: types.Creds{AccessToken: secretToken, RefreshToken: secretToken + "-rt", AccountID: "acct-upstream"},
			},
			{
				ID: 2, Label: "朋友的号", PlanType: "plus", Enabled: true,
				ViewToken: "vt-2", CreatedAt: now.Add(-time.Minute), OwnerID: 2,
				Creds: types.Creds{AccessToken: secretToken + "-b", RefreshToken: secretToken + "-b-rt", AccountID: "acct-b"},
			},
		},
		// 两个账号刻意挂在**两个不同的** owner 下：/u/ 这一页最要紧的保证是
		// 「A 的令牌看不见 B 的任何东西」，同一个 owner 下测不出来。
		owners: []types.Owner{
			{ID: 1, Token: ownerTok1, Label: "我", CreatedAt: now.Add(-time.Hour)},
			{ID: 2, Token: ownerTok2, Label: "朋友", CreatedAt: now.Add(-time.Minute)},
		},
		mints: map[string]types.RegionMint{
			"1|europe": {AccountID: 1, RegionKey: "europe", Count: 3, LastAt: now.Add(-30 * time.Minute)},
			// 台账里可能有「出口反查不到区域」的行（只会来自手改的 state.json）。
			// 九宫格照 types.Regions 画，所以这种行画不出第十个格子。
			"1|": {AccountID: 1, RegionKey: "", Count: 9, LastAt: now},
			// 2 号的格子不许串到 1 号头上。
			"2|oceania": {AccountID: 2, RegionKey: "oceania", Count: 1, LastAt: now},
		},
		gws: map[string]types.Gateway{"unified-142": {
			Name: "unified-142", FirstSeenBy: 1, FirstSeenAt: now.Add(-time.Hour),
			ReachableExits: []types.Exit{{Region: "IT"}},
		}},
		// CflbExpiresAt 和 ExpiresAt 一起铺：线上这两个字段永远同时有值
		// （ExpiresAt 就是两个组件取小，几乎总是 __cflb 那个），而 pair_remaining_s
		// 量的是 CflbExpiresAt——只铺一个就等于铺一个生产不存在的状态。
		pairs: map[string]types.Pair{"unified-142": {
			Gateway: "unified-142", Cookie: liveCookie, MintedBy: 1,
			ExpiresAt: now.Add(time.Hour), CflbExpiresAt: now.Add(time.Hour),
		}},
		slots: map[string]types.Slot{"1|unified-142": {
			AccountID: 1, Gateway: "unified-142", LastTouch: now.Add(-3 * time.Hour), LastVerdict: "full",
		}},
		samples: []types.Sample{{
			AccountID: 1, RegionKey: "IT", Requested: types.Exit{Region: "IT"},
			Observed: types.Observed{IP: "203.0.113.9", Country: "IT", City: "Milan", ASN: "AS1"},
			Gateway:  "unified-142", Status: 200, At: now.Add(-30 * time.Minute),
		}},
	}
	p := &fakePool{mode: mode, delivery: types.Delivery{
		Gateway: "unified-142", Cookie: liveCookie, ExpiresAt: now.Add(types.DeliverTTL),
		ValidForS: 150, VerifiedFull: true,
	}}
	sc := &fakeScanner{}
	srv, err := New(p, st, sc, Options{ConsumerKey: "ckey", AdminKey: "akey", Model: "gpt-6-luna"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv, srv.Handler(), p, st, sc
}

func do(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func authed(r *http.Request, key string) *http.Request {
	r.Header.Set("Authorization", "Bearer "+key)
	return r
}

// 公开页：能渲染、报得出网关，但一个字 cookie 都不许漏（SPEC 红线 1、2）。
func TestPublicPageLeaksNothing(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)
	w := do(h, httptest.NewRequest("GET", "/", nil))
	if w.Code != 200 {
		t.Fatalf("状态 %d，body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "unified-142") {
		t.Fatal("公开页没列出网关")
	}
	for _, leak := range []string{"OAILB-SECRET", "CFLB-SECRET", secretToken, "__oailb=O"} {
		if strings.Contains(body, leak) {
			t.Fatalf("公开页漏了 %q", leak)
		}
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("缺 no-store")
	}
}

// 每账号一把 key：派生得出来、互不相同、停用即失效、没配密钥就一把都不发。
func TestConsumerKeyDerivation(t *testing.T) {
	srv, _, _, st, _ := rig(t, types.ModePool)
	a, b := st.accounts[0], st.accounts[1]
	ka, kb := srv.ConsumerKeyFor(a), srv.ConsumerKeyFor(b)
	if ka == "" || len(ka) < 20 {
		t.Fatalf("key 太短或没生成: %q", ka)
	}
	if ka == kb {
		t.Fatal("两个账号派生出同一把 key")
	}
	if ka != srv.ConsumerKeyFor(a) {
		t.Fatal("同一个账号两次派生结果不同，重启后 key 就变了")
	}
	// key 不能等于 view_token，也不能等于全局密钥。
	for _, bad := range []string{a.ViewToken, "ckey", "akey"} {
		if ka == bad {
			t.Fatalf("key 和 %q 撞上了", bad)
		}
	}
	// 没托管凭据的占位账号不发 key：它换不到 pair。
	if k := srv.ConsumerKeyFor(types.Account{ID: 9, Creds: types.Creds{AccountID: "x"}}); k != "" {
		t.Fatalf("占位账号拿到了 key: %q", k)
	}
	// 服务端没配派生密钥 ⇒ 一把都不发（失败往关的方向失败）。
	bare, err := New(&fakePool{mode: types.ModeSingle}, st, nil, Options{AdminKey: "akey"})
	if err != nil {
		t.Fatal(err)
	}
	if k := bare.ConsumerKeyFor(a); k != "" {
		t.Fatalf("没配 consumer_key 却发出了 key: %q", k)
	}
}

// 调度正确性：A 的 key 取到的必须是 A 的槽位，不是 B 的。
func TestCookieIsPerAccount(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	ka := srv.ConsumerKeyFor(st.accounts[0])
	kb := srv.ConsumerKeyFor(st.accounts[1])

	if w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), ka)); w.Code != 200 {
		t.Fatalf("A 取不到: %d %s", w.Code, w.Body.String())
	}
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie?gateway=unified-142", nil), kb)); w.Code != 200 {
		t.Fatalf("B 取不到: %d", w.Code)
	}
	want := []string{"1||force=false", "2|unified-142|force=false"}
	if len(p.delivered) != 2 || p.delivered[0] != want[0] || p.delivered[1] != want[1] {
		t.Fatalf("池子收到的调用方不对: %v，应为 %v", p.delivered, want)
	}

	// ?account= 是管理员专用参数。普通调用方带上它必须**仍然交付给他自己**，
	// 否则谁都能点名替别人取，调度和账本一起串号。
	p.delivered = nil
	for _, ref := range []string{"acct-b", "2", "不存在的号"} {
		w := do(h, authed(httptest.NewRequest("GET", "/cookie?account="+url.QueryEscape(ref), nil), ka))
		if w.Code != 200 {
			t.Fatalf("A 带 account=%s 居然 %d", ref, w.Code)
		}
	}
	for i, got := range p.delivered {
		if got != "1||force=false" {
			t.Fatalf("第 %d 次被点名改派成了 %s，应恒为账号 1", i, got)
		}
	}

	// 停用的账号，key 立刻失效 —— 这就是吊销手段。
	st.accounts[1].Enabled = false
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), kb)); w.Code != 401 {
		t.Fatalf("停用账号的 key 还能用: %d", w.Code)
	}
}

// /cookie 的鉴权、交付形状、管理员兜底路径。
func TestCookieAuthAndShape(t *testing.T) {
	srv, h, p, st, sc := rig(t, types.ModePool)
	ka := srv.ConsumerKeyFor(st.accounts[0])

	for _, key := range []string{"", "wrong", "vt-1"} {
		r := httptest.NewRequest("GET", "/cookie", nil)
		if key != "" {
			r = authed(r, key)
		}
		if w := do(h, r); w.Code != 401 {
			t.Fatalf("key=%q 居然 %d", key, w.Code)
		}
	}

	w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), ka))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	var d types.Delivery
	if err := json.Unmarshal(w.Body.Bytes(), &d); err != nil {
		t.Fatalf("返回不是 Delivery JSON: %v", err)
	}
	if d.Gateway != "unified-142" || d.Cookie != liveCookie || d.ValidForS != 150 || !d.VerifiedFull {
		t.Fatalf("交付内容不对: %+v", d)
	}
	if !strings.Contains(w.Body.String(), `"valid_for_s":150`) {
		t.Fatalf("字段名对不上 SPEC: %s", w.Body.String())
	}

	if w := do(h, authed(httptest.NewRequest("GET", "/cookie?gateway=../etc", nil), ka)); w.Code != 400 {
		t.Fatalf("非法 gateway 居然 %d", w.Code)
	}

	// 管理员兜底：必须显式说替谁取，猜一个就等于把 A 的槽位发给 B。
	for _, key := range []string{"akey", "ckey"} {
		if w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), key)); w.Code != 400 {
			t.Fatalf("%s 没带 account 居然 %d", key, w.Code)
		}
	}
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie?account=404", nil), "akey")); w.Code != 404 {
		t.Fatalf("不存在的账号居然 %d", w.Code)
	}
	p.delivered = nil
	// 池内 ID 和上游标识两种写法都认。
	for i, ref := range []string{"2", "acct-b"} {
		if w := do(h, authed(httptest.NewRequest("GET", "/cookie?account="+ref, nil), "akey")); w.Code != 200 {
			t.Fatalf("account=%s 居然 %d", ref, w.Code)
		}
		if p.delivered[i] != "2||force=false" {
			t.Fatalf("account=%s 解析成了 %s", ref, p.delivered[i])
		}
	}

	// 全降级 ⇒ 503，宁可 503 也不放降智。
	p.err = sched.ErrNoFullSlot
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), ka)); w.Code != 503 {
		t.Fatalf("没有满血槽位时应 503，实为 %d", w.Code)
	}
	// 光回 503 就卡死了：这个号在所有已知网关上都烧完了，同一批网关上重铸也不解封，
	// 只有没碰过的新网关才有窗口 ⇒ 必须去找新落点，而且不能派它自己去。
	if want := []int64{1}; len(sc.discovery) != 1 || sc.discovery[0] != want[0] {
		t.Fatalf("槽位烧完没触发找新网关: %v", sc.discovery)
	}
}

// 上传：两种凭据形状都收，一律 both，排队首扫，当场发 key，坏输入在边界上拦掉。
func TestUpload(t *testing.T) {
	_, h, _, st, sc := rig(t, types.ModePool)
	form := url.Values{
		"label": {"朋友的号"}, "plan": {"plus"},
		"creds": {`{"tokens":{"access_token":"` + fakeToken("acct-friend", "pro", time.Now().Add(time.Hour)) + `","account_id":"acct-friend"}}`},
	}
	r := httptest.NewRequest("POST", "/upload", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := do(h, r)
	if w.Code != http.StatusSeeOther {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if len(st.added) != 1 {
		t.Fatalf("没建账号: %+v", st.added)
	}
	a := st.added[0]
	// view_token 是上传者唯一的页面凭证又进 URL：必须有、必须够长、必须每次不同。
	if len(a.ViewToken) < 20 {
		t.Fatalf("view_token 太短或没生成: %q", a.ViewToken)
	}
	if loc := w.Header().Get("Location"); loc != "/a/"+a.ViewToken {
		t.Fatalf("没跳到查看页: %q", loc)
	}
	// plan 取 **token 里** 的（这张票写的是 pro），不取表单手填的那个 plus：
	// 手填的只是标签，票里的是事实，而「这票其实是 free」正是会让整次扫描全军覆没的事。
	if a.Label != "朋友的号" || a.PlanType != "pro" || a.Creds.AccountID != "acct-friend" {
		t.Fatalf("字段没落对: label=%q plan=%q acct=%q", a.Label, a.PlanType, a.Creds.AccountID)
	}
	// **上传不扫**：早先这里会立刻扫九个区域，而那九发全打在「没人要票」的时候，
	// 还把这个新号在九个落点上的首次接触当场烧掉。落点改成要票时按需发现
	// （见 deliverFailed → EnqueueDiscovery）。
	if len(sc.discovery) != 0 {
		t.Fatalf("上传就排了扫描: %v", sc.discovery)
	}

	// 直接 POST 原始 auth 文件（顶层形状）也要收，回 JSON，并且当场给 key。
	srv2, h2, _, st2, _ := rig(t, types.ModeSingle)
	r = httptest.NewRequest("POST", "/upload?label=pro1",
		strings.NewReader(`{"access_token":"`+fakeToken("acct-pro1", "pro", time.Now().Add(time.Hour))+`","account_id":"acct-pro1"}`))
	r.Header.Set("Content-Type", "application/json")
	w = do(h2, r)
	if w.Code != 200 {
		t.Fatalf("JSON 上传失败 %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("返回不是 JSON: %v", err)
	}
	fresh := st2.added[0]
	if resp["view_token"] != fresh.ViewToken || resp["url"] != "/a/"+fresh.ViewToken {
		t.Fatalf("返回的 view_token 对不上: %v", resp)
	}
	if resp["consumer_key"] == "" || resp["consumer_key"] != srv2.ConsumerKeyFor(fresh) {
		t.Fatalf("没把 key 交给上传者: %v", resp)
	}
	if resp["consumer_key"] == resp["view_token"] {
		t.Fatal("key 和 view_token 不该是同一个值")
	}
	if fresh.ViewToken == a.ViewToken {
		t.Fatal("两次上传拿到了同一个 view_token")
	}
	// 拿到手就能用。
	if w := do(h2, authed(httptest.NewRequest("GET", "/cookie", nil), resp["consumer_key"])); w.Code != 200 {
		t.Fatalf("刚发的 key 用不了: %d %s", w.Code, w.Body.String())
	}

	for _, bad := range []string{`{}`, `{"access_token":"short"}`, `nope`,
		`{"access_token":"` + strings.Repeat("c", 40) + `"}`,
		// 形状够长但不是 JWT：贴成 refresh_token / api key 的典型样子。
		`{"access_token":"` + strings.Repeat("c", 40) + `","account_id":"x"}`,
		// 是 JWT，但票里写的是另一个账号——2026-10-01 真实踩过的那一脚。
		`{"access_token":"` + fakeToken("acct-OTHER", "free", time.Now().Add(time.Hour)) +
			`","account_id":"acct-pro1"}`,
		// 过期的票。
		`{"access_token":"` + fakeToken("acct-pro1", "pro", time.Now().Add(-time.Hour)) +
			`","account_id":"acct-pro1"}`} {
		r := httptest.NewRequest("POST", "/upload", strings.NewReader(bad))
		r.Header.Set("Content-Type", "application/json")
		if w := do(h, r); w.Code != 400 {
			t.Fatalf("%s 应 400，实为 %d", bad, w.Code)
		}
	}
}

// 上传者视图：那句交代、那把 key 都要在，access_token 不能在。
func TestAccountView(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModePool)
	if w := do(h, httptest.NewRequest("GET", "/a/nope", nil)); w.Code != 404 {
		t.Fatalf("错 token 居然 %d", w.Code)
	}
	// 空 token 不能把「没分配过 view_token 的账号」整条匹配出来。
	for _, p := range []string{"/a/", "/a"} {
		if w := do(h, httptest.NewRequest("GET", p, nil)); w.Code == 200 {
			t.Fatalf("%s 居然进去了", p)
		}
	}
	w := do(h, httptest.NewRequest("GET", "/a/vt-1", nil))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, want := range []string{"1 次请求", "0 token", "gpt-6-luna",
		"reasoning.effort=low", "1 个新网关", "IT/Milan", "unified-142",
		srv.ConsumerKeyFor(st.accounts[0])} {
		if !strings.Contains(body, want) {
			t.Fatalf("信任视图缺了 %q", want)
		}
	}
	if strings.Contains(body, secretToken) || strings.Contains(body, "OAILB-SECRET") {
		t.Fatal("信任视图漏了凭据或 cookie")
	}
	// 别人的 key 不能出现在我的页面上。
	if strings.Contains(body, srv.ConsumerKeyFor(st.accounts[1])) {
		t.Fatal("页面上出现了别人的 key")
	}
}

// ---------- owner 层 ----------

// ownerCookieOf 给请求带上身份 cookie。
func ownerCookieOf(r *http.Request, tok string) *http.Request {
	r.AddCookie(&http.Cookie{Name: ownerCookie, Value: tok})
	return r
}

// /u/{token} 的鉴权边界：令牌认得出就给**它名下那几个**账号，认不出一律 404，
// 而且 A 的令牌一个字都看不到 B 的东西（那一页上有每个账号的接入 key）。
func TestOwnerPageIsScopedToItsOwnAccounts(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModePool)
	k1, k2 := srv.ConsumerKeyFor(st.accounts[0]), srv.ConsumerKeyFor(st.accounts[1])

	// 认不出的令牌：不存在的、空的、别人的 view_token、乱填的，全是 404/非 200。
	for _, bad := range []string{"/u/nope", "/u/vt-1", "/u/", "/u"} {
		if w := do(h, httptest.NewRequest("GET", bad, nil)); w.Code == 200 {
			t.Fatalf("%s 居然进去了", bad)
		}
	}

	w := do(h, httptest.NewRequest("GET", "/u/"+ownerTok1, nil))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, want := range []string{"pro1", k1, "/a/vt-1", "1 次请求", "0</b><span>消耗 token"} {
		if !strings.Contains(body, want) {
			t.Fatalf("自己的总览页缺了 %q", want)
		}
	}
	// **别人的东西一个字都不许有。** 这是这一页唯一要紧的保证。
	for _, leak := range []string{"朋友的号", k2, "vt-2", ownerTok2, secretToken} {
		if strings.Contains(body, leak) {
			t.Fatalf("owner 1 的总览页漏了 owner 2 的 %q", leak)
		}
	}

	// 反空测：换成 owner 2 的令牌，那几样东西本来是出得来的。
	other := do(h, httptest.NewRequest("GET", "/u/"+ownerTok2, nil)).Body.String()
	for _, want := range []string{"朋友的号", k2, "/a/vt-2"} {
		if !strings.Contains(other, want) {
			t.Fatalf("owner 2 的总览页缺了 %q，上面那组断言什么都没测到", want)
		}
	}
	if strings.Contains(other, "pro1") || strings.Contains(other, k1) {
		t.Fatal("owner 2 的页面上出现了 owner 1 的账号")
	}
}

// 九宫格恒为 types.Regions 那九个：铸过的绿、没铸过的灰，而台账里「出口反查不到
// 区域」那种行（RegionKeyOf 回空串）**不许**画成第十个格子。
// 另外：纯靠颜色对色盲不友好，所以格子里必须有文字。
func TestOwnerPageDrawsNineRegionCells(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)
	body := do(h, httptest.NewRequest("GET", "/u/"+ownerTok1, nil)).Body.String()

	// 九个格子，一个不多一个不少（owner 1 名下只有一个账号）。
	// 带引号精确数，别把外面那个 class="regions" 容器也数进来。
	n := strings.Count(body, `class="region"`) + strings.Count(body, `class="region on"`)
	if n != len(types.Regions) {
		t.Fatalf("画了 %d 个格子，该是 %d 个：台账里那条空 RegionKey 画成了第十个？", n, len(types.Regions))
	}
	// europe 的最近一次在窗口内 ⇒ 绿 + 文字（✓ 多久前）+ 带时间的 title；别的区域是灰的。
	if eu := regionCellOf(t, body, "欧洲"); !strings.Contains(eu, `class="region on"`) ||
		!strings.Contains(eu, "✓ ") || !strings.Contains(eu, "前</b>") {
		t.Fatalf("europe 该标成窗口内取过票：\n%s", eu)
	}
	if !strings.Contains(body, "累计 3 次，最近一次 ") {
		t.Fatal("格子的 title 里没有累计次数和最近一次时间")
	}
	if !strings.Contains(body, "可铸票") {
		t.Fatal("窗口内没取过的区域该有文字说明，不能只靠颜色")
	}
	// 2 号的那一格（oceania）不许串到 1 号的页面上。
	if strings.Count(body, `class="region on"`) != 1 {
		t.Fatal("绿格子不止一个：别人的台账串过来了")
	}
}

// GET /u/ **一个字的身份 cookie 都不写**（审查 B1：身份固定漏洞）。
//
// 从前这一页会「顺手记住没有身份的访客」。那条路能被滥用：攻击者把自己的 /u/ 链接
// 发给新人，新人一点就被种上攻击者的身份，下一次上传把凭据连同接入 key 挂到攻击者
// 名下——而受害者只是点了个链接，自己连一个 owner 都没有，看不出异常。SameSite=Lax
// 挡不住顶层 GET 导航。所以身份只在用户自己动手的 POST（/enter、/upload）上写。
func TestOwnerPageNeverWritesIdentityOnGET(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)
	ownerCookieIn := func(w *httptest.ResponseRecorder) string {
		for _, c := range w.Result().Cookies() {
			if c.Name == ownerCookie {
				return c.Value
			}
		}
		return ""
	}

	// 没有身份点开自己的页：页照给，但不许种 cookie（这正是被滥用的那一支）。
	w := do(h, httptest.NewRequest("GET", "/u/"+ownerTok1, nil))
	if w.Code != 200 {
		t.Fatalf("状态 %d", w.Code)
	}
	if got := ownerCookieIn(w); got != "" {
		t.Fatalf("GET /u/ 给无身份访客种了身份 cookie（B1 身份固定）: %q", got)
	}
	// 已经是自己点开自己：也不写（没必要，刷新有效期的便利不值这个攻击面）。
	if got := ownerCookieIn(do(h, ownerCookieOf(
		httptest.NewRequest("GET", "/u/"+ownerTok1, nil), ownerTok1))); got != "" {
		t.Fatalf("自己的页面也不该在 GET 上碰 cookie: %q", got)
	}
	// 带着自己的身份点开**别人**分享的链接：那一页照给（令牌就是凭证），身份不动。
	w = do(h, ownerCookieOf(httptest.NewRequest("GET", "/u/"+ownerTok2, nil), ownerTok1))
	if w.Code != 200 {
		t.Fatalf("状态 %d", w.Code)
	}
	if got := ownerCookieIn(w); got != "" {
		t.Fatalf("身份被别人的链接换掉了: %q", got)
	}
}

// 没带身份来的上传：现铸一个 owner，下发 HttpOnly cookie，并把 /u/ 链接交出去
// （否则他下次换设备就再也回不来了）。
// 跨站发起的改状态 POST 一律拒。
//
// 这条挡的是**身份固定**：第三方页面自动提交 POST /enter 就能把受害者的身份 cookie 换成
// 攻击者的令牌，之后受害者上传的凭据和接入 key 全落到攻击者的 /u/ 页下，不用猜任何令牌。
// 「POST 比 GET 安全」是错的（表单能自动提交），SameSite=Lax 也救不了（它只约束 cookie
// 的发送，不约束响应里 Set-Cookie 的写入）。站点码开着时恰好挡得住，但 site_key 可以留空。
func TestCrossSitePostsAreRejected(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)

	body := func() *strings.Reader { return strings.NewReader("token=whatever") }
	post := func(path string, set func(*http.Request)) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, body())
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		set(r)
		return do(h, r)
	}

	for _, path := range []string{"/enter", "/logout", "/upload", "/admin/rescan", "/admin/config"} {
		// 现代浏览器：Sec-Fetch-Site 说了实话。
		w := post(path, func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") })
		if w.Code != http.StatusForbidden {
			t.Fatalf("%s 跨站 POST 没被拒: %d", path, w.Code)
		}
		if len(w.Result().Cookies()) != 0 {
			t.Fatalf("%s 被拒了还下发了 cookie", path)
		}
		// 老浏览器：没有 Sec-Fetch-Site，但跨源 POST 一定带 Origin。
		w = post(path, func(r *http.Request) { r.Header.Set("Origin", "https://evil.example") })
		if w.Code != http.StatusForbidden {
			t.Fatalf("%s 异源 Origin 没被拒: %d", path, w.Code)
		}
	}

	// 本站页面上操作照常放行（这里只看没被 403 掐掉，后续鉴权/参数错误不归这条闸管）。
	if w := post("/enter", func(r *http.Request) {
		r.Header.Set("Sec-Fetch-Site", "same-origin")
	}); w.Code == http.StatusForbidden {
		t.Fatal("本站的 POST 被当成跨站拒了")
	}
	// 脚本/curl 两个头都没有：它们不是 CSRF 的载体，放行——/upload 本来就支持脚本调用。
	if w := post("/enter", func(*http.Request) {}); w.Code == http.StatusForbidden {
		t.Fatal("没有浏览器头的调用被拒了，脚本上传这条路会断")
	}
}

func TestUploadMintsOwnerAndHandsBackTheLink(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModePool)
	r := httptest.NewRequest("POST", "/upload?label=x",
		strings.NewReader(`{"access_token":"`+fakeToken("acct-fresh", "pro", time.Now().Add(time.Hour))+`"}`))
	r.Header.Set("Content-Type", "application/json")
	w := do(h, r)
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if len(st.addedOwners) != 1 {
		t.Fatalf("没为这份新凭据建 owner: %+v", st.addedOwners)
	}
	o := st.addedOwners[0]
	if got := st.added[0].OwnerID; got != o.ID {
		t.Fatalf("账号没挂到刚建的 owner 下: OwnerID=%d owner=%d", got, o.ID)
	}
	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["owner_token"] != o.Token || resp["owner_url"] != "/u/"+o.Token {
		t.Fatalf("回执里没给总览链接: %v", resp)
	}
	var tok string
	for _, c := range w.Result().Cookies() {
		if c.Name == ownerCookie {
			tok = c.Value
			if !c.HttpOnly {
				t.Error("身份 cookie 没设 HttpOnly")
			}
			if c.SameSite != http.SameSiteLaxMode {
				t.Errorf("身份 cookie 的 SameSite 是 %v", c.SameSite)
			}
		}
	}
	if tok != o.Token {
		t.Fatalf("没下发身份 cookie（下次进首页就只能再贴一遍令牌）: %q", tok)
	}
	// 拿着 cookie 真的进得去那一页。
	if w2 := do(h, httptest.NewRequest("GET", "/u/"+tok, nil)); w2.Code != 200 {
		t.Fatalf("刚建的总览页进不去: %d", w2.Code)
	}
}

// 带着身份来的上传（cookie 或表单里贴令牌）挂到那个 owner 名下，不另建一个。
func TestUploadAttachesToExistingOwner(t *testing.T) {
	for _, c := range []struct {
		name string
		with func(*http.Request) *http.Request
	}{
		{"cookie", func(r *http.Request) *http.Request { return ownerCookieOf(r, ownerTok1) }},
		{"表单贴令牌", func(r *http.Request) *http.Request { return r }},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, h, _, st, _ := rig(t, types.ModePool)
			form := url.Values{
				"creds": {`{"access_token":"` + fakeToken("acct-second", "pro", time.Now().Add(time.Hour)) + `"}`},
			}
			if c.name == "表单贴令牌" {
				form.Set("owner_token", ownerTok1)
			}
			r := httptest.NewRequest("POST", "/upload", strings.NewReader(form.Encode()))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if w := do(h, c.with(r)); w.Code != http.StatusSeeOther {
				t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
			}
			if len(st.addedOwners) != 0 {
				t.Fatalf("已经有身份了还另建了 owner: %+v", st.addedOwners)
			}
			if got := st.added[0].OwnerID; got != 1 {
				t.Fatalf("没挂到 owner 1 下: OwnerID=%d", got)
			}
			// 总览页上要看得见这一份新凭据。
			if body := do(h, httptest.NewRequest("GET", "/u/"+ownerTok1, nil)).Body.String(); !strings.Contains(body, "acct-second") {
				t.Fatal("新挂的凭据没出现在总览页上")
			}
		})
	}
}

// 认不出的 owner_token 要拒掉，**不能静默当成「没带」**去新建一个 owner：
// 那样人贴错一个字就会凭空多出一页，而他以为挂上去了。
func TestUploadRejectsUnknownOwnerToken(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModePool)
	form := url.Values{
		"owner_token": {"0wn3rXcccccccccccccccccccccccccc"},
		"creds":       {`{"access_token":"` + fakeToken("acct-x", "pro", time.Now().Add(time.Hour)) + `"}`},
	}
	r := httptest.NewRequest("POST", "/upload", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if w := do(h, r); w.Code != 400 {
		t.Fatalf("乱填总览令牌拿到 %d", w.Code)
	}
	if len(st.added) != 0 || len(st.addedOwners) != 0 {
		t.Fatalf("拒掉了却还是建了东西: accounts=%d owners=%d", len(st.added), len(st.addedOwners))
	}
}

// **抢账号**：拿一份公开流传的凭据重新上传，不许把别人的账号搬到自己名下
// （那一页上有对方每个账号的接入 key）。而且不许在账本上留一个空 owner。
func TestUploadDoesNotStealAnotherOwnersAccount(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModePool)
	k2 := srv.ConsumerKeyFor(st.accounts[1])
	// acct-b 是 owner 2 的账号。带着 owner 1 的身份重新上传它的凭据。
	form := url.Values{
		"creds": {`{"access_token":"` + fakeToken("acct-b", "plus", time.Now().Add(time.Hour)) + `"}`},
	}
	r := httptest.NewRequest("POST", "/upload", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := do(h, ownerCookieOf(r, ownerTok1))
	// 归属属于别人 ⇒ 既不改归属，也不交出任何钥匙：回 409，不 303 到 /a/<别人的票>。
	if w.Code != http.StatusConflict {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if got := st.accounts[1].OwnerID; got != 2 {
		t.Fatalf("归属被抢走了: acct-b 现在挂在 owner %d 下", got)
	}
	// **回执本身**不许带对方的 view_token / 接入 key / 总览令牌（审查 B2/S6：
	// 从前这里只查状态码不查 body，而 vt-2 和接入 key 就在 body 里）。
	body := w.Body.String()
	for _, leak := range []string{k2, "vt-2", ownerTok2, "/a/vt-2"} {
		if strings.Contains(body, leak) {
			t.Fatalf("回执里漏了别人的钥匙: %q", leak)
		}
	}
	// Location 也不许指向对方的账号页。
	if loc := w.Header().Get("Location"); strings.Contains(loc, "vt-2") {
		t.Fatalf("303 到了别人的账号页: %q", loc)
	}
	// 伪造票不许覆盖对方存着的凭据 / 改掉它的备注 / 解开它的停用。
	if got := st.accounts[1].Label; got == "acct-b" {
		t.Fatalf("对方账号的 Label 被伪造票改成了上游 ID: %q", got)
	}
	// owner 1 的总览页上照旧没有它。
	mine := do(h, httptest.NewRequest("GET", "/u/"+ownerTok1, nil)).Body.String()
	for _, leak := range []string{"朋友的号", k2, "vt-2"} {
		if strings.Contains(mine, leak) {
			t.Fatalf("抢到自己页面上了: %q", leak)
		}
	}
	// 不许顺手下发 owner 2 的身份 cookie。
	for _, ck := range w.Result().Cookies() {
		if ck.Name == ownerCookie && ck.Value == ownerTok2 {
			t.Fatal("下发了别人的身份 cookie")
		}
	}
	// 没拿到归属的那次上传不该在账本上留一个空 owner（否则这条路可以被反复刷）。
	if len(st.addedOwners) != 0 {
		t.Fatalf("白建了 %d 个空 owner", len(st.addedOwners))
	}
}

// 用**真** store 把「抢账号」这条最关键的路再跑一遍（审查 S6）。
//
// httpapi 侧那些不泄漏断言平时跑在 fakeStore 上，而 fake 的去重分支是 store.AddAccount
// 的手抄本——两份一旦漂移，fake 可能照旧全绿在一个生产里不存在的行为上。这条绕开
// fake，直接验真实存储层接出来的 handler 不交出别人的钥匙、不改别人的归属、不被伪造票
// 覆盖凭据。
func TestUploadWiredToRealStore(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// samples.jsonl 是常开的追加句柄：Windows 上不关它，t.TempDir 的清理会删不掉目录
	// 而整条用例判失败（Linux 上删得掉，所以只在这台机器上露出来）。
	t.Cleanup(func() { _ = st.Close() })
	oA, err := st.AddOwner("A")
	if err != nil {
		t.Fatal(err)
	}
	oB, err := st.AddOwner("B")
	if err != nil {
		t.Fatal(err)
	}
	victim, err := st.AddAccount(types.Account{
		Label: "受害者", PlanType: "plus", Enabled: true, OwnerID: oB.ID,
		Creds: types.Creds{AccessToken: "victim-at", RefreshToken: "victim-rt", AccountID: "acct-victim"},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(&fakePool{mode: types.ModePool}, st, &fakeScanner{},
		Options{ConsumerKey: "ckey", AdminKey: "akey", Model: "gpt-6-luna"})
	if err != nil {
		t.Fatal(err)
	}
	h := srv.Handler()
	vk := srv.ConsumerKeyFor(victim)

	// 攻击者带着 A 的身份，用一张自制的、account_id 填受害者的 JWT 上传（oauth 不验签）。
	form := url.Values{"creds": {`{"access_token":"` +
		fakeToken("acct-victim", "plus", time.Now().Add(time.Hour)) + `"}`}}
	r := httptest.NewRequest("POST", "/upload", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := do(h, ownerCookieOf(r, oA.Token))

	if w.Code != http.StatusConflict {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, leak := range []string{vk, victim.ViewToken, oB.Token, "/a/" + victim.ViewToken, "victim-at", "victim-rt"} {
		if leak != "" && strings.Contains(body, leak) {
			t.Fatalf("回执漏了受害者的钥匙/凭据: %q", leak)
		}
	}
	got, _ := st.Account(victim.ID)
	if got.OwnerID != oB.ID {
		t.Fatalf("归属被抢到 A 下了: owner=%d", got.OwnerID)
	}
	if got.Creds.AccessToken != "victim-at" || got.Label != "受害者" {
		t.Fatalf("受害者的凭据/备注被伪造票覆盖了: token 前缀被换=%v label=%q",
			got.Creds.AccessToken != "victim-at", got.Label)
	}
}

// 没有身份、而且这份凭据已经属于别人：同样不许建空 owner、不许泄露那个人的令牌。
func TestUploadOfOwnedCredentialWithoutIdentity(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModePool)
	r := httptest.NewRequest("POST", "/upload",
		strings.NewReader(`{"access_token":"`+fakeToken("acct-b", "plus", time.Now().Add(time.Hour))+`"}`))
	r.Header.Set("Content-Type", "application/json")
	w := do(h, r)
	// **409，不是 200。** 这不是一次成功的上传，而上传表单的 JS 把「200 就一定有 url」
	// 当成了契约（layout.html：`if (r.ok) location.assign((await r.json()).url)`）⇒
	// 回 200 + 没有 url 的 JSON 会让浏览器跳到 /undefined 的裸 404，下面那句话一个人都看不到。
	// 走到这条路最常见的不是攻击，是「换了浏览器、令牌丢了，把同一份凭据再贴一次想找回链接」。
	if w.Code != 409 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "找它的上传者要") {
		t.Fatalf("没告诉他该去找谁: %s", w.Body.String())
	}
	if st.accounts[1].OwnerID != 2 {
		t.Fatalf("归属被动了: %d", st.accounts[1].OwnerID)
	}
	if len(st.addedOwners) != 0 {
		t.Fatalf("白建了 %d 个空 owner", len(st.addedOwners))
	}
	if strings.Contains(w.Body.String(), ownerTok2) {
		t.Fatal("回执里带了别人的总览令牌")
	}
	for _, ck := range w.Result().Cookies() {
		if ck.Name == ownerCookie {
			t.Fatalf("凭空下发了身份 cookie: %q", ck.Value)
		}
	}
}

// 首页那个入口：owner 令牌进总览页、view_token 进那个账号的页面，
// 认不出来时**不区分**「不存在」和「格式不对」（区分了就是个枚举令牌的工具）。
func TestEnterAcceptsBothTokenKinds(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)
	post := func(tok string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/enter",
			strings.NewReader(url.Values{"token": {tok}}.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return do(h, r)
	}

	w := post(ownerTok1)
	if w.Code != 303 || w.Header().Get("Location") != "/u/"+ownerTok1 {
		t.Fatalf("owner 令牌没进总览页: %d %q", w.Code, w.Header().Get("Location"))
	}
	// 顺手记住他：下次进首页就是一个按钮。
	var got string
	for _, c := range w.Result().Cookies() {
		if c.Name == ownerCookie {
			got = c.Value
		}
	}
	if got != ownerTok1 {
		t.Fatalf("没记住身份: %q", got)
	}

	if w := post("vt-2"); w.Code != 303 || w.Header().Get("Location") != "/a/vt-2" {
		t.Fatalf("view_token 没进账号页: %d %q", w.Code, w.Header().Get("Location"))
	}

	// 认不出的几种：回话必须**完全一样**，否则一次尝试就有了反馈。
	var answers []string
	for _, bad := range []string{"", "   ", "not-a-token", strings.Repeat("f", 32), "0wn3r1aaaaaaaaaaaaaaaaaaaaaaaaa"} {
		w := post(bad)
		answers = append(answers, fmt.Sprintf("%d|%s", w.Code, w.Header().Get("Location")))
		if strings.Contains(w.Header().Get("Location"), bad) && bad != "" {
			t.Fatalf("把令牌回显进了重定向地址: %q", w.Header().Get("Location"))
		}
	}
	for i, a := range answers {
		if a != answers[0] {
			t.Fatalf("第 %d 种认不出的令牌回了不一样的话: %q vs %q", i, a, answers[0])
		}
	}
	if !strings.Contains(answers[0], "enter=bad") {
		t.Fatalf("认不出时没回首页报错: %q", answers[0])
	}
	// 首页要把那句话显示出来。
	if body := do(h, httptest.NewRequest("GET", "/?enter=bad", nil)).Body.String(); !strings.Contains(body, "认不出这个令牌") {
		t.Fatal("首页没显示「认不出」那句话")
	}
}

// 首页的「进入我的账号后台」：带 cookie 给按钮，不带给表单。
func TestPoolPageOwnerEntry(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)

	bare := do(h, httptest.NewRequest("GET", "/", nil)).Body.String()
	if !strings.Contains(bare, `action="/enter"`) {
		t.Fatal("没带身份时该给一个贴令牌的表单")
	}
	if strings.Contains(bare, ownerTok1) || strings.Contains(bare, ownerTok2) {
		t.Fatal("首页对谁都印出了 owner 令牌")
	}

	withCookie := do(h, ownerCookieOf(httptest.NewRequest("GET", "/", nil), ownerTok1)).Body.String()
	if !strings.Contains(withCookie, `href="/u/`+ownerTok1+`"`) {
		t.Fatal("带身份 cookie 时该直接给进入总览页的按钮")
	}
	if strings.Contains(withCookie, ownerTok2) {
		t.Fatal("首页上出现了别人的令牌")
	}
	// 坏 cookie 当没带（不能把它当令牌直接拼进链接）。
	bad := do(h, ownerCookieOf(httptest.NewRequest("GET", "/", nil), "garbage")).Body.String()
	if strings.Contains(bad, "/u/garbage") {
		t.Fatal("坏 cookie 被原样拼进了链接")
	}
	if !strings.Contains(bad, `action="/enter"`) {
		t.Fatal("坏 cookie 该退回表单")
	}
}

// /a/ 那一页**只在浏览器自己带着 owner cookie 时**才回显总览令牌：
// 这一页的地址是转发得出去的，而那个令牌能打开这个人全部账号的页面和接入 key。
func TestAccountPageShowsOwnerTokenOnlyWithCookie(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)

	if body := do(h, httptest.NewRequest("GET", "/a/vt-1", nil)).Body.String(); strings.Contains(body, ownerTok1) {
		t.Fatal("裸看账号页就拿到了总览令牌（转发这一页等于交出全部账号）")
	}
	// 带着自己的 cookie：给链接。
	body := do(h, ownerCookieOf(httptest.NewRequest("GET", "/a/vt-1", nil), ownerTok1)).Body.String()
	if !strings.Contains(body, `href="/u/`+ownerTok1+`"`) {
		t.Fatal("带着自己的身份 cookie 时该给「返回我的账号总览」")
	}
	// 带着**别人**的 cookie 看这一页：什么都不该多出来。
	other := do(h, ownerCookieOf(httptest.NewRequest("GET", "/a/vt-1", nil), ownerTok2)).Body.String()
	for _, leak := range []string{ownerTok1, ownerTok2} {
		if strings.Contains(other, leak) {
			t.Fatalf("拿别人的 cookie 看这一页漏了 %q", leak)
		}
	}
}

// 管理页要把 owner 的令牌列出来：迁移出来那个 owner 的令牌没人见过，
// 不显示就等于现有账号的总览页谁都进不去。这也是丢了令牌唯一的找回入口。
func TestAdminShowsOwnerTokens(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)
	r := httptest.NewRequest("GET", "/admin/", nil)
	r.SetBasicAuth("admin", "akey")
	body := do(h, r).Body.String()
	for _, want := range []string{"上传者", ownerTok1, ownerTok2, "/u/" + ownerTok1} {
		if !strings.Contains(body, want) {
			t.Fatalf("管理页缺了 %q", want)
		}
	}
}

// 管理页：Basic 或 Bearer 都能进，内容不含凭据、cookie 本体和各账号的 key。
func TestAdminView(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModePool)
	w := do(h, httptest.NewRequest("GET", "/admin/", nil))
	if w.Code != 401 || !strings.Contains(w.Header().Get("WWW-Authenticate"), "Basic") {
		t.Fatalf("状态 %d，头 %q", w.Code, w.Header().Get("WWW-Authenticate"))
	}
	r := httptest.NewRequest("GET", "/admin/", nil)
	r.SetBasicAuth("admin", "akey")
	w = do(h, r)
	if w.Code != 200 {
		t.Fatalf("Basic 进不去 %d: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, want := range []string{"unified-142", "acct-upstream", "/a/vt-1", "full"} {
		if !strings.Contains(body, want) {
			t.Fatalf("管理页缺了 %q", want)
		}
	}
	for _, leak := range []string{secretToken, "OAILB-SECRET", "CFLB-SECRET",
		srv.ConsumerKeyFor(st.accounts[0])} {
		if strings.Contains(body, leak) {
			t.Fatalf("管理页漏了 %q", leak)
		}
	}
	if w := do(h, authed(httptest.NewRequest("GET", "/admin/", nil), "akey")); w.Code != 200 {
		t.Fatalf("Bearer 进不去 %d", w.Code)
	}
	// 账号自己的 key 不能当 admin 用。
	if w := do(h, authed(httptest.NewRequest("GET", "/admin/", nil),
		srv.ConsumerKeyFor(st.accounts[0]))); w.Code != 401 {
		t.Fatalf("账号 key 进了管理页: %d", w.Code)
	}
}

// 没配密钥时相关路由必须全拒，不能变成全放。
func TestEmptyKeyDeniesAll(t *testing.T) {
	st := &fakeStore{gws: map[string]types.Gateway{}, accounts: []types.Account{{
		ID: 1, Enabled: true,
		Creds: types.Creds{AccessToken: "x", AccountID: "acct-1"},
	}}}
	srv, err := New(&fakePool{mode: types.ModeSingle}, st, nil, Options{})
	if err != nil {
		t.Fatal(err)
	}
	h := srv.Handler()
	for _, r := range []*http.Request{
		httptest.NewRequest("GET", "/cookie", nil),
		authed(httptest.NewRequest("GET", "/cookie", nil), ""),
		httptest.NewRequest("GET", "/admin/", nil),
	} {
		if w := do(h, r); w.Code != 401 {
			t.Fatalf("%s 没配密钥却回 %d", r.URL, w.Code)
		}
	}
	// 没有 Scanner 也不该 panic（可以不接扫描 worker）。
	r := httptest.NewRequest("POST", "/upload",
		strings.NewReader(`{"access_token":"`+fakeToken("x", "pro", time.Now().Add(time.Hour))+`","account_id":"x"}`))
	r.Header.Set("Content-Type", "application/json")
	if w := do(h, r); w.Code != 303 && w.Code != 200 {
		t.Fatalf("上传回 %d: %s", w.Code, w.Body.String())
	}
}

// 票富裕时自动对外开放：没 key 也能取，但必须报自己的上游 account_id。
func TestAnonymousCookie(t *testing.T) {
	_, h, p, _, _ := rig(t, types.ModePool)

	// 开关没开 ⇒ 还是 401，和以前一样。
	if w := do(h, httptest.NewRequest("GET", "/cookie?account=acct-x", nil)); w.Code != 401 {
		t.Fatalf("开关没开却回 %d", w.Code)
	}

	// 开了但供给不够 ⇒ 503，并告诉他差多少。
	p.public = sched.PublicState{Enabled: true, Open: false, Accounts: 2, Threshold: 6}
	w := do(h, httptest.NewRequest("GET", "/cookie?account=acct-x", nil))
	if w.Code != 503 || !strings.Contains(w.Body.String(), "门槛 6") {
		t.Fatalf("供给不够时应 503 且说明门槛: %d %s", w.Code, w.Body.String())
	}

	// 开放了但不报 account ⇒ 400：不知道是谁就避不开他烧过的网关。
	p.public = sched.PublicState{Enabled: true, Open: true, Accounts: 7, Threshold: 6}
	if w := do(h, httptest.NewRequest("GET", "/cookie", nil)); w.Code != 400 {
		t.Fatalf("匿名没带 account 居然 %d", w.Code)
	}

	// 报了就放行，并把标识原样交给调度器。
	w = do(h, httptest.NewRequest("GET", "/cookie?account=acct-x&gateway=unified-142", nil))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if len(p.anon) != 1 || p.anon[0] != "acct-x|unified-142|force=false|authed=false" {
		t.Fatalf("没把匿名标识传下去: %v", p.anon)
	}
	// 有 key 的人永远走上传者那条路，不会被当成匿名。
	if len(p.delivered) != 0 {
		t.Fatalf("匿名请求跑到上传者路径上了: %v", p.delivered)
	}
	// 非法 gateway 在认人之前就该拦掉。
	if w := do(h, httptest.NewRequest("GET", "/cookie?account=a&gateway=../x", nil)); w.Code != 400 {
		t.Fatalf("非法 gateway 居然 %d", w.Code)
	}
}

// 公开页要如实报开放状态——这是对外承诺。
func TestPublicPageShowsOpenState(t *testing.T) {
	_, h, p, _, _ := rig(t, types.ModePool)
	p.public = sched.PublicState{Enabled: true, Open: true, Accounts: 7, Threshold: 6}
	body := do(h, httptest.NewRequest("GET", "/", nil)).Body.String()
	for _, want := range []string{"开放中", "7", "6", "account="} {
		if !strings.Contains(body, want) {
			t.Fatalf("公开页没报开放状态，缺 %q", want)
		}
	}
	p.public = sched.PublicState{Enabled: true, Open: false, Accounts: 1, Threshold: 6}
	if body := do(h, httptest.NewRequest("GET", "/", nil)).Body.String(); !strings.Contains(body, "已关闭") {
		t.Fatal("关闭状态没显示")
	}
}

// force=1 要传到调度器，并且 TTL 要标明是建议值。
func TestForceAndAdvisoryTTL(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	ka := srv.ConsumerKeyFor(st.accounts[0])

	w := do(h, authed(httptest.NewRequest("GET", "/cookie?force=1", nil), ka))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if len(p.delivered) != 1 || p.delivered[0] != "1||force=true" {
		t.Fatalf("force 没传下去: %v", p.delivered)
	}
	// TTL 是建议值不是承诺，响应里要写明。
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["ttl_is_advisory"] != true {
		t.Fatalf("缺 ttl_is_advisory: %s", w.Body.String())
	}
	// 原有字段一个都不能少。
	for _, k := range []string{"gateway", "cookie", "expires_at", "valid_for_s", "verified_full"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("包了一层之后丢了字段 %q: %s", k, w.Body.String())
		}
	}

	// 几种写法都认，没写就是 false。
	p.delivered = nil
	for _, v := range []string{"1", "true", "yes", "on"} {
		do(h, authed(httptest.NewRequest("GET", "/cookie?force="+v, nil), ka))
	}
	do(h, authed(httptest.NewRequest("GET", "/cookie?force=0", nil), ka))
	do(h, authed(httptest.NewRequest("GET", "/cookie", nil), ka))
	for i, d := range p.delivered {
		want := i < 4
		if strings.HasSuffix(d, "force=true") != want {
			t.Fatalf("第 %d 次 force 解析成了 %s", i, d)
		}
	}

	// 匿名那条路也要带上。
	p.public = sched.PublicState{Enabled: true, Open: true, Accounts: 9, Threshold: 6}
	do(h, httptest.NewRequest("GET", "/cookie?account=acct-x&force=1", nil))
	if len(p.anon) != 1 || !strings.Contains(p.anon[0], "force=true") {
		t.Fatalf("匿名 force 没传下去: %v", p.anon)
	}
}

// 凭据绝不许从任何端点漏出去。
//
// types.Creds 封了打印路径，但**刻意没有 MarshalJSON**（state.json 要靠 json
// 原样存回 token），所以 json.Marshal 一个 types.Account 照样会把 access_token
// 带出来。这道口子只能在 httpapi 守：出去的只能是本包自己的视图结构体。
func TestNoEndpointLeaksCredentials(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	p.public = sched.PublicState{Enabled: true, Open: true, Accounts: 9, Threshold: 6}
	ka := srv.ConsumerKeyFor(st.accounts[0])
	admin := func(r *http.Request) *http.Request { return authed(r, "akey") }
	self := func(r *http.Request) *http.Request { return authed(r, ka) }

	upload := func() *http.Request {
		r := httptest.NewRequest("POST", "/upload?label=x",
			strings.NewReader(`{"access_token":"`+fakeToken("acct-new", "pro", time.Now().Add(time.Hour))+`","account_id":"acct-new"}`))
		r.Header.Set("Content-Type", "application/json")
		return r
	}
	for _, c := range []struct {
		name string
		r    *http.Request
	}{
		{"公开页", httptest.NewRequest("GET", "/", nil)},
		{"上传回执", upload()},
		{"上传者页", httptest.NewRequest("GET", "/a/vt-1", nil)},
		// 总览页一页上摆着这个人全部凭据的读数，是最容易把 token 带出来的那一页。
		{"总览页", httptest.NewRequest("GET", "/u/"+ownerTok1, nil)},
		{"总览页（另一个人）", httptest.NewRequest("GET", "/u/"+ownerTok2, nil)},
		{"cookie", self(httptest.NewRequest("GET", "/cookie", nil))},
		{"cookie 管理员", admin(httptest.NewRequest("GET", "/cookie?account=2", nil))},
		{"cookie 匿名", httptest.NewRequest("GET", "/cookie?account=acct-anon", nil)},
		{"cookie 管理员缺参", admin(httptest.NewRequest("GET", "/cookie", nil))},
		{"admin", admin(httptest.NewRequest("GET", "/admin/", nil))},
		{"未授权", httptest.NewRequest("GET", "/admin/", nil)},
	} {
		w := do(h, c.r)
		body := w.Body.String()
		// 凭据原文一处都不许出现。
		if strings.Contains(body, secretToken) {
			t.Fatalf("%s 的响应里出现了凭据原文：\n%s", c.name, body)
		}
		// JSON 响应里连字段名都不该有——有就说明整个 types.Account 被塞进去了。
		// （HTML 页面上的 "access_token" 是上传表单的示例文案，那个该在。）
		if strings.Contains(w.Header().Get("Content-Type"), "json") &&
			strings.Contains(body, `"access_token"`) {
			t.Fatalf("%s 的 JSON 里带了 access_token 字段：\n%s", c.name, body)
		}
	}
	// 上传那一发确实建了号，才说明上面不是空跑。
	if len(st.added) == 0 || st.added[0].Creds.AccessToken == "" {
		t.Fatal("上传用例没真建号，这个检查什么都没测到")
	}
}

// 上传者自己停用账号：key 当场不认人，请求掉进匿名通道。
// 公开池关着 ⇒ 401，这就是用户要的「停用后没开公开池就请求不了」。
func TestAccountDisableKillsItsKey(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	a := st.accounts[0]
	ka := srv.ConsumerKeyFor(a)
	p.public = sched.PublicState{} // 公开池没开

	// 停用前：这把 key 认得出人，走的是实名通道（不是 401）。
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), ka)); w.Code == 401 {
		t.Fatal("停用前这把 key 就不好使，后面的断言证明不了是停用起的作用")
	}

	w := do(h, httptest.NewRequest("POST", "/a/"+a.ViewToken+"/enabled",
		strings.NewReader("enabled=0")))
	if w.Code != 303 {
		t.Fatalf("停用请求拿到 %d: %s", w.Code, w.Body.String())
	}
	if st.accounts[0].Enabled {
		t.Fatal("账本里还是启用状态")
	}
	// 停用后：掉进匿名通道，公开池没开 ⇒ 401。
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), ka)); w.Code != 401 {
		t.Fatalf("停用后这把 key 还能用，拿到 %d: %s", w.Code, w.Body.String())
	}
	// 能开回来。
	if w := do(h, httptest.NewRequest("POST", "/a/"+a.ViewToken+"/enabled",
		strings.NewReader("enabled=1"))); w.Code != 303 {
		t.Fatalf("重新启用拿到 %d", w.Code)
	}
	if !st.accounts[0].Enabled {
		t.Fatal("开不回来")
	}
}

// 换个 view_token 就能停用别人的账号的话，这个入口就是个开放的破坏按钮。
func TestAccountDisableNeedsItsOwnViewToken(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModePool)
	if w := do(h, httptest.NewRequest("POST", "/a/deadbeef/enabled",
		strings.NewReader("enabled=0"))); w.Code != 404 {
		t.Fatalf("乱填 token 拿到 %d", w.Code)
	}
	for _, a := range st.accounts {
		if !a.Enabled {
			t.Fatalf("账号 %d 被一个不存在的 token 停用了", a.ID)
		}
	}
}

// /admin/rescan 现在是「手动踢一发发现」：一个刚起的空池子在没人取票之前
// 不会自己去找落点（两条自动触发都要先有需求），而那正是最需要踢一脚的时候。
//
// account 可选，语义是「替谁找」⇒ 它自己**不会**被派出去。
func TestAdminRescanIsAManualDiscoveryKick(t *testing.T) {
	_, h, _, st, sc := rig(t, types.ModePool)
	// 脚本路径（带 Accept: json）拿 JSON；不带的是管理页那个表单按钮，拿 303。
	kick := func(q string) *httptest.ResponseRecorder {
		r := authed(httptest.NewRequest("POST", "/admin/rescan"+q, nil), "akey")
		r.Header.Set("Accept", "application/json")
		return do(h, r)
	}
	// 无 key：不许排队。
	if w := do(h, httptest.NewRequest("POST", "/admin/rescan?account=1", nil)); w.Code != 401 {
		t.Fatalf("没带 admin key 居然 %d", w.Code)
	}
	if len(sc.discovery) != 0 {
		t.Fatalf("没鉴权就排进队列了: %v", sc.discovery)
	}
	// 不点名账号也行：那就是「随便派一个有空区域的号」，exclude=0。
	if w := kick(""); w.Code != 202 {
		t.Fatalf("不点名账号拿到 %d", w.Code)
	}
	if len(sc.discovery) != 1 || sc.discovery[0] != 0 {
		t.Fatalf("不点名时 exclude 应为 0: %v", sc.discovery)
	}
	// 点名了就把他排除在外——找到的落点必须对他还是首次接触。
	w := kick("?account=1")
	if w.Code != 202 {
		t.Fatalf("正常踢一发拿到 %d: %s", w.Code, w.Body.String())
	}
	if len(sc.discovery) != 2 || sc.discovery[1] != st.accounts[0].ID {
		t.Fatalf("点名的账号没被当成 exclude 传下去: %v", sc.discovery)
	}
	// 账号不存在就别排：猜一个等于替别人烧一个槽位。
	if w := kick("?account=9999"); w.Code != 404 {
		t.Fatalf("不存在的账号拿到 %d", w.Code)
	}
	if len(sc.discovery) != 2 {
		t.Fatalf("账号不存在还是排了队: %v", sc.discovery)
	}
}

// 「踢了但其实没派出去」要如实说，不能照回 queued:true。
//
// 挑不出人（单账号冷池子）、九个区域本小时打满、挑中的号还在 30 分钟发现冷却里 ——
// 这三种从前都回 202 + queued:true，只有服务端日志说了实话。而这个按钮恰恰是冷启动时
// 唯一的出路，看不出踢没踢动的话运维只能一直点，每点一次还可能真花掉一发上游。
func TestAdminRescanTellsYouWhenItDidNotQueue(t *testing.T) {
	_, h, _, _, sc := rig(t, types.ModePool)
	sc.refuse = "挑不出可以派去找新网关的账号"

	r := authed(httptest.NewRequest("POST", "/admin/rescan", nil), "akey")
	r.Header.Set("Accept", "application/json")
	w := do(h, r)
	if w.Code != 200 {
		t.Fatalf("没派出去却回了 %d: %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"queued":true`) {
		t.Fatalf("没派出去还说排进去了: %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), sc.refuse) {
		t.Fatalf("没说原因: %s", w.Body.String())
	}

	// 管理页那个表单按钮：303 回管理页，原因挂在查询串上，页面要显示出来。
	sc.refuse = "挑中的号还在 30m0s 的发现冷却里，等它出来再踢"
	w = do(h, authed(httptest.NewRequest("POST", "/admin/rescan", nil), "akey"))
	if w.Code != 303 {
		t.Fatalf("表单提交拿到 %d", w.Code)
	}
	to := w.Header().Get("Location")
	if !strings.HasPrefix(to, "/admin/?rescan=") {
		t.Fatalf("没把结论带回管理页: %q", to)
	}
	body := do(h, authed(httptest.NewRequest("GET", to, nil), "akey")).Body.String()
	if !strings.Contains(body, "发现冷却里") {
		t.Fatalf("管理页没显示没派出去的原因：\n%s", body)
	}

	// 真排进去了也要有回话，否则点了之后页面一点变化都没有。
	sc.refuse = ""
	w = do(h, authed(httptest.NewRequest("POST", "/admin/rescan", nil), "akey"))
	body = do(h, authed(httptest.NewRequest("GET", w.Header().Get("Location"), nil), "akey")).Body.String()
	if !strings.Contains(body, "已排进发现队列") {
		t.Fatalf("排进去了却没回话：\n%s", body)
	}
}

// gated 起一个带访问码的实例。rig 的 Options 不带 SiteKey，所以其余用例全不受门禁影响。
func gated(t *testing.T) (http.Handler, *fakeStore) {
	t.Helper()
	_, _, p, st, sc := rig(t, types.ModePool)
	srv, err := New(p, st, sc, Options{ConsumerKey: "ckey", AdminKey: "akey", SiteKey: siteKey})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv.Handler(), st
}

const siteKey = "site-gate-key-abcdef"

// 门禁罩住给人看的页面：证书透明日志会把新域名广播出去，爬虫几分钟内就上门。
func TestGateBlocksHumanPages(t *testing.T) {
	h, st := gated(t)
	for _, c := range []struct {
		name string
		req  *http.Request
	}{
		{"公开页", httptest.NewRequest("GET", "/", nil)},
		{"上传者页", httptest.NewRequest("GET", "/a/vt-1", nil)},
		{"总览页", httptest.NewRequest("GET", "/u/"+ownerTok1, nil)},
		{"贴令牌进入", httptest.NewRequest("POST", "/enter", strings.NewReader("token="+ownerTok1))},
		{"上传", httptest.NewRequest("POST", "/upload", strings.NewReader("creds=x"))},
	} {
		w := do(h, c.req)
		if w.Code != 403 {
			t.Fatalf("%s 没被门禁拦住：%d", c.name, w.Code)
		}
		if body := w.Body.String(); strings.Contains(body, "已知网关") ||
			strings.Contains(body, "vt-1") || strings.Contains(body, "pro1") {
			t.Fatalf("%s 的门禁页漏出了内容：\n%s", c.name, body)
		}
	}
	// 反空测：没拦住的东西要是根本不存在，上面的 403 就什么都没证明。
	if len(st.accounts) == 0 {
		t.Fatal("rig 没账号，这个用例测不到「挡住了本该有的内容」")
	}
}

// 机器面各自带 key，罩上门禁等于把 sub2api 一起关在门外。
func TestGateDoesNotBlockMachineEndpoints(t *testing.T) {
	h, _ := gated(t)
	for _, c := range []struct {
		name string
		req  *http.Request
	}{
		{"/cookie", httptest.NewRequest("GET", "/cookie?account=acct-upstream", nil)},
		{"/admin/", httptest.NewRequest("GET", "/admin/", nil)},
	} {
		if w := do(h, c.req); w.Code == 403 && strings.Contains(w.Body.String(), "需要访问码") {
			t.Fatalf("%s 被页面门禁拦了，它该走自己的 key", c.name)
		}
	}
}

// ?k= 只当一次性入场券：认完换成 cookie 并把 key 从 URL 里去掉，
// 否则它会躺在浏览器历史、书签和用户随手贴的截图里。
func TestGateAcceptsKeyThenStripsItFromURL(t *testing.T) {
	h, _ := gated(t)
	w := do(h, httptest.NewRequest("GET", "/?k="+siteKey, nil))
	if w.Code != 303 {
		t.Fatalf("带对码该 303，拿到 %d：%s", w.Code, w.Body.String())
	}
	if loc := w.Header().Get("Location"); strings.Contains(loc, siteKey) || strings.Contains(loc, "k=") {
		t.Fatalf("重定向目标里还留着 key: %q", loc)
	}
	var tok string
	for _, c := range w.Result().Cookies() {
		if c.Name == gateCookie {
			tok = c.Value
			if !c.HttpOnly {
				t.Error("门禁 cookie 没设 HttpOnly")
			}
		}
	}
	if tok == "" {
		t.Fatal("没下发门禁 cookie，下一跳还会被挡回来")
	}
	// 拿着 cookie 再来，必须真的进得去。
	r := httptest.NewRequest("GET", "/", nil)
	r.AddCookie(&http.Cookie{Name: gateCookie, Value: tok})
	if w2 := do(h, r); w2.Code != 200 || !strings.Contains(w2.Body.String(), "已知网关") {
		t.Fatalf("带 cookie 仍进不去：%d", w2.Code)
	}
}

// 错码和错 cookie 都不许放行——门禁要往关的方向失败。
func TestGateRejectsWrongKey(t *testing.T) {
	h, _ := gated(t)
	if w := do(h, httptest.NewRequest("GET", "/?k=wrong", nil)); w.Code != 403 {
		t.Fatalf("错码拿到 %d", w.Code)
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.AddCookie(&http.Cookie{Name: gateCookie, Value: "wrong"})
	if w := do(h, r); w.Code != 403 {
		t.Fatalf("错 cookie 拿到 %d", w.Code)
	}
}

// 公开页是对外的招揽面，「托管账号」只能数真有凭据的号。匿名调用方的占位行
// （sched.anonAccount 建的）算进来的话，一个真上传者能显示成四个。
func TestPublicPageCountsOnlyHostedAccounts(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModePool)
	for _, up := range []string{"anon:x", "anon:y", "anon:z"} {
		st.accounts = append(st.accounts, types.Account{
			ID: int64(len(st.accounts) + 1), Label: "匿名 " + up, Enabled: true,
			Creds: types.Creds{AccountID: up}, // 刻意没有 AccessToken
		})
	}
	body := do(h, httptest.NewRequest("GET", "/", nil)).Body.String()
	if !strings.Contains(body, "<b>2</b><span>托管账号") {
		t.Fatalf("2 个真上传者 + 3 个匿名占位，托管账号没报 2：\n%s", body)
	}
	// 占位行铸不了票，也不该算成这个网关「歇着的槽位」：两个托管号都没被交付过票，
	// 该是 2，把占位行算进来就是 5。
	if !strings.Contains(body, `<td class="num">2</td>`) {
		t.Fatalf("歇着的槽位没报 2，占位行被算进去了：\n%s", body)
	}
}

// 「空闲槽位」必须和真正发票那把尺子一致：slot_cooldown_minutes（可配）+ LastDeliver，
// 而不是写死的 1 小时 + LastTouch。
//
// 两把尺子的话两个方向都会说谎：两小时前取过票的槽位在页面上显示「可用」，而接下来两小时
// 每次取票都 503（2026-10-02 本地实测：首页 1，/cookie 回 all_cooling retry_after 7190）；
// 反过来，只被一次失败续期 touch 过的槽位显示 0，其实照常能发——notCooling 刻意不拿
// LastTouch 当冷却起点。图例那句「还能用这个网关的账号数」是对使用者的承诺，不是内部读数。
func TestPublicPageRestedSlotsUseTheSameRulerAsDelivery(t *testing.T) {
	now := time.Now()
	// rig 有两个托管号。每个用例只给**一个**号铺槽位，另一个号没有槽位行 ⇒ 恒为「可用」，
	// 于是两把尺子算出来的总数一定不同，不会像「两边都算 1」那样互相遮住。
	cases := []struct {
		name string
		slot types.Slot
		want int // 新尺子（slot_cooldown + LastDeliver）下的空闲槽位数
	}{
		{
			// 两小时前取走过票 ⇒ 4 小时冷却下不可用。旧尺子（1 小时 + LastTouch）会算成
			// 可用，而取票实际 503（2026-10-02 本地实测：首页 1，/cookie 回 all_cooling）。
			name: "取过票的还在冷却里",
			slot: types.Slot{AccountID: 1, Gateway: "unified-142",
				LastDeliver: now.Add(-2 * time.Hour), LastTouch: now.Add(-2 * time.Hour)},
			want: 1,
		},
		{
			// 只被一次失败的续期碰过：LastTouch 很新、LastDeliver 为零 ⇒ 照常能发。
			// 旧尺子会算成不可用 —— notCooling 刻意不拿 LastTouch 当冷却起点。
			name: "只被 touch 过的照样能发",
			slot: types.Slot{AccountID: 1, Gateway: "unified-142", LastTouch: now.Add(-time.Minute)},
			want: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, h, _, st, _ := rig(t, types.ModePool)
			st.slots = map[string]types.Slot{"1|unified-142": tc.slot}

			body := do(h, httptest.NewRequest("GET", "/", nil)).Body.String()
			if want := fmt.Sprintf("空闲槽位 %d　", tc.want); !strings.Contains(body, want) {
				t.Fatalf("没报 %q：\n%s", want, body)
			}
		})
	}

	// 图例上那个门槛也要跟着冷却走，不能是写死的「1.0 小时」。
	_, h, _, _, _ := rig(t, types.ModePool)
	body := do(h, httptest.NewRequest("GET", "/", nil)).Body.String()
	if want := shortDur(types.SlotCooldown); !strings.Contains(body, want) {
		t.Fatalf("图例门槛不是 %s：\n%s", want, body)
	}
}

// sanitizeLabel 按**字符**截。按字节截（`v[:labelMaxRunes]`）会把末尾那个汉字
// 劈成半个，落盘是坏字节、整个 state.json 解不出来，而且是事后极难查的那种。
func TestSanitizeLabelCutsOnRunes(t *testing.T) {
	got := sanitizeLabel(strings.Repeat("汉", 100), "x")
	if want := strings.Repeat("汉", labelMaxRunes); got != want {
		t.Fatalf("100 个汉字截出 %q（%d 字节 / %d 字符）", got, len(got), utf8.RuneCountInString(got))
	}
	// 1/3/4 字节混排：切点必须落在字符边界上，不在第 64 个字节上。
	mixed := sanitizeLabel(strings.Repeat("a中🎉", 40), "x")
	if n := utf8.RuneCountInString(mixed); n != labelMaxRunes {
		t.Fatalf("混排截出 %d 个字符，要 %d：%q", n, labelMaxRunes, mixed)
	}
	for _, s := range []string{got, mixed} {
		if !utf8.ValidString(s) || strings.ContainsRune(s, utf8.RuneError) {
			t.Fatalf("截出了坏字节：%q", s)
		}
	}
}

// 管理员删账号：要 admin key，表单提交后 303 回管理页，账号真的没了。
func TestAdminDeleteAccount(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModePool)
	id := st.accounts[0].ID
	before := len(st.accounts)

	// 没有 admin key 不许删。
	if w := do(h, httptest.NewRequest("POST", fmt.Sprintf("/admin/accounts/%d/delete", id), nil)); w.Code != 401 {
		t.Fatalf("没带 key 删账号拿到 %d", w.Code)
	}
	if len(st.accounts) != before {
		t.Fatal("没带 key 居然删掉了")
	}

	w := do(h, authed(httptest.NewRequest("POST",
		fmt.Sprintf("/admin/accounts/%d/delete", id), nil), "akey"))
	if w.Code != http.StatusSeeOther {
		t.Fatalf("删账号拿到 %d: %s", w.Code, w.Body.String())
	}
	if loc := w.Header().Get("Location"); loc != "/admin/" {
		t.Fatalf("没跳回管理页: %q", loc)
	}
	if len(st.deleted) != 1 || st.deleted[0] != id {
		t.Fatalf("store 收到的删除请求: %v", st.deleted)
	}
	// 不存在的 ID 要 404，不能静默成功。
	if w := do(h, authed(httptest.NewRequest("POST", "/admin/accounts/99999/delete", nil),
		"akey")); w.Code != 404 {
		t.Fatalf("删不存在的账号拿到 %d", w.Code)
	}
	// 非数字 ID 不该变成「删了 0 号」。
	if w := do(h, authed(httptest.NewRequest("POST", "/admin/accounts/abc/delete", nil),
		"akey")); w.Code != 400 {
		t.Fatalf("乱填 ID 拿到 %d", w.Code)
	}
}

// 只贴 access_token，不贴 account_id：从票里读，不让人再贴一遍。
// 多贴一遍就多一个贴错的机会——2026-10-01 就是这么进来一张标着 pro1 实际是
// 另一个 free 账号的票。
func TestUploadDerivesAccountIDFromToken(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModeSingle)
	tok := fakeToken("acct-from-claim", "pro", time.Now().Add(time.Hour))
	r := httptest.NewRequest("POST", "/upload?label=x", strings.NewReader(`{"access_token":"`+tok+`"}`))
	r.Header.Set("Content-Type", "application/json")
	if w := do(h, r); w.Code != 200 && w.Code != http.StatusSeeOther {
		t.Fatalf("只给 access_token 被拒了 %d: %s", w.Code, w.Body.String())
	}
	if len(st.added) != 1 {
		t.Fatalf("没建号: %+v", st.added)
	}
	if got := st.added[0].Creds.AccountID; got != "acct-from-claim" {
		t.Fatalf("account_id 没从票里读出来: %q", got)
	}
	if st.added[0].PlanType != "pro" {
		t.Fatalf("plan 没从票里读出来: %q", st.added[0].PlanType)
	}
}

// 只贴 refresh_token：当场换一张 access_token 再建号。
// 换不出来就别建——建了也是个废号，要等第一次扫描白打一轮才发现。
func TestUploadAcceptsRefreshTokenOnly(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModeSingle)
	fresh := fakeToken("acct-refreshed", "pro", time.Now().Add(time.Hour))
	var asked []string
	srv.opt.Refresh = func(_ context.Context, rt string) (string, string, error) {
		asked = append(asked, rt)
		return fresh, "new-refresh-" + strings.Repeat("r", 30), nil
	}
	rt := "rt-" + strings.Repeat("x", 40)
	r := httptest.NewRequest("POST", "/upload", strings.NewReader(`{"refresh_token":"`+rt+`"}`))
	r.Header.Set("Content-Type", "application/json")
	if w := do(h, r); w.Code != 200 && w.Code != http.StatusSeeOther {
		t.Fatalf("只给 refresh_token 被拒了 %d: %s", w.Code, w.Body.String())
	}
	if len(asked) != 1 || asked[0] != rt {
		t.Fatalf("没拿这张 refresh_token 去换: %v", asked)
	}
	a := st.added[0]
	if a.Creds.AccessToken != fresh || a.Creds.AccountID != "acct-refreshed" {
		t.Fatalf("换回来的票没落对: acct=%q", a.Creds.AccountID)
	}
	// 上游轮换了 refresh_token 就得存新的，否则下次换票用的是作废的那张。
	if !strings.HasPrefix(a.Creds.RefreshToken, "new-refresh-") {
		t.Fatalf("轮换后的 refresh_token 没存: %q 开头", a.Creds.RefreshToken[:12])
	}
}

// 换票失败就不建号。
func TestUploadRefreshFailureDoesNotCreateAccount(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModeSingle)
	srv.opt.Refresh = func(context.Context, string) (string, string, error) {
		return "", "", errors.New("换票回 HTTP 400：invalid_grant")
	}
	r := httptest.NewRequest("POST", "/upload",
		strings.NewReader(`{"refresh_token":"rt-`+strings.Repeat("x", 40)+`"}`))
	r.Header.Set("Content-Type", "application/json")
	w := do(h, r)
	if w.Code != 400 {
		t.Fatalf("换票失败却回了 %d", w.Code)
	}
	if len(st.added) != 0 {
		t.Fatalf("换票失败还是建了号: %+v", st.added)
	}
	// 上游原话要透出来：invalid_grant 和「网络抖了一下」得分得开。
	if !strings.Contains(w.Body.String(), "invalid_grant") {
		t.Fatalf("没把换票失败的原因说出来: %s", w.Body.String())
	}
}

// 过期的 access_token + 有 refresh_token：自己续上，不必让人回去重新导出。
func TestUploadRefreshesExpiredAccessToken(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModeSingle)
	fresh := fakeToken("acct-exp", "pro", time.Now().Add(time.Hour))
	srv.opt.Refresh = func(context.Context, string) (string, string, error) {
		return fresh, "", nil
	}
	stale := fakeToken("acct-exp", "pro", time.Now().Add(-time.Hour))
	rt := "rt-" + strings.Repeat("x", 40)
	r := httptest.NewRequest("POST", "/upload",
		strings.NewReader(`{"access_token":"`+stale+`","refresh_token":"`+rt+`"}`))
	r.Header.Set("Content-Type", "application/json")
	if w := do(h, r); w.Code != 200 && w.Code != http.StatusSeeOther {
		t.Fatalf("过期票带着 refresh 还是被拒了 %d: %s", w.Code, w.Body.String())
	}
	if st.added[0].Creds.AccessToken != fresh {
		t.Fatal("没换成新票")
	}
	// 上游没轮换 ⇒ 继续用原来那张，不能被空串覆盖掉（覆盖了就再也换不了票）。
	if st.added[0].Creds.RefreshToken != rt {
		t.Fatalf("refresh_token 被空串盖掉了: %q", st.added[0].Creds.RefreshToken)
	}
}

// 过期且没有 refresh_token：拒，并且告诉人可以贴 refresh_token。
func TestUploadRejectsExpiredWithoutRefresh(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModeSingle)
	stale := fakeToken("acct-exp", "pro", time.Now().Add(-time.Hour))
	r := httptest.NewRequest("POST", "/upload", strings.NewReader(`{"access_token":"`+stale+`"}`))
	r.Header.Set("Content-Type", "application/json")
	if w := do(h, r); w.Code != 400 {
		t.Fatalf("过期票回了 %d", w.Code)
	}
	if len(st.added) != 0 {
		t.Fatal("过期票还是建了号")
	}
}

// 表单里只贴一个裸 token（不交整份 auth.json）也要收。
func TestUploadAcceptsBareToken(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModeSingle)
	// 每次换票返回**不同** account_id：refresh 路径（post2、post3）要是都落到同一个
	// account_id，第二发就会撞上「这份凭据已属于别人」的归属守卫（每发无身份上传各建
	// 一个 owner），而这条测的是 kind 解析不是归属。首发按 acct-bare-rt 断言。
	rtN := 0
	srv.opt.Refresh = func(context.Context, string) (string, string, error) {
		rtN++
		acct := "acct-bare-rt"
		if rtN > 1 {
			acct = fmt.Sprintf("acct-bare-rt%d", rtN)
		}
		return fakeToken(acct, "pro", time.Now().Add(time.Hour)), "", nil
	}

	post := func(kind, creds string) *httptest.ResponseRecorder {
		form := url.Values{"kind": {kind}, "creds": {creds}}
		r := httptest.NewRequest("POST", "/upload", strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return do(h, r)
	}

	// auto + JWT ⇒ 当 access_token
	jwt := fakeToken("acct-bare-at", "plus", time.Now().Add(time.Hour))
	if w := post("auto", jwt); w.Code != http.StatusSeeOther {
		t.Fatalf("裸 access_token 被拒 %d: %s", w.Code, w.Body.String())
	}
	if got := st.added[0].Creds.AccountID; got != "acct-bare-at" {
		t.Fatalf("没当成 access_token: acct=%q", got)
	}
	if st.added[0].Creds.RefreshToken != "" {
		t.Fatal("凭空多了个 refresh_token")
	}

	// auto + 不是 JWT ⇒ 当 refresh_token，去换票
	if w := post("auto", "rt-"+strings.Repeat("x", 40)); w.Code != http.StatusSeeOther {
		t.Fatalf("裸 refresh_token 被拒 %d: %s", w.Code, w.Body.String())
	}
	if got := st.added[1].Creds.AccountID; got != "acct-bare-rt" {
		t.Fatalf("没当成 refresh_token 去换票: acct=%q", got)
	}

	// 明确选了 refresh，哪怕贴的是 JWT 也按人说的算（他可能真有个 JWT 形状的 refresh）。
	// 用一个**不同 account_id** 的 JWT：复用前面那个会撞上「这份凭据已属于别人」的
	// 归属守卫（每发无身份上传各建一个 owner），而这条测的是 kind 解析，不是归属。
	jwt2 := fakeToken("acct-bare-at2", "plus", time.Now().Add(time.Hour))
	if w := post("refresh", jwt2); w.Code != http.StatusSeeOther {
		t.Fatalf("显式 refresh 被拒 %d: %s", w.Code, w.Body.String())
	}
	if st.added[2].Creds.RefreshToken != jwt2 {
		t.Fatal("显式选 refresh 却没当 refresh_token 用")
	}
}

// 管理页要列出出口代理，但**一个字节的凭据都不许出现**：密码不出现，
// 用户名只给前 4 个字符（novproxy 的用户名里嵌着账号标识）。
func TestAdminShowsProxiesWithoutCredentials(t *testing.T) {
	const (
		proxyUser = "user-SECRET-ACCOUNT-ID"
		proxyPass = "pass-MUST-NEVER-APPEAR"
	)
	_, h, _, _, _ := rig(t, types.ModePool)
	srv, err := New(&fakePool{mode: types.ModePool}, &fakeStore{
		gws: map[string]types.Gateway{}, pairs: map[string]types.Pair{},
		slots: map[string]types.Slot{},
	}, nil, Options{AdminKey: "akey", ProxyStates: func() []mint.ProxyState {
		return []mint.ProxyState{
			{Name: "novproxy", Kind: "novproxy", Host: "us.example.io:1000",
				UserMask: proxyUser[:4] + "***", Enabled: true, Sticky: true,
				Fails: 2, SkipUntil: time.Now().Add(90 * time.Second), BoundSIDs: 7},
			{Name: "webshare", Kind: "webshare", Host: "p.example.io:1080",
				UserMask: "abcd***", Enabled: false},
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	_ = h
	w := do(srv.Handler(), authed(httptest.NewRequest("GET", "/admin/", nil), "akey"))
	if w.Code != 200 {
		t.Fatalf("管理页 %d", w.Code)
	}
	body := w.Body.String()
	for _, leak := range []string{proxyPass, proxyUser} {
		if strings.Contains(body, leak) {
			t.Fatalf("管理页泄了凭据 %q", leak)
		}
	}
	for _, want := range []string{"出口代理", "novproxy", "us.example.io:1000", "user***", "webshare"} {
		if !strings.Contains(body, want) {
			t.Fatalf("管理页缺了 %q", want)
		}
	}
	// 熔断剩余要显示出来：被熔断了却看不出来，就只会去怀疑别的地方。
	if !strings.Contains(body, "1 分钟") {
		t.Fatal("没显示熔断剩余时间")
	}
}

// 没接出口状态时这一节不该崩，只是不显示。
func TestAdminWithoutProxyStates(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)
	w := do(h, authed(httptest.NewRequest("GET", "/admin/", nil), "akey"))
	if w.Code != 200 {
		t.Fatalf("管理页 %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "没有配出口代理") {
		t.Fatal("没接出口状态时该显示一句说明")
	}
}

// /gateways 只列**此刻就有活票**的网关（剩余 ≥ DeliverFloor），used_by_you 必须是
// **问的那个账号**的历史。字段名和形状一个都不许动——sub2api 拿它给落点起名。
func TestGatewaysList(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	ka := srv.ConsumerKeyFor(st.accounts[0])
	p.target = 7
	now := time.Now().UTC()

	st.gws = map[string]types.Gateway{
		"unified-2": {Name: "unified-2"},
		"unified-1": {Name: "unified-1"},
		"unified-3": {Name: "unified-3"},
	}
	st.pairs = map[string]types.Pair{
		// 够交付
		"unified-1": {Gateway: "unified-1", ExpiresAt: now.Add(types.PairLifetime)},
		// 快死了：列出来客户端也拿不到票，只会白跑一次 /cookie
		"unified-2": {Gateway: "unified-2", ExpiresAt: now.Add(types.DeliverFloor / 2)},
		"unified-3": {Gateway: "unified-3", ExpiresAt: now.Add(types.PairLifetime)},
	}
	// 1 号在 unified-1 上用掉过一张票（full/degraded 只有交付路径写得出来）。
	st.slots = map[string]types.Slot{
		"1|unified-1": {AccountID: 1, Gateway: "unified-1", LastTouch: now,
			LastVerdict: types.VerdictFull},
		// 2 号的历史不许串到 1 号头上。
		"2|unified-3": {AccountID: 2, Gateway: "unified-3", LastTouch: now,
			LastVerdict: types.VerdictDegraded},
	}

	w := do(h, authed(httptest.NewRequest("GET", "/gateways", nil), ka))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	var got struct {
		Account  string `json:"account"`
		Live     int    `json:"live"`
		Target   int    `json:"target"`
		Gateways []struct {
			Name       string `json:"name"`
			PairReady  bool   `json:"pair_ready"`
			ValidForS  int    `json:"valid_for_s"`
			UsedByYou  bool   `json:"used_by_you"`
			LastUsedAt string `json:"last_used_at"`
		} `json:"gateways"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Account != "acct-upstream" || got.Target != 7 || got.Live != 2 {
		t.Fatalf("汇总不对: %+v", got)
	}
	// 稳定顺序：客户端按这个列表挑，抖动会让它的选择无端变化。
	// 没活票的 unified-2 直接不出现：列它只会让客户端白跑一次 /cookie。
	if len(got.Gateways) != 2 || got.Gateways[0].Name != "unified-1" ||
		got.Gateways[1].Name != "unified-3" {
		t.Fatalf("清单不对（该只剩有活票的两个、按名字排序）: %+v", got.Gateways)
	}
	for _, g := range got.Gateways {
		if !g.PairReady || g.ValidForS <= 0 {
			t.Fatalf("列出来的都该是可交付的: %+v", g)
		}
	}
	if !got.Gateways[0].UsedByYou || got.Gateways[0].LastUsedAt == "" {
		t.Fatalf("unified-1 该标成用过: %+v", got.Gateways[0])
	}
	if got.Gateways[1].UsedByYou {
		t.Fatalf("2 号的历史串到 1 号头上了: %+v", got.Gateways[1])
	}
	// 没有 key 又没开放 ⇒ 401，不许把网关清单白送。
	if w := do(h, httptest.NewRequest("GET", "/gateways", nil)); w.Code != 401 {
		t.Fatalf("裸请求应 401，实为 %d", w.Code)
	}
}

// 一把 key 替**别的**上游账号取票：槽位按报上来的那个账号记，而且池子验不了满血
// （state-echo 要用那个账号的凭据，池子只有上传者的）⇒ 走匿名那条路。
func TestCookieForAnotherUpstreamAccount(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	ka := srv.ConsumerKeyFor(st.accounts[0])

	w := do(h, authed(httptest.NewRequest("GET", "/cookie?account=acct-别人的号", nil), ka))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if len(p.anon) != 1 || p.anon[0] != "acct-别人的号||force=false|authed=true" {
		t.Fatalf("没按报上来的账号记槽位: anon=%v delivered=%v", p.anon, p.delivered)
	}
	if len(p.delivered) != 0 {
		t.Fatalf("还是按上传者那一行取了: %v", p.delivered)
	}

	// 报的就是自己 ⇒ 照旧走验满血那条路。
	w = do(h, authed(httptest.NewRequest("GET", "/cookie?account=acct-upstream", nil), ka))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if len(p.delivered) != 1 || len(p.anon) != 1 {
		t.Fatalf("报自己时不该走匿名路: anon=%v delivered=%v", p.anon, p.delivered)
	}
}

// 跨账号问清单：/cookie 把这种票记在 AnonPrefix+<account_id> 那一行上，/gateways 必须照
// 同一个键读。读错行的后果是静默的——used_by_you 恒为 false，客户端照着一张全是
// 「没用过」的清单挑，等于没挑。
func TestGatewaysListForAnotherUpstreamAccount(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModePool)
	ka := srv.ConsumerKeyFor(st.accounts[0])
	now := time.Now().UTC()

	st.gws = map[string]types.Gateway{"unified-1": {Name: "unified-1"}, "unified-2": {Name: "unified-2"}}
	// 两个都得有活票，否则清单里根本不会出现（/gateways 只列此刻拿得到票的）。
	st.pairs = map[string]types.Pair{
		"unified-1": {Gateway: "unified-1", ExpiresAt: now.Add(types.PairLifetime)},
		"unified-2": {Gateway: "unified-2", ExpiresAt: now.Add(types.PairLifetime)},
	}
	// 池子给外部账号建的占位行（见 sched.anonAccount）。
	st.accounts = append(st.accounts, types.Account{ID: 9, Label: "匿名 acct-friend",
		Enabled: true, Creds: types.Creds{AccountID: types.AnonPrefix + "acct-friend"}})
	st.slots = map[string]types.Slot{
		// 匿名/跨账号交付只写 LastTouch 和 LastDeliver，一个判定都不落。
		"9|unified-1": {AccountID: 9, Gateway: "unified-1", LastTouch: now, LastDeliver: now},
		// 出了冷却的那次不算「用过」，否则用久了每个网关都是用过，挑不出落点。
		"9|unified-2": {AccountID: 9, Gateway: "unified-2",
			LastTouch: now.Add(-types.SlotCooldown - time.Minute)},
	}

	w := do(h, authed(httptest.NewRequest("GET", "/gateways?account=acct-friend", nil), ka))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	var got struct {
		Account  string `json:"account"`
		Gateways []struct {
			Name       string `json:"name"`
			UsedByYou  bool   `json:"used_by_you"`
			LastUsedAt string `json:"last_used_at"`
		} `json:"gateways"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Account != "acct-friend" || len(got.Gateways) != 2 {
		t.Fatalf("汇总不对: %+v", got)
	}
	if !got.Gateways[0].UsedByYou || got.Gateways[0].LastUsedAt == "" {
		t.Fatalf("占位行的历史没读出来，客户端会把烧过的网关当新落点: %+v", got.Gateways[0])
	}
	if got.Gateways[1].UsedByYou {
		t.Fatalf("出了冷却还算用过: %+v", got.Gateways[1])
	}
}

// 交付失败要在页面上说得出为什么：客户端那头只看到一个 502/503。
// 上传者页只给原因，管理页才给原始错误（里面是出口名、上游状态码这类内部读数）。
func TestDeliverFailuresShowOnPages(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	ka := srv.ConsumerKeyFor(st.accounts[0])
	p.err = fmt.Errorf("unified-142 上没有验过满血的槽位: %w", sched.ErrNoFullSlot)

	if w := do(h, authed(httptest.NewRequest("GET", "/cookie?gateway=unified-142", nil), ka)); w.Code != 503 {
		t.Fatalf("该 503，实为 %d", w.Code)
	}

	mine := do(h, httptest.NewRequest("GET", "/a/vt-1", nil)).Body.String()
	if !strings.Contains(mine, "没有可交付的活票") || !strings.Contains(mine, "unified-142") {
		t.Fatal("上传者页没说为什么取不到票")
	}
	// 错误码也要落在页面上：同一句人话底下可能是两种完全不同的处置。
	if !strings.Contains(mine, codeNoLivePair) {
		t.Fatal("上传者页没显示错误码")
	}
	if strings.Contains(mine, "ErrNoFullSlot") || strings.Contains(mine, "上没有验过满血的槽位:") {
		t.Fatal("上传者页漏了原始错误")
	}
	// 另一个账号的页面不该看见这条。
	if other := do(h, httptest.NewRequest("GET", "/a/vt-2", nil)).Body.String(); strings.Contains(other, "没有可交付的活票") {
		t.Fatal("失败记录串到别人页面上了")
	}

	r := httptest.NewRequest("GET", "/admin/", nil)
	r.SetBasicAuth("admin", "akey")
	admin := do(h, r).Body.String()
	if !strings.Contains(admin, "没有可交付的活票") || !strings.Contains(admin, "上没有验过满血的槽位:") {
		t.Fatal("管理页该连原始错误一起给")
	}
	if !strings.Contains(admin, codeNoLivePair) {
		t.Fatal("管理页没显示错误码")
	}
}

// regionCellOf 掐出某个区域那一格的 HTML。按整页 Contains 断言的话，「绿」和
// 「列出了某个网关」会互相串台——两件事都在页面上，但可能落在不同的格子里。
func regionCellOf(t *testing.T, body, label string) string {
	t.Helper()
	i := strings.Index(body, `title="`+label+`（`)
	if i < 0 {
		t.Fatalf("页面上没有 %s 这一格", label)
	}
	// 往前退到这一格的 <div，再往后切到它的 </div>。
	start := strings.LastIndex(body[:i], "<div")
	end := strings.Index(body[i:], "</div>")
	if start < 0 || end < 0 {
		t.Fatalf("%s 那一格的 HTML 不完整", label)
	}
	return body[start : i+end]
}

// poolRegionRowOf 掐出号池页「各区域的落点」里某个区域那一格。那张卡是一排 KPI 小卡
// （每格一个 <a class="kpi">），不是 /a/ 页那种格子，所以切的边界是 <a> 而不是 <div>。
func poolRegionRowOf(t *testing.T, body, label string) string {
	t.Helper()
	i := strings.Index(body, `title="`+label+`（`)
	if i < 0 {
		t.Fatalf("号池页上没有 %s 这一行", label)
	}
	start := strings.LastIndex(body[:i], "<a ")
	end := strings.Index(body[i:], "</a>")
	if start < 0 || end < 0 {
		t.Fatalf("%s 那一行的 HTML 不完整", label)
	}
	return body[start : i+end]
}

// /a/ 这一页也要有九区域格子：拿着它的人判断「我这份凭据在不在干活」最直观的就是这个，
// 而他手上可能只有这一个链接——总览令牌只在他自己的浏览器里认得出他时才显示。
//
// 格子的语义是**滑窗**（绿＝窗口内取到过、灰＝可以派去铸），终身计数退到悬停文案里：
// 这一页回答的是「现在还能往哪儿派」，终身绿会把早凉了的区域画成绿、正好盖掉答案。
func TestAccountPageShowsRegionGrid(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModePool)
	now := time.Now().UTC()
	win := shortDur(types.SlotCooldown)
	// 另一个账号先发现的网关，用来验「重」。
	st.gws["unified-73"] = types.Gateway{Name: "unified-73", FirstSeenBy: 2, FirstSeenAt: now.Add(-time.Hour)}
	// 终身铸过很多次、但最近一次已经在窗口外 ⇒ 必须是灰的（这一格正是「现在还能去」的答案）。
	st.mints["1|us-west"] = types.RegionMint{AccountID: 1, RegionKey: "us-west", Count: 5, LastAt: now.Add(-2 * types.SlotCooldown)}
	st.samples = append(st.samples,
		// 欧洲第一次撞见 unified-142 ⇒ 这一格算「新」。
		types.Sample{AccountID: 1, RegionKey: "europe", Gateway: "unified-142", Status: 200, At: now.Add(-30 * time.Minute)},
		// 同一个网关又在美东出现 ⇒ 对美东是「重」（它自己在更早的区域撞到过）。
		types.Sample{AccountID: 1, RegionKey: "us-east", Gateway: "unified-142", Status: 200, At: now.Add(-20 * time.Minute)},
		// 别人先发现的 ⇒ 「重」。
		types.Sample{AccountID: 1, RegionKey: "south-america", Gateway: "unified-73", Status: 200, At: now.Add(-10 * time.Minute)},
		// 窗口**外**：名字照列（终身读数），但格子必须是灰的。
		types.Sample{AccountID: 1, RegionKey: "middle-east", Gateway: "unified-900", Status: 200, At: now.Add(-2 * types.SlotCooldown)},
		// 取不到路由的那几发 Gateway 为空，不许把格子染绿。
		types.Sample{AccountID: 1, RegionKey: "oceania", Gateway: "", Status: 429, At: now.Add(-time.Minute)},
	)
	body := do(h, httptest.NewRequest("GET", "/a/vt-1", nil)).Body.String()
	if !strings.Contains(body, "各区域探测情况") {
		t.Fatalf("账号页没有九区域格子：\n%s", body)
	}
	for _, r := range types.Regions {
		if !strings.Contains(body, regionLabel(r.Key)) {
			t.Fatalf("缺了区域 %s 的格子", r.Key)
		}
	}

	// 窗口内 ⇒ 绿，且终身计数（rig 里 europe 是 3 次）退到悬停文案里。
	eu := regionCellOf(t, body, "欧洲")
	if !strings.Contains(eu, "region on") {
		t.Fatalf("窗口内取到过票的格子没变绿：%s", eu)
	}
	if !strings.Contains(eu, "前在此区域取得路由 Cookie") || !strings.Contains(eu, "累计 3 次") {
		t.Fatalf("欧洲那一格的读数不对：%s", eu)
	}
	if !strings.Contains(eu, "unified-142 新增") {
		t.Fatalf("欧洲那一格没标出自己发现的网关：%s", eu)
	}

	// 同一个网关在更晚的区域再出现 ⇒ 重复。
	if us := regionCellOf(t, body, "美国东部"); !strings.Contains(us, "unified-142 重复") {
		t.Fatalf("美东那一格没把重复网关标出来：%s", us)
	}
	// 别人先发现的 ⇒ 重复。
	if sa := regionCellOf(t, body, "南美"); !strings.Contains(sa, "unified-73 重复") {
		t.Fatalf("南美那一格没把别人先发现的网关标成重复：%s", sa)
	}

	// 窗口外 ⇒ 灰（不带 on），但网关名还得在：那一格正是最该被派出去的。
	me := regionCellOf(t, body, "中东")
	if strings.Contains(me, "region on") {
		t.Fatalf("窗口外的格子不该是绿的：%s", me)
	}
	if !strings.Contains(me, "unified-900") || !strings.Contains(me, win+"内未在此区域取得路由 Cookie，可安排铸票") {
		t.Fatalf("中东那一格丢了终身网关名或没说可以去铸：%s", me)
	}

	// 终身绿不算绿：台账上最近一次已经在窗口外的格子必须是灰的，终身次数退到悬停。
	uw := regionCellOf(t, body, "美国西部")
	if strings.Contains(uw, "region on") {
		t.Fatalf("最近一次在窗口外，格子不该是绿的：%s", uw)
	}
	if !strings.Contains(uw, "累计 5 次") {
		t.Fatalf("美西那一格丢了终身读数：%s", uw)
	}

	// Gateway 为空的样本不算取到票。
	if oc := regionCellOf(t, body, "大洋洲"); strings.Contains(oc, "region on") {
		t.Fatalf("没取到路由的样本把格子染绿了：%s", oc)
	}

	// 2 号没在 europe 取过票，不许把 1 号的数算到它头上。
	b2 := do(h, httptest.NewRequest("GET", "/a/vt-2", nil)).Body.String()
	if eu2 := regionCellOf(t, b2, "欧洲"); strings.Contains(eu2, "region on") || strings.Contains(eu2, "unified-142") {
		t.Fatalf("1 号的格子串到 2 号页面上了：%s", eu2)
	}
}

// 号池页按九个出口区域摊开已知网关：这一页要回答的是「哪个区域的落点还不够」，
// 一条平表答不了。一个网关能从几个区域连通就在几格里出现；反查不到区域的落在「未归类」，
// 否则九格加起来对不上页面自己写的网关总数。
func TestPoolRegionCells(t *testing.T) {
	rows := []gatewayRow{
		{Name: "unified-142", PairReady: true},
		{Name: "unified-73"},
		{Name: "unified-900", PairReady: true},
		{Name: "unified-404"},
	}
	gws := map[string]types.Gateway{
		// 两个区域都连得通 ⇒ 两格里都出现。
		"unified-142": {ReachableExits: []types.Exit{{Region: "IT"}, {Region: "JP"}}},
		"unified-73":  {ReachableExits: []types.Exit{{Region: "IT"}}},
		// 代理商给的出口不在 types.Regions 那张表里（精确匹配 Region+City）⇒ 未归类。
		"unified-900": {ReachableExits: []types.Exit{{Region: "US", City: "Dallas"}}},
		// 一个出口都没记过的也要有地方落。
		"unified-404": {},
	}
	cells := poolRegionCells(rows, gws)

	byKey := map[string]poolRegionCell{}
	for _, c := range cells {
		byKey[c.Key] = c
	}
	if len(cells) != len(types.Regions)+2 {
		t.Fatalf("画了 %d 格，该是「全部」加九格加一个未归类", len(cells))
	}
	// 「全部」排在最前面：它是默认态，也是从某一格退出来的那条路。
	if all := cells[0]; !all.All || all.Total != len(rows) || all.Ready != 2 {
		t.Fatalf("「全部」那一格的读数不对：%+v", all)
	}
	if len(cells[0].Gateways) != 0 {
		t.Fatal("「全部」那一格不该列网关名：几十个名字堆在一格里什么也读不出来")
	}
	if eu := byKey["europe"]; eu.Total != 2 || eu.Ready != 1 {
		t.Fatalf("欧洲那一格的读数不对：%+v", eu)
	}
	// 可交付的排前面：这一格最常被问的是「现在这儿能不能拿到票」。
	if eu := byKey["europe"]; eu.Gateways[0].Name != "unified-142" || !eu.Gateways[0].Ready {
		t.Fatalf("欧洲那一格没把可交付的排前面：%+v", eu.Gateways)
	}
	if ea := byKey["east-asia"]; ea.Total != 1 || ea.Gateways[0].Name != "unified-142" {
		t.Fatalf("跨区域的网关没在东亚那一格出现：%+v", ea)
	}
	if un := byKey[""]; un.Label != "未归类" || un.Total != 2 {
		t.Fatalf("未归类那一格该收下反查不到区域的和没有出口的：%+v", un)
	}
	if oc := byKey["oceania"]; oc.Total != 0 || !strings.Contains(oc.Note, "还没在这个区域发现过网关") {
		t.Fatalf("空区域该如实说没发现过网关：%+v", oc)
	}
	// 回填的那一列要和格子出自同一次计算，否则两处读数会对不上。
	if got := rows[0].Regions; len(got) != 2 || got[0] != "欧洲" || got[1] != "东亚" {
		t.Fatalf("网关行的区域列没回填：%v", got)
	}
	if got := rows[2].Regions; len(got) != 0 {
		t.Fatalf("反查不到区域的不该回填区域名：%v", got)
	}

	// 全都不可交付的区域不许标绿。
	cells = poolRegionCells([]gatewayRow{{Name: "unified-73"}}, map[string]types.Gateway{
		"unified-73": {ReachableExits: []types.Exit{{Region: "IT"}}},
	})
	for _, c := range cells {
		if c.Key == "europe" && (c.Ready != 0 || !strings.Contains(c.Note, "都没有可交付")) {
			t.Fatalf("没有活票的区域不该报可交付：%+v", c)
		}
	}
}

// 点一格只看那个区域：筛选走查询串、服务端过滤，关掉 JS 也能用。
// 顶上那几个 KPI 和页签计数**不跟着缩水**——筛着欧洲时写「已知网关 2」是在说池子只剩两个。
func TestPoolPageRegionFilter(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModePool)
	st.gws["unified-73"] = types.Gateway{Name: "unified-73", ReachableExits: []types.Exit{{Region: "JP"}}}
	st.gws["unified-900"] = types.Gateway{Name: "unified-900", ReachableExits: []types.Exit{{Region: "US", City: "Dallas"}}}

	// 没带参数 ⇒ 整张表，「全部」那一格选中。
	all := do(h, httptest.NewRequest("GET", "/", nil)).Body.String()
	for _, name := range []string{"unified-142", "unified-73", "unified-900"} {
		if !strings.Contains(all, name) {
			t.Fatalf("默认该列出全部网关，少了 %s", name)
		}
	}

	// 只看东亚：rig 的 unified-142 出口是 IT（欧洲），不该出现。
	ea := do(h, httptest.NewRequest("GET", "/?region=east-asia", nil)).Body.String()
	if !strings.Contains(ea, "unified-73") {
		t.Fatal("筛东亚把东亚的网关也筛掉了")
	}
	if strings.Contains(ea, `<td class="mono">unified-142</td>`) {
		t.Fatal("筛东亚时表里还列着欧洲的网关")
	}
	if !strings.Contains(ea, "显示全部") {
		t.Fatal("筛选时没给退出去的路")
	}
	// 九宫格自己报的是整池读数，不跟着筛选缩水——不然点进去就看不到别的区域有多少了。
	if !strings.Contains(ea, "unified-142") {
		t.Fatal("筛选把九宫格里别的区域也清空了")
	}

	// 未归类单独一格：它的值不能是空串，空串就是「全部」。
	other := do(h, httptest.NewRequest("GET", "/?region="+poolRegionQuery, nil)).Body.String()
	if !strings.Contains(other, `<td class="mono">unified-900</td>`) ||
		strings.Contains(other, `<td class="mono">unified-73</td>`) {
		t.Fatal("未归类那一格没筛对")
	}

	// 认不出的值当「全部」，不报错：把拼错的链接变成错误页比直接给整张表更糟。
	bad := do(h, httptest.NewRequest("GET", "/?region=%E4%B8%8D%E5%AD%98%E5%9C%A8", nil))
	if bad.Code != http.StatusOK || !strings.Contains(bad.Body.String(), `<td class="mono">unified-73</td>`) {
		t.Fatalf("认不出的区域值该当全部，得到 %d", bad.Code)
	}

	// 有网关但一个都不在该区域 ⇒ 空表要说清是哪一格空，而不是「尚未发现网关」。
	empty := do(h, httptest.NewRequest("GET", "/?region=oceania", nil)).Body.String()
	if !strings.Contains(empty, "大洋洲还没有已知网关") {
		t.Fatal("空区域的空表文案不对")
	}
}

func TestPoolPageShowsRegionCells(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)
	body := do(h, httptest.NewRequest("GET", "/", nil)).Body.String()
	if !strings.Contains(body, "各区域的落点") {
		t.Fatalf("号池页没有区域分格：\n%s", body)
	}
	// rig 里 unified-142 的出口是 IT ⇒ 落在欧洲那一格，而且它有活票 ⇒ 绿。
	cell := poolRegionRowOf(t, body, "欧洲")
	if !strings.Contains(cell, `class="kpi ok"`) || !strings.Contains(cell, "unified-142") {
		t.Fatalf("欧洲那一格没收下 unified-142：%s", cell)
	}
	for _, r := range types.Regions {
		if !strings.Contains(body, regionLabel(r.Key)) {
			t.Fatalf("号池页缺了区域 %s 的格子", r.Key)
		}
	}
}

// /admin/accounts 是「所有账号一人一张卡」：管理页那张扁平表看不见九宫格、
// 接入 key 和取票失败，排查「谁的号在哪个区域还没落点」只能一个个点开 /a/。
func TestAdminAccountsPage(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)
	// 没带 admin key 一律 401：这一页上有每个账号的接入 key。
	if w := do(h, httptest.NewRequest("GET", "/admin/accounts", nil)); w.Code != http.StatusUnauthorized {
		t.Fatalf("/admin/accounts 没鉴权就给了：%d", w.Code)
	}
	r := httptest.NewRequest("GET", "/admin/accounts", nil)
	r.SetBasicAuth("admin", "akey")
	w := do(h, r)
	if w.Code != http.StatusOK {
		t.Fatalf("/admin/accounts = %d", w.Code)
	}
	body := w.Body.String()
	// 两个账号都在（它们挂在**不同的** owner 下，这一页不按归属筛）。
	for _, want := range []string{"pro1", "朋友的号", "各区域探测情况", "全部账号"} {
		if !strings.Contains(body, want) {
			t.Fatalf("/admin/accounts 少了 %q：\n%s", want, body)
		}
	}
	// 这一页不是某个人的总览：别把「本页地址/令牌」那套上传者文案摆上来。
	if strings.Contains(body, "换设备时在首页粘这个令牌") || strings.Contains(body, ownerTok1) {
		t.Fatal("/admin/accounts 混进了上传者总览令牌")
	}
	// 凭据一个字都不许出现（和其他页面同一条哨兵）。
	if strings.Contains(body, secretToken) {
		t.Fatal("/admin/accounts 泄漏了凭据")
	}
}
