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
	"gwpool/internal/types"
)

const (
	secretToken = "sk-must-never-appear-in-any-page"
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
	anon     []string
	// 按账号记流水：调度正确性全看池子有没有认出是谁在要。
	delivered []string
}

func (p *fakePool) Deliver(_ context.Context, accountID int64, gw string, force bool) (types.Delivery, error) {
	p.delivered = append(p.delivered, fmt.Sprintf("%d|%s|force=%v", accountID, gw, force))
	return p.delivery, p.err
}

func (p *fakePool) DeliverAnonymous(_ context.Context, upstream, gw string, force bool) (types.Delivery, error) {
	p.anon = append(p.anon, fmt.Sprintf("%s|%s|force=%v", upstream, gw, force))
	return p.delivery, p.err
}

func (p *fakePool) Public() sched.PublicState { return p.public }

func (p *fakePool) Mode() types.Mode { return p.mode }

type fakeStore struct {
	accounts []types.Account
	gws      map[string]types.Gateway
	pairs    map[string]types.Pair
	slots    map[string]types.Slot
	samples  []types.Sample
	added    []types.Account
	deleted  []int64
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

// 照真实 store 的行为：分配 ID，并在 ViewToken 为空时兜底生成一个。
func (f *fakeStore) AddAccount(a types.Account) (types.Account, error) {
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

type fakeScanner struct {
	queued    []int64
	discovery []int64
}

func (s *fakeScanner) Enqueue(id int64) { s.queued = append(s.queued, id) }

func (s *fakeScanner) EnqueueDiscovery(exclude int64) {
	s.discovery = append(s.discovery, exclude)
}

func rig(t *testing.T, mode types.Mode) (*Server, http.Handler, *fakePool, *fakeStore, *fakeScanner) {
	t.Helper()
	now := time.Now().UTC()
	st := &fakeStore{
		accounts: []types.Account{
			{
				ID: 1, Label: "pro1", PlanType: "pro", Enabled: true,
				ViewToken: "vt-1", CreatedAt: now.Add(-time.Hour),
				Creds: types.Creds{AccessToken: secretToken, AccountID: "acct-upstream"},
			},
			{
				ID: 2, Label: "朋友的号", PlanType: "plus", Enabled: true,
				ViewToken: "vt-2", CreatedAt: now.Add(-time.Minute),
				Creds: types.Creds{AccessToken: secretToken + "-b", AccountID: "acct-b"},
			},
		},
		gws: map[string]types.Gateway{"unified-142": {
			Name: "unified-142", FirstSeenBy: 1, FirstSeenAt: now.Add(-time.Hour),
			ReachableExits: []types.Exit{{Region: "IT"}},
		}},
		pairs: map[string]types.Pair{"unified-142": {
			Gateway: "unified-142", Cookie: liveCookie, ExpiresAt: now.Add(time.Hour), MintedBy: 1,
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
	if len(sc.queued) != 1 || sc.queued[0] != a.ID {
		t.Fatalf("没排队首扫: %v", sc.queued)
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
	p.public = sched.PublicState{Enabled: true, Open: false, Free: 2, Threshold: 6}
	w := do(h, httptest.NewRequest("GET", "/cookie?account=acct-x", nil))
	if w.Code != 503 || !strings.Contains(w.Body.String(), "门槛 6") {
		t.Fatalf("供给不够时应 503 且说明门槛: %d %s", w.Code, w.Body.String())
	}

	// 开放了但不报 account ⇒ 400：不知道是谁就避不开他烧过的网关。
	p.public = sched.PublicState{Enabled: true, Open: true, Free: 7, Threshold: 6}
	if w := do(h, httptest.NewRequest("GET", "/cookie", nil)); w.Code != 400 {
		t.Fatalf("匿名没带 account 居然 %d", w.Code)
	}

	// 报了就放行，并把标识原样交给调度器。
	w = do(h, httptest.NewRequest("GET", "/cookie?account=acct-x&gateway=unified-142", nil))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if len(p.anon) != 1 || p.anon[0] != "acct-x|unified-142|force=false" {
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
	p.public = sched.PublicState{Enabled: true, Open: true, Free: 7, Threshold: 6}
	body := do(h, httptest.NewRequest("GET", "/", nil)).Body.String()
	for _, want := range []string{"开放中", "7", "6", "account="} {
		if !strings.Contains(body, want) {
			t.Fatalf("公开页没报开放状态，缺 %q", want)
		}
	}
	p.public = sched.PublicState{Enabled: true, Open: false, Free: 1, Threshold: 6}
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
	p.public = sched.PublicState{Enabled: true, Open: true, Free: 9, Threshold: 6}
	do(h, httptest.NewRequest("GET", "/cookie?account=acct-x&force=1", nil))
	if len(p.anon) != 1 || !strings.HasSuffix(p.anon[0], "force=true") {
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
	p.public = sched.PublicState{Enabled: true, Open: true, Free: 9, Threshold: 6}
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

// 首扫只在 /upload 排一次。那一次全败的账号必须有办法重来，否则永远停在零网关。
func TestAdminRescanRequiresAdminAndAccount(t *testing.T) {
	_, h, _, st, sc := rig(t, types.ModePool)
	// 无 key：不许排队。
	if w := do(h, httptest.NewRequest("POST", "/admin/rescan?account=1", nil)); w.Code != 401 {
		t.Fatalf("没带 admin key 居然 %d", w.Code)
	}
	if len(sc.queued) != 0 {
		t.Fatalf("没鉴权就排进队列了: %v", sc.queued)
	}
	// 带 key 但不点名账号：槽位是 (账号 × 网关)，猜一个就是替别人烧。
	if w := do(h, authed(httptest.NewRequest("POST", "/admin/rescan", nil), "akey")); w.Code != 400 {
		t.Fatalf("没带 account 居然 %d", w.Code)
	}
	if len(sc.queued) != 0 {
		t.Fatalf("没点名账号也排了队: %v", sc.queued)
	}
	// 点名了就排。
	w := do(h, authed(httptest.NewRequest("POST", "/admin/rescan?account=1", nil), "akey"))
	if w.Code != 202 {
		t.Fatalf("正常重扫拿到 %d: %s", w.Code, w.Body.String())
	}
	if len(sc.queued) != 1 || sc.queued[0] != st.accounts[0].ID {
		t.Fatalf("排进队列的不是点名的那个账号: %v", sc.queued)
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
		{"上传", httptest.NewRequest("POST", "/upload", strings.NewReader("creds=x"))},
	} {
		w := do(h, c.req)
		if w.Code != 403 {
			t.Fatalf("%s 没被门禁拦住：%d", c.name, w.Code)
		}
		if body := w.Body.String(); strings.Contains(body, "已知网关") || strings.Contains(body, "vt-1") {
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
	// 占位行铸不了票，也不该算成这个网关「歇着的槽位」：两个托管号都歇够了
	// restedThreshold，该是 2，把占位行算进来就是 5。
	if !strings.Contains(body, `<td class="num">2</td>`) {
		t.Fatalf("歇着的槽位没报 2，占位行被算进去了：\n%s", body)
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
	fresh := fakeToken("acct-bare-rt", "pro", time.Now().Add(time.Hour))
	srv.opt.Refresh = func(context.Context, string) (string, string, error) { return fresh, "", nil }

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

	// 明确选了 refresh，哪怕贴的是 JWT 也按人说的算（他可能真有个 JWT 形状的 refresh）
	if w := post("refresh", jwt); w.Code != http.StatusSeeOther {
		t.Fatalf("显式 refresh 被拒 %d: %s", w.Code, w.Body.String())
	}
	if st.added[2].Creds.RefreshToken != jwt {
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
