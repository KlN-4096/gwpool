package mint

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

var testCreds = types.Creds{AccessToken: "tok-SECRET", AccountID: "acc-123"}

// golden 文件里的模型名。固定成常量，别跟着 defaultModel 漂。
const testModel = "gpt-6-luna"

var novProxy = config.Proxy{Name: "nov", Kind: config.KindNovproxy,
	Host: "us.novproxy.io:1000", User: "u100806", Pass: "pw", Enabled: true}
var wsProxy = config.Proxy{Name: "ws", Kind: config.KindWebshare,
	Host: "p.webshare.io:1080", User: "wsu", Pass: "wsp", Enabled: true}
var seedProxy = config.Proxy{Name: "bv", Kind: config.KindSeed,
	Host: "127.0.0.1:35444", User: "su", Pass: "sp", Enabled: true}

// fakeOailb 造一个形状正确的 __oailb：ES256 JWT，payload.host 的第三段是网关名。
// 签名是假的——实现刻意不校验签名（密钥在上游手里，校验不了）。
func fakeOailb(gateway string, exp time.Time) string {
	payload := fmt.Sprintf(`{"host":"chatgpt-prod.edge.%s.internal","exp":%d}`, gateway, exp.Unix())
	return "eyJhbGciOiJFUzI1NiJ9." +
		base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".fake-sig"
}

func newClient(t *testing.T, h http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &Client{Upstream: srv.URL, GeoURL: srv.URL, HTTP: srv.Client(),
		Proxies: []config.Proxy{novProxy}}, srv
}

// liveCflb 是「送进去那张 pair 的 __cflb 还活得好好的」。
//
// 这个参数不是装饰：「上游没下发新 cookie」只有在它可证还活着时才等于「路由没变」，
// 死了的话那一发是随机路由走的（见 Mint 里那段注释）。
func liveCflb() time.Time { return time.Now().Add(types.PairLifetime) }

func TestCflbOnlyKeepsOnlyTheRoutingCookie(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"__cflb=CF; __oailb=OAI", "__cflb=CF"},
		{"__oailb=OAI; __cflb=CF", "__cflb=CF"},
		{"__cflb=CF", "__cflb=CF"},
		{"__oailb=OAI", ""}, // 没有 __cflb 就钉不住任何路由，当种子是零价值的
		{"", ""},
	} {
		if got := CflbOnly(tc.in); got != tc.want {
			t.Errorf("CflbOnly(%q) = %q，想要 %q", tc.in, got, tc.want)
		}
	}
}

// 绑在种子出口上的 sid，出站**必须**只带 __cflb。
//
// 为什么要在这一层钉死：两件齐送时上游一个 cookie 都不回（2026-10-02 实测），落点就
// 读不出来，而带种子铸票唯一想要的就是那个读数 —— 漏摘的话这一发静默白打，没人会发现。
// 顺带钉住第二件事：回来那张新 pair 不能带着种子那张**别人的** __oailb。
func TestSeedBoundSIDSendsOnlyTheCflb(t *testing.T) {
	exp := time.Now().Add(types.OailbLifetime).Truncate(time.Second).UTC()
	var gotCookie string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCookie = r.Header.Get("cookie")
		// 只送 cflb 时上游必回新 __oailb（这正是落点读得出来的原因），不回新 cflb。
		w.Header().Add("set-cookie", "__oailb="+fakeOailb("unified-94", exp)+"; Path=/")
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)
	c := &Client{Upstream: srv.URL, GeoURL: srv.URL, HTTP: srv.Client(),
		Proxies: []config.Proxy{seedProxy}}

	sid, err := c.NewSeedSID()
	if err != nil {
		t.Fatal(err)
	}
	seed := "__cflb=SEEDCF; __oailb=" + fakeOailb("unified-88", exp)
	res, err := c.Mint(context.Background(), testCreds, types.Exit{Region: "JP"}, sid, seed, liveCflb())
	if err != nil {
		t.Fatal(err)
	}
	if gotCookie != "__cflb=SEEDCF" {
		t.Errorf("出站 cookie 该只有 __cflb，得到 %q", gotCookie)
	}
	// 落点是这一发自己摸到的那个，不是种子票指向的 unified-88。
	if res.Gateway != "unified-94" {
		t.Errorf("落点该是 unified-94，得到 %q", res.Gateway)
	}
	if strings.Contains(res.NewPair, "unified-88") {
		t.Errorf("新 pair 不该带着种子那张别人的 __oailb: %q", res.NewPair)
	}
	if !strings.Contains(res.NewPair, "__cflb=SEEDCF") {
		t.Errorf("新 pair 该留着钉路由的 __cflb（上游没补发）: %q", res.NewPair)
	}
}

// 非种子出口一个字都不该变：照旧两件齐送。
func TestNonSeedSIDStillSendsTheWholePair(t *testing.T) {
	exp := time.Now().Add(types.OailbLifetime).Truncate(time.Second).UTC()
	var gotCookie string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCookie = r.Header.Get("cookie")
		w.Header().Add("set-cookie", "__oailb="+fakeOailb("unified-88", exp)+"; Path=/")
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)
	c := &Client{Upstream: srv.URL, GeoURL: srv.URL, HTTP: srv.Client(),
		Proxies: []config.Proxy{novProxy}}

	sid, err := c.NewSID(true)
	if err != nil {
		t.Fatal(err)
	}
	seed := "__cflb=CF; __oailb=" + fakeOailb("unified-88", exp)
	if _, err := c.Mint(context.Background(), testCreds, types.Exit{Region: "JP"}, sid, seed, liveCflb()); err != nil {
		t.Fatal(err)
	}
	if gotCookie != seed {
		t.Errorf("住宅出口该原样两件齐送，得到 %q", gotCookie)
	}
}

// 种子那条路上游没回新 __oailb ⇒ 落点读不出来 ⇒ 必须报 ErrNoLanding。
//
// 不能把种子票里那个旧名字当落点：那是**别人的**网关，认下来就等于把一张没有落点的票
// 写进账本，还会把触碰记在没打到的网关上。
func TestSeedShotWithoutNewOailbHasNoLanding(t *testing.T) {
	exp := time.Now().Add(types.OailbLifetime).Truncate(time.Second).UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200) // 一个 cookie 都不回
	}))
	t.Cleanup(srv.Close)
	c := &Client{Upstream: srv.URL, GeoURL: srv.URL, HTTP: srv.Client(),
		Proxies: []config.Proxy{seedProxy}}

	sid, err := c.NewSeedSID()
	if err != nil {
		t.Fatal(err)
	}
	seed := "__cflb=SEEDCF; __oailb=" + fakeOailb("unified-88", exp)
	res, err := c.Mint(context.Background(), testCreds, types.Exit{Region: "JP"}, sid, seed, liveCflb())
	if !errors.Is(err, ErrNoLanding) {
		t.Fatalf("该报 ErrNoLanding，得到 err=%v res=%+v", err, res)
	}
	if res.Gateway != "" {
		t.Errorf("落点该留空，不该认种子票里那个别人的网关，得到 %q", res.Gateway)
	}
}

// 没配种子出口时 NewSeedSID 要报 ErrNoProxy，调用方据此回落住宅。
func TestNewSeedSIDNeedsASeedProxy(t *testing.T) {
	if _, err := (&Client{Proxies: []config.Proxy{novProxy}}).NewSeedSID(); !errors.Is(err, ErrNoProxy) {
		t.Fatalf("只有住宅出口时该报 ErrNoProxy，得到 %v", err)
	}
	off := seedProxy
	off.Enabled = false
	if _, err := (&Client{Proxies: []config.Proxy{off}}).NewSeedSID(); !errors.Is(err, ErrNoProxy) {
		t.Fatalf("种子出口停用了也该报 ErrNoProxy，得到 %v", err)
	}
}

// 种子出口的用户名原样是 p.User：不带 region/sid 段，也不该被出口合法性闸拦下来。
func TestSeedProxyUserIsVerbatim(t *testing.T) {
	// 带城市的出口：住宅那边会把它拼进用户名，种子这边必须一个字节都不拼。
	got, err := proxyUser(seedProxy, types.Exit{Region: "US", City: "Seattle"}, "sid1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got != seedProxy.User {
		t.Fatalf("种子出口用户名该是 %q，得到 %q", seedProxy.User, got)
	}
}

// SPEC 第 2 节第一条：下发了新 __oailb ⇒ 落点 = JWT host 第三段。
func TestMintNewCookieGivesGateway(t *testing.T) {
	exp := time.Now().Add(types.OailbLifetime).Truncate(time.Second).UTC()
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("set-cookie", "__cflb=NEWCFLB; Path=/; Max-Age=3600")
		w.Header().Add("set-cookie", "__oailb="+fakeOailb("unified-167", exp)+"; Path=/")
		w.Header().Set("x-codex-turn-state", "ticket-abc")
		w.WriteHeader(200)
	})
	res, err := c.Mint(context.Background(), testCreds, types.Exit{Region: "US"}, "sid1", "", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Gateway != "unified-167" {
		t.Errorf("落点该是 unified-167，得到 %q", res.Gateway)
	}
	if !res.PairExpires.Equal(exp) {
		t.Errorf("PairExpires 该取 __oailb 的 exp，得到 %v 要 %v", res.PairExpires, exp)
	}
	if res.TurnState != "ticket-abc" || res.Status != 200 {
		t.Errorf("票或状态码不对: %+v", res)
	}
	if !strings.Contains(res.NewPair, "__cflb=NEWCFLB") || !strings.Contains(res.NewPair, "__oailb=") {
		t.Errorf("NewPair 该是完整 cookie 头，得到 %q", res.NewPair)
	}
}

// 钉路由的是 __cflb，而它的死期只能从 Set-Cookie 的 Max-Age 读出来。
// 读不出来就只能按实测差值兜底，而兜底值必须比 __oailb 的 exp 早 300 秒。
func TestMintReadsCflbDeadline(t *testing.T) {
	exp := time.Now().Add(types.OailbLifetime).Truncate(time.Second).UTC()
	withHeaders := func(cflb string) types.MintResult {
		t.Helper()
		c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
			if cflb != "" {
				w.Header().Add("set-cookie", cflb)
			}
			w.Header().Add("set-cookie", "__oailb="+fakeOailb("unified-167", exp))
			w.WriteHeader(200)
		})
		res, err := c.Mint(context.Background(), testCreds, types.Exit{Region: "US"}, "sid1", "", time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	// 一侧：给了 Max-Age ⇒ 死期按它算，路由寿命跟着它（比 __oailb 早）。
	res := withHeaders("__cflb=NEW; Max-Age=3600")
	left := time.Until(res.CflbExpires)
	if left < types.PairLifetime-time.Minute || left > types.PairLifetime {
		t.Fatalf("__cflb 死期按 Max-Age 该是 %v 之后，实际还剩 %v", types.PairLifetime, left)
	}
	route, _ := res.RouteDeadline(time.Time{})
	if !route.Equal(res.CflbExpires) {
		t.Fatalf("路由寿命没跟着 __cflb：route=%v cflb=%v", route, res.CflbExpires)
	}
	if !res.PairExpires.After(route) {
		t.Fatalf("__oailb 的 exp 该比路由死期晚：%v vs %v", res.PairExpires, route)
	}

	// 另一侧：一个 __cflb 都没下发 ⇒ 死期读不出（零值），寿命走兜底、比 exp 早 300 秒。
	res = withHeaders("")
	if !res.CflbExpires.IsZero() {
		t.Fatalf("没下发 __cflb 却算出了死期 %v（那会让继承来的旧票被盖掉）", res.CflbExpires)
	}
	route, _ = res.RouteDeadline(time.Time{})
	if want := res.PairExpires.Add(-(types.OailbLifetime - types.PairLifetime)); !route.Equal(want) {
		t.Fatalf("兜底寿命 %v，应为 %v", route, want)
	}
}

// 上游常只补发变动的那一项。只拿新的那半会丢掉真正钉路由的 __cflb。
func TestMintMergesOntoSentPair(t *testing.T) {
	sent := "__cflb=KEEPME; __oailb=" + fakeOailb("unified-84", time.Now().Add(time.Hour))
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("set-cookie", "__oailb="+fakeOailb("unified-202", time.Now().Add(time.Hour)))
		w.WriteHeader(200)
	})
	res, err := c.Mint(context.Background(), testCreds, types.Exit{}, "sid1", sent, liveCflb())
	if err != nil {
		t.Fatal(err)
	}
	if res.Gateway != "unified-202" {
		t.Errorf("下发了新 __oailb 就该跟新的走，得到 %q", res.Gateway)
	}
	if !strings.Contains(res.NewPair, "__cflb=KEEPME") {
		t.Errorf("没补发的 __cflb 该沿用送进去的那个，得到 %q", res.NewPair)
	}
	if !strings.HasPrefix(res.NewPair, "__cflb=") {
		t.Errorf("输出顺序该固定 __cflb 在前，得到 %q", res.NewPair)
	}
}

// SPEC 第 2 节第二条 + 红线 3：没回新 cookie 但发了 pair = **成功**，落点就是送进去那个。
func TestMintNoNewCookieWithSentPairIsSuccess(t *testing.T) {
	sent := "__cflb=abc; __oailb=" + fakeOailb("unified-142", time.Now().Add(time.Hour))
	var sawCookie string
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		sawCookie = r.Header.Get("cookie")
		w.Header().Set("x-codex-turn-state", "ticket-xyz")
		w.WriteHeader(200) // 一个 set-cookie 都不给
	})
	res, err := c.Mint(context.Background(), testCreds, types.Exit{}, "sid1", sent, liveCflb())
	if err != nil {
		t.Fatalf("不下发新 cookie 不是失败: %v", err)
	}
	if res.Gateway != "unified-142" {
		t.Errorf("落点该沿用送进去的 pair（上游接受了、没换），得到 %q", res.Gateway)
	}
	if res.NewPair != "" {
		t.Errorf("没下发新 cookie 时 NewPair 该为空，得到 %q", res.NewPair)
	}
	if sawCookie != sent {
		t.Errorf("pair 没被原样发出去: %q", sawCookie)
	}
}

// 「没下发新 cookie」的另一半：送进去那张的 __cflb 已经不保证活着时，
// 旧名字**不能**当落点。
//
// 钉路由的是 __cflb：它死了这一发就是按出口地理随机路由走的，而 __oailb 还能读出
// 那个过期的名字。认它的话三处账本一起写坏——触碰记在没打到的网关上（真正打到的
// 那个仍是「从没碰过」，下次当首次接触发给别人）、定向铸票的漂移永远检测不出来、
// 续期的失败预算永远攒不满。
func TestMintRefusesStaleCflbAsLanding(t *testing.T) {
	sent := "__cflb=abc; __oailb=" + fakeOailb("unified-142", time.Now().Add(time.Hour))
	srv := func() *Client {
		c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("x-codex-turn-state", "ticket-xyz")
			w.WriteHeader(200) // 一个 set-cookie 都不给
		})
		return c
	}
	for _, c := range []struct {
		name string
		exp  time.Time
	}{
		{"已经过期", time.Now().Add(-time.Second)},
		{"死期读不出（零值）", time.Time{}},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := srv().Mint(context.Background(), testCreds, types.Exit{}, "sid1", sent, c.exp)
			if !errors.Is(err, ErrNoLanding) {
				t.Fatalf("err=%v，应为 ErrNoLanding（读不出落点）", err)
			}
			if res.Gateway != "" {
				t.Fatalf("把过期 __cflb 里的旧名字当落点了: %q", res.Gateway)
			}
		})
	}
}

// SPEC 第 2 节第三条：没回新 cookie 又没发 pair ⇒ 读不出落点，算失败。
func TestMintNoNewCookieNoSentPairFails(t *testing.T) {
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-codex-turn-state", "ticket-xyz")
		w.WriteHeader(200)
	})
	res, err := c.Mint(context.Background(), testCreds, types.Exit{}, "sid1", "", time.Time{})
	if !errors.Is(err, ErrNoLanding) {
		t.Fatalf("该是 ErrNoLanding，得到 %v", err)
	}
	if res.Gateway != "" {
		t.Errorf("失败时 Gateway 必须为空，得到 %q", res.Gateway)
	}
	if res.Status != 200 {
		t.Errorf("状态码仍要带回来给调用方落样本，得到 %d", res.Status)
	}
}

// 401 只下发 __cflb、没有 __oailb（geo_sampler.py 头注释里实证过的那条）。
func TestMintUnauthorizedHasNoLanding(t *testing.T) {
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("set-cookie", "__cflb=onlythis")
		w.WriteHeader(401)
	})
	res, err := c.Mint(context.Background(), testCreds, types.Exit{}, "sid1", "", time.Time{})
	if !errors.Is(err, ErrNoLanding) || res.Status != 401 {
		t.Fatalf("401 该报读不出落点并带回状态码: res=%+v err=%v", res, err)
	}
}

// 限流响应也可能带 set-cookie：落点要带状态码回去，由调用方决定收不收。
func TestMintRateLimitedStillReportsStatus(t *testing.T) {
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("set-cookie", "__oailb="+fakeOailb("unified-9", time.Now().Add(time.Hour)))
		w.WriteHeader(429)
	})
	res, err := c.Mint(context.Background(), testCreds, types.Exit{}, "sid1", "", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != 429 || res.Gateway != "unified-9" {
		t.Errorf("状态码和落点都要带回去: %+v", res)
	}
}

func TestVerifyFull(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		newState string
		wantNil  bool
		wantFull bool
	}{
		{"200 不下发新 state ⇒ 满血", 200, "", false, true},
		{"200 回下同一张票 ⇒ 满血", 200, "live-ticket", false, true},
		{"200 下发不同的新 state ⇒ 降级", 200, "other-state", false, false},
		{"429 ⇒ 不下结论（原版在这里假阳）", 429, "", true, false},
		{"403 ⇒ 不下结论", 403, "", true, false},
		{"500 ⇒ 不下结论", 500, "", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var sawTicket string
			c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
				sawTicket = r.Header.Get(stateHeader)
				if tc.newState != "" {
					w.Header().Set(stateHeader, tc.newState)
				}
				w.WriteHeader(tc.status)
			})
			v, err := c.VerifyFull(context.Background(), testCreds, types.Exit{},
				"sid1", "__cflb=a", "live-ticket")
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantNil {
				if v != nil {
					t.Fatalf("非 200 必须不下结论，得到 %+v", v)
				}
				return
			}
			if v == nil {
				t.Fatal("200 该给出判定")
			}
			if v.Full != tc.wantFull {
				t.Errorf("Full = %v，要 %v（%s）", v.Full, tc.wantFull, v.Note)
			}
			if sawTicket != "live-ticket" {
				t.Errorf("必须真发一张活票，否则上游必回 state、判据失效；发出去的是 %q", sawTicket)
			}
		})
	}
}

func TestVerifyFullRequiresTicket(t *testing.T) {
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("没带活票就不该发请求出去")
	})
	if _, err := c.VerifyFull(context.Background(), testCreds, types.Exit{}, "sid1", "", ""); err == nil {
		t.Fatal("空票该直接报错")
	}
}

func TestGeoParsesIpinfo(t *testing.T) {
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"ip":"126.109.254.208","country":"JP","city":"Kyoto",
		                "region":"Kyoto","org":"AS2516 KDDI CORPORATION"}`)
	})
	obs, err := c.Geo(context.Background(), types.Exit{Region: "JP"}, "sid1")
	if err != nil {
		t.Fatal(err)
	}
	want := types.Observed{IP: "126.109.254.208", Country: "JP", State: "Kyoto", City: "Kyoto",
		ASN: "AS2516", ISP: "KDDI CORPORATION"}
	if obs != want {
		t.Errorf("得到 %+v，要 %+v", obs, want)
	}
}

func TestGeoRetriesOnce(t *testing.T) {
	n := 0
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		n++
		if n == 1 {
			w.WriteHeader(503) // 住宅 IP 打 ipinfo 偶发抽风
			return
		}
		_, _ = fmt.Fprint(w, `{"ip":"1.2.3.4","country":"US","org":"AS1 X"}`)
	})
	obs, err := c.Geo(context.Background(), types.Exit{Region: "US"}, "sid1")
	if err != nil || obs.IP != "1.2.3.4" {
		t.Fatalf("该重试一次并成功: obs=%+v err=%v", obs, err)
	}
	if n != 2 {
		t.Errorf("该正好打两次，打了 %d 次", n)
	}
}

// ---------- 出口与代理用户名 ----------

func TestProxyUserPerKind(t *testing.T) {
	got, err := proxyUser(novProxy, types.Exit{Region: "US", City: "Seattle"}, "ab12cd34", 5)
	if err != nil || got != "u100806-region-US-city-Seattle-sid-ab12cd34-t-5" {
		t.Errorf("novproxy 带城市: %q err=%v", got, err)
	}
	got, err = proxyUser(novProxy, types.Exit{Region: "JP"}, "zz99", 7)
	if err != nil || got != "u100806-region-JP-sid-zz99-t-7" {
		t.Errorf("novproxy 不带城市: %q err=%v", got, err)
	}
	// webshare 没有城市段，只能按国家定向；尾段是会话号，**必须纯数字**
	// （2026-10-01 实测字母数字的会话号在 SOCKS5 认证就被拒）。
	got, err = proxyUser(wsProxy, types.Exit{Region: "DE"}, "ab12cd34", 5)
	if err != nil {
		t.Fatalf("webshare: %v", err)
	}
	tail, ok := strings.CutPrefix(got, "wsu-DE-")
	if !ok {
		t.Fatalf("webshare 用户名形状不对: %q", got)
	}
	if tail == "" || strings.TrimLeft(tail, "0123456789") != "" {
		t.Errorf("会话号不是纯数字，认证会被拒: %q", got)
	}
	// 同 sid 必须映到同一个号：铸票和随后那一发 state-echo 靠它共用出口 IP。
	again, _ := proxyUser(wsProxy, types.Exit{Region: "DE"}, "ab12cd34", 5)
	if again != got {
		t.Errorf("同 sid 映出两个会话号: %q / %q", got, again)
	}
	other, _ := proxyUser(wsProxy, types.Exit{Region: "DE"}, "zz99", 5)
	if other == got {
		t.Errorf("不同 sid 映到同一个会话号，换不了出口: %q", other)
	}
	if _, err := proxyUser(wsProxy, types.Exit{}, "sid", 5); err == nil {
		t.Error("webshare 没国家该报错")
	}
}

// B5：观测到的城市是自由文本，裸空格和非 ASCII 进了 SOCKS5 用户名会认证失败，
// 或者库存不足回退到别国 ⇒ 落到别的网关 ⇒ 被误报成「落点漂移」。
func TestProxyUserRejectsFreeTextGeography(t *testing.T) {
	bad := []types.Exit{
		{Region: "BR", City: "Frankfurt am Main"}, // 裸空格
		{Region: "BR", City: "São Paulo"},         // 非 ASCII
		{Region: "BR", City: "a:b"},               // 分隔符
		{Region: "Brazil"},                        // 不是 ISO-2
		{Region: "B"},
		{City: "Milan"}, // 只有城市：novproxy 会把 city 段整段吞掉，等于悄悄取消定向
	}
	for _, e := range bad {
		if got, err := proxyUser(novProxy, e, "sid", 5); err == nil {
			t.Errorf("%+v 该被拒，却拼出了 %q", e, got)
		}
	}
}

// ---------- 出口轮转与熔断 ----------

func TestNewSIDRotatesAndRespectsSticky(t *testing.T) {
	c := &Client{Proxies: []config.Proxy{novProxy, wsProxy,
		{Name: "nov2", Kind: config.KindNovproxy, Host: "h", User: "u2", Pass: "p", Enabled: true},
		{Name: "off", Kind: config.KindNovproxy, Host: "h", User: "u3", Pass: "p", Enabled: false}}}

	// 三家都能粘（webshare 的数字会话号 2026-10-01 实测固定 IP），停用的那条跳过。
	// 这正是要的：一家熔断了还有另外两家顶着，不至于整池停摆。
	seen := map[string]int{}
	for i := 0; i < 6; i++ {
		sid, err := c.NewSID(true)
		if err != nil {
			t.Fatal(err)
		}
		if !c.Sticky(sid) {
			t.Fatalf("要粘性却给了非粘性出口 %q", c.proxyName(sid))
		}
		seen[c.proxyName(sid)]++
	}
	if seen["nov"] != 2 || seen["nov2"] != 2 || seen["ws"] != 2 {
		t.Errorf("该在三条粘性出口之间轮询，得到 %v", seen)
	}
	if seen["off"] != 0 {
		t.Errorf("停用的出口被选中了: %v", seen)
	}
}

func TestNewSIDNoProxyAvailable(t *testing.T) {
	off := wsProxy
	off.Enabled = false
	c := &Client{Proxies: []config.Proxy{off}}
	if _, err := c.NewSID(true); !errors.Is(err, ErrNoProxy) {
		t.Fatalf("出口全停用时该报 ErrNoProxy，得到 %v", err)
	}
	if _, err := (&Client{}).NewSID(false); !errors.Is(err, ErrNoProxy) {
		t.Fatal("一条出口都没有该报 ErrNoProxy")
	}
}

// 同一个 sid 必须永远绑同一条出口：铸票和随后的 state-echo 换了出口就是换了 IP，
// 可能换网关，判据配不上对。别的包自己生成的 sid 也要能用。
func TestSIDStaysOnOneProxy(t *testing.T) {
	c := &Client{Proxies: []config.Proxy{novProxy,
		{Name: "nov2", Kind: config.KindNovproxy, Host: "h", User: "u2", Pass: "p", Enabled: true}}}
	foreign := "abcd1234" // 不是 Client.NewSID 生成的
	first := c.proxyName(foreign)
	if first == "" {
		t.Fatal("外来 sid 该被自动绑一条粘性出口")
	}
	for i := 0; i < 10; i++ {
		_, _ = c.NewSID(true) // 把游标转起来
		if got := c.proxyName(foreign); got != first {
			t.Fatalf("同一个 sid 换了出口：%q → %q", first, got)
		}
	}
}

func TestProxyBreaker(t *testing.T) {
	c := &Client{Proxies: []config.Proxy{novProxy,
		{Name: "nov2", Kind: config.KindNovproxy, Host: "h", User: "u2", Pass: "p", Enabled: true}}}
	for i := 0; i < proxyFailThreshold; i++ {
		c.noteFailure("nov", "acc", errors.New("认证被拒"))
	}
	for i := 0; i < 5; i++ {
		sid, err := c.NewSID(true)
		if err != nil {
			t.Fatal(err)
		}
		if c.proxyName(sid) == "nov" {
			t.Fatal("连续失败的出口该被临时跳过")
		}
	}
	// 一次成功就清账：novproxy 实测约 10% 抖动，不该把它一直关在外面。
	c.noteSuccess("nov")
	hit := false
	for i := 0; i < 4; i++ {
		sid, _ := c.NewSID(true)
		if c.proxyName(sid) == "nov" {
			hit = true
		}
	}
	if !hit {
		t.Error("成功过之后该重新进轮转")
	}
}

// ---------- JWT / sid ----------

func TestParseOailbRejectsGarbage(t *testing.T) {
	bad := []string{
		"",
		"notajwt",
		"a." + base64.RawURLEncoding.EncodeToString([]byte(`{"host":"only.two"}`)) + ".c",
		"a.!!!notbase64!!!.c",
		"a." + base64.RawURLEncoding.EncodeToString([]byte(`not json`)) + ".c",
	}
	for _, tok := range bad {
		if gw, _, err := parseOailb(tok); err == nil {
			t.Errorf("%q 该被拒，却解出 %q", tok, gw)
		}
	}
}

func TestRandSIDShape(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		s := randSID()
		if len(s) != 8 || strings.Trim(s, sidAlphabet) != "" {
			t.Fatalf("sid 形状不对: %q", s)
		}
		seen[s] = true
	}
	if len(seen) < 45 {
		t.Errorf("50 个 sid 只有 %d 个不重复，随机性有问题", len(seen))
	}
}

// ---------- 出站形状 golden ----------

// 身份和 sid 都是随机的，golden 要可比就得把它们钉住。
func pinIdentity(t *testing.T) {
	t.Helper()
	old7, old4 := newUUIDv7, newUUIDv4
	n := 0
	newUUIDv7 = func() string {
		n++
		return fmt.Sprintf("00000000-0000-7000-8000-%012d", n)
	}
	newUUIDv4 = func() string { return "00000000-0000-4000-8000-000000000000" }
	t.Cleanup(func() { newUUIDv7, newUUIDv4 = old7, old4 })
}

func TestWireGolden(t *testing.T) {
	pinIdentity(t)
	for _, fp := range []config.Fingerprint{config.FingerprintSub2api, config.FingerprintMint} {
		t.Run(string(fp), func(t *testing.T) {
			body, header := buildWire(fp, testModel, testCreds, "__cflb=c1; __oailb=o1", "live-ticket")
			var b strings.Builder
			keys := make([]string, 0, len(header))
			for k := range header {
				keys = append(keys, k)
			}
			sort.Strings(keys) // HTTP/2 本来也不保证头序，按名字排好比对
			for _, k := range keys {
				fmt.Fprintf(&b, "%s: %s\n", strings.ToLower(k), header.Get(k))
			}
			b.WriteString("\n")
			b.Write(body)
			b.WriteString("\n")
			compareGolden(t, string(fp)+".golden", b.String())
		})
	}
}

func compareGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	want, err := os.ReadFile(path)
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err != nil {
		t.Fatalf("读 golden 失败（首次生成跑 UPDATE_GOLDEN=1 go test ./internal/mint/）: %v", err)
	}
	if strings.ReplaceAll(string(want), "\r\n", "\n") != got {
		t.Errorf("出站形状和 golden 不一致。\n--- golden %s ---\n%s\n--- 实际 ---\n%s", name, want, got)
	}
}

// golden 只钉字节，这里钉「为什么是这些字节」：身份内部的引用关系。
func TestSub2apiWireIdentityRelations(t *testing.T) {
	pinIdentity(t)
	body, header := buildWire(config.FingerprintSub2api, testModel, testCreds, "", "")
	session := header.Get("session-id")
	switch {
	case session == "":
		t.Fatal("没发 session-id")
	case header.Get("thread-id") != session:
		t.Error("新线程的 session 和 thread 该是同一个（照 sub2api 的探测身份）")
	case header.Get("x-client-request-id") != session:
		t.Error("x-client-request-id 该等于 thread")
	case header.Get("x-codex-window-id") != session+":1":
		t.Errorf("window 该是 <thread>:1，得到 %q", header.Get("x-codex-window-id"))
	case header.Get("version") != wireVersion:
		t.Errorf("version 头该是 %q", wireVersion)
	case !strings.Contains(header.Get("user-agent"), wireVersion):
		t.Error("UA 的版本段必须和 version 头同源，否则上游 404")
	}
	// 头里的 turn-metadata 和体内 client_metadata 的那份必须是同一份。
	meta := header.Get("x-codex-turn-metadata")
	if meta == "" || !strings.Contains(string(body), jsonEscape(meta)) {
		t.Error("体内 client_metadata 该带上同一份 turn-metadata")
	}
	if !strings.Contains(string(body), `"prompt_cache_key":"`+session+`"`) {
		t.Error("prompt_cache_key 该等于 session")
	}
	// 真客户端从不把 installation id 当请求头发，它只藏在体里。
	if header.Get("x-codex-installation-id") != "" {
		t.Error("installation id 不该作为请求头出现")
	}
	// 这两个是 0.156.1 已经不存在的东西，带上就是一条真客户端没有的指纹。
	if header.Get("x-codex-beta-features") != "" {
		t.Error("0.156.1 已删掉 remote_compaction_v2，不该发这个头")
	}
	if header.Get("accept-encoding") != "" {
		t.Error("真客户端一条 accept-encoding 都不发（Go 那边由 DisableCompression 兜住）")
	}
	if strings.Contains(string(body), `"instructions"`) || strings.Contains(string(body), `"tools"`) {
		t.Error("真 tui 不发 instructions / tools")
	}
}

func jsonEscape(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`)
}

// 两套形状都要带上这些：少一个就不是「同一个账号的同一发 ping」。
func TestBothFingerprintsCarryCredentialsAndCookie(t *testing.T) {
	for _, fp := range []config.Fingerprint{config.FingerprintSub2api, config.FingerprintMint} {
		_, header := buildWire(fp, testModel, testCreds, "__cflb=c1", "tk")
		want := map[string]string{
			"authorization":      "Bearer tok-SECRET",
			"chatgpt-account-id": "acc-123",
			"content-type":       "application/json",
			"accept":             "text/event-stream",
			"originator":         "codex-tui",
			"cookie":             "__cflb=c1",
			stateHeader:          "tk",
		}
		for k, v := range want {
			if got := header.Get(k); got != v {
				t.Errorf("[%s] 头 %s = %q，要 %q", fp, k, got, v)
			}
		}
	}
}

func TestFingerprintDefaultsToSub2api(t *testing.T) {
	if (&Client{}).fingerprint() != config.FingerprintSub2api {
		t.Error("没配指纹时该用 sub2api 形状")
	}
	if (&Client{Fingerprint: config.FingerprintMint}).fingerprint() != config.FingerprintMint {
		t.Error("显式配了 mint 就该用 mint 形状")
	}
}

// 整条出站路径（含指纹）过一遍 httptest：验证体和头真的发出去了。
func TestMintSendsSelectedFingerprint(t *testing.T) {
	for _, fp := range []config.Fingerprint{config.FingerprintSub2api, config.FingerprintMint} {
		var got http.Header
		var body string
		c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Clone()
			b, _ := io.ReadAll(r.Body)
			body = string(b)
			w.Header().Add("set-cookie", "__oailb="+fakeOailb("unified-1", time.Now().Add(time.Hour)))
		})
		c.Fingerprint = fp
		if _, err := c.Mint(context.Background(), testCreds, types.Exit{}, "sid1", "", time.Time{}); err != nil {
			t.Fatal(err)
		}
		if got.Get("authorization") != "Bearer tok-SECRET" || got.Get("originator") != "codex-tui" {
			t.Errorf("[%s] 身份头没发出去: %v", fp, got)
		}
		isSub2api := fp == config.FingerprintSub2api
		if hasMeta := got.Get("x-codex-turn-metadata") != ""; hasMeta != isSub2api {
			t.Errorf("[%s] turn-metadata 头该 %v", fp, isSub2api)
		}
		if hasMeta := strings.Contains(body, "client_metadata"); hasMeta != isSub2api {
			t.Errorf("[%s] 体里的 client_metadata 该 %v", fp, isSub2api)
		}
		if fp == config.FingerprintMint && body != mintBody(testModel) {
			t.Errorf("mint 形状的体和 SPEC 第 2 节不一致:\n%s", body)
		}
	}
}

// 手写的 SOCKS5 CONNECT 握手是本包风险最高的一段，用一个假代理把整条
// 「挑出口 → 拼用户名 → 认证 → CONNECT → 发 HTTP」的路走通一次。
func TestSocks5ConnectThroughFakeProxy(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("set-cookie", "__oailb="+fakeOailb("unified-73", time.Now().Add(time.Hour)))
	}))
	defer upstream.Close()

	gotUser := make(chan string, 1)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		user, target, err := fakeSocks5Handshake(conn)
		if err != nil {
			t.Error(err)
			return
		}
		gotUser <- user
		back, err := net.Dial("tcp", target)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = back.Close() }()
		go func() { _, _ = io.Copy(back, conn) }()
		_, _ = io.Copy(conn, back)
	}()

	p := novProxy
	p.Host = ln.Addr().String()
	p.User = "u1"
	c := &Client{
		Proxies:   []config.Proxy{p},
		StickyMin: 5,
		Upstream:  upstream.URL, // http://，Transport 不做 TLS，正好只验 SOCKS5 那段
	}
	sid, err := c.NewSID(true)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Mint(context.Background(), testCreds, types.Exit{Region: "AU"}, sid, "", time.Time{})
	if err != nil {
		t.Fatalf("经 SOCKS5 铸票失败: %v", err)
	}
	if res.Gateway != "unified-73" {
		t.Errorf("落点 %q", res.Gateway)
	}
	if u := <-gotUser; u != "u1-region-AU-sid-"+sid+"-t-5" {
		t.Errorf("代理看到的用户名 %q", u)
	}
}

// 死掉的住宅出口不该把整条供应商熔断掉。
//
// 2026-10-01 现网：住宅池里约一成的粘性会话摸到死节点（实测 2/20），us-east 连败
// 三次就把 novproxy 熔断了 30s，而 webshare 不粘性顶不上 ⇒ 剩下八个区域一个网络
// 请求都没发就全败（failures=9，samples=0）。协议上这两种失败本来分得很清楚：
// SOCKS5 CONNECT 成功了才叫供应商活着。
func TestDeadExitDoesNotTripSupplierBreaker(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	// 握手照回成功，然后立刻挂断：这就是死掉的住宅节点的样子。
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, _, _ = fakeSocks5Handshake(conn)
			_ = conn.Close()
		}
	}()

	p := novProxy
	p.Host = ln.Addr().String()
	c := &Client{Proxies: []config.Proxy{p}, StickyMin: 5, Upstream: "http://example.invalid/"}

	// 打到超过熔断阈值还要多一发：阈值本身是 3。
	for i := 0; i < proxyFailThreshold+1; i++ {
		sid, err := c.NewSID(true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Mint(context.Background(), testCreds, types.Exit{Region: "US"}, sid, "", time.Time{}); err == nil {
			t.Fatalf("第 %d 发居然成功了，这个假代理应该每次都断", i)
		}
	}
	// 供应商必须还在：下一个 sid 还能挑到它。
	sid, err := c.NewSID(true)
	if err != nil {
		t.Fatalf("出口断了 %d 次就把供应商熔断了: %v", proxyFailThreshold+1, err)
	}
	if got := c.proxyName(sid); got != p.Name {
		t.Fatalf("挑到的供应商是 %q，不是 %q —— 被熔断了", got, p.Name)
	}
}

// 反面：代理本身连不上，该熔断就得熔断，否则每次请求都要为一条死供应商等满超时。
func TestUnreachableProxyTripsBreaker(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // 端口立刻没人听，连上去就是 connection refused

	p := novProxy
	p.Host = addr
	c := &Client{Proxies: []config.Proxy{p}, StickyMin: 5, Upstream: "http://example.invalid/"}
	for i := 0; i < proxyFailThreshold; i++ {
		sid, err := c.NewSID(true)
		if err != nil {
			t.Fatalf("第 %d 发还没到阈值就挑不出供应商: %v", i, err)
		}
		if _, err := c.Mint(context.Background(), testCreds, types.Exit{Region: "US"}, sid, "", time.Time{}); err == nil {
			t.Fatalf("第 %d 发居然成功了", i)
		}
	}
	if _, err := c.NewSID(true); !errors.Is(err, ErrNoProxy) {
		t.Fatalf("连不上的代理失败 %d 次还没熔断，拿到 %v", proxyFailThreshold, err)
	}
	// **根本没碰到上游的那几发要把配额退回去**：spend 是发之前占的名额，而每个区域
	// 一小时只有 6 发。不退的话一次代理故障就能把这个区域的预算吃光，而那一发
	// 连 TCP 都没建起来，上游那边什么都没发生。
	if used, _ := c.Rate(testCreds.AccountID); used["us-east"] != 0 {
		t.Fatalf("连不上代理的 %d 发没退配额: used=%v", proxyFailThreshold, used)
	}
}

// 反过来：隧道建起来又断（住宅死节点）**不许退**——那一发可能已经落到上游了，
// 退了就是低报，而这个闸是防封号的安全阀。
func TestDeadExitDoesNotRefundRateWindow(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_, _, _ = fakeSocks5Handshake(conn)
			_ = conn.Close()
		}
	}()

	p := novProxy
	p.Host = ln.Addr().String()
	c := &Client{Proxies: []config.Proxy{p}, StickyMin: 5, Upstream: "http://example.invalid/"}

	sid, err := c.NewSID(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mint(context.Background(), testCreds, types.Exit{Region: "US"}, sid, "", time.Time{}); err == nil {
		t.Fatal("这个假代理该在握手之后断开")
	}
	if used, _ := c.Rate(testCreds.AccountID); used["us-east"] != 1 {
		t.Fatalf("中途断开的那一发被退了配额（会低报，撞上封号阈值）: used=%v", used)
	}
}

// 调用方自己取消的那一发也不退：同理，请求可能已经出站了。
//
// **必须走真实的代理路径**：注入 HTTP 客户端时 do 在第一行就返回了，
// 那三支「谁的账」的判断（调用方取消 / 出口中途断开 / 连不上代理）一支都走不到，
// refund 根本不可达 —— 那样这个用例无论怎么改实现都是绿的。
// 所以照 TestDeadExitDoesNotRefundRateWindow 起一个假 socks 端口：握手照收，
// 之后把连接吊住，让调用方在响应回来之前撤。
func TestCallerCancelDoesNotRefundRateWindow(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_, _, _ = fakeSocks5Handshake(conn)
				<-done // 隧道建好之后什么都不回：这一发就卡在等响应上
			}()
		}
	}()

	p := novProxy
	p.Host = ln.Addr().String()
	c := &Client{Proxies: []config.Proxy{p}, StickyMin: 5, Upstream: "http://example.invalid/"}
	sid, err := c.NewSID(true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	if _, err := c.Mint(ctx, testCreds, types.Exit{Region: "US"}, sid, "", time.Time{}); err == nil {
		t.Fatal("调用方取消了还成功？")
	}
	if used, _ := c.Rate(testCreds.AccountID); used["us-east"] != 1 {
		t.Fatalf("被取消的那一发退了配额（会低报，撞上封号阈值）: used=%v", used)
	}
	// 也不该记成供应商的账：客户端的重试风暴会把一条好供应商熔断掉
	// （2026-10-01 两次全池 502 就毁在这上面）。
	if st := c.ProxyStates(); len(st) != 1 || st[0].Fails != 0 || !st[0].SkipUntil.IsZero() {
		t.Fatalf("调用方取消被记成供应商故障: %+v", st)
	}
}

// fakeSocks5Handshake 收一次 SOCKS5 握手，返回 (用户名, "host:port")。
func fakeSocks5Handshake(conn net.Conn) (string, string, error) {
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	head := make([]byte, 2)
	if _, err := io.ReadFull(conn, head); err != nil {
		return "", "", err
	}
	if head[0] != 0x05 {
		return "", "", fmt.Errorf("版本 0x%02x", head[0])
	}
	methods := make([]byte, head[1])
	if _, err := io.ReadFull(conn, methods); err != nil {
		return "", "", err
	}
	if _, err := conn.Write([]byte{0x05, 0x02}); err != nil { // 要求用户名密码认证
		return "", "", err
	}
	ver := make([]byte, 2)
	if _, err := io.ReadFull(conn, ver); err != nil {
		return "", "", err
	}
	user := make([]byte, ver[1])
	if _, err := io.ReadFull(conn, user); err != nil {
		return "", "", err
	}
	plen := make([]byte, 1)
	if _, err := io.ReadFull(conn, plen); err != nil {
		return "", "", err
	}
	if _, err := io.ReadFull(conn, make([]byte, plen[0])); err != nil {
		return "", "", err
	}
	if _, err := conn.Write([]byte{0x01, 0x00}); err != nil {
		return "", "", err
	}
	req := make([]byte, 4)
	if _, err := io.ReadFull(conn, req); err != nil {
		return "", "", err
	}
	if req[1] != 0x01 || req[3] != 0x03 {
		return "", "", fmt.Errorf("要 CONNECT + 域名（socks5h），得到 cmd=0x%02x atyp=0x%02x", req[1], req[3])
	}
	n := make([]byte, 1)
	if _, err := io.ReadFull(conn, n); err != nil {
		return "", "", err
	}
	host := make([]byte, n[0])
	if _, err := io.ReadFull(conn, host); err != nil {
		return "", "", err
	}
	port := make([]byte, 2)
	if _, err := io.ReadFull(conn, port); err != nil {
		return "", "", err
	}
	// 回复里塞一个 IPv4 绑定地址，顺便验证实现会把它读干净。
	if _, err := conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0}); err != nil {
		return "", "", err
	}
	_ = conn.SetDeadline(time.Time{})
	return string(user), fmt.Sprintf("%s:%d", host, int(port[0])<<8|int(port[1])), nil
}

// 出口状态里不许出现用户名原文和密码：那一页是给人看的，
// 而 novproxy 的用户名里嵌着账号标识，整串贴出来等于泄一半凭据。
func TestProxyStatesMasksCredentials(t *testing.T) {
	const user = "user-SECRET-ACCOUNT-ID"
	const pass = "pass-MUST-NEVER-APPEAR"
	c := &Client{Proxies: []config.Proxy{{
		Name: "novproxy", Kind: config.KindNovproxy, Host: "h:1",
		User: user, Pass: config.Secret(pass), Enabled: true,
	}}}
	states := c.ProxyStates()
	if len(states) != 1 {
		t.Fatalf("出口数 %d", len(states))
	}
	st := states[0]
	all := st.UserMask + st.Host + st.Name + st.Kind
	if strings.Contains(all, user) {
		t.Fatalf("用户名原文漏出来了: %q", st.UserMask)
	}
	if strings.Contains(all, pass) {
		t.Fatal("密码漏进出口状态了")
	}
	if !strings.HasSuffix(st.UserMask, "***") {
		t.Fatalf("打码格式不对: %q", st.UserMask)
	}
}

// 速率闸：满了之后一发都不许出去，而且必须能被 errors.Is 认出来——
// 当成普通失败会让调度器拿一次限流去给每个试过的槽位记退避。
func TestUpstreamRateGate(t *testing.T) {
	var hits int
	c, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Add("set-cookie", "__oailb="+fakeOailb("unified-9", time.Now().Add(time.Hour))+"; Path=/")
		w.WriteHeader(http.StatusOK)
	})
	c.UpstreamPerRegionHour = 3

	cr := types.Creds{AccessToken: "tok", AccountID: "acc"}
	for i := 0; i < 3; i++ {
		if _, err := c.Mint(context.Background(), cr, types.Exit{Region: "US"}, "sid-x", "", time.Time{}); err != nil {
			t.Fatalf("第 %d 发就被拦了: %v", i+1, err)
		}
	}
	_, err := c.Mint(context.Background(), cr, types.Exit{Region: "US"}, "sid-x", "", time.Time{})
	if !errors.Is(err, types.ErrRateLimited) {
		t.Fatalf("第 4 发应该被速率闸拦下，实为 %v", err)
	}
	// VerifyFull 走同一个预算：两条路各算一套就等于把上限翻倍。
	if _, err := c.VerifyFull(context.Background(), cr, types.Exit{Region: "US"}, "sid-x", "p", "tk"); !errors.Is(err, types.ErrRateLimited) {
		t.Fatalf("VerifyFull 应该共用同一个预算，实为 %v", err)
	}
	if hits != 3 {
		t.Fatalf("被拦的那两发还是发出去了：上游收到 %d 发", hits)
	}
	// 闸是**按区域**分桶的：us-east 打满了，另外八个区域各有自己的一整份预算——
	// 而那八个区域才是能发现新落点的地方。
	if _, err := c.Mint(context.Background(), cr, types.Exit{Region: "JP"}, "sid-x", "", time.Time{}); err != nil {
		t.Fatalf("east-asia 被 us-east 的配额拖累了: %v", err)
	}
	if used, limit := c.Rate("acc"); used["us-east"] != 3 || used["east-asia"] != 1 || limit != 3 {
		t.Fatalf("读数不对: used=%v limit=%d", used, limit)
	}
	// 没在表上的出口（手改 state.json 才会有）**挤同一个空桶**，不凭空多出一份配额。
	// 要真的往那个桶里花钱才测得到：只用表上的出口时 used[""] 恒为 0，断言恒真。
	wild := []types.Exit{{Region: "NG"}, {Region: "US", City: "Dallas"}}
	for i, e := range wild {
		if _, err := c.Mint(context.Background(), cr, e, "sid-w", "", time.Time{}); err != nil {
			t.Fatalf("野出口第 %d 发被拦了: %v", i+1, err)
		}
	}
	if used, _ := c.Rate("acc"); used[""] != len(wild) {
		t.Fatalf("两个野出口没挤进同一个空桶（各自多出一份配额）: %v", used)
	}
	// 挤满了就一起被拦：limit=3，上面已经花了 2 发。
	if _, err := c.Mint(context.Background(), cr, types.Exit{Region: "ZZ"}, "sid-w", "", time.Time{}); err != nil {
		t.Fatalf("空桶第 3 发就被拦了: %v", err)
	}
	if _, err := c.Mint(context.Background(), cr, types.Exit{Region: "QQ"}, "sid-w", "", time.Time{}); !errors.Is(err, types.ErrRateLimited) {
		t.Fatalf("空桶打满了还放行，实为 %v", err)
	}
	// 闸是按上游账号算的：另一个号自己有一整份预算，不该被这个号打满而挨饿。
	other := types.Creds{AccessToken: "tok", AccountID: "acc-2"}
	if _, err := c.Mint(context.Background(), other, types.Exit{Region: "US"}, "sid-y", "", time.Time{}); err != nil {
		t.Fatalf("另一个账号被别人的配额拖累了: %v", err)
	}
}
