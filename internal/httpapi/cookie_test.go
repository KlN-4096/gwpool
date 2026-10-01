package httpapi

// 这一份盯的是 2026-10-02 那轮协议改动：四个新请求参数、cookie_version、
// 十一个错误码的闭集、以及 POST /release。
//
// **参数名 / JSON 字段名 / 错误码是和消费端冻结的**，所以这里的字面量是故意写死的：
// 它们一变就是改协议，必须让这几个用例红。

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"gwpool/internal/config"
	"gwpool/internal/mint"
	"gwpool/internal/sched"
	"gwpool/internal/types"
)

// failBody 解失败响应那个信封。
type failBody struct {
	Error struct {
		Code              string `json:"code"`
		Message           string `json:"message"`
		RetryAfterSeconds int    `json:"retry_after_seconds"`
	} `json:"error"`
}

func decodeFail(t *testing.T, w *httptest.ResponseRecorder) failBody {
	t.Helper()
	var got failBody
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("失败响应解不开: %v（body=%s）", err, w.Body.String())
	}
	return got
}

// 成功响应里要多一个 cookie_version：调用方拿它回头指名这张票
// （exclude_versions / release）。现有字段一个都不许动。
func TestCookieResponseCarriesVersion(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModePool)
	w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), srv.ConsumerKeyFor(st.accounts[0])))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	var got struct {
		Gateway       string `json:"gateway"`
		Cookie        string `json:"cookie"`
		ValidForS     int    `json:"valid_for_s"`
		VerifiedFull  bool   `json:"verified_full"`
		TTLIsAdvisory bool   `json:"ttl_is_advisory"`
		CookieVersion string `json:"cookie_version"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := types.CookieVersion(liveCookie)
	if got.CookieVersion != want {
		t.Fatalf("cookie_version=%q，应为 %q", got.CookieVersion, want)
	}
	if len(got.CookieVersion) != types.CookieVersionLen {
		t.Fatalf("cookie_version 长度 %d，约定是 %d", len(got.CookieVersion), types.CookieVersionLen)
	}
	// 版本号是**哈希**，不许把 cookie 本体漏进去（SPEC 红线 1）。
	// 只查真·标记：`cf` 本身就是合法的十六进制双字符，查它对任何给定常量要么恒绿
	// 要么恒红，和想证明的事无关。
	if strings.Contains(got.CookieVersion, "OAILB") {
		t.Fatalf("cookie_version 里带了 cookie 本体: %q", got.CookieVersion)
	}
	// 老字段照旧。
	if got.Gateway != "unified-142" || got.ValidForS != 150 || !got.VerifiedFull || !got.TTLIsAdvisory {
		t.Fatalf("老字段被动过了: %+v", got)
	}
}

// 错误码是闭集，每个都要能从真实的失败路径上走出来，而且带 retry 的要同时发
// Retry-After 头（不认识这份协议的中间件也能照着退避）。
func TestCookieFailureCodes(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantCode  string
		wantRetry int
		// noGateways：池子是空的（冷启动）。
		noGateways bool
	}{
		{name: "all_cooling", wantCode: codeAllCooling, wantRetry: 7200,
			err: &sched.CoolingError{Gateways: 3, Cooldown: 4 * time.Hour, Wait: 2 * time.Hour}},
		{name: "rate_limited", wantCode: codeRateLimited, wantRetry: 90,
			err: fmt.Errorf("%w（最后一个原因：%w）", sched.ErrNoFullSlot,
				&mint.RateLimited{Region: "europe", Limit: 6, Wait: 90 * time.Second})},
		{name: "degraded", wantCode: codeDegraded, wantRetry: int(types.FullWindow / time.Second),
			err: fmt.Errorf("%w: %w", sched.ErrNoFullSlot, sched.ErrDegraded)},
		{name: "upstream_rejected", wantCode: codeUpstreamRejected,
			err: fmt.Errorf("%w: %w", sched.ErrNoFullSlot, sched.ErrUpstreamRejected)},
		{name: "mint_failed", wantCode: codeMintFailed,
			err: fmt.Errorf("%w: %w", sched.ErrNoFullSlot, sched.ErrMintFailed)},
		{name: "no_live_pair", wantCode: codeNoLivePair, err: sched.ErrNoFullSlot},
		{name: "no_gateway", wantCode: codeNoGateway, err: sched.ErrNoFullSlot, noGateways: true},
		// 出口全熔断：重试建议来自熔断剩余，而不是凭空编一个数。
		{name: "no_exit", wantCode: codeNoExit, wantRetry: 45,
			err: fmt.Errorf("%w（要不到粘性出口: %w）", sched.ErrNoFullSlot, types.ErrNoExit)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv, h, p, st, _ := rig(t, types.ModePool)
			srv.opt.ProxyStates = func() []mint.ProxyState {
				return []mint.ProxyState{{Name: "novproxy", Enabled: true,
					SkipUntil: time.Now().Add(45 * time.Second)}}
			}
			if c.noGateways {
				st.gws = map[string]types.Gateway{}
			}
			p.err = c.err
			w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), srv.ConsumerKeyFor(st.accounts[0])))
			// 状态码沿用今天的：客户端（sub2api）只认 503 → ErrNoSlot。
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("状态 %d，应为 503", w.Code)
			}
			got := decodeFail(t, w)
			if got.Error.Code != c.wantCode {
				t.Fatalf("code=%q，应为 %q", got.Error.Code, c.wantCode)
			}
			if got.Error.Message == "" {
				t.Fatal("没有给人看的那句话")
			}
			if c.wantRetry == 0 {
				if got.Error.RetryAfterSeconds != 0 || w.Header().Get("Retry-After") != "" {
					t.Fatalf("不该给重试建议: %+v / %q", got.Error, w.Header().Get("Retry-After"))
				}
				return
			}
			// 向上取整，允许差一秒（熔断剩余是按真实时钟算的）。
			if d := got.Error.RetryAfterSeconds - c.wantRetry; d < 0 || d > 1 {
				t.Fatalf("retry_after_seconds=%d，应约为 %d", got.Error.RetryAfterSeconds, c.wantRetry)
			}
			if w.Header().Get("Retry-After") != strconv.Itoa(got.Error.RetryAfterSeconds) {
				t.Fatalf("Retry-After 头和响应体对不上: %q vs %d",
					w.Header().Get("Retry-After"), got.Error.RetryAfterSeconds)
			}
		})
	}
}

// A-10：「你的 key 不对」要有自己的码。
//
// 没码的 401 会让调用方掉进通用路径、不退避，于是「每发推理请求敲一次池子」
// 无限敲下去——而这种错要人去改配置，敲得再勤也不会好。
// 三个机器端点一把钥匙，所以三条路都要给同一个码。
func TestConsumerRejectedCode(t *testing.T) {
	for _, c := range []struct {
		name string
		req  func() *http.Request
	}{
		{"cookie", func() *http.Request { return httptest.NewRequest("GET", "/cookie", nil) }},
		{"release", func() *http.Request { return releaseReq(types.CookieVersion(liveCookie)) }},
		{"gateways", func() *http.Request { return httptest.NewRequest("GET", "/gateways", nil) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, h, _, _, _ := rig(t, types.ModePool) // 公开池默认关着
			w := do(h, authed(c.req(), "这不是任何人的 key"))
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("状态 %d，应为 401", w.Code)
			}
			got := decodeFail(t, w)
			if got.Error.Code != codeConsumerRejected {
				t.Fatalf("code=%q，应为 %q", got.Error.Code, codeConsumerRejected)
			}
			// 要给退避建议，否则调用方会每发请求都照敲一次。
			if got.Error.RetryAfterSeconds != 300 || w.Header().Get("Retry-After") != "300" {
				t.Fatalf("没给退避建议: %+v / %q", got.Error, w.Header().Get("Retry-After"))
			}
			// 「上游否决池子的账号」是另一回事，别混。
			if strings.Contains(w.Body.String(), codeUpstreamRejected) {
				t.Fatal("把消费端认证失败说成了上游否决")
			}
		})
	}
}

// A-11：换票时「候选非空但没有活票」要归成可等待的那一格，wait 才真的能用。
func TestForceRotationIsWaitable(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	p.err = fmt.Errorf("%w（%w，最后一个原因：%w）",
		sched.ErrNoFullSlot, sched.ErrNoLivePair, sched.ErrMintFailed)
	ver := types.CookieVersion(liveCookie)
	polled := false
	p.hasCand = func(_ int64, req sched.Request) bool {
		polled = true
		if !req.ExcludeVersions[ver] || !req.Force {
			t.Errorf("轮询丢了请求参数: %+v", req)
		}
		return false
	}
	w := do(h, authed(httptest.NewRequest("GET",
		"/cookie?force=1&exclude_versions="+ver+"&wait=1", nil), srv.ConsumerKeyFor(st.accounts[0])))
	if w.Code != 503 {
		t.Fatalf("状态 %d", w.Code)
	}
	// no_live_pair 压过 mint_failed：契约里前者的定义就是「没有活票且现铸没成」。
	if got := decodeFail(t, w); got.Error.Code != codeNoLivePair {
		t.Fatalf("code=%q，应为 %q（归成别的码 wait 就在每次轮换上失效了）",
			got.Error.Code, codeNoLivePair)
	}
	// 等不到候选 ⇒ 不重跑，但确实等过；而且轮询用的是**同一份**请求参数
	// （池子里那张票正是被排除的那一张，判据必须看得见这件事）。
	if p.calls != 1 {
		t.Fatalf("取票次数 %d", p.calls)
	}
	if !polled {
		t.Fatal("可等待的码上根本没去轮询")
	}
}

// A-13：/gateways 顶层要报出两个池子侧门槛，否则消费端只能瞎猜 min_remaining。
func TestGatewaysReportsThresholds(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModePool)
	w := do(h, authed(httptest.NewRequest("GET", "/gateways", nil), srv.ConsumerKeyFor(st.accounts[0])))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	var got struct {
		Account       string `json:"account"`
		Live          int    `json:"live"`
		Target        int    `json:"target"`
		DeliverTTLS   int    `json:"deliver_ttl_s"`
		DeliverFloorS int    `json:"deliver_floor_s"`
		Gateways      []struct {
			Name      string `json:"name"`
			PairReady bool   `json:"pair_ready"`
		} `json:"gateways"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.DeliverTTLS != int(types.DeliverTTL/time.Second) ||
		got.DeliverFloorS != int(types.DeliverFloor/time.Second) {
		t.Fatalf("门槛报错了: %+v", got)
	}
	// 现有形状一个字不动。
	if got.Account != "acct-upstream" || len(got.Gateways) != 1 || !got.Gateways[0].PairReady {
		t.Fatalf("现有字段被动过了: %+v", got)
	}
}

// 匿名那条路：开关开着但没到门槛 ⇒ public_closed，并给一个短的重试建议
// （这条路一发上游都没花，让它来得勤一点不花钱）。
func TestPublicClosedCode(t *testing.T) {
	_, h, p, _, _ := rig(t, types.ModePool)
	p.public = sched.PublicState{Enabled: true, Open: false, Free: 1, Threshold: 6}
	w := do(h, httptest.NewRequest("GET", "/cookie?account=acct-outsider", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("状态 %d", w.Code)
	}
	got := decodeFail(t, w)
	if got.Error.Code != codePublicClosed || got.Error.RetryAfterSeconds != 60 {
		t.Fatalf("%+v", got.Error)
	}
}

// 「你的请求错了」有自己的码：400 和 404 合成一个 bad_request，**不给 retry_after**
// （重试不会变好）。闭集留着洞就意味着调用方的 default 分支要同时兜「我配错了」
// 和「池子暂时没货」两件语义相反的事。
func TestBadRequestCode(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModePool)
	key := srv.ConsumerKeyFor(st.accounts[0])
	tooMany := make([]string, maxExcludeItems+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("unified-%d", i)
	}
	cases := []struct {
		name   string
		req    *http.Request
		key    string
		status int
	}{
		{"网关名不合法", httptest.NewRequest("GET", "/cookie?gateway=../etc/passwd", nil), key, 400},
		{"exclude 超项", httptest.NewRequest("GET", "/cookie?exclude="+strings.Join(tooMany, ","), nil), key, 400},
		{"版本名单超项", httptest.NewRequest("GET", "/cookie?exclude_versions="+strings.Join(tooMany, ","), nil), key, 400},
		{"min_remaining 不是数", httptest.NewRequest("GET", "/cookie?min_remaining=abc", nil), key, 400},
		{"wait 不是数", httptest.NewRequest("GET", "/cookie?wait=abc", nil), key, 400},
		// 管理员那条路：不说替谁取就等于让池子猜一个账号。
		{"管理员没带 account", httptest.NewRequest("GET", "/cookie", nil), "akey", 400},
		{"没有这个账号", httptest.NewRequest("GET", "/cookie?account=不存在的号", nil), "akey", 404},
		{"release 版本号畸形", releaseReq("zz"), key, 400},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := do(h, authed(c.req, c.key))
			if w.Code != c.status {
				t.Fatalf("状态 %d，应为 %d: %s", w.Code, c.status, w.Body.String())
			}
			got := decodeFail(t, w)
			if got.Error.Code != codeBadRequest || got.Error.Message == "" {
				t.Fatalf("%+v", got.Error)
			}
			if got.Error.RetryAfterSeconds != 0 || w.Header().Get("Retry-After") != "" {
				t.Fatalf("请求错了还给退避建议: %+v / %q", got.Error, w.Header().Get("Retry-After"))
			}
		})
	}
	// 上限的另一侧：正好 64 项要收下。先数逗号再切（省掉一次几十万元素的分配）
	// 很容易把边界挪一位。
	ok := strings.Join(tooMany[:maxExcludeItems], ",")
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie?exclude="+ok, nil), key)); w.Code != 200 {
		t.Fatalf("正好 %d 项被拒了: %d %s", maxExcludeItems, w.Code, w.Body.String())
	}
}

// 四个旋钮必须**原样传到调度**：钳位在边界做完，调度那边直接信。
func TestCookieRequestParamsReachThePool(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	key := srv.ConsumerKeyFor(st.accounts[0])
	ver := types.CookieVersion(liveCookie)
	q := "?gateway=unified-142&force=1&exclude=unified-1,unified-2,,bogus" +
		"&exclude_versions=" + strings.ToUpper(ver) + "&min_remaining=5"
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie"+q, nil), key)); w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if len(p.reqs) != 1 {
		t.Fatalf("取票次数 %d", len(p.reqs))
	}
	req := p.reqs[0]
	if req.Gateway != "unified-142" || !req.Force {
		t.Fatalf("老参数丢了: %+v", req)
	}
	if len(req.Exclude) != 3 || !req.Exclude["unified-1"] || !req.Exclude["bogus"] {
		t.Fatalf("exclude 没传对: %+v", req.Exclude)
	}
	// 大小写不敏感：十六进制本来就两种写法都对，静默不匹配是最难查的那种错。
	if !req.ExcludeVersions[ver] {
		t.Fatalf("exclude_versions 没传对: %+v", req.ExcludeVersions)
	}
	// 5 秒被钳到下限 30 秒：比这还短的票发出去没有意义。
	if req.MinRemaining != 30*time.Second {
		t.Fatalf("min_remaining 没钳到下限: %v", req.MinRemaining)
	}

	// 上限同样钳住，而缺省是 0（= 用服务端的 DeliverFloor）。
	p.reqs = nil
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie?min_remaining=99999", nil), key)); w.Code != 200 {
		t.Fatalf("状态 %d", w.Code)
	}
	if p.reqs[0].MinRemaining != minRemainingCap {
		t.Fatalf("min_remaining 没钳到上限: %v", p.reqs[0].MinRemaining)
	}
	p.reqs = nil
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), key)); w.Code != 200 {
		t.Fatalf("状态 %d", w.Code)
	}
	if p.reqs[0].MinRemaining != 0 || p.reqs[0].Exclude != nil || p.reqs[0].ExcludeVersions != nil {
		t.Fatalf("缺省值不干净: %+v", p.reqs[0])
	}
}

// wait 只在**等得起**的失败上等，而且等到票了才重跑：重跑一次可能真打一发上游，
// 而槽位是这个池子最紧的资源。
//
// 判据只有一份，在调度那边（sched.HasCandidate）：它才看得见「这个账号在这个网关上
// 还在冷却」。这一层照它的答案办，自己绝不照着 pairs 表再算一份。
func TestWaitOnlyRetriesWhenATicketShowsUp(t *testing.T) {
	t.Run("等到票就重跑一次", func(t *testing.T) {
		srv, h, p, st, _ := rig(t, types.ModePool)
		p.err = sched.ErrNoFullSlot // 分类成 no_live_pair：可等待
		var asked []int64
		p.hasCand = func(id int64, _ sched.Request) bool { asked = append(asked, id); return true }
		w := do(h, authed(httptest.NewRequest("GET", "/cookie?wait=5", nil),
			srv.ConsumerKeyFor(st.accounts[0])))
		if w.Code != 503 {
			t.Fatalf("状态 %d", w.Code)
		}
		if p.calls != 2 {
			t.Fatalf("取票次数 %d，应为 2（原始一次 + 等到票之后重跑一次）", p.calls)
		}
		// 问的必须是**真正在要票的那一行**，否则判据又按别人的冷却算了。
		if len(asked) == 0 || asked[0] != st.accounts[0].ID {
			t.Fatalf("轮询问的是 %v，应为账号 %d", asked, st.accounts[0].ID)
		}
	})

	t.Run("池子里有票但这个账号拿不到就不重跑", func(t *testing.T) {
		srv, h, p, st, _ := rig(t, types.ModePool)
		p.err = sched.ErrNoFullSlot
		// 池子里那张票还活着（rig 自带），但调度说这个账号拿不到它——
		// 「刚交付过的网关在冷却里、pair 还活着」是这个池子最常见的稳态。
		p.hasCand = func(int64, sched.Request) bool { return false }
		if len(st.pairs) == 0 {
			t.Fatal("这个用例的前提就是池子里有票")
		}
		w := do(h, authed(httptest.NewRequest("GET", "/cookie?wait=1", nil),
			srv.ConsumerKeyFor(st.accounts[0])))
		if w.Code != 503 {
			t.Fatalf("状态 %d", w.Code)
		}
		if p.calls != 1 {
			t.Fatalf("取票次数 %d：照着 pairs 表判就会每次等待都白铸一发", p.calls)
		}
	})

	t.Run("等不到就不重跑", func(t *testing.T) {
		srv, h, p, st, _ := rig(t, types.ModePool)
		p.err = sched.ErrNoFullSlot
		st.pairs = map[string]types.Pair{} // 一张活票都没有
		start := time.Now()
		w := do(h, authed(httptest.NewRequest("GET", "/cookie?wait=1", nil),
			srv.ConsumerKeyFor(st.accounts[0])))
		if w.Code != 503 {
			t.Fatalf("状态 %d", w.Code)
		}
		if p.calls != 1 {
			t.Fatalf("取票次数 %d：等不到票还重跑，白烧一个槽位", p.calls)
		}
		if took := time.Since(start); took < waitPoll {
			t.Fatalf("根本没等（%v）", took)
		}
	})

	t.Run("冷却不等", func(t *testing.T) {
		srv, h, p, st, _ := rig(t, types.ModePool)
		p.err = &sched.CoolingError{Gateways: 1, Cooldown: 4 * time.Hour, Wait: 3 * time.Hour}
		start := time.Now()
		// wait 给到上限：冷却是几小时量级，等它等于把连接耗光。
		w := do(h, authed(httptest.NewRequest("GET", "/cookie?wait=30", nil),
			srv.ConsumerKeyFor(st.accounts[0])))
		if w.Code != 503 || decodeFail(t, w).Error.Code != codeAllCooling {
			t.Fatalf("状态 %d body=%s", w.Code, w.Body.String())
		}
		if p.calls != 1 {
			t.Fatalf("取票次数 %d", p.calls)
		}
		if took := time.Since(start); took > waitPoll {
			t.Fatalf("在冷却上等了 %v", took)
		}
	})
}

// wait 的钳位也在边界做完：超过上限的值不该把一次请求挂上几分钟。
//
// 直接测解析函数，不靠墙上时钟：按行为测就得真等 30 秒，而这只是一个 min/max。
func TestWaitIsClamped(t *testing.T) {
	for _, c := range []struct {
		q    string
		want time.Duration
	}{
		{"", 0},
		{"?wait=0", 0},
		{"?wait=-5", 0},
		{"?wait=3", 3 * time.Second},
		{"?wait=999", maxWait},
	} {
		_, got, err := parseCookieRequest(httptest.NewRequest("GET", "/cookie"+c.q, nil))
		if err != nil {
			t.Fatalf("%q: %v", c.q, err)
		}
		if got != c.want {
			t.Fatalf("%q → wait=%v，应为 %v", c.q, got, c.want)
		}
	}
}

// ---------- POST /release ----------

func releaseReq(version string) *http.Request {
	return httptest.NewRequest("POST", "/release",
		strings.NewReader(`{"cookie_version":"`+version+`"}`))
}

// 归还成功回 204，并把「还了几个槽位」记进那张卡片（运营要看的读数）。
func TestReleaseRestoresAndIsLogged(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	p.relN = 1
	ver := types.CookieVersion(liveCookie)
	w := do(h, authed(releaseReq(strings.ToUpper(ver)), srv.ConsumerKeyFor(st.accounts[0])))
	if w.Code != http.StatusNoContent {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	// 认的是**上传者那一行**，和 /cookie 逐条对齐。
	if len(p.released) != 1 || p.released[0] != "1|"+ver {
		t.Fatalf("归还记到别人头上了: %+v", p.released)
	}
	mine := do(h, httptest.NewRequest("GET", "/a/vt-1", nil)).Body.String()
	if !strings.Contains(mine, "归还了 1 个槽位") {
		t.Fatal("页面上看不见这次归还")
	}
}

// 太晚 / 对不上 / 已经还过了都是 409，而且**一个字段都不改**（失败即安全）。
func TestReleaseConflicts(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	p.relErr = sched.ErrNoRelease
	w := do(h, authed(releaseReq(types.CookieVersion(liveCookie)), srv.ConsumerKeyFor(st.accounts[0])))
	if w.Code != http.StatusConflict {
		t.Fatalf("状态 %d", w.Code)
	}
	if got := decodeFail(t, w); got.Error.Message == "" {
		t.Fatal("409 也要说句人话")
	}
}

// F4：**占位行（`anon:` 命名空间）的交付一律不许还**，真实行照常。
//
// 归还的方向是**少记**一次接触：被还原成「从没碰过」的那一行会排到挑选队列最前，
// 于是它立刻拿回自己刚烧过的落点——降智的票，而且无从察觉。而占位行的命名空间谁都能
// 填、没有任何归属证据（pair 不互斥，自己取一张同网关的票就知道 cookie_version）。
// 前缀隔离只对取票够用：那边是**多**记一次，方向保守。
//
// 判据是**解出来的那一行**而不是「带没带 account 参数」：消费端每次取票都带自己的
// 上游 account_id，按参数判会把它的正常归还全拒掉。四条调用方各一侧。
func TestReleaseRefusesPlaceholderRows(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	// 占位行**真的存在**：这正是「池子里没那一行」挡不住的那一格。
	st.accounts = append(st.accounts, types.Account{ID: 7, Enabled: true,
		Creds: types.Creds{AccountID: types.AnonPrefix + "acct-friend"}})
	key := srv.ConsumerKeyFor(st.accounts[0])
	ver := types.CookieVersion(liveCookie)
	p.relN = 1

	withAccount := func(ref string) *http.Request {
		r := releaseReq(ver)
		r.URL.RawQuery = "account=" + ref
		return r
	}
	for _, c := range []struct {
		name     string
		req      *http.Request
		key      string
		status   int
		wantRows []string // 期望 Release 收到的 (行|版本)
	}{
		// 消费端的常态：带着**自己的**上游 account_id 来，解到的是它自己那一行。
		{"自己的 account_id", withAccount("acct-upstream"), key, 204, []string{"1|" + ver}},
		{"不带 account", releaseReq(ver), key, 204, []string{"1|" + ver}},
		// 别人的 ref ⇒ 占位行 ⇒ 拒，而且一个字段都不许动（Release 根本不该被调）。
		{"别人的 ref", withAccount("acct-friend"), key, 409, nil},
		// 管理员带 ref 是管理动作，解到的是那个托管行本身。
		{"管理员带 ref", withAccount("acct-b"), "akey", 204, []string{"2|" + ver}},
	} {
		t.Run(c.name, func(t *testing.T) {
			p.released = nil
			w := do(h, authed(c.req, c.key))
			// 先查账本再查状态码：闸漏了的时候要一眼看见是**哪一行**被改了。
			if len(p.released) != len(c.wantRows) {
				t.Fatalf("Release 收到 %+v，应为 %+v", p.released, c.wantRows)
			}
			if w.Code != c.status {
				t.Fatalf("状态 %d，应为 %d: %s", w.Code, c.status, w.Body.String())
			}
			for i, want := range c.wantRows {
				if p.released[i] != want {
					t.Fatalf("归还记到了 %q，应为 %q", p.released[i], want)
				}
			}
		})
	}
}

// 版本号是外部输入：定长十六进制，别的一律 400。
func TestReleaseValidatesVersion(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	key := srv.ConsumerKeyFor(st.accounts[0])
	for _, bad := range []string{"", "zz", "../../etc", strings.Repeat("a", 13), "0123456789ab__"} {
		if w := do(h, authed(releaseReq(bad), key)); w.Code != http.StatusBadRequest {
			t.Fatalf("version=%q → 状态 %d，应为 400", bad, w.Code)
		}
	}
	if len(p.released) != 0 {
		t.Fatalf("畸形请求也调了 Release: %+v", p.released)
	}
	// 请求体不是 JSON 也一样。
	r := httptest.NewRequest("POST", "/release", strings.NewReader("not json"))
	if w := do(h, authed(r, key)); w.Code != http.StatusBadRequest {
		t.Fatalf("状态 %d", w.Code)
	}
}

// 鉴权和 /cookie 完全一致：没 key 且不对外开放 ⇒ 401，一个槽位都不许还。
func TestReleaseNeedsSameAuthAsCookie(t *testing.T) {
	_, h, p, _, _ := rig(t, types.ModePool)
	if w := do(h, releaseReq(types.CookieVersion(liveCookie))); w.Code != http.StatusUnauthorized {
		t.Fatalf("状态 %d，应为 401", w.Code)
	}
	if len(p.released) != 0 {
		t.Fatalf("没鉴权就还了: %+v", p.released)
	}
	// 管理员必须显式说替谁还（槽位是 (账号 × 网关)，猜一个就等于还别人的）。
	if w := do(h, authed(releaseReq(types.CookieVersion(liveCookie)), "akey")); w.Code != http.StatusBadRequest {
		t.Fatalf("管理员没带 account 却受理了: %d", w.Code)
	}
	// F7：鉴权排在**读请求体之前**。顺序反过来的话畸形体的未鉴权请求拿到的是 400，
	// 等于白送扫描器一个「版本号格式对不对」的区分信号，还让它免费使唤一次 JSON 解析。
	if w := do(h, httptest.NewRequest("POST", "/release", strings.NewReader("not json"))); w.Code != http.StatusUnauthorized {
		t.Fatalf("畸形体 + 没鉴权 → %d，应为 401", w.Code)
	}
	if len(p.released) != 0 {
		t.Fatalf("没鉴权就还了: %+v", p.released)
	}
}

// F9：no_gateway 上**不等**。
//
// 那是结构性空等：这个码的定义就是「网关表是空的」，而候选判据要求命中的票属于表里
// 的某个网关 ⇒ 恒为假、必然等满 30 秒；而能把表填上的发现扫描恰好要等这次失败完
// 才入队（单账号模式连保温都不跑）。立刻把码交回调用方更有用。
func TestNoGatewayIsNotWaitable(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	st.gws = map[string]types.Gateway{}
	p.err = sched.ErrNoFullSlot
	p.hasCand = func(int64, sched.Request) bool {
		t.Error("在 no_gateway 上还去轮询了：网关表空着，判据不可能变真")
		return false
	}
	start := time.Now()
	w := do(h, authed(httptest.NewRequest("GET", "/cookie?wait=30", nil),
		srv.ConsumerKeyFor(st.accounts[0])))
	if w.Code != 503 || decodeFail(t, w).Error.Code != codeNoGateway {
		t.Fatalf("状态 %d body=%s", w.Code, w.Body.String())
	}
	if p.calls != 1 {
		t.Fatalf("取票次数 %d", p.calls)
	}
	if took := time.Since(start); took > waitPoll {
		t.Fatalf("空等了 %v", took)
	}
}

// F2：「失败记在谁头上」和「谁是消费者」是两件事。
//
// 跨账号取票时消费者是占位行，而上传者在这里恰恰是最合适的铸票者（烧它的窗口不影响
// 消费者对新落点的首次接触）。把上传者当成消费者排掉的话，单上传者的池子里这种失败
// 永远挑不出铸票者 ⇒ 发现扫描一次也不会跑，而那是这种失败唯一的出路。
func TestCrossAccountFailureDoesNotExcludeUploader(t *testing.T) {
	srv, h, p, st, sc := rig(t, types.ModePool)
	p.err = sched.ErrNoFullSlot
	key := srv.ConsumerKeyFor(st.accounts[0])
	uploader := st.accounts[0].ID

	if w := do(h, authed(httptest.NewRequest("GET", "/cookie?account=acct-friend", nil), key)); w.Code != 503 {
		t.Fatalf("状态 %d", w.Code)
	}
	if len(sc.discovery) != 1 || sc.discovery[0] == uploader {
		t.Fatalf("排除的是 %v，上传者（%d）在这里是铸票者，不是消费者", sc.discovery, uploader)
	}
	// 记账照旧归上传者：那是他自己页面上该看见的那一行。
	if mine := do(h, httptest.NewRequest("GET", "/a/vt-1", nil)).Body.String(); !strings.Contains(mine, codeNoLivePair) {
		t.Fatal("上传者页面上看不见这次失败")
	}
	// 另一侧：自己取自己的票时，要排除的就是他本人（派他去找落点等于白跑）。
	sc.discovery = nil
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), key)); w.Code != 503 {
		t.Fatalf("状态 %d", w.Code)
	}
	if len(sc.discovery) != 1 || sc.discovery[0] != uploader {
		t.Fatalf("自己取票时该排除自己，实际 %v", sc.discovery)
	}
}

// F6：这几个码也要落进「取票失败记录」——那张卡片是运营唯一不用 SSH 的读数，
// 而 consumer_rejected 正是「谁的 key 配错了在狂敲」唯一的线索。
//
// 但灌环面要堵住：**未鉴权**的请求不许往那 200 条里写，否则谁都能把真实诊断挤出去。
func TestCodesReachTheFailLog(t *testing.T) {
	_, h, p, _, _ := rig(t, types.ModePool)
	adminPage := func() string {
		return do(h, authed(httptest.NewRequest("GET", "/admin/", nil), "akey")).Body.String()
	}

	// consumer_rejected：只进日志。公开池默认关着 ⇒ 认不出的 key = 401。
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), "谁的 key 都不是")); w.Code != 401 {
		t.Fatalf("状态 %d", w.Code)
	}
	if strings.Contains(adminPage(), codeConsumerRejected) {
		t.Fatal("401 进了环：未鉴权的请求能把真实诊断挤出去")
	}

	// bad_request：鉴权通过之后的那些才进环（管理员没说替谁取）。
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), "akey")); w.Code != 400 {
		t.Fatalf("状态 %d", w.Code)
	}
	// public_closed：匿名撞上关着的公开池。
	p.public = sched.PublicState{Enabled: true, Open: false, Free: 1, Threshold: 6}
	if w := do(h, httptest.NewRequest("GET", "/cookie?account=acct-outsider", nil)); w.Code != 503 {
		t.Fatalf("状态 %d", w.Code)
	}
	page := adminPage()
	for _, want := range []string{codeBadRequest, codePublicClosed} {
		if !strings.Contains(page, want) {
			t.Fatalf("管理页上没有 %s", want)
		}
	}
}

// B-1：跨账号取票（一把合法 key 替别的上游账号取）走的是「池子没有那个号的凭据」
// 这条路，但调用方**鉴权过了** ⇒ 不该吃 public_when_surplus 这道闸。
//
// 默认公开池是关的、门槛缺省又是 96 个网关（冷却 ÷ 单张可用时长），不区分的话这条
// 文档明写支持的路径每一发都是 503 public_closed——而那个码说的是「**匿名**此刻不开放」。
func TestCrossAccountIsNotGatedByPublicSwitch(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	key := srv.ConsumerKeyFor(st.accounts[0])
	if p.public.Open {
		t.Fatal("前提是公开池关着（默认就是关的）")
	}

	w := do(h, authed(httptest.NewRequest("GET", "/cookie?account=acct-friend", nil), key))
	if w.Code != 200 {
		t.Fatalf("状态 %d（公开池开关把鉴权过的跨账号调用挡了）: %s", w.Code, w.Body.String())
	}
	if len(p.anon) != 1 || !strings.HasSuffix(p.anon[0], "authed=true") {
		t.Fatalf("没报成「鉴权过的」: %v", p.anon)
	}
	// 另一侧：真匿名照旧被门槛挡住，而且码不变。
	w = do(h, httptest.NewRequest("GET", "/cookie?account=acct-outsider", nil))
	if w.Code != 401 { // 开关没开 ⇒ 没带 key 就是 key 不认
		t.Fatalf("匿名状态 %d", w.Code)
	}
	p.public = sched.PublicState{Enabled: true, Open: false, Free: 1, Threshold: 6}
	w = do(h, httptest.NewRequest("GET", "/cookie?account=acct-outsider", nil))
	if w.Code != 503 || decodeFail(t, w).Error.Code != codePublicClosed {
		t.Fatalf("匿名该被门槛挡住: %d %s", w.Code, w.Body.String())
	}
}

// S-2：「你点的这个号用不了」要有自己的分流，不能掉进 no_live_pair。
//
// 运营方逐个启用账号来验、或者管理员拿 ?account=<已停用的号> 取票时，回「没有可交付
// 的活票，稍后再试」会让他挂满 wait、还白排一次发现扫描，而真实原因是那个号本身。
func TestAccountUnusableIsBadRequest(t *testing.T) {
	srv, h, p, st, sc := rig(t, types.ModePool)
	p.err = fmt.Errorf("%w：账号 1（停用理由：铸票 HTTP 401：token_revoked）", sched.ErrAccountUnusable)
	key := srv.ConsumerKeyFor(st.accounts[0])

	start := time.Now()
	w := do(h, authed(httptest.NewRequest("GET", "/cookie?wait=5", nil), key))
	if w.Code != 503 {
		t.Fatalf("状态 %d", w.Code)
	}
	got := decodeFail(t, w)
	if got.Error.Code != codeBadRequest {
		t.Fatalf("code=%q，应为 %q", got.Error.Code, codeBadRequest)
	}
	if got.Error.RetryAfterSeconds != 0 {
		t.Fatalf("等多久那个号都还是停用的，不该给退避建议: %+v", got.Error)
	}
	// 不可等待：挂满 wait 只是把连接耗在一件不会变的事上。
	if took := time.Since(start); took > waitPoll {
		t.Fatalf("在「账号用不了」上等了 %v", took)
	}
	if p.calls != 1 {
		t.Fatalf("取票次数 %d", p.calls)
	}
	// 也不该排发现扫描：找新落点救不了一个已停用的号。
	if len(sc.discovery) != 0 {
		t.Fatalf("白排了发现扫描: %v", sc.discovery)
	}
	// 停用理由要落在卡片的原始错误里（「手动暂停」和「凭据已死」要做的事不同）。
	admin := do(h, authed(httptest.NewRequest("GET", "/admin/", nil), "akey")).Body.String()
	if !strings.Contains(admin, "token_revoked") {
		t.Fatal("管理页上看不到停用理由")
	}
}

// S-3 / S-4：开局最可能的两种故障要留下痕迹。
//
// 鉴权前的 bad_request 刻意不进那个 200 条的环（谁都能灌），但**必须进日志**——
// 三处都没有记录的话，「消费端每一发都带着畸形参数」和「池子没货」长得一模一样。
// 而冷启动（网关表空着）池子自己走不出来，那句「要人踢一脚」得写在卡片上。
func TestColdStartAndBadParamsLeaveATrace(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	var buf bytes.Buffer
	srv.opt.Log = slog.New(slog.NewTextHandler(&buf, nil))

	// S-3：畸形参数（鉴权之前就拒了）⇒ 日志里要有，环里不许有。
	if w := do(h, httptest.NewRequest("GET", "/cookie?wait=abc", nil)); w.Code != 400 {
		t.Fatalf("状态 %d", w.Code)
	}
	if !strings.Contains(buf.String(), codeBadRequest) {
		t.Fatalf("参数不合法没进日志: %s", buf.String())
	}
	admin := func() string {
		return do(h, authed(httptest.NewRequest("GET", "/admin/", nil), "akey")).Body.String()
	}
	if strings.Contains(admin(), codeBadRequest) {
		t.Fatal("鉴权前的 400 进了环（未鉴权的请求能把真实诊断挤出去）")
	}

	// S-4：冷启动 ⇒ no_gateway，卡片上要写清「池子自己走不出来」。
	st.gws = map[string]types.Gateway{}
	p.err = sched.ErrNoFullSlot
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil),
		srv.ConsumerKeyFor(st.accounts[0]))); w.Code != 503 {
		t.Fatalf("状态 %d", w.Code)
	}
	page := admin()
	if !strings.Contains(page, codeNoGateway) || !strings.Contains(page, "/admin/rescan") {
		t.Fatal("冷启动的卡片上没写该怎么踢")
	}
}

// ---------- POST /pair/renew ----------

// routeCookie 拼一张形状对的完整路由 cookie：__cflb 一串不透明内容，__oailb 是
// payload 解得开的假 JWT（上游的签名密钥在上游手上，池子本来就不验签）。
func routeCookie(cflb, gateway string, oailbExp time.Time) string {
	payload := fmt.Sprintf(`{"host":"chatgpt-prod.edge.%s.internal","exp":%d}`,
		gateway, oailbExp.Unix())
	return "__cflb=" + cflb + "; __oailb=eyJhbGciOiJFUzI1NiJ9." +
		base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".fake-sig"
}

func renewReq(version, cookie string) *http.Request {
	body, _ := json.Marshal(map[string]string{"cookie_version": version, "cookie": cookie})
	return httptest.NewRequest("POST", "/pair/renew", bytes.NewReader(body))
}

// grantTicket 让归属闸认「这一行确实拿到过池子现持的那张票」。
//
// 默认是**谁都没拿到过**（`fakePool.deliveredTo` 为 nil ⇒ 恒假），所以凡是要走到
// 归属闸之后的用例都得显式开一下 —— 这正是那道闸真的在挡事的证据。
// 按行而不按票号：好几个用例中途会换掉池子手上那张（票号跟着变），而它们要测的
// 不是归属闸；归属闸自己由 TestRenewRefusesTicketsNotDeliveredToYou 钉住。
func grantTicket(p *fakePool, row int64) {
	p.deliveredTo = func(a int64, _ string) bool { return a == row }
}

// agePair 把池子手上那张票的剩余寿命改成 left，并把它的 cookie 换成一张解得开的。
//
// 两件都必要：rig 默认那张剩一整小时 ⇒ 会命中 RenewAccept 那条「不落盘」的兜底
// （所以凡是要测**落盘**的用例都得先让它旧一点）；而默认那张的 __oailb 是个不透明
// 常量、解不开 ⇒ 「交回来的和手上这张一模一样」那一格永远走不到。
func agePair(st *fakeStore, left time.Duration) types.Pair {
	p := st.pairs["unified-142"]
	p.Cookie = routeCookie("OLD", "unified-142", time.Now().Add(left+types.OailbLifetime-types.PairLifetime))
	p.ExpiresAt = time.Now().Add(left)
	p.CflbExpiresAt = p.ExpiresAt
	st.pairs["unified-142"] = p
	return p
}

// renewed 解续期的成功响应。ok 和 cookie_version 是消费端唯一读的两个字段
// （valid_for_s 它刻意不读），所以这三个名字一个字都不许改。
func renewed(t *testing.T, w *httptest.ResponseRecorder) struct {
	OK            bool   `json:"ok"`
	CookieVersion string `json:"cookie_version"`
	ValidForS     int    `json:"valid_for_s"`
} {
	t.Helper()
	var got struct {
		OK            bool   `json:"ok"`
		CookieVersion string `json:"cookie_version"`
		ValidForS     int    `json:"valid_for_s"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("续期响应解不开: %v（body=%s）", err, w.Body.String())
	}
	return got
}

// 正路：收下那张新 cookie，寿命按它自己的 __oailb 重算，**别的一个字段都不动**。
//
// 寿命必须走 types.MintResult.RouteDeadline 的第三级兜底：消费端那一跳的
// Set-Cookie 池子没见过 ⇒ 读不出 __cflb 的 Max-Age ⇒ 按实测差值从 __oailb 的 exp
// 倒推。继承旧 __cflb 的死期是**错的**（那张已经被换掉了），所以这里要钉住
// 「新寿命比旧的长、而且正好是倒推那个值」。
func TestRenewStoresTheNewCookie(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	old := agePair(st, 10*time.Minute)
	grantTicket(p, 1)
	oailbExp := time.Now().Add(types.OailbLifetime).Truncate(time.Second)
	fresh := routeCookie("NEWCFLB", "unified-142", oailbExp)

	w := do(h, authed(renewReq(types.CookieVersion(old.Cookie), fresh),
		srv.ConsumerKeyFor(st.accounts[0])))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	got := renewed(t, w)
	if !got.OK {
		t.Fatal("ok 不为 true，消费端会当失败处理")
	}
	if want := types.CookieVersion(fresh); got.CookieVersion != want {
		t.Fatalf("cookie_version=%q，应为新那张的 %q", got.CookieVersion, want)
	}
	if len(st.puts) != 1 {
		t.Fatalf("落盘了 %d 次，应为 1 次", len(st.puts))
	}
	put := st.puts[0]
	if put.Cookie != fresh {
		t.Fatalf("落盘的不是交回来那张: %q", put.Cookie)
	}
	// 寿命 = 倒推值，而且必须比旧那张长（不然这一发白续）。
	want := types.FallbackCflbExpiry(oailbExp)
	if !put.ExpiresAt.Equal(want) || !put.CflbExpiresAt.Equal(want) {
		t.Fatalf("死期 ExpiresAt=%v CflbExpiresAt=%v，应同为 %v", put.ExpiresAt, put.CflbExpiresAt, want)
	}
	if !put.ExpiresAt.After(old.ExpiresAt) {
		t.Fatalf("续完还是 %v，比旧的 %v 没长", put.ExpiresAt, old.ExpiresAt)
	}
	// 别的一个字段都不动：铸它的还是那个号，网关也没变。
	if put.Gateway != old.Gateway || put.MintedBy != old.MintedBy {
		t.Fatalf("动了别的字段: %+v，原来是 gateway=%s mintedBy=%d", put, old.Gateway, old.MintedBy)
	}
	// 一发上游都不打、一个槽位都不碰。
	//
	// 「不碰槽位账本」这条**不在这儿断言**，因为它是**编译期**保证、比断言强：
	// 这一层的 Store 接口刻意不暴露任何写槽位的方法（只有 Slots()，见 api.go），
	// handleRenew 想改 LastTouch / LastDeliver / Prev* 连编译都过不去。
	// 第一版在这儿放了一条 reflect.DeepEqual(槽位快照)，那是**恒绿**的——
	// 没有可破的对象。能间接写到槽位的只剩 s.pool，由下面这一条钉住。
	if p.calls != 0 || len(p.released) != 0 {
		t.Fatalf("续期动了调度: calls=%d released=%+v", p.calls, p.released)
	}
	if !strings.Contains(do(h, httptest.NewRequest("GET", "/a/vt-1", nil)).Body.String(), "续到还剩") {
		t.Fatal("页面上看不见这次续期")
	}
}

// 形状是信任边界：票号定长十六进制、cookie 必须是完整两件而且 __oailb 解得开。
// 畸形的一律 bad_request（闭集里的码）而且**一个字节都不落盘**；对的那一侧照收。
func TestRenewValidatesShape(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	old := agePair(st, 10*time.Minute)
	grantTicket(p, 1)
	ver := types.CookieVersion(old.Cookie)
	key := srv.ConsumerKeyFor(st.accounts[0])
	good := routeCookie("NEW", "unified-142", time.Now().Add(types.OailbLifetime))

	// want 是**拒的理由**里必须出现的片段。
	//
	// 只断言 400 + bad_request 是**恒绿**的：形状解不开时 landed 是空串，而空串
	// 永远不等于 cur.Gateway ⇒ 下一道「落点不一致」顺手就把它挡了，把形状闸整段
	// 删掉这八个子例照样全绿（审查员实跑过）。附带代价也是真的：形状错误会被记成
	// Warn「落点不一致」、响应说「落在另一个网关上」，把消费端的排查引到错方向。
	// 所以每一格都要钉住**是哪一道闸**在说话。
	const shapeMsg = "完整的路由 cookie"
	const versionMsg = "位十六进制"
	for _, c := range []struct{ name, version, cookie, want string }{
		{"票号空", "", good, versionMsg},
		{"票号超长", strings.Repeat("a", 13), good, versionMsg},
		{"票号非十六进制", "0123456789zz", good, versionMsg},
		{"cookie 空", ver, "", shapeMsg},
		// 钉路由的是 __cflb：没有它那张 cookie 一个网关都钉不住。
		{"只有 oailb", ver, strings.SplitN(good, "; ", 2)[1], shapeMsg},
		{"只有 cflb", ver, "__cflb=NEW", shapeMsg},
		{"oailb 不是 JWT", ver, "__cflb=NEW; __oailb=garbage", shapeMsg},
		{"oailb 的 host 没有第三段", ver, routeCookie("NEW", "", time.Now().Add(types.OailbLifetime)), shapeMsg},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := do(h, authed(renewReq(c.version, c.cookie), key))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("状态 %d，应为 400: %s", w.Code, w.Body.String())
			}
			got := decodeFail(t, w).Error
			if got.Code != codeBadRequest {
				t.Fatalf("码 %q，应为 %q（闭集不许留洞）", got.Code, codeBadRequest)
			}
			if !strings.Contains(got.Message, c.want) {
				t.Fatalf("拒的理由是 %q，没提到 %q —— 说明挡它的是别的闸", got.Message, c.want)
			}
			if len(st.puts) != 0 {
				t.Fatalf("畸形请求也落盘了: %+v", st.puts)
			}
		})
	}
	// 请求体不是 JSON 同理。
	r := httptest.NewRequest("POST", "/pair/renew", strings.NewReader("not json"))
	if w := do(h, authed(r, key)); w.Code != http.StatusBadRequest {
		t.Fatalf("非 JSON 的体 → %d，应为 400", w.Code)
	}
	// 另一侧：形状全对就收下。
	if w := do(h, authed(renewReq(ver, good), key)); w.Code != 200 {
		t.Fatalf("形状全对却 %d: %s", w.Code, w.Body.String())
	}
	if len(st.puts) != 1 {
		t.Fatalf("落盘 %d 次，应为 1 次", len(st.puts))
	}
}

// 票号必须对上池子**此刻持有的**那张。对不上说明这张早被换掉了（或者从来不是池子
// 发的）⇒ 照它写盘就是用一个来历不明的 cookie 盖掉现役那张。
func TestRenewRefusesTicketsThePoolNoLongerHolds(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	old := agePair(st, 10*time.Minute)
	grantTicket(p, 1)
	key := srv.ConsumerKeyFor(st.accounts[0])
	fresh := routeCookie("NEW", "unified-142", time.Now().Add(types.OailbLifetime))

	// 一侧：票号是另一张 cookie 的（形状合法、池子不认）。
	stale := types.CookieVersion("__cflb=GONE; __oailb=GONE")
	w := do(h, authed(renewReq(stale, fresh), key))
	if w.Code != http.StatusConflict {
		t.Fatalf("状态 %d，应为 409: %s", w.Code, w.Body.String())
	}
	// 409 也要带码：唯一不带码的那个 409 是 /release 的（见 apiError 的注释）。
	if code := decodeFail(t, w).Error.Code; code != codeBadRequest {
		t.Fatalf("码 %q，应为 %q", code, codeBadRequest)
	}
	if len(st.puts) != 0 {
		t.Fatalf("票号对不上也落盘了: %+v", st.puts)
	}
	// 另一侧：票号就是池子手上那张 ⇒ 受理。
	if w := do(h, authed(renewReq(types.CookieVersion(old.Cookie), fresh), key)); w.Code != 200 {
		t.Fatalf("票号对得上却 %d: %s", w.Code, w.Body.String())
	}
}

// 完整性：交回来那张读出的落点必须就是这张票所在的网关。
//
// 对不上说明消费端把两张票搞混了，而账本一错，后面所有「这个网关还能不能交付」
// 都跟着错。要 Warn 不要 Info：这是对方的 bug，不是一次正常的拒绝。
func TestRenewRefusesACookieFromAnotherGateway(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	old := agePair(st, 10*time.Minute)
	grantTicket(p, 1)
	ver := types.CookieVersion(old.Cookie)
	key := srv.ConsumerKeyFor(st.accounts[0])
	var buf bytes.Buffer
	srv.opt.Log = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	exp := time.Now().Add(types.OailbLifetime)

	// 一侧：同一张票号，但 cookie 落在别的网关上。
	w := do(h, authed(renewReq(ver, routeCookie("NEW", "unified-999", exp)), key))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("状态 %d，应为 400: %s", w.Code, w.Body.String())
	}
	if len(st.puts) != 0 {
		t.Fatalf("落点不一致也落盘了: %+v", st.puts)
	}
	if !strings.Contains(buf.String(), "level=WARN") {
		t.Fatalf("落点不一致没记成 WARN: %s", buf.String())
	}
	// 另一侧：换成这张票真正的网关就收下。
	if w := do(h, authed(renewReq(ver, routeCookie("NEW", "unified-142", exp)), key)); w.Code != 200 {
		t.Fatalf("落点一致却 %d: %s", w.Code, w.Body.String())
	}
}

// 必须真的换了一张。一字不差交回来说明它那头根本没续到（上游没补发），
// 收下只是把同一份内容再落一次盘，而「续期在生效」这个读数会变成假的。
func TestRenewRefusesAnIdenticalCookie(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	// 场景得挑在**两道邻居闸之间**，不然原样交回根本走不到这道闸：
	// 池子手上那张老过 renewExpSkew（5 min）时，它自己的 exp 就偏出「像刚下发的」
	// 那个窗口 ⇒ 前面那道先拒；而新过 RenewAccept（60 s）时走「跳过落盘」。
	// 中间这段（1 min ~ 5 min）才是这道闸真正在干活的地方。
	old := agePair(st, types.PairLifetime-2*time.Minute)
	grantTicket(p, 1)
	ver := types.CookieVersion(old.Cookie)
	key := srv.ConsumerKeyFor(st.accounts[0])

	w := do(h, authed(renewReq(ver, old.Cookie), key))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("原样交回却 %d，应为 400: %s", w.Code, w.Body.String())
	}
	if len(st.puts) != 0 {
		t.Fatalf("原样交回也落盘了: %+v", st.puts)
	}
	// 必须是**这道闸**拒的，不是后面「死得更早」那道顺手挡掉的。
	//
	// 两道闸在这个夹具里恰好都能拦住它（原样交回 ⇒ 算出来的死期和账本上那个一模一样
	// ⇒ 也不满足「必须更长」），所以只断言 400 的话，把这道闸删掉用例照样绿——
	// 第一版就栽在这儿。线上两个数来自不同算法（账本上那个来自真实 Max-Age，
	// 这边是按差值倒推），差个几百毫秒就会从那道闸底下漏过去。
	if msg := decodeFail(t, w).Error.Message; !strings.Contains(msg, "一模一样") {
		t.Fatalf("拒的理由是 %q，不是「和手上这张一模一样」那道闸", msg)
	}
	// 另一侧：真的换了一张就收。两件一起换是常态——消费端只在上游补回**完整一组**
	// 新两件时才回传（2026-10-02 契约），所以「只换了 __cflb 那一半」这种半张
	// 在线上不会出现，而且它换不到寿命、会被「死得更早」那道闸拦掉。
	fresh := routeCookie("NEW", "unified-142", time.Now().Add(types.OailbLifetime))
	if fresh == old.Cookie {
		t.Fatal("夹具没构造出一张不一样的 cookie")
	}
	if w := do(h, authed(renewReq(ver, fresh), key)); w.Code != 200 {
		t.Fatalf("换了一张却 %d: %s", w.Code, w.Body.String())
	}
}

// RenewAccept 兜底：池子手上这张本来就还有近乎整条命 ⇒ 受理但**不落盘**。
//
// 这条防的是**异常**而不是常态：正常节奏由消费端定——它只在「取票后的第一发业务
// 请求」里续一次，那时候手上这张通常已经用掉一截寿命了。它拦的是重试风暴、
// 或者两个消费者拿着同一张票各续一次，而 state.json 是整份原子重写。
//
// 两侧只有账本能分开：响应都是 200 + ok=true，光看响应区分不出来。
func TestRenewSkipsTheWriteWhenThePoolsTicketIsStillFresh(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	grantTicket(p, 1)
	key := srv.ConsumerKeyFor(st.accounts[0])
	exp := time.Now().Add(types.OailbLifetime)
	fresh := routeCookie("NEW", "unified-142", exp)

	// 一侧：手上那张刚换过不到 RenewAccept ⇒ 不落盘，而票号报的是**旧**那张
	// （这个字段说的是池子的实际状态，不是「你交上来的那张」）。
	old := agePair(st, types.PairLifetime-types.RenewAccept/2)
	w := do(h, authed(renewReq(types.CookieVersion(old.Cookie), fresh), key))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	got := renewed(t, w)
	if !got.OK {
		t.Fatal("跳过落盘也要 ok=true（消费端把 ok=false 当失败）")
	}
	if want := types.CookieVersion(old.Cookie); got.CookieVersion != want {
		t.Fatalf("cookie_version=%q，跳过落盘时应报旧那张 %q", got.CookieVersion, want)
	}
	if len(st.puts) != 0 {
		t.Fatalf("还几乎满寿命却落盘了: %+v", st.puts)
	}

	// 另一侧：旧一点点（刚过 RenewAccept）就落盘。
	old = agePair(st, types.PairLifetime-types.RenewAccept-time.Minute)
	if w := do(h, authed(renewReq(types.CookieVersion(old.Cookie), fresh), key)); w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if len(st.puts) != 1 {
		t.Fatalf("过了兜底线却落盘 %d 次，应为 1 次", len(st.puts))
	}
}

// 续不出寿命就不收：交回来一张比现持那张死得更早的 cookie（过期的、或者拿错了一张
// 旧的），收下就是自己把池子最紧的供给换短。信任边界就在这一层。
func TestRenewRefusesACookieThatDiesSooner(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	old := agePair(st, 30*time.Minute)
	grantTicket(p, 1)
	ver := types.CookieVersion(old.Cookie)
	key := srv.ConsumerKeyFor(st.accounts[0])

	// 场景得挑在**容差窗口之内**：明显过期/明显短的 cookie 由前面那道「不像是刚
	// 下发的」闸挡掉（见 TestRenewRefusesACookieThatIsNotFreshlyIssued），走不到这儿。
	// 这道闸兜的是剩下那一格：提交的 exp 贴着容差下沿 ⇒ 倒推出的死期仍比现持那张早。
	//
	// 算一下（容差 5 min）：提交 exp = now+OailbLifetime−4min ⇒ 路由死期
	// = now+PairLifetime−4min = now+3360；现持那张剩 3450（低于 RenewAccept 的
	// 3540 线，所以不会走「跳过落盘」）⇒ 3360 < 3450 ⇒ 这一发白续。
	old = agePair(st, types.PairLifetime-150*time.Second)
	ver = types.CookieVersion(old.Cookie)
	short := routeCookie("NEW", "unified-142",
		time.Now().Add(types.OailbLifetime-renewExpSkew+time.Minute))
	w := do(h, authed(renewReq(ver, short), key))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("更短的 cookie 却 %d，应为 400: %s", w.Code, w.Body.String())
	}
	// 必须是**这道闸**拒的（前后两道都可能顺手挡住它）。
	if msg := decodeFail(t, w).Error.Message; !strings.Contains(msg, "死得更早") {
		t.Fatalf("拒的理由是 %q，不是「死得更早」那道闸", msg)
	}
	if len(st.puts) != 0 {
		t.Fatalf("更短的也落盘了: %+v", st.puts)
	}
	// 另一侧：真的更长就收。
	long := routeCookie("NEW", "unified-142", time.Now().Add(types.OailbLifetime))
	if w := do(h, authed(renewReq(ver, long), key)); w.Code != 200 {
		t.Fatalf("更长的 cookie 却 %d: %s", w.Code, w.Body.String())
	}
}

// B1：**归属闸**——只能续自己确实被交付过的那张票。
//
// 这是整条路上唯一挡得住人的那道。cookie 本体全是调用方自己写的字符串（`__oailb`
// 验不了签、`__cflb` 只查得出非空），所以「落点对不对」是拿外部输入校验外部输入。
// 没有这道闸，任何一把合法 consumer key 两步就能换掉**别人**现役的 pair：
// GET /cookie 拿票号 → POST /pair/renew 提交伪造的 host + 任意 exp，之后
// Deliver / redeliver / livePairs 全部只看 ExpiresAt ⇒ 一张路由已死的 cookie 被当
// 活票发给还没碰过这个网关的账号 ⇒ 他被按出口地理随机路由 ⇒ 降智票当满血票发出去。
func TestRenewRefusesTicketsNotDeliveredToYou(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	old := agePair(st, 10*time.Minute)
	ver := types.CookieVersion(old.Cookie)
	fresh := routeCookie("ATTACKER-CONTROLLED-GARBAGE", "unified-142",
		time.Now().Add(types.OailbLifetime))

	// 一侧：account 2 是**另一个上传者**，和 unified-142 毫无关系。
	// 它的 key 完全合法、落点也「对」（host 是它自己写的），只有归属闸能拦住它。
	other := srv.ConsumerKeyFor(st.accounts[1])
	w := do(h, authed(renewReq(ver, fresh), other))
	if w.Code != http.StatusConflict {
		t.Fatalf("别人的票却 %d，应为 409: %s", w.Code, w.Body.String())
	}
	if code := decodeFail(t, w).Error.Code; code != codeBadRequest {
		t.Fatalf("码 %q，应为 %q", code, codeBadRequest)
	}
	if len(st.puts) != 0 {
		t.Fatalf("别人的票也落盘了: %+v", st.puts)
	}
	if got := st.pairs["unified-142"].Cookie; got != old.Cookie {
		t.Fatalf("现役 cookie 被换成了 %q", got)
	}
	// 问的必须是**解出来那一行 + 这张票号**：问错行等于这道闸形同虚设。
	if want := "2|" + ver; len(p.askedDelivered) == 0 || p.askedDelivered[0] != want {
		t.Fatalf("归属闸问的是 %+v，应为 %q", p.askedDelivered, want)
	}

	// 另一侧：这张票确实交付给过这一行 ⇒ 受理。
	grantTicket(p, 2)
	if w := do(h, authed(renewReq(ver, fresh), other)); w.Code != 200 {
		t.Fatalf("自己的票却 %d: %s", w.Code, w.Body.String())
	}
}

// B2 之一：`__oailb` 的 exp 必须落在上游定死的那个跨度上。
//
// exp−iat 恒为 OailbLifetime ⇒ 刚下发那张的 exp 必然在 now+OailbLifetime 附近。
// 偏出去的要么是拿错了旧票，要么是编出来的远期 exp —— 后者正是把 pair 放大成
// 「永不临期」的那条路。
func TestRenewRefusesACookieThatIsNotFreshlyIssued(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	grantTicket(p, 1)
	key := srv.ConsumerKeyFor(st.accounts[0])

	for _, c := range []struct {
		name string
		exp  time.Time
	}{
		{"远期 48 小时", time.Now().Add(48 * time.Hour)},
		{"远期 72 小时", time.Now().Add(72 * time.Hour)},
		{"已经过期", time.Now().Add(-time.Hour)},
		{"只剩 10 分钟", time.Now().Add(10 * time.Minute)},
		{"刚超出容差上沿", time.Now().Add(types.OailbLifetime + renewExpSkew + time.Minute)},
		{"刚超出容差下沿", time.Now().Add(types.OailbLifetime - renewExpSkew - time.Minute)},
	} {
		t.Run(c.name, func(t *testing.T) {
			// 每格独立重铺：闸拆掉时第一发会真落盘，后面几格就变成 409
			// 「票号对不上」——那样只有第一格真的在验这道闸。
			st.puts = nil
			ver := types.CookieVersion(agePair(st, 10*time.Minute).Cookie)
			w := do(h, authed(renewReq(ver, routeCookie("NEW", "unified-142", c.exp)), key))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("状态 %d，应为 400: %s", w.Code, w.Body.String())
			}
			if msg := decodeFail(t, w).Error.Message; !strings.Contains(msg, "不是刚下发的") {
				t.Fatalf("拒的理由是 %q，不是这道闸", msg)
			}
			if len(st.puts) != 0 {
				t.Fatalf("落盘了: %+v", st.puts)
			}
		})
	}
	// 另一侧：容差之内就收（上下沿各一个，防着把窗口钉得比实现窄）。
	for _, exp := range []time.Time{
		time.Now().Add(types.OailbLifetime),
		time.Now().Add(types.OailbLifetime - renewExpSkew + time.Minute),
		time.Now().Add(types.OailbLifetime + renewExpSkew - time.Minute),
	} {
		st.puts = nil
		ver := types.CookieVersion(agePair(st, 10*time.Minute).Cookie)
		w := do(h, authed(renewReq(ver, routeCookie("NEW", "unified-142", exp)), key))
		if w.Code != 200 {
			t.Fatalf("exp=%v 在容差内却 %d: %s", exp, w.Code, w.Body.String())
		}
	}
}

// B2 之二：死期一律钳到 now+PairLifetime，**而且这个钳位必须在容差窗口内就够得着**。
//
// 真实 __cflb 的 Max-Age 死死是 PairLifetime，所以一张票无论怎么续都活不过
// now+PairLifetime。漏掉钳位就没有回头路：被放大的 pair 永不临期 ⇒
// renewStalePairs 永不重铸 ⇒ 漂移检测与失败预算全部失效 ⇒ 那个网关从此只发这张
// 死票，还持久化进 state.json、重启不自愈。
func TestRenewClampsTheDeadlineToOneLifetime(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	grantTicket(p, 1)
	key := srv.ConsumerKeyFor(st.accounts[0])

	// 一侧：exp 在容差上沿（比 now+OailbLifetime 还晚几分钟）⇒ 倒推出来的死期
	// 超过一条命 ⇒ 必须被钳回去。
	old := agePair(st, 10*time.Minute)
	exp := time.Now().Add(types.OailbLifetime + renewExpSkew - time.Minute)
	w := do(h, authed(renewReq(types.CookieVersion(old.Cookie),
		routeCookie("NEW", "unified-142", exp)), key))
	if w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if len(st.puts) != 1 {
		t.Fatalf("落盘 %d 次，应为 1 次", len(st.puts))
	}
	put := st.puts[0]
	// 钳位前这张会是 now+PairLifetime+4min，所以上限必须真的生效。
	hardCap := time.Now().Add(types.PairLifetime)
	if put.ExpiresAt.After(hardCap) {
		t.Fatalf("死期 %v 超过一条命的上限 %v（放大的 pair 永不临期、永不重铸）",
			put.ExpiresAt.UTC(), hardCap.UTC())
	}
	if !put.CflbExpiresAt.Equal(put.ExpiresAt) {
		t.Fatalf("两个死期不一致: ExpiresAt=%v CflbExpiresAt=%v", put.ExpiresAt, put.CflbExpiresAt)
	}
	// 钳完还得是一条**接近满的**命，别钳过头。
	if left := time.Until(put.ExpiresAt); left < types.PairLifetime-time.Minute {
		t.Fatalf("钳过头了，只剩 %v", left)
	}
	// 顺带钉住 RenewAccept 的等价性：钳住之后「还剩近乎整条命」立刻为真 ⇒
	// 紧接着再续一次就该走「跳过落盘」。没有钳位的话这条等价性会静默失效。
	st.puts = nil
	again := routeCookie("NEWER", "unified-142", time.Now().Add(types.OailbLifetime))
	if w := do(h, authed(renewReq(types.CookieVersion(put.Cookie), again), key)); w.Code != 200 {
		t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
	}
	if len(st.puts) != 0 {
		t.Fatalf("刚钳满的票又落盘了（RenewAccept 的等价性没救回来）: %+v", st.puts)
	}
}

// 落盘失败要回 500，而且**不许回 ok=true**：消费端只认 200+ok，回错了它就把一张
// 池子根本没记下来的 cookie 当成现役那张。
func TestRenewWriteFailureIsNotSilent(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	old := agePair(st, 10*time.Minute)
	grantTicket(p, 1)
	st.putErr = fmt.Errorf("磁盘满了")
	w := do(h, authed(renewReq(types.CookieVersion(old.Cookie),
		routeCookie("NEW", "unified-142", time.Now().Add(types.OailbLifetime))),
		srv.ConsumerKeyFor(st.accounts[0])))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("状态 %d，应为 500: %s", w.Code, w.Body.String())
	}
	if code := decodeFail(t, w).Error.Code; code == "" {
		t.Fatal("500 也要带闭集里的码")
	}
	if strings.Contains(w.Body.String(), `"ok":true`) {
		t.Fatalf("落盘失败却回了 ok=true: %s", w.Body.String())
	}
	// 报错里不许带 cookie 本体或磁盘路径（红线 1）。
	if strings.Contains(w.Body.String(), "磁盘满了") {
		t.Fatalf("把底层错误抄进了响应: %s", w.Body.String())
	}
}

// **占位行一律拒**，和 /release 同一个判据。
//
// 这条是 2026-10-02 复审抓到的 blocker。占位行的「归属」是**自助办下来的**：
// acctID 来自 ?account= 这个查询参数（零证据），而 GET /cookie 会用同一个 ref
// 现场建行、把槽位记上去 ⇒ 归属闸对它形同虚设。换个 ref 连冷却都不吃（全新占位行
// = 零槽位历史），实测一把合法 key 能把 5/5 个网关的共享 pair 全换成垃圾。
//
// 代价是跨账号和匿名消费端拿不到这个端点 —— 刻意的取舍，见 handleRenew 的注释。
func TestRenewRefusesPlaceholderRows(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	old := agePair(st, 10*time.Minute)
	ver := types.CookieVersion(old.Cookie)
	key := srv.ConsumerKeyFor(st.accounts[0])
	fresh := routeCookie("ATTACKER-CONTROLLED-GARBAGE", "unified-142",
		time.Now().Add(types.OailbLifetime))
	// 归属闸**全放开**：这个用例要证的是占位行连归属闸都走不到。
	// 只靠归属闸是不够的 —— 它恰好是占位行能自助办下来的那个东西。
	p.deliveredTo = func(int64, string) bool { return true }

	// 复现实测的两步攻击链：同一个随口编的 ref 先取票、再续。
	// 第一步**本来就该成**（匿名/跨账号取票是产品功能），拦的是第二步。
	for _, c := range []struct {
		name string
		key  string
		ref  string
	}{
		{"鉴权过的 key 替别人的 ref 续", key, "acct-made-up"},
		{"换一个全新的 ref（不吃冷却）", key, "acct-another-one"},
		{"真匿名、零 header", "", "acct-stranger"},
	} {
		t.Run(c.name, func(t *testing.T) {
			p.public = sched.PublicState{Enabled: true, Open: true}
			get := httptest.NewRequest("GET", "/cookie?gateway=unified-142&account="+c.ref, nil)
			ren := renewReq(ver, fresh)
			ren.URL.RawQuery = "account=" + c.ref
			if c.key != "" {
				get, ren = authed(get, c.key), authed(ren, c.key)
			}
			if w := do(h, get); w.Code != 200 {
				t.Fatalf("第一步取票该成（那是产品功能），得到 %d: %s", w.Code, w.Body.String())
			}
			w := do(h, ren)
			if w.Code != http.StatusConflict {
				t.Fatalf("占位行的续期该 409，得到 %d: %s", w.Code, w.Body.String())
			}
			if code := decodeFail(t, w).Error.Code; code != codeBadRequest {
				t.Fatalf("码 %q，应为 %q", code, codeBadRequest)
			}
			if len(st.puts) != 0 {
				t.Fatalf("占位行也落盘了: %+v", st.puts)
			}
			if got := st.pairs["unified-142"].Cookie; got != old.Cookie {
				t.Fatalf("共享 pair 被换成了 %q", got)
			}
		})
	}
	// 另一侧：**真上传者照旧能续**（别把功能一起锁死）。
	// 重铺一次：闸拆掉时上面那三格会真写盘，不重铺这一侧就会红在票号闸上、
	// 让人以为是功能被锁死了。
	st.puts = nil
	ver = types.CookieVersion(agePair(st, 10*time.Minute).Cookie)
	if w := do(h, authed(renewReq(ver, fresh), key)); w.Code != 200 {
		t.Fatalf("真上传者续不了了: %d %s", w.Code, w.Body.String())
	}
	if len(st.puts) != 1 {
		t.Fatalf("真上传者的续期没落盘: %+v", st.puts)
	}
}

// 一次交付只换到**一次**改写。
//
// 复审实测的放大系数就在这儿：原先归属闸比的是「池子现持那张的号」，而那个号谁都
// 查得到 ⇒ 一次交付换到 5/5 次连续改写（LastDeliver 不被刷新，窗口又放宽到了
// PairLifetime）。改成比「交付出去那一张」之后，第二发无论报哪个号都过不去：
// 报旧号 → 票号闸（池子现持的已经不是它）；报新号 → 归属闸（交付的不是它）。
//
// 这里的 deliveredTo 照真实 sched 的语义建模：只认**最初交付出去**那一张。
func TestRenewGivesOneRewritePerDelivery(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	old := agePair(st, 10*time.Minute)
	delivered := types.CookieVersion(old.Cookie)
	key := srv.ConsumerKeyFor(st.accounts[0])
	p.deliveredTo = func(row int64, v string) bool { return row == 1 && v == delivered }

	first := routeCookie("FIRST", "unified-142", time.Now().Add(types.OailbLifetime))
	if w := do(h, authed(renewReq(delivered, first), key)); w.Code != 200 {
		t.Fatalf("第一次续该成，得到 %d: %s", w.Code, w.Body.String())
	}
	if len(st.puts) != 1 {
		t.Fatalf("第一次续落盘 %d 次，应为 1 次", len(st.puts))
	}

	// 第二发无论报哪个号都该被拒，而且要钉住**是哪一道闸**在说话：
	// 两道闸互为对方的兜底，只断言「被拒了」的话删掉任一道用例都照样绿。
	second := routeCookie("SECOND", "unified-142", time.Now().Add(types.OailbLifetime))
	for _, c := range []struct{ name, version, want string }{
		{"报旧号 ⇒ 票号闸", delivered, "不是池子此刻持有的那张"},
		{"报新号 ⇒ 归属闸", types.CookieVersion(first), "不是交付给你的"},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := do(h, authed(renewReq(c.version, second), key))
			if w.Code != http.StatusConflict {
				t.Fatalf("状态 %d，应为 409: %s", w.Code, w.Body.String())
			}
			if msg := decodeFail(t, w).Error.Message; !strings.Contains(msg, c.want) {
				t.Fatalf("拒的理由是 %q，没提到 %q —— 挡它的是别的闸", msg, c.want)
			}
		})
	}
	if len(st.puts) != 1 {
		t.Fatalf("一次交付换到了 %d 次改写，应为 1 次: %+v", len(st.puts), st.puts)
	}
	if got := st.pairs["unified-142"].Cookie; got != first {
		t.Fatalf("第二次改写还是落了盘: %q", got)
	}
}

// 和 /release 刻意相反的两处之二：verify_full_at_deliver 开着时**不拒**。
//
// /release 拒是因为那条路上池子真用消费者的凭据打了两发、槽位是实烧的，还了就是
// 说谎（见 sched.Release，那一侧由 sched 自己的用例钉住）。续期一个槽位都不碰，
// 和那个开关无关 —— 这个用例就是防着有人照搬 /release 在这儿补一道闸。
func TestRenewIsNotBlockedByVerifyAtDeliver(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	grantTicket(p, 1)
	cfg := config.Defaults()
	for _, on := range []bool{false, true} {
		cfg.VerifyFullAtDeliver = on
		srv.opt.Live = config.NewLive("", cfg)
		// 每一轮都重新铺：上一轮**真的落盘了**，池子手上那张已经换成新的，
		// 再拿旧票号来就是 409（那是票号闸在起作用，不是这个开关）。
		old := agePair(st, 10*time.Minute)
		st.puts = nil
		w := do(h, authed(renewReq(types.CookieVersion(old.Cookie),
			routeCookie("NEW", "unified-142", time.Now().Add(types.OailbLifetime))),
			srv.ConsumerKeyFor(st.accounts[0])))
		if w.Code != 200 {
			t.Fatalf("verify_full_at_deliver=%v 时 %d，应恒为 200: %s", on, w.Code, w.Body.String())
		}
		if len(st.puts) != 1 {
			t.Fatalf("verify_full_at_deliver=%v 时没落盘: %+v", on, st.puts)
		}
	}
}

// 鉴权和 /cookie 完全一致，而且排在**读请求体之前**（同 /release，见 F7）。
func TestRenewNeedsSameAuthAsCookie(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	old := agePair(st, 10*time.Minute)
	grantTicket(p, 1)
	ver := types.CookieVersion(old.Cookie)
	fresh := routeCookie("NEW", "unified-142", time.Now().Add(types.OailbLifetime))

	w := do(h, renewReq(ver, fresh))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("没带 key → %d，应为 401", w.Code)
	}
	// 码必须是 consumer_rejected：没码的 401 会让调用方掉进通用路径、不退避，
	// 于是「每发推理请求敲一次池子」无限敲下去——而这种错要人去改配置。
	if code := decodeFail(t, w).Error.Code; code != codeConsumerRejected {
		t.Fatalf("401 的码是 %q，应为 %q", code, codeConsumerRejected)
	}
	// **不进那个 200 条的环**：这条路按定义是未鉴权的，进环就是给任何人一个
	// 把真实诊断挤出去的开关。
	if page := do(h, authed(httptest.NewRequest("GET", "/admin/", nil), "akey")).Body.String(); strings.Contains(page, codeConsumerRejected) {
		t.Fatal("未鉴权的 401 进了环")
	}
	// 畸形体 + 没鉴权还是 401：顺序反过来就白送扫描器一个区分信号。
	r := httptest.NewRequest("POST", "/pair/renew", strings.NewReader("not json"))
	if w := do(h, r); w.Code != http.StatusUnauthorized {
		t.Fatalf("畸形体 + 没鉴权 → %d，应为 401", w.Code)
	}
	// 管理员必须显式说替谁办（和 /cookie、/release 同一条规矩）。
	if w := do(h, authed(renewReq(ver, fresh), "akey")); w.Code != http.StatusBadRequest {
		t.Fatalf("管理员没带 account 却受理了: %d", w.Code)
	}
	if len(st.puts) != 0 {
		t.Fatalf("没鉴权就落盘了: %+v", st.puts)
	}
	// 另一侧：带对 key 就受理。
	if w := do(h, authed(renewReq(ver, fresh), srv.ConsumerKeyFor(st.accounts[0]))); w.Code != 200 {
		t.Fatalf("带了 key 却 %d: %s", w.Code, w.Body.String())
	}
}

// pair_remaining_s 和 valid_for_s 量的是**两件事**，必须能同时取到不同的值：
// 前者是路由剩余（小时量级，消费端拿它决定要不要顺手续一发），
// 后者是满血窗口剩余（150 秒量级，过了就该换票）。混成一个的后果是每发都续、
// 或者永不续。
func TestCookieReportsPairRemainingSeparately(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	key := srv.ConsumerKeyFor(st.accounts[0])
	read := func() (validFor, pairLeft int) {
		t.Helper()
		w := do(h, authed(httptest.NewRequest("GET", "/cookie", nil), key))
		if w.Code != 200 {
			t.Fatalf("状态 %d: %s", w.Code, w.Body.String())
		}
		var got struct {
			ValidForS      int `json:"valid_for_s"`
			PairRemainingS int `json:"pair_remaining_s"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		return got.ValidForS, got.PairRemainingS
	}

	// 窗口剩 183 秒、pair 剩 3000 秒 ⇒ 两个字段必须不相等。
	p.delivery.ValidForS = 183
	pr := st.pairs["unified-142"]
	pr.CflbExpiresAt = time.Now().Add(3000*time.Second + 500*time.Millisecond)
	st.pairs["unified-142"] = pr
	validFor, pairLeft := read()
	if validFor != 183 {
		t.Fatalf("valid_for_s=%d，应为 183（它的含义一个字都没动）", validFor)
	}
	if pairLeft != 3000 {
		t.Fatalf("pair_remaining_s=%d，应为 3000（向下取整）", pairLeft)
	}
	if validFor == pairLeft {
		t.Fatal("两个字段取到了同一个值，分不出来是不是同一个来源")
	}

	// 边界：pair 已经过期 ⇒ 钳 0，不许出负数（消费端把 0 当「不续」）。
	pr.CflbExpiresAt = time.Now().Add(-time.Hour)
	st.pairs["unified-142"] = pr
	if _, pairLeft = read(); pairLeft != 0 {
		t.Fatalf("过期的 pair 报了 %d，应钳到 0", pairLeft)
	}
	// 零值同理：那是老记录的标志，绝不能当成「不过期」。
	pr.CflbExpiresAt = time.Time{}
	st.pairs["unified-142"] = pr
	if _, pairLeft = read(); pairLeft != 0 {
		t.Fatalf("CflbExpiresAt 零值报了 %d，应钳到 0", pairLeft)
	}
	// 池子手上那张已经不是交付出去的这张 ⇒ 它的死期和消费端手里这张无关，报 0。
	pr.Cookie, pr.CflbExpiresAt = "__cflb=OTHER", time.Now().Add(time.Hour)
	st.pairs["unified-142"] = pr
	if _, pairLeft = read(); pairLeft != 0 {
		t.Fatalf("票对不上却报了 %d，应为 0", pairLeft)
	}
}

// N3：Detail 按**字符**截，不按字节——它基本全是中文，而它进的是 HTML 页面，
// 按字节切会把半个汉字落进去。
func TestFailDetailTruncatesByRune(t *testing.T) {
	l := &failLog{}
	// 开头故意放一个 ASCII：这样第 300 个**字节**正好落在一个汉字中间。
	l.add(deliverFail{Detail: "x" + strings.Repeat("网关", failDetailMax)})
	got := l.recent(0, 1)[0].Detail
	if !utf8.ValidString(got) {
		t.Fatalf("截出了半个汉字: %q", got)
	}
	if n := utf8.RuneCountInString(strings.TrimSuffix(got, "…")); n != failDetailMax {
		t.Fatalf("截了 %d 个字符，应为 %d", n, failDetailMax)
	}
}
