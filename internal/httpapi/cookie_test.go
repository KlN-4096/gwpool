package httpapi

// 这一份盯的是 2026-10-02 那轮协议改动：四个新请求参数、cookie_version、
// 九个错误码的闭集、以及 POST /release。
//
// **参数名 / JSON 字段名 / 错误码是和消费端冻结的**，所以这里的字面量是故意写死的：
// 它们一变就是改协议，必须让这几个用例红。

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

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
	if strings.Contains(got.CookieVersion, "OAILB") || strings.Contains(got.CookieVersion, "cf") {
		t.Fatalf("cookie_version 里带了 cookie 本体: %q", got.CookieVersion)
	}
	// 老字段照旧。
	if got.Gateway != "unified-142" || got.ValidForS != 150 || !got.VerifiedFull || !got.TTLIsAdvisory {
		t.Fatalf("老字段被动过了: %+v", got)
	}
}

// 九个码是闭集，每个都要能从真实的失败路径上走出来，而且带 retry 的要同时发
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
	// 池子里那张票正是被排除的那一张 ⇒ 等不到候选 ⇒ 不重跑，但确实等过。
	if p.calls != 1 {
		t.Fatalf("取票次数 %d", p.calls)
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
func TestWaitOnlyRetriesWhenATicketShowsUp(t *testing.T) {
	t.Run("等到票就重跑一次", func(t *testing.T) {
		srv, h, p, st, _ := rig(t, types.ModePool)
		p.err = sched.ErrNoFullSlot // 分类成 no_live_pair：可等待
		w := do(h, authed(httptest.NewRequest("GET", "/cookie?wait=5", nil),
			srv.ConsumerKeyFor(st.accounts[0])))
		if w.Code != 503 {
			t.Fatalf("状态 %d", w.Code)
		}
		// rig 的池子里本来就有一张活票 ⇒ 第一次轮询就命中 ⇒ 正好重跑一次。
		if p.calls != 2 {
			t.Fatalf("取票次数 %d，应为 2（原始一次 + 等到票之后重跑一次）", p.calls)
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

	// 跨账号：/cookie 把那种票算在占位行上，这边也只能读那一行——
	// 池子里还没有那一行，就等于没有可还的交付，而且**不许**退回上传者那一行
	// （那会把上传者的槽位还掉）。
	p.released = nil
	r := releaseReq(types.CookieVersion(liveCookie))
	r.URL.RawQuery = "account=acct-friend"
	if w := do(h, authed(r, srv.ConsumerKeyFor(st.accounts[0]))); w.Code != http.StatusConflict {
		t.Fatalf("状态 %d，应为 409", w.Code)
	}
	if len(p.released) != 0 {
		t.Fatalf("把别人的槽位还了: %+v", p.released)
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
}
