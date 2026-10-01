package httpapi

// 这一份盯的是 2026-10-02 那轮协议改动：四个新请求参数、cookie_version、
// 十一个错误码的闭集、以及 POST /release。
//
// **参数名 / JSON 字段名 / 错误码是和消费端冻结的**，所以这里的字面量是故意写死的：
// 它们一变就是改协议，必须让这几个用例红。

import (
	"bytes"
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
