// Package mint 经指定出口向上游铸票，并用 state-echo 判一次满血。
//
// cookie 解析、落点判定、代理用户名拼法照 geo_sampler.py 和 state_echo_probe.py
// （SUCK_MY_ASTRA relay 的 mintHeaders / verifyStateOnce）翻译——这两份是实测跑通的
// （170+ 样本、state-echo 与糖果题 10/10 一致）。出站报文形状见 wire.go。
package mint

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

const (
	defaultUpstream = "https://chatgpt.com/backend-api/codex/responses"
	defaultGeoURL   = "https://ipinfo.io/json"
	defaultTimeout  = 60 * time.Second
	geoTimeout      = 25 * time.Second

	stateHeader = "x-codex-turn-state"

	cookieCflb  = "__cflb"  // Cloudflare 亲和 cookie，真正钉网关的是它
	cookieOailb = "__oailb" // ES256 JWT，让你**读出**落在哪个网关

	// 连续这么多次连接层失败就临时跳过这个供应商。novproxy 实测约 10% 抖动，
	// 一两次失败是常态，不该让它下线。
	// 退避刻意压得很短（10s 起、封顶 1 分钟）：熔断是为了别对着一条死供应商反复撞墙，
	// 不是惩罚。而只要还有别的粘性供应商顶着，退避长一点的收益也不大；
	// 退避太长的代价倒是实打实的——2026-10-01 一次误判就让整池停了 70 秒。
	proxyFailThreshold = 3
	proxySkipBase      = 10 * time.Second
	proxySkipMax       = time.Minute
)

// ErrNoLanding 是「没下发新 __oailb，本次又没发 pair」——读不出落点，按失败处理。
//
// 注意反过来不成立：没下发新 cookie 但**发了** pair 是**成功**，
// 说明上游接受了那张 pair、没换网关（SPEC 第 2 节、红线 3）。
var ErrNoLanding = errors.New("上游既没下发 __oailb，本次也没发 pair，读不出落点")

// ErrNoProxy 是「能用的出口一个都没有」。定义搬到了 types（调度要按它分流，
// 见 types.ErrNoExit），这里留个别名，免得改一串调用点。
var ErrNoProxy = types.ErrNoExit

// Client 实现 types.Minter。
type Client struct {
	// Proxies 是出口供应商清单，下标即 sid 前两位编的那个下标，**顺序不要在运行中变**。
	Proxies     []config.Proxy
	StickyMin   int                // novproxy 的 -t-，0 时按 5 分钟
	Fingerprint config.Fingerprint // 空时按 sub2api 形状
	Model       string             // 铸票用的模型，空时按 defaultModel
	Upstream    string             // 空时用 chatgpt.com 的 /responses
	GeoURL      string             // 空时用 ipinfo.io
	Timeout     time.Duration

	// HTTP 非 nil 时直接用它，**不拼代理**。测试注入 httptest 的客户端走这条路
	// （SPEC 红线 8：测试不许打真实上游）。
	HTTP *http.Client

	// UpstreamPerRegionHour 是**每个上游账号、每个区域**每小时最多打多少发上游
	// （Mint + VerifyFull）。0 时按 config.Defaults。
	// Geo 不算在内：它打的是 ipinfo，不是上游。
	UpstreamPerRegionHour int

	// Live 非 nil 时，模型和速率上限每次**现读**它，于是改配置不用重启。
	// 为空就用上面那两个字段（测试走这条路，不必搭一份配置）。
	//
	// 代理清单刻意**不**走它：Proxies 的下标编进了 sid，运行中换顺序会让已经发出去的
	// sid 指向另一条出口，铸票和 state-echo 就配不成同一个 IP 了。改代理要重启。
	Live *config.Live

	// Log 非 nil 时记熔断的开合。**只记这一件事**：别的失败都原路返回给调用方，
	// 由它决定记不记；而熔断是进程内部的状态变化，不记的话事后只能看到
	// 「没有可用的出口代理」，查不出是哪条、为什么、什么时候恢复
	// （2026-10-01 的一次全池 503 就是这么丢掉现场的）。
	Log *slog.Logger

	mu        sync.Mutex
	cursor    int                    // 轮询游标
	fails     map[string]int         // 供应商 -> 连续失败次数
	skipUntil map[string]time.Time   // 供应商 -> 跳到什么时候
	sids      map[string]sidBind     // sid -> 绑定的供应商
	sent      map[string][]time.Time // 账号|区域 -> 过去一小时每一发请求的时刻
}

// rateKey 是速率闸的分桶键：(上游账号 × 区域)。
//
// 区域反查不到（出口不在 types.Regions 里，只可能来自手改的 state.json）时
// 整批挤进同一个空桶：宁可让它们互相挤，也不要每个野出口凭空多出一整份配额。
func rateKey(acct string, e types.Exit) string {
	return acct + "|" + types.RegionKeyOf(e)
}

// spend 过速率闸。满了就回 types.ErrRateLimited，一发都不发。
//
// **按 (上游账号 × 区域) 算，不是全局、也不是只按账号**：一个号打太猛不该让别的号
// 跟着挨饿，而一个区域打不通时的重试更不该吃掉另外八个区域的预算——那八个区域才是
// 发现新落点的地方。九个区域各 6 发合起来正好是原先按账号算的 54 发/小时。
//
// 滑动窗口而不是漏桶：「每小时 6 发」就按字面实现，几十个时间戳扫一遍不值得优化。
func (c *Client) spend(acct string, e types.Exit) error {
	lim := c.perRegionHour()
	now := time.Now()
	cut := now.Add(-time.Hour)
	key := rateKey(acct, e)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sent == nil {
		c.sent = map[string][]time.Time{}
	}
	win := c.sent[key]
	kept := win[:0]
	for _, t := range win {
		if t.After(cut) {
			kept = append(kept, t)
		}
	}
	if len(kept) >= lim {
		c.sent[key] = kept
		// 说出「还要等多久」：没有它的话调用方只知道被拒，不知道该等 3 秒还是 50 分钟。
		wait := kept[0].Add(time.Hour).Sub(now).Round(time.Second)
		return fmt.Errorf("%w（区域 %s 每小时上限 %d，%s 后腾出一发）",
			types.ErrRateLimited, types.RegionKeyOf(e), lim, wait)
	}
	c.sent[key] = append(kept, now)
	return nil
}

// Rate 报某个上游账号当前窗口**各区域**各用掉几发，以及每区的上限。
//
// 整个账号一次报完，不是一区一问：挑铸票者要同时看九个区域还剩多少配额
// （见 sched.minterFor），而页面上那张格子也是九个一起画。
// key 是区域 key；没打过的区域不出现在 map 里。
func (c *Client) Rate(acct string) (used map[string]int, limit int) {
	limit = c.perRegionHour()
	cut := time.Now().Add(-time.Hour)
	used = map[string]int{}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, ts := range c.sent {
		// 从**最后**一个竖线切：区域 key 里没有竖线，而上游 account_id 是外面贴进来的
		// 自由文本，从前面切会把带竖线的账号整个读错行。
		i := strings.LastIndex(key, "|")
		if i < 0 || key[:i] != acct {
			continue
		}
		region := key[i+1:]
		for _, t := range ts {
			if t.After(cut) {
				used[region]++
			}
		}
	}
	return used, limit
}

// sidBind 把一个 sid 钉在一条出口上。
//
// 为什么要记：types.Minter 的签名里没有代理参数（已定稿），而铸票和随后的 state-echo
// 必须走**同一个出口**——同一个 sid 落到另一个供应商就是另一个 IP，可能换网关，
// 判据就配不上对。所以 sid 第一次出现时定下供应商，之后一律沿用。
type sidBind struct {
	idx int
	at  time.Time
}

var _ types.Minter = (*Client)(nil)

// NewSID 挑一个出口并返回绑定到它的 sid。sticky=true 时只挑粘性供应商。
//
// 轮转就在这里发生：每次调用往后挪一格，连续失败的供应商按指数退避临时跳过。
// 别的包自己生成的 sid 也能用——第一次进 Mint/Geo 时会自动绑一条**粘性**出口。
func (c *Client) NewSID(sticky bool) (string, error) {
	sid := randSID()
	if _, err := c.resolve(sid, sticky); err != nil {
		return "", err
	}
	return sid, nil
}

// Sticky 说明这个 sid 绑定的出口能不能钉住同一个 IP。
// 不能钉住时就不该把 Geo 的结果当成铸票那一发的地理（每连接换 IP）。
func (c *Client) Sticky(sid string) bool {
	p, err := c.resolve(sid, true)
	return err == nil && p.Sticky()
}

// proxyName 返回这个 sid 绑定的供应商名（不含凭据），给测试和日志看。
func (c *Client) proxyName(sid string) string {
	p, err := c.resolve(sid, true)
	if err != nil {
		return ""
	}
	return p.Name
}

// resolve 取 sid 绑定的出口；没绑过就现在挑一条绑上。
func (c *Client) resolve(sid string, sticky bool) (config.Proxy, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.sids[sid]; ok && b.idx < len(c.Proxies) {
		return c.Proxies[b.idx], nil
	}
	now := time.Now()
	n := len(c.Proxies)
	for i := 0; i < n; i++ {
		idx := (c.cursor + i) % n
		p := c.Proxies[idx]
		if !p.Enabled || (sticky && !p.Sticky()) || now.Before(c.skipUntil[p.Name]) {
			continue
		}
		c.cursor = (idx + 1) % n
		c.bindLocked(sid, idx, now)
		return p, nil
	}
	return config.Proxy{}, fmt.Errorf("%w（sticky=%v，共 %d 条）", ErrNoProxy, sticky, n)
}

// bindLocked 记下绑定，顺手清掉过期的。
// 绑定只需要活过粘性窗口：窗口一过出口 IP 本来就会变，留着也没用。
func (c *Client) bindLocked(sid string, idx int, now time.Time) {
	if c.sids == nil {
		c.sids = map[string]sidBind{}
	}
	if len(c.sids) >= 2000 {
		ttl := 2 * time.Duration(c.stickyMin()) * time.Minute
		for k, v := range c.sids {
			if now.Sub(v.at) > ttl {
				delete(c.sids, k)
			}
		}
	}
	c.sids[sid] = sidBind{idx: idx, at: now}
}

// Mint 经 e 指定的出口打一发最小 ping。sentPair 为空表示裸打。
func (c *Client) Mint(ctx context.Context, cr types.Creds, e types.Exit, sid, sentPair string) (types.MintResult, error) {
	if err := c.spend(cr.AccountID, e); err != nil {
		return types.MintResult{}, err
	}
	body, header := buildWire(c.fingerprint(), c.model(), cr, sentPair, "")
	req, err := c.newRequest(ctx, http.MethodPost, c.upstream(), body, header)
	if err != nil {
		return types.MintResult{}, err
	}
	resp, err := c.do(req, e, sid, cr.AccountID)
	if err != nil {
		return types.MintResult{}, err
	}
	// 只读响应头就断：body 是 SSE 流，读完它要等上游把整轮推完，白等几十秒。
	defer resp.Body.Close()

	res := types.MintResult{
		Status:    resp.StatusCode,
		TurnState: resp.Header.Get(stateHeader),
	}
	if resp.StatusCode/100 != 2 {
		// 非 2xx 的体是一小段 JSON，不是 SSE 流，读它不会挂住。
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<10))
		res.ErrBody = strings.TrimSpace(string(b))
	}
	fresh := newCookies(resp)
	if oailb := fresh[cookieOailb]; oailb != "" {
		gw, exp, err := parseOailb(oailb)
		if err != nil {
			return res, fmt.Errorf("解 __oailb: %w", err)
		}
		res.Gateway = gw
		res.PairExpires = exp
		// 上游常只补发变动的那一项。只拿新的那半会丢掉钉路由的 __cflb，
		// 所以在送进去的 pair 上**覆盖**，而不是替换。
		res.NewPair = mergePair(sentPair, fresh)
		return res, nil
	}
	if sentPair != "" {
		// 没下发新 cookie = 网关没变（relay index.js 的判读同款）。这是成功。
		res.Gateway = pairGateway(sentPair)
		if res.Gateway == "" {
			return res, fmt.Errorf("送进去的 pair 里读不出网关")
		}
		return res, nil
	}
	return res, ErrNoLanding
}

// VerifyFull 是 state-echo：带一张活票再打一发，只读响应头。
//
// 判据有假阴、无假阳，所以只在交付槽位时验一次，不要当持续闸门（SPEC 红线 4）。
// 返回 nil Verdict = 不下结论。
func (c *Client) VerifyFull(ctx context.Context, cr types.Creds, e types.Exit, sid, pair, ticket string) (*types.Verdict, error) {
	if ticket == "" {
		// 不带活票上游必回一个 state，一律判成「降级」，判据直接失效。
		return nil, errors.New("state-echo 必须带一张活票")
	}
	// 和铸票那一发共用同一个 (账号 × 区域) 预算：两条路各算一套就等于把上限翻倍。
	if err := c.spend(cr.AccountID, e); err != nil {
		return nil, err
	}
	body, header := buildWire(c.fingerprint(), c.model(), cr, pair, ticket)
	req, err := c.newRequest(ctx, http.MethodPost, c.upstream(), body, header)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req, e, sid, cr.AccountID)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// 非 200 不下结论：429/403 的响应头里本来就没有 turn-state，
	// 照「无新 state = 满血」读就是假阳（作者原版的漏洞，实测 12 发撞出 2 个）。
	if resp.StatusCode != http.StatusOK {
		return nil, nil
	}
	newState := resp.Header.Get(stateHeader)
	v := &types.Verdict{Status: resp.StatusCode}
	switch {
	case newState == "":
		v.Full, v.Note = true, "未下发新 state"
	case newState == ticket:
		v.Full, v.Note = true, "回下同一张票"
	default:
		v.Full, v.Note = false, "下发了不同的新 state"
	}
	return v, nil
}

// Geo 查这个 sid 当前的出口地理。
//
// 只有粘性出口下的结果才配得上同一发铸票（非粘性每连接换 IP，查到的不是铸票那个 IP）。
// 一切统计按它来，不按请求的 region（SPEC 红线 6）。
func (c *Client) Geo(ctx context.Context, e types.Exit, sid string) (types.Observed, error) {
	var lastErr error
	// 住宅 IP 打 ipinfo 偶发超时，重试一次。同 sid ⇒ 同出口 IP，不影响和铸票的配对。
	for try := 0; try < 2; try++ {
		if try > 0 {
			select {
			case <-ctx.Done():
				return types.Observed{}, ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
		obs, err := c.geoOnce(ctx, e, sid)
		if err == nil {
			return obs, nil
		}
		lastErr = err
	}
	return types.Observed{}, lastErr
}

func (c *Client) geoOnce(ctx context.Context, e types.Exit, sid string) (types.Observed, error) {
	url := c.GeoURL
	if url == "" {
		url = defaultGeoURL
	}
	ctx, cancel := context.WithTimeout(ctx, geoTimeout)
	defer cancel()
	req, err := c.newRequest(ctx, http.MethodGet, url, nil, nil)
	if err != nil {
		return types.Observed{}, err
	}
	resp, err := c.do(req, e, sid, "")
	if err != nil {
		return types.Observed{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return types.Observed{}, fmt.Errorf("查 geo 返回 HTTP %d", resp.StatusCode)
	}
	var body struct {
		IP      string `json:"ip"`
		Country string `json:"country"`
		City    string `json:"city"`
		Org     string `json:"org"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&body); err != nil {
		return types.Observed{}, fmt.Errorf("解 geo 响应: %w", err)
	}
	if body.IP == "" {
		return types.Observed{}, errors.New("geo 响应里没有 ip")
	}
	obs := types.Observed{IP: body.IP, Country: body.Country, City: body.City}
	// ipinfo 的 org 是 "AS12345 Some ISP Name"，首段是 ASN。
	if asn, isp, ok := strings.Cut(body.Org, " "); ok {
		obs.ASN, obs.ISP = asn, truncate(isp, 40)
	} else {
		obs.ASN = body.Org
	}
	return obs, nil
}

// ---------- 请求构造 ----------

func (c *Client) fingerprint() config.Fingerprint {
	if c.Fingerprint == config.FingerprintMint {
		return config.FingerprintMint
	}
	return config.FingerprintSub2api
}

// model 是铸票用的模型。为空时兜底，不报错：mint 包被裸用（测试、一次性脚本）时
// 不该先要一份配置。生产的值由 config.Load 校验过。
func (c *Client) model() string {
	if c.Live != nil {
		if m := c.Live.Get().DefaultModel(); m != "" {
			return m
		}
	}
	if c.Model != "" {
		return c.Model
	}
	return defaultModel
}

// perRegionHour 是当前的「每账号每区域每小时」上限。
//
// 兜底值去 config.Defaults 要，不在这儿再留一个常量：两处各写一个数，
// 迟早只改一处。
func (c *Client) perRegionHour() int {
	if c.Live != nil {
		if n := c.Live.Get().UpstreamPerRegionHour; n > 0 {
			return n
		}
	}
	if c.UpstreamPerRegionHour > 0 {
		return c.UpstreamPerRegionHour
	}
	return config.Defaults().UpstreamPerRegionHour
}

func (c *Client) upstream() string {
	if c.Upstream != "" {
		return c.Upstream
	}
	return defaultUpstream
}

func (c *Client) newRequest(ctx context.Context, method, url string, body []byte, header http.Header) (*http.Request, error) {
	var rd io.Reader
	if body != nil {
		rd = strings.NewReader(string(body))
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return nil, fmt.Errorf("建请求: %w", err)
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	return req, nil
}

// do 发请求。HTTP 字段注入时直接用它，否则按 (sid 绑定的供应商, 出口) 临时拼一个客户端。
func (c *Client) do(req *http.Request, e types.Exit, sid, acct string) (*http.Response, error) {
	if c.HTTP != nil {
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return nil, fmt.Errorf("请求失败: %w", err)
		}
		return resp, nil
	}
	p, err := c.resolve(sid, true)
	if err != nil {
		return nil, err
	}
	user, err := proxyUser(p, e, sid, c.stickyMin())
	if err != nil {
		return nil, err
	}
	// SOCKS5 CONNECT 成功就是「供应商活着」的协议级证据，拿它把两种失败分开。
	var tunneled atomic.Bool
	cl := proxyClient(p, user, c.timeout(), c.fingerprint(), func() { tunneled.Store(true) })
	resp, err := cl.Do(req)
	switch {
	case err != nil && req.Context().Err() != nil:
		// **调用方自己撤了**（sub2api 放弃重试、客户端断开、上层超时），不是供应商的账。
		// 2026-10-01 现网两次全池 502 就毁在这一条上：重试风暴里每发都被很快掐断，
		// 三发 "context canceled" 就把 novproxy 熔断，而它一直是好的——
		// 池子被客户端的重试把自己打停了。也不记成功：这一发什么都没证明。
		return nil, fmt.Errorf("经 %s 的请求被调用方取消: %w", p.Name, err)
	case err != nil && tunneled.Load():
		// 隧道建起来了又断 ⇒ **出口节点**死了，不是供应商的账。住宅池里约一成的
		// 粘性会话会摸到这种死节点（2026-10-01 实测 2/20），记在供应商头上就会
		// 让一个倒霉区域连败三次把 novproxy 整条熔断掉，而 webshare 不粘性顶不上
		// ⇒ 剩下八个区域一个网络请求都没发就全败（当天 failures=9 的真正成因）。
		// 换个 sid 就是换个出口 IP，重试归 scan 那层做。
		c.noteSuccess(p.Name)
		return nil, fmt.Errorf("经 %s 的出口中途断开: %w", p.Name, err)
	case err != nil:
		// 连不上代理本身、认证被拒：这才是供应商的账。
		// err 里会带 URL，代理用户名在 Transport 里，不会被 net/http 带进来。
		c.noteFailure(p.Name, acct, err)
		return nil, fmt.Errorf("经 %s 请求失败: %w", p.Name, err)
	}
	c.noteSuccess(p.Name)
	return resp, nil
}

// noteFailure 记一次供应商失败。acct 只进日志：出问题时第一个要回答的问题是
// 「是谁在打」——一条没有账号的熔断记录，事后对着 20 个账号的池子没有指向性。
func (c *Client) noteFailure(name, acct string, cause error) {
	c.mu.Lock()
	if c.fails == nil {
		c.fails, c.skipUntil = map[string]int{}, map[string]time.Time{}
	}
	c.fails[name]++
	n := c.fails[name]
	var skip time.Duration
	if n >= proxyFailThreshold {
		skip = proxySkipBase << (n - proxyFailThreshold)
		if skip > proxySkipMax || skip <= 0 {
			skip = proxySkipMax
		}
		c.skipUntil[name] = time.Now().Add(skip)
	}
	c.mu.Unlock()

	if c.Log != nil && skip > 0 {
		// 一条粘性供应商熔断就等于整池停摆（非粘性的那条顶不上 sticky 请求），
		// 所以这行是 WARN 不是 INFO。
		c.Log.Warn("出口熔断", "proxy", name, "upstream", acct, "fails", n, "skip", skip, "cause", cause)
	}
}

func (c *Client) noteSuccess(name string) {
	c.mu.Lock()
	was := !c.skipUntil[name].IsZero()
	delete(c.fails, name)
	delete(c.skipUntil, name)
	c.mu.Unlock()
	if c.Log != nil && was {
		c.Log.Info("出口恢复", "proxy", name)
	}
}

func (c *Client) stickyMin() int {
	if c.StickyMin > 0 {
		return c.StickyMin
	}
	return 5
}

func (c *Client) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return defaultTimeout
}

// proxyClient 每次现建：每个 sid 就是一条独立的会话，连接本来就不该复用。
// onTunnel 在 SOCKS5 CONNECT 成功、隧道可用时调用一次（可为 nil）。
// 调用方拿它区分「代理连不上」和「出口节点中途死了」——见 do。
func proxyClient(p config.Proxy, user string, timeout time.Duration, fp config.Fingerprint, onTunnel func()) *http.Client {
	pass := p.Pass.Reveal()
	host := p.Host
	return &http.Client{
		Timeout: timeout,
		// 不跟重定向：跟过去就读不到这一跳的 set-cookie 了，落点全靠它。
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
				conn, err := socks5Connect(ctx, host, user, pass, addr)
				if err != nil {
					return nil, err
				}
				if onTunnel != nil {
					onTunnel()
				}
				return conn, nil
			},
			DisableKeepAlives: true,
			// 真客户端一条 Accept-Encoding 都不发，而 Go 会自动加 gzip。
			// sub2api 形状靠这个开关做到「不发」；mint 形状自己显式发 identity。
			DisableCompression: fp != config.FingerprintMint,
		},
	}
}

// proxyUser 按供应商拼用户名。加新供应商 = 加一个 case。
//
//	novproxy: <user>-region-<CC>[-city-<City>]-sid-<SID>-t-<N>   同 sid 在 N 分钟内固定出口 IP
//	webshare: <user>-<CC>-<数字会话号>                            同号固定出口 IP，粘多久在面板里设
func proxyUser(p config.Proxy, e types.Exit, sid string, mins int) (string, error) {
	if err := validateExit(e); err != nil {
		return "", err
	}
	switch p.Kind {
	case config.KindNovproxy:
		var b strings.Builder
		b.WriteString(p.User)
		if e.Region != "" {
			b.WriteString("-region-")
			b.WriteString(e.Region)
			if e.City != "" {
				b.WriteString("-city-")
				b.WriteString(e.City)
			}
		}
		// 整串 sid 都进去（含前两位的代理下标）：它只影响「哪些请求共享同一个出口 IP」，
		// 而同下标就是同供应商，语义正好。
		fmt.Fprintf(&b, "-sid-%s-t-%d", sid, mins)
		return b.String(), nil
	case config.KindWebshare:
		if e.Region == "" {
			return "", errors.New("webshare 必须指定国家")
		}
		// 没有城市段可用，只能按国家定向（ASN 也能挑，但那要在面板里配，不走用户名）。
		return p.User + "-" + e.Region + "-" + webshareSession(sid), nil
	}
	return "", fmt.Errorf("不认识的代理 kind %q", p.Kind)
}

// webshareSession 把 sid 映成 webshare 要的会话号。
//
// **必须是纯数字**：2026-10-01 实测字母数字和带连字符的会话号在 SOCKS5 认证这一步
// 就被拒（而认证被拒会直接记进熔断计数，把整条供应商打停），纯数字两发同 IP。
// 用 FNV-32 而不是截取 sid：sid 的字符集本来就不是十进制，截出来会撞一堆。
// 同一个 sid 必须永远映到同一个号——铸票和随后那一发 state-echo 靠它共用出口。
func webshareSession(sid string) string {
	h := fnv.New32a()
	_, _ = io.WriteString(h, sid)
	return strconv.FormatUint(uint64(h.Sum32()), 10)
}

// validateExit 卡住能进代理用户名的内容。
//
// 这是 SOCKS5 协议字段的边界：Exit 可能来自 state.json（上一版本写进去的、手改过的），
// 而 ipinfo 的 city 是自由文本（"Frankfurt am Main"、"São Paulo"）。裸空格和非 ASCII
// 进了用户名，要么认证失败、要么库存不足回退到别国 ⇒ 落到别的网关 ⇒ 报成「落点漂移」。
// 所以宁可在这里大声报错，也不要静默送出去。
func validateExit(e types.Exit) error {
	if e.Region != "" && !isISOCountry(e.Region) {
		return fmt.Errorf("出口国家 %q 不是 ISO-2 代码", e.Region)
	}
	if e.City != "" {
		if e.Region == "" {
			// 只有城市没有国家时 novproxy 的 city 段会被整段吞掉，等于悄悄取消定向。
			return fmt.Errorf("出口只给了城市 %q 没给国家，定向会被静默取消", e.City)
		}
		if !isProxyToken(e.City) {
			return fmt.Errorf("出口城市 %q 不是代理面板的 token（只允许字母数字和 -_）", e.City)
		}
	}
	return nil
}

func isISOCountry(s string) bool {
	if len(s) != 2 {
		return false
	}
	for i := 0; i < 2; i++ {
		if c := s[i]; (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
			return false
		}
	}
	return true
}

func isProxyToken(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

// ---------- cookie / JWT ----------

// newCookies 收本次响应下发的 __cflb / __oailb。
func newCookies(resp *http.Response) map[string]string {
	out := map[string]string{}
	for _, ck := range resp.Cookies() {
		switch ck.Name {
		case cookieCflb, cookieOailb:
			if ck.Value != "" {
				out[ck.Name] = ck.Value
			}
		}
	}
	return out
}

// mergePair 把本次新下发的项盖在送进去的 pair 上，输出完整的 cookie 头。
func mergePair(sentPair string, fresh map[string]string) string {
	cur := parsePair(sentPair)
	for k, v := range fresh {
		cur[k] = v
	}
	// 固定 __cflb 在前，便于比对和落盘去重。
	parts := make([]string, 0, 2)
	for _, k := range []string{cookieCflb, cookieOailb} {
		if v := cur[k]; v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	return strings.Join(parts, "; ")
}

func parsePair(pair string) map[string]string {
	out := map[string]string{}
	for _, seg := range strings.Split(pair, ";") {
		k, v, ok := strings.Cut(strings.TrimSpace(seg), "=")
		if ok && v != "" {
			out[k] = v
		}
	}
	return out
}

// pairGateway 从一张完整 cookie 头里读网关名。
func pairGateway(pair string) string {
	gw, _, err := parseOailb(parsePair(pair)[cookieOailb])
	if err != nil {
		return ""
	}
	return gw
}

// parseOailb 解 __oailb 这个 ES256 JWT：网关名 = payload.host 按 "." 分割的第三段。
//
// 不校验签名：我们只是**读**上游给的东西，不自己签发；而且签名密钥在上游手里，
// 校验不了（伪造 __oailb 这条路 2026-10-01 已证伪）。
func parseOailb(token string) (gateway string, exp time.Time, err error) {
	if token == "" {
		return "", time.Time{}, errors.New("空的 __oailb")
	}
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return "", time.Time{}, errors.New("__oailb 不是 JWT")
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("payload 不是 base64url: %w", err)
	}
	var payload struct {
		Host string `json:"host"`
		Exp  int64  `json:"exp"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", time.Time{}, fmt.Errorf("payload 不是 JSON: %w", err)
	}
	seg := strings.Split(payload.Host, ".")
	if len(seg) < 3 || seg[2] == "" {
		return "", time.Time{}, fmt.Errorf("host %q 里没有第三段", payload.Host)
	}
	if payload.Exp > 0 {
		exp = time.Unix(payload.Exp, 0).UTC()
	}
	return seg[2], exp, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// ProxyState 是一条出口供应商此刻的状态，给管理页看。
//
// **不含凭据**：Host 是公开域名可以照打，User 要打码——novproxy 的用户名里
// 嵌着账号标识，整串贴出来等于泄一半凭据（密码一个字节都不出现）。
type ProxyState struct {
	Name      string
	Kind      string
	Host      string
	UserMask  string // 前 4 个字符 + ***
	Enabled   bool
	Sticky    bool
	Fails     int       // 连续失败次数，到 3 熔断
	SkipUntil time.Time // 零值 = 没被熔断
	BoundSIDs int       // 当前钉在这条出口上的 sid 数
}

// ProxyStates 报各出口的状态。顺序同 Proxies，即 sid 前两位编的那个下标。
func (c *Client) ProxyStates() []ProxyState {
	c.mu.Lock()
	defer c.mu.Unlock()
	bound := map[int]int{}
	for _, b := range c.sids {
		bound[b.idx]++
	}
	out := make([]ProxyState, 0, len(c.Proxies))
	for i, p := range c.Proxies {
		out = append(out, ProxyState{
			Name:      p.Name,
			Kind:      string(p.Kind),
			Host:      p.Host,
			UserMask:  maskUser(p.User),
			Enabled:   p.Enabled,
			Sticky:    p.Sticky(),
			Fails:     c.fails[p.Name],
			SkipUntil: c.skipUntil[p.Name],
			BoundSIDs: bound[i],
		})
	}
	return out
}

// maskUser 只留前 4 个字符。按**字符**切，别把一个汉字劈成半个坏字节贴到页面上。
func maskUser(u string) string {
	r := []rune(u)
	if len(r) <= 4 {
		return "***"
	}
	return string(r[:4]) + "***"
}
