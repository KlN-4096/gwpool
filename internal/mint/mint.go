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
	"gwpool/internal/proxycatalog"
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

// ErrNoLanding 是「读不出这一发落到哪儿了」——按失败处理。两种情形：
//   - 没下发新 __oailb，本次又没发 pair
//   - 没下发新 __oailb，发了 pair，但那张的 __cflb 已经不保证还活着 ⇒ 旧名字不作数
//
// 注意第二条的反面才是成功：没下发新 cookie 而送进去那张**确定还活着**，
// 说明上游接受了它、没换网关（SPEC 第 2 节、红线 3）。
var ErrNoLanding = errors.New("上游既没下发 __oailb，本次也没发 pair，读不出落点")

// ErrNoProxy 是「能用的出口一个都没有」。定义搬到了 types（调度要按它分流，
// 见 types.ErrNoExit），这里留个别名，免得改一串调用点。
var ErrNoProxy = types.ErrNoExit

// RateLimited 是速率闸拒绝的结构化形式。
//
// 为什么要个类型而不只是一句话：HTTP 那头要把「还要等多久」变成 Retry-After，
// 而原来这个数只活在错误文案里（"%s 后腾出一发"），取出来只能去 parse 中文。
// Unwrap 回 types.ErrRateLimited ⇒ 所有 errors.Is 的分流照旧成立。
type RateLimited struct {
	Region string
	Limit  int
	Wait   time.Duration // 腾出一发还要多久
}

func (e *RateLimited) Error() string {
	return fmt.Sprintf("%s（区域 %s 每小时上限 %d，%s 后腾出一发）",
		types.ErrRateLimited, e.Region, e.Limit, e.Wait)
}

func (e *RateLimited) Unwrap() error { return types.ErrRateLimited }

// Client 实现 types.Minter。
type Client struct {
	// Proxies 是出口供应商清单（含停用的），sid 绑的就是它的下标，**顺序不要在运行中变**。
	// 构造之后只许经 AddProxy / SetProxyEnabled 改（见 proxyadmin.go），别直接动。
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
	// 代理清单刻意**不**走它：sid 绑的是 Proxies 的下标，整份换掉会让已经发出去的
	// sid 指向另一条出口，铸票和 state-echo 就配不成同一个 IP 了。管理页只能往末尾加、
	// 改启用标志（AddProxy / SetProxyEnabled）；删和改别的字段要改文件再重启。
	Live *config.Live

	// Log 非 nil 时记熔断的开合。**只记这一件事**：别的失败都原路返回给调用方，
	// 由它决定记不记；而熔断是进程内部的状态变化，不记的话事后只能看到
	// 「没有可用的出口代理」，查不出是哪条、为什么、什么时候恢复
	// （2026-10-01 的一次全池 503 就是这么丢掉现场的）。
	Log *slog.Logger

	mu         sync.Mutex
	cursor     int                    // 动态出口轮询游标
	seedCursor int                    // 静态续票出口独立轮询游标
	fails      map[string]int         // 供应商 -> 连续失败次数
	skipUntil  map[string]time.Time   // 供应商 -> 跳到什么时候
	sids       map[string]sidBind     // sid -> 绑定的供应商
	sent       map[string][]time.Time // 账号|区域 -> 过去一小时每一发请求的时刻
}

// rateKey 是速率闸的分桶键：(上游账号 × 区域)。
//
// 区域反查不到（出口不在 types.Regions 里，只可能来自手改的 state.json）时
// 整批挤进同一个空桶：宁可让它们互相挤，也不要每个野出口凭空多出一整份配额。
func (c *Client) rateKey(acct string, e types.Exit) string {
	return acct + "|" + c.Live.Get().RegionKeyOf(e)
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
	key := c.rateKey(acct, e)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sent == nil {
		c.sent = map[string][]time.Time{}
	}
	total, oldest := 0, time.Time{}
	for bucket, times := range c.sent {
		separator := strings.LastIndex(bucket, "|")
		if separator < 0 || bucket[:separator] != acct {
			continue
		}
		for _, at := range times {
			if at.After(cut) {
				total++
				if oldest.IsZero() || at.Before(oldest) {
					oldest = at
				}
			}
		}
	}
	if total >= types.AccountHourLimit(lim) {
		return &RateLimited{Region: "all", Limit: types.AccountHourLimit(lim),
			Wait: oldest.Add(time.Hour).Sub(now).Round(time.Second)}
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
		return &RateLimited{Region: c.Live.Get().RegionKeyOf(e), Limit: lim,
			Wait: kept[0].Add(time.Hour).Sub(now).Round(time.Second)}
	}
	c.sent[key] = append(kept, now)
	return nil
}

// refund 把刚占下的那个窗口名额退回去。
//
// 只给「根本没碰到上游」用（连不上代理、代理认证被拒）：那一发没动账号的任何额度，
// 扣着它只是让这个区域的配额凭空少一发，而配额本来就只有 6。
// 谁调用它由 do 决定——那边已经在区分这几类失败了（熔断只记供应商自己的账），
// 判据只该有一份。
//
// 砍掉的是**最后**一个时间戳而不是精确那一个：单账号并发硬性为 1，所以正常情况下
// 它就是我们刚加的那一发；真撞上了也只是让窗口里某个时间戳差几百毫秒，而这个闸
// 看的是发数不是时刻。
func (c *Client) refund(acct string, e types.Exit) {
	key := c.rateKey(acct, e)
	c.mu.Lock()
	defer c.mu.Unlock()
	if w := c.sent[key]; len(w) > 0 {
		c.sent[key] = w[:len(w)-1]
	}
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

// NewSIDFor 只选择能按指定出口定向的动态供应商，不能把城市要求静默降成国家。
func (c *Client) NewSIDFor(exit types.Exit, sticky bool) (string, error) {
	sid := randSID()
	if _, err := c.resolveFor(sid, sticky, &exit); err != nil {
		return "", err
	}
	return sid, nil
}

// NewSeedSID 开一个绑在静态代理（config.KindSeed）上的 sid，仅给已有票续期用。
//
// 为什么能复用整条铸票路径：do 里那句 resolve(sid, true) 是**绑定优先**的——
// 已经绑过的 sid 直接返回它绑的那条，不再过 Sticky 闸。所以这里先把绑定种下，
// 后面 Geo / Mint 全部自然走这条出口，mintOnce 一行都不用改。
//
// 没配这条代理就回 ErrNoProxy，调用方据此回落到住宅出口。
func (c *Client) NewSeedSID() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for i := 0; i < len(c.Proxies); i++ {
		idx := (c.seedCursor + i) % len(c.Proxies)
		p := c.Proxies[idx]
		if p.Enabled && p.Kind == config.KindSeed && !now.Before(c.skipUntil[p.Name]) {
			sid := randSID()
			c.seedCursor = (idx + 1) % len(c.Proxies)
			c.bindLocked(sid, idx, now)
			return sid, nil
		}
	}
	return "", fmt.Errorf("%w（没有启用着的 kind=%q 出口）", ErrNoProxy, config.KindSeed)
}

// SeedBound 报这个 sid 是不是绑在种子出口上。
//
// 直接查绑定表、**不走 resolve**：resolve 对没绑过的 sid 会顺手挑一条绑上，而这里
// 只是问一句「要不要摘 __oailb」，不该有副作用——那会让一个还没出手的 sid 被钉在
// 轮询游标当时指着的那条出口上。下标越界的判法照抄 resolve 里那一句（配置热加过
// 代理时旧绑定可能指向已经不存在的下标）。
func (c *Client) SeedBound(sid string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.sids[sid]
	return ok && b.idx < len(c.Proxies) && c.Proxies[b.idx].Kind == config.KindSeed
}

func (c *Client) Provider(sid string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.sids[sid]
	if !ok || b.idx < 0 || b.idx >= len(c.Proxies) {
		return ""
	}
	return string(c.Proxies[b.idx].Kind)
}

// SupportsDynamicExit checks capabilities without binding a SID or consuming
// rotation/budget. Temporary proxy backoff is still handled by NewSIDFor.
func (c *Client) SupportsDynamicExit(exit types.Exit) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.Proxies {
		if !p.Enabled || p.Kind == config.KindSeed {
			continue
		}
		if _, err := proxyUser(p, exit, strings.Repeat("0", proxySessionIDLength), c.stickyMin()); err == nil {
			return true
		}
	}
	return false
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
	return c.resolveFor(sid, sticky, nil)
}

func (c *Client) resolveFor(sid string, sticky bool, exit *types.Exit) (config.Proxy, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.sids[sid]; ok && b.idx < len(c.Proxies) {
		p := c.Proxies[b.idx]
		if exit != nil {
			if _, err := proxyUser(p, *exit, sid, c.stickyMin()); err != nil {
				return config.Proxy{}, err
			}
		}
		return p, nil
	}
	now := time.Now()
	n := len(c.Proxies)
	for i := 0; i < n; i++ {
		idx := (c.cursor + i) % n
		p := c.Proxies[idx]
		if !p.Enabled || p.Kind == config.KindSeed ||
			(sticky && !p.Sticky()) || now.Before(c.skipUntil[p.Name]) {
			continue
		}
		if exit != nil {
			if _, err := proxyUser(p, *exit, sid, c.stickyMin()); err != nil {
				continue
			}
		}
		c.cursor = (idx + 1) % n
		c.bindLocked(sid, idx, now)
		return p, nil
	}
	return config.Proxy{}, fmt.Errorf("%w（sticky=%v，共 %d 条，含停用的）", ErrNoProxy, sticky, n)
}

// bindTTL 是一条 sid 绑定有意义的时长：粘性窗口一过出口 IP 本来就会变。
func (c *Client) bindTTL() time.Duration { return 2 * time.Duration(c.stickyMin()) * time.Minute }

// bindLocked 记下绑定，顺手清掉过期的。
// 绑定只需要活过粘性窗口：窗口一过出口 IP 本来就会变，留着也没用。
func (c *Client) bindLocked(sid string, idx int, now time.Time) {
	if c.sids == nil {
		c.sids = map[string]sidBind{}
	}
	if len(c.sids) >= 2000 {
		ttl := c.bindTTL()
		for k, v := range c.sids {
			if now.Sub(v.at) > ttl {
				delete(c.sids, k)
			}
		}
	}
	c.sids[sid] = sidBind{idx: idx, at: now}
}

// Mint 经 e 指定的出口打一发最小 ping。sentPair 为空表示裸打。
//
// sentCflbExp 是送进去那张 pair 的 __cflb 死期（types.Pair.CflbExpiresAt；裸打传零值）。
// 它决定「上游没下发新 cookie」这件事怎么判，见下面那段注释——没有它就只能瞎猜，
// 而猜错是**静默写坏账本**。
func (c *Client) Mint(ctx context.Context, cr types.Creds, e types.Exit, sid, sentPair string,
	sentCflbExp time.Time) (types.MintResult, error) {
	if err := c.spend(cr.AccountID, e); err != nil {
		return types.MintResult{}, err
	}
	// 种子出口上**必须**只送 `__cflb`，见 CflbOnly。钉在这里而不是交给调用方：
	// 违反它不报错，只会静默白打一发（两件齐送上游一个 cookie 都不回 ⇒ 落点读不出
	// ⇒ 这条路唯一想要的那个读数没了），这种错误没人会发现。
	cookie := sentPair
	if c.SeedBound(sid) {
		cookie = CflbOnly(sentPair)
	}
	body, header := buildWire(c.fingerprint(), c.model(), cr, cookie, "")
	req, err := c.newRequest(ctx, http.MethodPost, c.upstream(), body, header)
	if err != nil {
		return types.MintResult{}, err
	}
	resp, err := c.do(req, e, sid, cr.AccountID)
	if err != nil {
		return types.MintResult{}, err
	}
	// 只读响应头就断：body 是 SSE 流，读完它要等上游把整轮推完，白等几十秒。
	defer func() { _ = resp.Body.Close() }()

	// Max-Age 是**相对收到响应的时刻**算的，所以时刻要在这儿取，别拖到后面。
	at := time.Now().UTC()
	res := types.MintResult{
		Status:    resp.StatusCode,
		TurnState: resp.Header.Get(stateHeader),
	}
	if resp.StatusCode/100 != 2 {
		// 非 2xx 的体是一小段 JSON，不是 SSE 流，读它不会挂住。
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<10))
		res.ErrBody = strings.TrimSpace(string(b))
		res.RetryAfter = retryAfterOf(resp.Header.Get("Retry-After"), at)
	}
	fresh := newCookies(resp)
	if oailb := fresh[cookieOailb]; oailb != nil {
		gw, exp, err := parseOailb(oailb.Value)
		if err != nil {
			return res, fmt.Errorf("解 __oailb: %w", err)
		}
		res.Gateway = gw
		res.PairExpires = exp
		// 钉路由的是 __cflb（见 cookieCflb 和 types.PairLifetime），而它比 __oailb
		// 早死 300 秒 ⇒ 它的死期必须单独带出去。只按 __oailb 算寿命的话，
		// 「刚好够交付」那张票的路由其实已经死了，亲和失效、落点重新分配。
		res.CflbExpires = cookieDeadline(fresh[cookieCflb], at)
		// 上游常只补发变动的那一项。只拿新的那半会丢掉钉路由的 __cflb，
		// 所以在送进去的 pair 上**覆盖**，而不是替换。
		//
		// 覆盖在 cookie 上而不是 sentPair 上：种子那条路已经把 __oailb 摘掉了，
		// 而摘掉的那张**指向别人的网关**，合回来就是给这张新票贴一个错的落点。
		res.NewPair = mergePair(cookie, fresh)
		return res, nil
	}
	if cookie != "" {
		// 「没下发新 cookie」对应两件**相反**的事，判据是送进去那张的 __cflb 还活着没：
		//
		//   活着 ⇒ 上游接受了它、路由没变 ⇒ 落点就是旧名字（SPEC 第 2 节）
		//   死了 ⇒ 亲和已经失效，这一发按出口地理随机路由走了，而 __oailb 还能读出
		//          那个**过期的**名字（实验 D：只带 __oailb ⇒ 无新 cookie、读不出落点）
		//
		// 无条件认旧名字会静默写坏三处账本：触碰标在没打到的网关上（真正打到的那个
		// 仍是「从没碰过」，下次当首次接触发给别人）、定向铸票的漂移永远检测不出来、
		// 续期的失败预算永远攒不满（死票不会被丢，每轮还重发）。
		//
		// **判据必须「可证」**：要求死期晚于**响应时刻**。响应时刻之后才死 ⇒ 整个
		// 往返期间它都活着。读不出死期（零值）一律按死了算——猜错的代价不对称。
		if sentCflbExp.After(at) {
			res.Gateway = pairGateway(cookie)
			if res.Gateway == "" {
				// 种子那条路会落到这儿：只送了 `__cflb`，旧名字根本不在 cookie 里，
				// 而旧名字本来也是**别人的**网关、不能当自己的落点。上游接受了种子、
				// 路由钉在那个大区了，但落到哪个网关读不出来 ⇒ 这一发对「摸落点」零产出。
				// 必须报 ErrNoLanding，不能静默当成功（那会把一张没有落点的票写进账本）。
				return res, fmt.Errorf("%w（cookie 里没有能读出落点的 __oailb）", ErrNoLanding)
			}
			return res, nil
		}
		return res, fmt.Errorf("%w（送进去那张的 __cflb 已经不保证还活着，旧名字不能当落点）", ErrNoLanding)
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
	defer func() { _ = resp.Body.Close() }()

	// 非 200 不下结论：429/403 的响应头里本来就没有 turn-state，
	// 照「无新 state = 满血」读就是假阳（作者原版的漏洞，实测 12 发撞出 2 个）。
	if resp.StatusCode != http.StatusOK {
		return nil, nil
	}
	newState := resp.Header.Get(stateHeader)
	v := &types.Verdict{Status: resp.StatusCode}
	switch newState {
	case "":
		v.Full, v.Note = true, "未下发新 state"
	case ticket:
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
	ctx, cancel := context.WithTimeout(ctx, geoTimeout)
	defer cancel()
	req, err := c.geoRequest(ctx)
	if err != nil {
		return types.Observed{}, err
	}
	resp, err := c.do(req, e, sid, "")
	if err != nil {
		return types.Observed{}, err
	}
	return readGeo(resp)
}

// geoRequest 是查出口地理的那一发 GET：打的是 ipinfo，不是上游。
func (c *Client) geoRequest(ctx context.Context) (*http.Request, error) {
	url := c.GeoURL
	if url == "" {
		url = defaultGeoURL
	}
	return c.newRequest(ctx, http.MethodGet, url, nil, nil)
}

// readGeo 读 ipinfo 的回答，读完关掉响应体。
func readGeo(resp *http.Response) (types.Observed, error) {
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return types.Observed{}, fmt.Errorf("查 geo 返回 HTTP %d", resp.StatusCode)
	}
	var body struct {
		IP      string `json:"ip"`
		Country string `json:"country"`
		State   string `json:"region"`
		City    string `json:"city"`
		Org     string `json:"org"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&body); err != nil {
		return types.Observed{}, fmt.Errorf("解 geo 响应: %w", err)
	}
	if body.IP == "" {
		return types.Observed{}, errors.New("geo 响应里没有 ip")
	}
	obs := types.Observed{IP: body.IP, Country: body.Country, State: body.State, City: body.City}
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
	p, err := c.resolveFor(sid, true, &e)
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
		// 这一发**根本没碰到上游**（连接都没建起来），不该花这个账号的配额。
		// 和它刻意分开的是上面两支：隧道建起来又断、以及调用方取消——那两种
		// 请求都可能已经落到上游了，退了就是低报，而这个闸是防封号的安全阀。
		c.refund(acct, e)
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
	// 种子出口的用户名**原样是 p.User**，一个外来字节都不进去，所以它不过 validateExit：
	// 那个闸是「Exit 的内容要拼进 SOCKS5 用户名」这件事的边界，而这条路上 Exit 只用来
	// 分速率闸的桶和记 RegionKey。放在闸前面而不是 switch 里的一个 case，是为了让这件事
	// 在代码上就是显然的：种子出口和 Exit 无关，别让下一个人以为它也要国家合法。
	if p.Kind == config.KindSeed {
		return p.User, nil
	}
	if err := validateExit(e); err != nil {
		return "", err
	}
	switch p.Kind {
	case config.KindNovproxy:
		if e.State != "" || (e.City != "" && !isProxyToken(e.City)) {
			return "", errors.New("novproxy 不支持州参数，城市必须使用面板的 ASCII token")
		}
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
		// 整串 sid 都进去：它只影响「哪些请求共享同一个出口 IP」，而同一个 sid
		// 永远绑在同一条供应商上（见 resolve），语义正好。
		fmt.Fprintf(&b, "-sid-%s-t-%d", sid, mins)
		return checkedProxyUser(b.String())
	case config.KindWebshare:
		if e.Region == "" {
			return "", errors.New("webshare 必须指定国家")
		}
		if e.State != "" || e.City != "" {
			return "", errors.New("webshare 只支持国家定向，不能指定州或城市")
		}
		// 没有城市段可用，只能按国家定向（ASN 也能挑，但那要在面板里配，不走用户名）。
		return checkedProxyUser(p.User + "-" + e.Region + "-" + webshareSession(sid))
	case config.KindCliproxy:
		if e.City != "" {
			return "", errors.New("cliproxy 尚无已确认的城市参数，不能静默忽略城市")
		}
		if !proxycatalog.CliproxySupports(e.Region, e.State) {
			return "", errors.New("cliproxy 目录中没有此国家/州组合")
		}
		country := e.Region
		if country == "" {
			country = "Rand"
		}
		location := "-region-" + country
		if e.State != "" {
			location += "-st-" + e.State
		}
		return checkedProxyUser(fmt.Sprintf("%s%s-sid-%s-t-%d", p.User, location, sid, mins))
	case config.KindIpipgo:
		var b strings.Builder
		b.WriteString(p.User)
		if e.Region != "" {
			b.WriteString("-country-")
			b.WriteString(e.Region)
		}
		if e.State != "" {
			b.WriteString("-region-")
			b.WriteString(e.State)
		}
		if e.City != "" {
			if e.State == "" {
				return "", errors.New("ipipgo 城市定向必须同时指定州/省")
			}
			b.WriteString("-city-")
			b.WriteString(e.City)
		}
		fmt.Fprintf(&b, "-session-%s-time-%d", sid, mins)
		return checkedProxyUser(b.String())
	}
	return "", fmt.Errorf("不认识的代理 kind %q", p.Kind)
}

const maxProxyUsernameBytes = 255

func checkedProxyUser(user string) (string, error) {
	if len(user) > maxProxyUsernameBytes {
		return "", errors.New("地区与会话参数拼接后的代理用户名超过 255 字节")
	}
	return user, nil
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
	if e.State != "" {
		if e.Region == "" || !types.ValidLocationName(e.State) {
			return errors.New("州/省定向必须带国家，且使用合法的供应商目录值")
		}
	}
	if e.City != "" {
		if e.Region == "" {
			// 只有城市没有国家时 novproxy 的 city 段会被整段吞掉，等于悄悄取消定向。
			return fmt.Errorf("出口只给了城市 %q 没给国家，定向会被静默取消", e.City)
		}
		if !types.ValidLocationName(e.City) {
			return errors.New("出口城市不是合法的供应商目录值")
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

// retryAfterOf 解 Retry-After：RFC 7231 允许「秒数」和「HTTP-date」两种写法，
// 上游两种都用过，只认秒数会在它发日期时静默回落到默认时长。
// 负数和解不开的一律回 0 = 「上游没给」，钳上限的事归调用方（见 sched.acctHoldMax）。
func retryAfterOf(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil {
		if n <= 0 {
			return 0
		}
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

// newCookies 收本次响应下发的 __cflb / __oailb。
// newCookies 留着整个 *http.Cookie 而不只是值：__cflb 的死期要从它的 Max-Age 读
// （见 cookieDeadline），而那是钉路由那个 cookie 唯一能读出时间的地方。
func newCookies(resp *http.Response) map[string]*http.Cookie {
	out := map[string]*http.Cookie{}
	for _, ck := range resp.Cookies() {
		switch ck.Name {
		case cookieCflb, cookieOailb:
			if ck.Value != "" {
				out[ck.Name] = ck
			}
		}
	}
	return out
}

// cookieDeadline 把一个 Set-Cookie 的生存期换成绝对时刻。
//
// Max-Age 优先（上游给的就是它），其次 Expires；都没给就回零值 = 「这一发没说」，
// 由调用方兜底（types.MintResult.RouteDeadline）。负的 Max-Age 按 RFC 6265 是
// 「立刻删掉」⇒ 当场就算死。
func cookieDeadline(ck *http.Cookie, now time.Time) time.Time {
	switch {
	case ck == nil:
		return time.Time{}
	case ck.MaxAge > 0:
		return now.Add(time.Duration(ck.MaxAge) * time.Second)
	case ck.MaxAge < 0:
		return now
	case !ck.Expires.IsZero():
		return ck.Expires.UTC()
	}
	return time.Time{}
}

// mergePair 把本次新下发的项盖在送进去的 pair 上，输出完整的 cookie 头。
func mergePair(sentPair string, fresh map[string]*http.Cookie) string {
	cur := parsePair(sentPair)
	for k, ck := range fresh {
		cur[k] = ck.Value
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

// CflbOnly 把一张完整 pair 砍成只剩 `__cflb`，没有 `__cflb` 就回空串。
//
// 这是「带种子票铸票」的出站形态，而且**必须**摘掉 `__oailb`：
//   - 两件齐送时上游一个 cookie 都不回（2026-10-02 实测），落点读不出来 ⇒ 这一发
//     等于白打，因为我们要的正是「这个号在那个大区落到哪」这个读数
//   - 只送 `__cflb` 时上游必回新 `__oailb` ⇒ 落点现场可读（2026-10-03，3/3）
//
// 大区跟着 `__cflb` 走，而具体网关是 (账号 × 大区) 决定的：三个 free 号带同一张
// east-asia 票从同一个机房出口打，分别落到 unified-88 / 94 / 75，裸打基线是
// 146 / 120 / 128。所以这张票是**路由种子**，不是「别人的网关」。
func CflbOnly(pair string) string {
	v := parsePair(pair)[cookieCflb]
	if v == "" {
		return ""
	}
	return cookieCflb + "=" + v
}

// CflbOf 取一张 pair 里的 `__cflb` 值，没有就回空串。
func CflbOf(pair string) string { return parsePair(pair)[cookieCflb] }

// CflbLease 是某个大区现有的、最长寿的那张 `__cflb`。
type CflbLease struct {
	Cflb string
	Exp  time.Time
}

// BestCflb 按大区挑出最长寿的那张 `__cflb`。只看 `CflbExpiresAt`，不解 JWT。
func BestCflb(pairs map[string]types.Pair, now time.Time) map[string]CflbLease {
	out := map[string]CflbLease{}
	for _, p := range pairs {
		if p.RegionKey == "" || !p.CflbExpiresAt.After(now) {
			continue
		}
		c := CflbOf(p.Cookie)
		if c == "" {
			continue
		}
		if cur, ok := out[p.RegionKey]; !ok || p.CflbExpiresAt.After(cur.Exp) {
			out[p.RegionKey] = CflbLease{Cflb: c, Exp: p.CflbExpiresAt}
		}
	}
	return out
}

// Rehome 把一张票的 `__cflb` 换成同大区最长寿的那张，寿命跟着延长。
//
// 为什么可以换：**有效的 `__oailb` 自己就钉死网关**（2026-10-03 pro1 实测，SPEC 第 2 节），
// `__cflb` 只决定大区。种子铸票时一个大区的好几张票共用同一张种子 cflb ⇒ 种子一到点
// 它们一起死，而各自的 `__oailb` 还有命（3900s > 3600s，而且是种子中途才长出来的）。
// 换一张就把那部分寿命救回来，一发上游都不用打。
//
// 三条红线：
//   - **果实过期就不换，原样返回**。果实一死落点就退回大区规则 ⇒ 这张票会把消费方送到
//     他自己在那个大区的网关，而票上写的还是原来那个 ⇒ 降智票当满血票发出去。
//   - **只在读的时候算，绝不写回库里**。`types.CookieVersion` 是整串 cookie 的哈希，
//     换了 `__cflb` 票号就变，而 `/release`、`/pair/renew` 的归属闸押的是
//     `Slot.LastDeliverVersion`（交付时真正发出去那一份）。在取票那一刻换、把换过的
//     那一份记下去，两边自洽；在别处偷偷改库，所有在途的票号全部失配。
//   - `RegionKey` 为空的老记录原样走，不猜大区。
func Rehome(p types.Pair, best map[string]CflbLease, now time.Time) types.Pair {
	if p.Cookie == "" || p.RegionKey == "" {
		return p
	}
	b, ok := best[p.RegionKey]
	if !ok || !b.Exp.After(p.CflbExpiresAt) {
		return p
	}
	_, oailbExp, err := PairInfo(p.Cookie)
	if err != nil || !oailbExp.After(now) {
		return p
	}
	swapped := SwapCflb(p.Cookie, b.Cflb)
	if swapped == p.Cookie {
		return p
	}
	p.Cookie = swapped
	p.CflbExpiresAt = b.Exp
	p.ExpiresAt = b.Exp
	if oailbExp.Before(p.ExpiresAt) {
		p.ExpiresAt = oailbExp
	}
	return p
}

// SwapCflb 把 pair 的 `__cflb` 换成 cflb，`__oailb` 原样留着。
//
// 为什么这么做是对的（2026-10-03 pro1 实测，见 SPEC 第 2 节）：**有效的 `__oailb`
// 自己就钉死网关**，`__cflb` 只在没有有效 `__oailb` 时才起作用（那时按大区 × 账号定）。
// 所以同一个大区内换哪张 `__cflb` 都不影响落点，只影响这张票还能活多久。
//
// 换之前**必须**确认 `__oailb` 没过期（`PairInfo` 读得出 exp）：果实一死，落点就退回
// 大区规则 ⇒ 这张票会把消费方送到他自己在那个大区的网关，而票上写的还是原来那个
// ⇒ 降智票当满血票发出去。调用方见 `sched.rehome`。
func SwapCflb(pair, cflb string) string {
	kv := parsePair(pair)
	if cflb == "" || kv[cookieOailb] == "" {
		return pair
	}
	kv[cookieCflb] = cflb
	parts := make([]string, 0, 2)
	for _, k := range []string{cookieCflb, cookieOailb} {
		if v := kv[k]; v != "" {
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
	gw, _, err := PairInfo(pair)
	if err != nil {
		return ""
	}
	return gw
}

// PairInfo 读一张**外面交进来的** cookie 头里的两件事实：落点（__oailb 的 host
// 第三段）和 __oailb 自己的硬过期。
//
// 导出是给 /pair/renew 用的：消费端自己续出来的那张要在收下之前验完整性
// （落点必须就是那张票所在的网关）并算寿命，而 JWT 解析只该有这一份。
//
// **缺 __cflb 也报错**：钉路由的是它（见 cookieCflb），没有它那张 cookie
// 一个网关都钉不住，收下来只会在账本上记一张假票。
func PairInfo(pair string) (gateway string, oailbExp time.Time, err error) {
	kv := parsePair(pair)
	if kv[cookieCflb] == "" {
		return "", time.Time{}, errors.New("缺 " + cookieCflb + "，这张 cookie 钉不住任何网关")
	}
	return parseOailb(kv[cookieOailb])
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
	BoundSIDs int       // 粘性时限内钉在这条出口上的 sid 数（停用后降到 0 = 在用的都用完了）
}

// ProxyStates 报各出口的状态。顺序同 Proxies，即 sid 绑定的那个下标。
func (c *Client) ProxyStates() []ProxyState {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 只数还在粘性时限内的绑定：过期的绑定要攒满 2000 条才清，全数进去的话，
	// 一条停用的出口要挂好几个小时才降到 0，看不出在用的会话用完没有。
	now, ttl := time.Now(), c.bindTTL()
	bound := map[int]int{}
	for _, b := range c.sids {
		if now.Sub(b.at) <= ttl {
			bound[b.idx]++
		}
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
