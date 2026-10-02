package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gwpool/internal/mint"
	"gwpool/internal/sched"
	"gwpool/internal/types"
)

// 这一份是 /cookie 和 /release 的对外协议：请求参数的校验、错误码、归还。
//
// **参数名、JSON 字段名、错误码三样是和消费端冻结的**（2026-10-02 契约，sub2api 侧
// 照同一份实现）。改一个字就是改协议，不是重构。

// 错误码闭集。十一个，不多不少。
//
// 为什么要码而不只是一句人话：客户端那头只看到一个 503，而「你的槽位全在冷却里」
// （等几小时）和「出口全熔断了」（等几十秒）要做的事完全相反。
// 文案归调用方 i18n，码归这边。
const (
	codeNoGateway        = "no_gateway"        // 池子里一个网关都没有（冷启动）
	codeAllCooling       = "all_cooling"       // 你的槽位全在冷却里
	codeNoLivePair       = "no_live_pair"      // 没有活票，且现铸没成
	codeRateLimited      = "rate_limited"      // 撞速率闸
	codeNoExit           = "no_exit"           // 没有可用出口代理（整池故障）
	codeUpstreamRejected = "upstream_rejected" // 上游（OpenAI）否决了池子的账号：400/401/403
	codeMintFailed       = "mint_failed"       // 其他铸票失败
	codePublicClosed     = "public_closed"     // 匿名：此刻不对外开放
	codeDegraded         = "degraded"          // 验证开着且判了降级
	// codeConsumerRejected 是**消费端自己的** key 不对/被吊销，和 upstream_rejected
	// 不是一件事（那条说的是池子的上游被 OpenAI 否决）。
	//
	// 必须有自己的码：没有码的 401 会让调用方掉进通用路径、不退避，于是
	// 「每发推理请求敲一次池子」无限敲下去——而这种错要人去改配置，敲得再勤也不会好。
	codeConsumerRejected = "consumer_rejected"
	// codeBadRequest 是「你的请求错了」：参数不合法、管理员没带 account、
	// 点名的账号不存在（400 和 404 合成一个码——对调用方是同一件事）。
	//
	// 闭集不许留洞：没有它的话调用方的 default 分支要同时兜「我配错了」和
	// 「池子暂时没货」两件语义**相反**的事。**不给 retry_after**：重试不会变好。
	codeBadRequest = "bad_request"
)

// consumerRejectedRetry 是 consumer_rejected 的重试建议：要人去改配置，敲勤了也没用。
const consumerRejectedRetry = 5 * time.Minute

// publicClosedRetry 是 public_closed 的重试建议。
//
// 开放状态是现算的（空闲网关数 ≥ 门槛），而供给是秒级变化的 ⇒ 一分钟后再问一次
// 是合理的节奏，而且这条路上一发上游都没花，让它来得勤一点不花钱。
const publicClosedRetry = time.Minute

// apiError 是失败响应的 error 字段。
//
// code 留空的只剩 /release 的 409（太晚 / 票对不上 / 已经还过了）：那不是「请求错了」
// 也不是一次交付失败，而调用方对它要做的事只有「记一行日志走人」——
// sub2api 侧连响应体都不读（只看状态码不是 204）。别为它编一个码。
type apiError struct {
	Code              string `json:"code,omitempty"`
	Message           string `json:"message"`
	RetryAfterSeconds int    `json:"retry_after_seconds,omitempty"`
}

// writeFail 回写失败。带 retry_after_seconds 时同时发 Retry-After 头：
// 标准头让不认识这份协议的中间件也能照着退避。
func writeFail(w http.ResponseWriter, status int, e apiError) {
	if e.RetryAfterSeconds > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(e.RetryAfterSeconds))
	}
	writeJSON(w, status, map[string]apiError{"error": e})
}

// badRequest 是「你的请求错了」：400 和 404 共用一个码，一律不给 retry_after。
//
// **不记卡片，但要记日志。** 不进环是因为参数校验排在鉴权前面（信任边界就该在最前），
// 未鉴权的请求往那个 200 条的环里灌几下就能把真实诊断全挤出去；而三处都没有记录的话
// 「消费端每一发都带着畸形参数」和「池子没货」在运营眼里长得一模一样——偏偏这两种
// 是最可能的开局故障。鉴权通过之后的 400/404 走 badRequestFor（那条进环）。
func (s *Server) badRequest(w http.ResponseWriter, status int, msg string) {
	s.opt.Log.Info("请求参数不合法", "code", codeBadRequest, "status", status, "reason", msg)
	writeFail(w, status, apiError{Code: codeBadRequest, Message: msg})
}

// badRequestFor 是**鉴权通过之后**的 400/404：同一个码，另外记一行卡片。
// 管理员少带 account、点名的账号不存在这类错只有在页面上看得见才查得动。
func (s *Server) badRequestFor(w http.ResponseWriter, status int, accountID int64, msg string) {
	s.fails.add(deliverFail{At: time.Now(), AccountID: accountID, Code: codeBadRequest, Reason: msg})
	s.badRequest(w, status, msg)
}

// unauthorized 是「你的 key 不认」。三个机器端点（/cookie、/release、/gateways）
// 共用它：三条路同一把钥匙，分类当然也该是同一个。
//
// **只进日志不进环**：这条路按定义是未鉴权的，进环就是给任何人一个挤掉诊断的开关。
// 而「谁的 key 配错了在狂敲」这个读数日志里有（它本来就要 SSH 才查得到的那类事）。
func (s *Server) unauthorized(w http.ResponseWriter, msg string) {
	s.opt.Log.Info("消费端鉴权失败", "code", codeConsumerRejected, "reason", msg)
	writeFail(w, http.StatusUnauthorized, apiError{Code: codeConsumerRejected,
		Message: msg, RetryAfterSeconds: retrySeconds(consumerRejectedRetry)})
}

// publicClosed 是「此刻不对外开放」。两个直接拒掉匿名的入口（/cookie 的匿名支、
// /gateways）和 DeliverAnonymous 回来那条共用一个码，也共用这一行卡片——
// 运营判断门槛设得对不对，看的就是这个码出现的密度。
func (s *Server) publicClosed(w http.ResponseWriter, msg string) {
	s.fails.add(deliverFail{At: time.Now(), Code: codePublicClosed, Reason: msg})
	writeFail(w, http.StatusServiceUnavailable, apiError{Code: codePublicClosed,
		Message: msg, RetryAfterSeconds: retrySeconds(publicClosedRetry)})
}

// retrySeconds 把时长换成「至少 1 秒」的整数秒。向上取整：报小了客户端会提前回来，
// 而它回来那一下可能又要花一发上游。
func retrySeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int((d + time.Second - 1) / time.Second)
}

// ---------- 请求参数 ----------

const (
	// maxExcludeItems 封住两个排除名单的项数。64 项 ≈ 四倍于「已知网关数」的量级，
	// 够用；上限的意义是别让一个请求参数变成任意长的循环。
	maxExcludeItems = 64
	// maxExcludeItemLen 和 validName 的上限一致：比它长的名字不可能是网关名。
	maxExcludeItemLen = 64

	// min_remaining 的钳位区间。
	//
	// 下限 30 s 就是 sched 的 minDeliverWindow：比这还短的票发出去没有意义。
	// 上限 3600 s 就是一张票的整个寿命（types.PairLifetime），再大必然永远筛不出候选。
	// 数值本身是冻结的契约（SPEC 第 10 节写着钳到 [30, 3600]），别跟着常量改名动它。
	minRemainingFloor = 30 * time.Second
	minRemainingCap   = 3600 * time.Second

	// wait 的钳位上限。30 s 是「一次 HTTP 请求等得起」的量级；
	// 真正长的等待（冷却是几小时）**永远不等**，见 waitableCode。
	maxWait = 30 * time.Second
)

// parseCookieRequest 是 /cookie 的信任边界：查完长度和范围，往后各层直接信。
//
// 第二个返回值是调用方愿意等多久（wait），它只活在 HTTP 这一层，不进调度。
func parseCookieRequest(r *http.Request) (sched.Request, time.Duration, int, error) {
	q := r.URL.Query()
	req := sched.Request{Force: truthy(q.Get("force"))}

	if gw := q.Get("gateway"); gw != "" {
		if !validName(gw) {
			return req, 0, 0, errors.New("gateway 名字不合法")
		}
		req.Gateway = gw
	}

	// 认不出的名字静默忽略：调用方那本账可能记着池子早就忘掉的网关，
	// 为此拒掉整个请求等于让它去猜池子的状态。
	ex, err := parseList(q.Get("exclude"), "exclude")
	if err != nil {
		return req, 0, 0, err
	}
	req.Exclude = ex
	vs, err := parseList(strings.ToLower(q.Get("exclude_versions")), "exclude_versions")
	if err != nil {
		return req, 0, 0, err
	}
	req.ExcludeVersions = vs

	if v := q.Get("min_remaining"); v != "" {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return req, 0, 0, errors.New("min_remaining 不是整数秒")
		}
		req.MinRemaining = clampDur(time.Duration(n)*time.Second, minRemainingFloor, minRemainingCap)
	}

	wait := time.Duration(0)
	if v := q.Get("wait"); v != "" {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return req, 0, 0, errors.New("wait 不是整数秒")
		}
		wait = clampDur(time.Duration(n)*time.Second, 0, maxWait)
	}

	count := 1
	if v := q.Get("count"); v != "" {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return req, 0, 0, errors.New("count 不是整数")
		}
		// **拒而不钳**，和 min_remaining / wait 刻意不同口径：那两个钳的是时长，钳过头只是
		// 建议值差一点；count 决定这一发烧掉几个 (账号 × 网关)，把 20 悄悄钳成 5 会让调用方
		// 一直以为自己拿到了 20 张、而「少拿的那 15 张去哪了」查不出来。
		if n < 1 || n > cookieBatchMax {
			return req, 0, 0, fmt.Errorf("count 要在 1..%d 之间", cookieBatchMax)
		}
		count = n
	}
	// 点名了网关就只可能有一张：那个参数的意思是「我要这一个落点」。
	// 静默收到 1 而不是报错：带着 gateway 和 count 一起来是「顺带多拿几张」的合理写法，
	// 为此拒掉整个请求没有收益（响应里的张数自己会说清拿到了几张）。
	if req.Gateway != "" {
		count = 1
	}
	return req, wait, count, nil
}

// parseList 解一个逗号分隔的名单。项数超限就拒（那是畸形请求，不是口味），
// 单项超长的静默丢掉（它不可能匹配任何东西，而拒掉整个请求没有好处）。
func parseList(raw, name string) (map[string]bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	// 先数逗号再切：Split 一个几兆的 URL 参数会先分配出几十万个 string，
	// 而它注定要被下一行拒掉。
	if strings.Count(raw, ",") >= maxExcludeItems {
		return nil, fmt.Errorf("%s 最多 %d 项", name, maxExcludeItems)
	}
	parts := strings.Split(raw, ",")
	out := make(map[string]bool, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || len(p) > maxExcludeItemLen {
			continue
		}
		out[p] = true
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func clampDur(d, lo, hi time.Duration) time.Duration {
	if d < lo {
		return lo
	}
	if d > hi {
		return hi
	}
	return d
}

// ---------- 失败分类 ----------

// classify 把调度那边的错误翻成闭集里的一个码、一句人话、和一个重试建议。
//
// 顺序就是**从内到外**：一个错误可能同时裹着好几层（ErrNoFullSlot 包着
// ErrMintFailed 包着 ErrRateLimited），而最里面那层才是真正的原因。
// 细节一个字都不进 message：上游的错误串里可能裹着请求头之类的东西（SPEC 红线 1）。
func (s *Server) classify(err error) (code, msg string, retry time.Duration) {
	var rl *mint.RateLimited
	var cool *sched.CoolingError
	switch {
	case errors.Is(err, sched.ErrPublicClosed):
		return codePublicClosed, "池子此刻不对外开放", publicClosedRetry
	case errors.As(err, &rl):
		return codeRateLimited, "这个账号这一小时的上游请求用完了，等窗口滑过去", rl.Wait
	case errors.Is(err, types.ErrRateLimited):
		// 没带时长的速率闸（测试、或将来别处抛的）：有码没建议，总比没码好。
		return codeRateLimited, "这个账号这一小时的上游请求用完了，等窗口滑过去", 0
	case errors.Is(err, types.ErrNoExit):
		return codeNoExit, "没有可用的出口代理（整池故障）", s.proxySkipLeft()
	case errors.As(err, &cool):
		return codeAllCooling, "池子此刻正在你的所有候选网关上烧满血窗口，稍后再来", cool.Wait
	case errors.Is(err, sched.ErrDegraded):
		return codeDegraded, "交付前的满血验证判了降级，不发降智的票", types.FullWindow
	case errors.Is(err, sched.ErrUpstreamRejected):
		return codeUpstreamRejected, "上游否决了池子的这个账号", 0
	case errors.Is(err, sched.ErrAccountUnusable):
		// 「你点的这个号用不了」正好是 bad_request 的语义（请求错了，改了再来），
		// 而**不是**「池子暂时没货」：后者会让调用方挂满 wait、还去排一次发现扫描。
		// 不给 retry_after：等多久那个号都还是停用的。
		return codeBadRequest, "点名的这个账号用不了（不存在、已停用、或凭据已死）", 0
	case errors.Is(err, sched.ErrNoLivePair):
		// 必须排在 mint_failed **前面**：这一格是「候选非空、只是没有活票，
		// 而现铸也没成」，而契约里 no_live_pair 的定义就是这句话。它等得起
		// （别人铸出票来之后那次重试一发上游都不用打），mint_failed 等不起。
		return codeNoLivePair, "没有可交付的活票（现铸也没成），稍后再试", 0
	case errors.Is(err, sched.ErrMintFailed):
		return codeMintFailed, "铸票失败，稍后再试", 0
	case len(s.st.Gateways()) == 0:
		// 冷启动：池子一个落点都没摸到。和「你的槽位烧完了」要做的事不一样——
		// 前者等发现扫描，后者等冷却。
		return codeNoGateway, "池子里还没有任何网关，等发现扫描摸到落点", 0
	default:
		return codeNoLivePair, "没有可交付的活票，稍后再试", 0
	}
}

// waitableCode 报告这个码等一会儿有没有可能变好。
//
// **只有这一个。** all_cooling 2026-10-03 之后已经降到 183 秒量级（消费冷却那条闸删了，
// 见 sched.burning），理论上等得起 —— 但仍然不挂：那 183 秒里池子正拿这个号的凭据打上游，
// 等满了也只是**可能**有票，而一条连接挂三分钟的代价是确定的。retry_after 已经如实报了
// 真实剩余时间（原先报的是 7200 秒），客户端按它重试比我们替它挂着便宜。
// rate_limited 同理（滑窗最长一小时）；upstream_rejected 等多久都一样。
//
// no_gateway 也不等，而且是**结构性**的空等：它的定义就是「网关表是空的」，而判据
// 要求命中的票属于表里的某个网关 ⇒ 恒为假、必然等满；能让表非空的发现扫描恰好要等
// 这个请求失败完才入队（单账号模式连保温都不跑）。立刻把这个码交回调用方更有用。
func waitableCode(code string) bool {
	return code == codeNoLivePair
}

// proxySkipLeft 是最早一条出口出熔断还要多久。没有熔断中的出口（或没接出口状态）回 0。
func (s *Server) proxySkipLeft() time.Duration {
	if s.opt.ProxyStates == nil {
		return 0
	}
	now := time.Now()
	best := time.Duration(0)
	for _, p := range s.opt.ProxyStates() {
		if !p.Enabled {
			continue
		}
		if left := p.SkipUntil.Sub(now); left > 0 && (best == 0 || left < best) {
			best = left
		}
	}
	return best
}

// ---------- 等票 ----------

// waitPoll 是等票的轮询间隔。
//
// 500 ms 够钝：等票这条路一天走不了几次，为它造通知机制（条件变量、channel 广播）
// 等于为一个不存在的热点加一份要维护的并发状态。
const waitPoll = 500 * time.Millisecond

// waitForCandidate 等到池子里出现一张这次请求收得下的票，或者超时 / 调用方断开。
//
// 返回「等到了吗」。**等不到就不重跑**：重跑一次选择可能真打一发上游，而
// 「轮询期间什么都没变」的情况下那一发必然重复刚才那次失败，纯浪费一个槽位。
// 等到了才重跑，那一次大概率一发不打（硬筛直接命中新出现的票）。
// 判据**不在这一层**：它要用 accountID 应用那两条冷却闸，而冷却是调度的事
// （见 sched.HasCandidate）。这边重写一份的后果不是「略有偏差」，而是每一次等待
// 都看见一张自己拿不到的票、立刻重跑、白铸一发。
func (s *Server) waitForCandidate(ctx context.Context, d time.Duration, accountID int64, req sched.Request) bool {
	deadline := time.Now().Add(d)
	t := time.NewTicker(waitPoll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			if s.pool.HasCandidate(accountID, req) {
				return true
			}
			if !time.Now().Before(deadline) {
				return false
			}
		}
	}
}

// cookieBatchMax 是一次 /cookie 最多下发几张票（count 参数的上限，用户定的 5）。
//
// 为什么要有批量：客户端那头的流程是「取一张 → 自己验满血 → 不满血再取一张」，每一轮
// 都是一个 HTTP 往返，而池子内部还可能顺带现铸一发（几十秒）。它自己能学出平均要试几次
// （比如 3），一次把 3 张要走、并发验，等待时间从 3 个往返压成 1 个 —— 减少客户端等待
// 时间是这个参数的全部目的。
//
// 封顶而不是不限：每一张都真的烧掉一个 (账号 × 网关)，而那是池子最紧的资源。要多了就是
// 让一个消费者把自己的触达面一轮摸干净，后面的人无票可用（同一个坑写在
// sched.Options.MaxTriesPerDeliver 那边）。
const cookieBatchMax = 5

// serveCookie 是 /cookie 三条身份路径共用的尾段：取票 → 成了就回，没成按码回话。
//
// 两个账号参数是**两件事**，刻意分开：acctID 是记账用的（卡片、日志，上传者该在
// 自己页面上看见的那一行），consumerID 是**真正在要票的那一行**（跨账号/匿名是占位行）。
// 合成一个的后果是静默的：轮询会按上传者的那本账去判，而发现扫描会把上传者排除掉。
//
// get 收 Request 而不是闭包捕获它：重跑那一次必须用**同一份**请求参数，
// 否则等到的票和筛选条件可能对不上；批量那几张也靠它逐张换掉排除名单。
//
// count > 1 时语义是「**最多**这么多张」：拿到一张就算成功，剩下的拿不到照样 200。
// 不是「全有或全无」——调用方要的是少等，而为了凑满张数把已经拿到的那几张退回去
// （/release）再回 503，等待时间反而更长，而且那几张的槽位已经烧了。
func (s *Server) serveCookie(w http.ResponseWriter, r *http.Request, acctID, consumerID int64,
	req sched.Request, wait time.Duration, count int,
	get func(sched.Request) (types.Delivery, error)) {
	if count < 1 {
		count = 1
	}
	got := make([]deliveryResponse, 0, count)
	cur := req
	var err error
	for len(got) < count {
		var d types.Delivery
		d, err = get(cur)
		// **只给第一张挂等。** 后面那几张是「顺带多拿」，让它们各挂一次 wait 会把这一发的
		// 墙上时间乘上 count，而调用方带 count 来就是为了少等。
		if err != nil && wait > 0 && len(got) == 0 {
			if code, _, _ := s.classify(err); waitableCode(code) &&
				s.waitForCandidate(r.Context(), wait, consumerID, cur) {
				d, err = get(cur)
			}
		}
		if err != nil {
			break
		}
		got = append(got, s.delivered(d))
		if len(got) >= count {
			break
		}
		// 下一张换个落点：不排掉的话候选排序是确定的，下一发会原地再给同一个网关
		// （而且那张票刚被这一发烧过）。**复制一份再改**——Exclude 是调用方那个 map，
		// 就地改会让后面 deliverFailed / noteMinRemainingTooHigh 看到的请求和它发来的不一样。
		next := make(map[string]bool, len(cur.Exclude)+1)
		for k := range cur.Exclude {
			next[k] = true
		}
		next[d.Gateway] = true
		cur.Exclude = next
	}
	if len(got) == 0 {
		s.noteMinRemainingTooHigh(consumerID, req)
		s.deliverFailed(w, err, acctID, consumerID, req.Gateway)
		return
	}
	// 没凑满要留痕：200 + 少几张和「正好要这么多」在读数上一模一样，不说一句的话
	// 「批量到底有没有用」事后查不出来。
	if len(got) < count {
		s.opt.Log.Info("批量取票没凑满", "account", acctID, "consumer", consumerID,
			"want", count, "got", len(got), "reason", err)
	}
	// count 没带（或只要一张）时**原样回老形状**，一个字节都不变：存量调用方都在读
	// 顶层那几个字段。
	if count == 1 {
		writeJSON(w, http.StatusOK, got[0])
		return
	}
	writeJSON(w, http.StatusOK, batchResponse{Tickets: got, Count: len(got), Want: count})
}

// noteMinRemainingTooHigh 在「只有这个参数把候选筛空了」时单说一句。
//
// 为什么值得一条专门的日志：min_remaining 配得比池子里任何 pair 的寿命都长时，
// 每一发都取不到票，而失败读数和「真的没票」一模一样 —— 没有这行，
// 运维翻遍页面也看不出是客户端的一个参数在作怪。
func (s *Server) noteMinRemainingTooHigh(accountID int64, req sched.Request) {
	if req.MinRemaining == 0 || s.pool.HasCandidate(accountID, req) {
		return
	}
	// 同一份筛选，只把门槛换回服务端默认：换回去就有候选 ⇒ 就是这个参数。
	relaxed := req
	relaxed.MinRemaining = 0
	if !s.pool.HasCandidate(accountID, relaxed) {
		return
	}
	s.opt.Log.Info("min_remaining 把候选筛空了（不是真的没票）", "account", accountID,
		"min_remaining_s", int(req.MinRemaining/time.Second),
		"deliver_floor_s", int(types.DeliverFloor/time.Second))
}

// ---------- POST /release ----------

// releaseBodyMax 封住请求体：里面只有一个 12 字符的版本号。
const releaseBodyMax = 1 << 10

// handleRelease 受理「这张票我取了，但一发请求都没发出去，把我的槽位还给我」。
//
// 为什么值得有这个端点：交付一张票就烧掉一个 (账号 × 网关)，而那是整个池子最紧的
// 资源（见 sched.Release）。消费端临时改主意、它那头的请求被取消、上游先一步拒了——
// 这些情形里槽位其实一点没动，不还回来就是白扔一小时的供给。
//
// 鉴权和 /cookie 逐条一致，**复用 whoAsks**：那个函数的全部要点就是「同一个请求参数
// 在 /cookie 写哪一行，这边就读哪一行」。另写一份对齐逻辑迟早走散，而走散的后果是
// 静默的——还的是别人的槽位。
//
// **失败即安全**：没人调它时行为和以前完全一样；受理窗口过了、票对不上、已经还过了
// 都是 409，一个字段都不改。
func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	// 鉴权排在读请求体**前面**：顺序反过来的话畸形体的未鉴权请求拿到的是 400 而不是
	// 401（和「鉴权与 /cookie 完全一致」不符，给扫描器一个区分信号），而且没鉴权就能
	// 让服务端读一遍体、解一次 JSON。
	_, acctID, placeholder, ok := s.whoAsks(w, r)
	if !ok {
		return
	}
	// **占位行（`anon:` 命名空间）一律拒。** 那条路上没有任何归属证据——pair 不互斥，
	// 谁都能自己取一张同网关的票拿到 cookie_version——而归还的方向是**少记**一次接触：
	// 受害者的槽位被还原成「从没碰过」，LongestRestedSlot 把它排到最前，于是他立刻
	// 拿回自己刚烧过的落点，是降智的票而且无从察觉。AnonPrefix 那层前缀隔离只对取票
	// 够用（在别人账本上多记一次是保守方向，见 types.AnonPrefix）。
	//
	// 判据是**解出来的那一行**，不是「带没带 account 参数」：消费端每次取票都带自己的
	// 上游 account_id（解到的就是它自己那一行），按参数判会把它的正常归还全拒掉。
	if placeholder {
		// 不进卡片：这条路匿名也走得到（见 badRequest 的注释）。
		s.opt.Log.Info("拒绝归还占位行的交付", "row", acctID)
		writeFail(w, http.StatusConflict, apiError{Message: "只能归还自己账号的交付"})
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, releaseBodyMax))
	if err != nil {
		s.badRequestFor(w, http.StatusBadRequest, acctID, "请求体太大")
		return
	}
	var in struct {
		CookieVersion string `json:"cookie_version"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		s.badRequestFor(w, http.StatusBadRequest, acctID, "请求体不是合法 JSON")
		return
	}
	version := strings.ToLower(strings.TrimSpace(in.CookieVersion))
	if !validCookieVersion(version) {
		s.badRequestFor(w, http.StatusBadRequest, acctID,
			fmt.Sprintf("cookie_version 必须是 %d 位十六进制", types.CookieVersionLen))
		return
	}
	// 不单独拦「没有池内行」（acctID == 0）：拒掉占位行之后走到这儿的都是真实行，
	// 真要是 0，Release 也只会找不到交付、照样 409。
	n, err := s.pool.Release(acctID, version)
	if err != nil {
		s.noteRelease(acctID, 0, "归还被拒："+err.Error())
		writeFail(w, http.StatusConflict, apiError{Message: "没有可归还的交付（过了受理窗口、票对不上、或者已经还过了）"})
		return
	}
	s.noteRelease(acctID, n, fmt.Sprintf("归还了 %d 个槽位", n))
	w.WriteHeader(http.StatusNoContent)
}

// validCookieVersion 校验外部贴进来的版本号：定长小写十六进制。
func validCookieVersion(v string) bool {
	if len(v) != types.CookieVersionLen {
		return false
	}
	for _, c := range v {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

// ---------- POST /pair/renew ----------

// renewBodyMax 封住请求体：一个 12 字符的票号 + 一张完整 cookie 头（几百字节量级）。
const renewBodyMax = 4 << 10

// renewExpSkew 是「这张 __oailb 是不是刚下发的」那道闸的容差。
//
// exp−iat 是上游定死的 OailbLifetime，所以刚下发那张的 exp 必然在
// now+OailbLifetime 附近；差的只有这一趟的网络延迟（毫秒到秒）和两边时钟的偏差。
// 5 分钟对 65 分钟的跨度是宽松到不可能误伤正常续期，同时把「编一个远期 exp」
// 挡在 7% 以内。它不是安全闸（exp 本身来自调用方），真正兜住的是落盘前那个钳位。
const renewExpSkew = 5 * time.Minute

// renewResponse 是续期的成功响应。
//
// **形状和消费端冻结**（2026-10-02，sub2api 侧先落地）：它只在 HTTP 200 **且**
// ok 为真时认成功，并拿 cookie_version 当新票号；非 200 走统一错误体 + 闭集十一码。
// valid_for_s 它**刻意不读**（本地那个 until 不跟着延长），这边照样给——
// 运维对着它能一眼看出续到了多久，而且少一个字段比多一个难加回来。
type renewResponse struct {
	OK bool `json:"ok"`
	// CookieVersion 是**池子此刻持有的那张**的票号。写了盘就是新那张，
	// 跳过落盘（RenewAccept 兜底）就还是旧那张——两种情形下这个字段说的都是
	// 池子的实际状态，而不是「你交上来的那张」。
	CookieVersion string `json:"cookie_version"`
	ValidForS     int    `json:"valid_for_s"`
}

// handleRenew 收下消费端**自己续出来的**那张新 cookie。
//
// 为什么续期由消费端做、而不是池子自己打一发：续 __cflb 要往上游发一发有效请求，
// 池子自己发就要烧掉一个 (铸票者 × 这个网关)——那是整个池子最紧的资源。消费端反正
// 要发业务请求，顺手在第一发里把 __oailb 摘掉，上游就补一张新 __cflb 回来
// （实测见 SPEC 第 2 节）⇒ 这条路**零上游成本**换到一条新的路由寿命。
//
// 所以这个端点只做一件事：**把它交回来的那张记进账本**。不打上游、不验满血、
// 不动槽位、不动交付记录、不动 MintedBy。
//
// # 安全边界在哪儿（别再搞错一次）
//
// 交回来的 cookie 本体**全是调用方自己写的字符串**，池子验不了：`__oailb` 是 JWT
// 但**签名验不了**（上游的公钥不在我们手上，见 mint.parseOailb——它只 base64 解
// payload），`__cflb` 是 AEAD 密文、只查得出非空。所以「落点对不对」「exp 像不像
// 真的」这类判据是**拿外部输入校验外部输入**：挡消费端的 bug 够用，挡不住人。
//
// 真正的边界有三条，都不依赖 cookie 的自述：
//  1. **归属**：这张票必须是「这一行在这个网关上**刚被交付的那一张**」
//     （s.pool.DeliveredTo，判据是槽位上记着的票号，和 /release 同一份比对）。
//     没有它，任何一把合法 consumer key 都能换掉**别人**现役的 pair：伪造一个 host
//     指向那个网关、exp 随便填，池子就把一张路由已死的 cookie 当活票发给还没碰过
//     那个网关的账号 ⇒ 他被按地理随机路由 ⇒ 降智票当满血票发出去。
//     判据**不能**是「你报的号 == 池子现持那张的号」：那个号谁都查得到（自己取一张
//     同网关的票就知道），于是一次交付能换到无限次改写（2026-10-02 复审实测 5/5）。
//  2. **拒占位行**：匿名和跨账号那两条路上的「归属」是**自助办下来的**——acctID 是
//     anon:<?account=>，而那个 ref 就是查询参数、零证据，GET /cookie 还会用同一个
//     ref 现场建行并把槽位记上去。所以归属闸对它形同虚设：换个 ref 连冷却都不吃
//     （全新占位行 = 零槽位历史），实测一把合法 key 能扫掉 5/5 个网关。
//  3. **钳位**：死期一律不许超过 now+PairLifetime。没有它，一个远期 exp 就能把
//     这张 pair 放大成「永不过期」⇒ 它是唯一的退场条件（池子不再有任何重铸/丢弃
//     临期票的路径，2026-10-03 删掉了 renewStalePairs）⇒ 那个网关从此只发这张死票，
//     还计入 livePairs() 压住别的网关的预铸，并持久化进 state.json、重启不自愈。
//
// # 剩下的残余面（别再写成「只能害自己」）
//
// 一个**真上传者**能把他自己刚被交付的那张换成一串垃圾：一次（改写之后池子现持的
// 已经不是交付那张了，票号闸挡住第二次），有日志，受他自己的 SlotCooldown 约束。
// 而那张 pair 是**全池共享**的 ⇒ 这一次改写确实会影响别人。没有上游公钥时这是能
// 拿到的边界；再往前要么给 Pair 加「只给续期者用」的字段（端点的收益清零），
// 要么回传给第三方前先 VerifyFull 验一次（花掉它本来想省的那一发）——都不做，
// 见 SPEC 第 10 节。
//
// 和 /release 刻意有一处不同：verify_full_at_deliver 开着时**不拒**。/release 拒是
// 因为那条路上的槽位是实烧的、还了就是对池子说谎；续期和槽位无关，和那个开关也就无关。
//
// 副作用说在明处：换掉池子手上那张之后，消费端原来那张票的 /release 会开始 409
// （Release 按池子现持的票号认）。这是**对的**——触发续期的那一发业务请求已经把
// 槽位烧掉了，还回去就是说谎。
func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	// 鉴权在读请求体之前，和 /release 同一个顺序、同一个 whoAsks：
	// 三个机器端点同一把钥匙，身份解析也只该有一份（对不齐的后果是静默的）。
	_, acctID, placeholder, ok := s.whoAsks(w, r)
	if !ok {
		return
	}
	// **占位行一律拒**（和 /release 同一个判据、同一个理由模板，见函数头第 2 条）。
	//
	// 代价说在明处：**跨账号和匿名的消费端拿不到这个端点。** 这是刻意的取舍——
	// 池子手上没有那个号的凭据、也验不了它说的任何话，而续期是往**全池共享**的资源
	// 里写。真上传者（池子持有凭据的那一行）照旧能续：那条路有归属、有日志、
	// 受他自己的 SlotCooldown 约束，可辩护可追责。
	if placeholder {
		// 不进卡片：这条路匿名也走得到（见 badRequest 的注释）。
		s.opt.Log.Info("拒绝占位行的续期", "row", acctID)
		writeFail(w, http.StatusConflict, apiError{Code: codeBadRequest,
			Message: "只能续自己账号拿到的票（跨账号和匿名取用没有归属证据）"})
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, renewBodyMax))
	if err != nil {
		s.badRequestFor(w, http.StatusBadRequest, acctID, "请求体太大")
		return
	}
	var in struct {
		CookieVersion string `json:"cookie_version"`
		Cookie        string `json:"cookie"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		s.badRequestFor(w, http.StatusBadRequest, acctID, "请求体不是合法 JSON")
		return
	}
	version := strings.ToLower(strings.TrimSpace(in.CookieVersion))
	if !validCookieVersion(version) {
		s.badRequestFor(w, http.StatusBadRequest, acctID,
			fmt.Sprintf("cookie_version 必须是 %d 位十六进制", types.CookieVersionLen))
		return
	}
	// 形状：必须是完整两件，而且 __oailb 解得开（落点和它自己的硬过期都要从里面读）。
	// 解析只借 mint.PairInfo 这一份——JWT 那段逻辑在这儿抄一遍就是第二把尺子。
	cookie := strings.TrimSpace(in.Cookie)
	landed, oailbExp, perr := mint.PairInfo(cookie)
	if perr != nil {
		// 形状描述只进日志，不抄进响应：那串里可能带着 cookie 的片段（红线 1）。
		s.opt.Log.Info("续期交回的 cookie 形状不对", "account", acctID, "err", perr)
		s.badRequestFor(w, http.StatusBadRequest, acctID,
			"cookie 不是一张完整的路由 cookie（要 __cflb 和能解开的 __oailb 两件）")
		return
	}
	// __oailb 的 exp−iat 是上游定死的 OailbLifetime，所以一张**刚下发的**票的 exp
	// 必然落在 now+OailbLifetime 附近（差的只是这一趟的延迟和两边时钟的偏差）。
	// 偏出去就说明它不是刚下发的：要么是旧票拿错了，要么是个编出来的远期 exp——
	// 后者正是把 pair 放大成「永不临期」的那条路（见上面那段的第 2 条）。
	//
	// 这道闸**不是**安全闸（exp 和落点一样来自调用方自己写的字符串），它只是把
	// 荒唐的数挡在账本外面；真正兜住的是下面的钳位。
	if off := oailbExp.Sub(time.Now().Add(types.OailbLifetime)); off > renewExpSkew || off < -renewExpSkew {
		s.opt.Log.Warn("续期交回的 cookie 不像是刚下发的，拒收",
			"account", acctID, "off_s", int(off/time.Second),
			"allow_s", int(renewExpSkew/time.Second))
		s.badRequestFor(w, http.StatusBadRequest, acctID,
			"cookie 的 __oailb 过期时刻不在上游定死的那个跨度上（这张不是刚下发的）")
		return
	}
	// 票号必须对上池子**此刻持有的**那张。对不上说明这张票早被换掉了（或者从来不是
	// 池子发的）⇒ 照它写盘就是用一个来历不明的 cookie 盖掉现役那张。
	// 带码的 409：闭集不许留洞，而唯一不带码的那个 409 是 /release 的（见 apiError）。
	cur, found := s.pairByVersion(version)
	if !found {
		s.badRequestFor(w, http.StatusConflict, acctID,
			"这张票不是池子此刻持有的那张（已经被换过了，或者从来不是池子发的）")
		return
	}
	// **归属闸**：这张票必须确实被交付给过这一行。整条路上唯一不依赖 cookie 自述的
	// 判据，也是唯一挡得住人的那条（完整论证见函数头上那段）。
	// 判据借调度那一份（和 /release 同一个比对），这儿绝不自己算。
	if !s.pool.DeliveredTo(acctID, version) {
		s.opt.Log.Warn("续期的票不是交付给这一行的，拒收",
			"account", acctID, "gateway", cur.Gateway)
		s.badRequestFor(w, http.StatusConflict, acctID,
			"这张票不是交付给你的（只能续你自己拿到过的那张）")
		return
	}
	// 落点一致：交回来那张读出的落点必须就是这张票所在的网关。
	// **这不是安全闸**（落点是调用方自己写的 host），它挡的是消费端把两张票搞混
	// 这类 bug——而账本一错，后面所有「这个网关还能不能交付」都跟着错。
	// Warn 不是 Info：这是对方的 bug，不是一次正常的拒绝。
	if landed != cur.Gateway {
		s.opt.Log.Warn("续期交回的 cookie 落点和票所在的网关不一致，拒收",
			"account", acctID, "ticket_gateway", cur.Gateway, "cookie_gateway", landed)
		s.badRequestFor(w, http.StatusBadRequest, acctID, "交回的 cookie 落在另一个网关上")
		return
	}
	// 必须真的换了一张。一字不差交回来说明它那头根本没续到（上游没补发），
	// 收下只是把同一份内容再落一次盘，而「续期成功」这个读数会变成假的。
	if cookie == cur.Cookie {
		s.badRequestFor(w, http.StatusBadRequest, acctID,
			"交回的 cookie 和池子手上这张一模一样（没有续到新的）")
		return
	}
	// 写放大兜底：池子手上这张本来就还有近乎整条命 ⇒ 这一发换不到寿命，只是一次
	// 整份 state.json 的原子重写。受理但不落盘（见 types.RenewAccept——它防的是
	// 异常而不是常态）。
	if time.Until(cur.ExpiresAt) > types.PairLifetime-types.RenewAccept {
		s.opt.Log.Info("续期跳过落盘：池子手上这张还几乎是满寿命",
			"account", acctID, "gateway", cur.Gateway,
			"left_s", int(time.Until(cur.ExpiresAt)/time.Second))
		writeJSON(w, http.StatusOK, renewResponse{OK: true, CookieVersion: version,
			ValidForS: int(max(time.Until(cur.ExpiresAt), 0) / time.Second)})
		return
	}
	// 寿命按**新的那张**算，所以第二个参数刻意传零值而不是 cur.CflbExpiresAt：
	// 旧 __cflb 已经被换掉了，拿它的死期当上限等于把新票钉死在旧票的死期上。
	// 真正的 Max-Age 读不出来（它在消费端那一跳的 Set-Cookie 里，池子没见过），
	// 所以走 RouteDeadline 的第三级兜底——两件是同一个响应下发的，差值是实测的。
	route, cflb := types.MintResult{PairExpires: oailbExp}.RouteDeadline(time.Time{})
	// **钳位**：真实 __cflb 的 Max-Age 死死是 PairLifetime，所以一张票无论怎么续都
	// 不可能活过 now+PairLifetime。这一层必须自己钳，因为上面那个死期是从调用方写的
	// exp 推出来的。
	//
	// 漏掉它不是「数字难看一点」，而是**这张票再也不会自己退场**：真实 __cflb 一小时
	// 就死，而被放大的死期让它在账本上永远「还活着」—— 一直计入 livePairs() 压住别的
	// 网关的预铸，一直被当成可交付的票发出去（实际路由已经失效），而且持久化进
	// state.json、重启不自愈。池子**没有**别的机制会丢掉它：唯一会按行为丢票的那条
	// （定向续期的漂移检测 + 失败预算）2026-10-03 已随 renew_pairs 一起删掉，
	// 现在所有票都只靠 ExpiresAt 自然过期退场。
	// 它顺带救回 RenewAccept 的等价性：「> PairLifetime−RenewAccept ⟺ 刚换过不到
	// 60 秒」只在死期不超过 now+PairLifetime 时成立。
	if hardCap := time.Now().Add(types.PairLifetime); route.After(hardCap) {
		route, cflb = hardCap, hardCap
	}
	// 续不出寿命就不收：交回来一张比现持那张死得更早的 cookie，收下就是自己把池子
	// 最紧的供给换短。这条只兜「上面两道闸都放过、但确实更短」那一格。
	if !route.After(cur.ExpiresAt) {
		s.opt.Log.Warn("续期交回的 cookie 活不过池子手上那张，拒收",
			"account", acctID, "gateway", cur.Gateway,
			"new_left_s", int(time.Until(route)/time.Second),
			"cur_left_s", int(time.Until(cur.ExpiresAt)/time.Second))
		s.badRequestFor(w, http.StatusBadRequest, acctID,
			"交回的 cookie 比池子手上这张死得更早（没有续到寿命）")
		return
	}
	// 只改 Cookie 和两个死期。从 cur 拷过来是刻意的：Gateway 和 MintedBy 原样留着
	// （铸它的还是那个号，续期没换铸票者），将来 Pair 加字段也不会在这儿被抹掉。
	next := cur
	next.Cookie, next.ExpiresAt, next.CflbExpiresAt = cookie, route, cflb
	if err := s.st.PutPair(next); err != nil {
		s.opt.Log.Error("续期落盘失败", "account", acctID, "gateway", cur.Gateway, "err", err)
		writeFail(w, http.StatusInternalServerError,
			apiError{Code: codeMintFailed, Message: "续期写盘失败"})
		return
	}
	left := int(max(time.Until(route), 0) / time.Second)
	s.noteRenew(acctID, cur.Gateway, left)
	writeJSON(w, http.StatusOK, renewResponse{OK: true,
		CookieVersion: types.CookieVersion(cookie), ValidForS: left})
}

// pairByVersion 找池子此刻持有的、票号等于 version 的那张。
//
// 网关名不在请求里（消费端手上只有票号和 cookie 本体），所以只能扫一遍。
// 网关数是十几个量级，为它建一份索引是白开销，而且那是第二份要和账本同步的状态。
// 两个票号都要认：库里存的那一份，以及**换过 `__cflb` 之后真正发出去的**那一份
// （见 mint.Rehome —— 换 cflb 会改票号，而消费端手上只有发出去那一份）。
func (s *Server) pairByVersion(version string) (types.Pair, bool) {
	raw := s.st.Pairs()
	best := mint.BestCflb(raw, time.Now())
	for _, p := range raw {
		if types.CookieVersion(p.Cookie) == version {
			return p, true
		}
		if r := mint.Rehome(p, best, time.Now()); r.Cookie != p.Cookie &&
			types.CookieVersion(r.Cookie) == version {
			return r, true
		}
	}
	return types.Pair{}, false
}

// noteRenew 把续期记进那张卡片，和归还同一个理由（见 noteRelease）：
// 「续期在不在生效」和取票失败是同一个问题的两面，分两张表只会让人少看一张。
// **票号和 cookie 本体一个字都不进去**——那张卡片是给页面看的。
func (s *Server) noteRenew(accountID int64, gateway string, leftS int) {
	s.opt.Log.Info("续期收下新 cookie", "account", accountID, "gateway", gateway, "left_s", leftS)
	s.fails.add(deliverFail{At: time.Now(), AccountID: accountID, Gateway: gateway,
		Code: "renew", Reason: fmt.Sprintf("续到还剩 %d 秒", leftS)})
}

// noteRelease 把归还也记进那张卡片。
//
// 它不是「失败」，但运营要看的读数（还成功了几个槽位、谁在还、还是不是总被拒）
// 和取票失败是同一个问题的两面，分两张表只会让人少看一张。
func (s *Server) noteRelease(accountID int64, restored int, note string) {
	s.opt.Log.Info("归还槽位", "account", accountID, "restored", restored, "note", note)
	s.fails.add(deliverFail{At: time.Now(), AccountID: accountID,
		Code: "release", Reason: note})
}
