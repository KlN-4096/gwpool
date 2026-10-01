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
// **只回话、不记卡片**：参数校验排在鉴权前面（信任边界就该在最前），所以这条路
// 未鉴权的请求也走得到，往那个 200 条的环里灌几下就能把真实诊断全挤出去。
// 鉴权通过之后的 400/404 走 badRequestFor。
func badRequest(w http.ResponseWriter, status int, msg string) {
	writeFail(w, status, apiError{Code: codeBadRequest, Message: msg})
}

// badRequestFor 是**鉴权通过之后**的 400/404：同一个码，另外记一行卡片。
// 管理员少带 account、点名的账号不存在这类错只有在页面上看得见才查得动。
func (s *Server) badRequestFor(w http.ResponseWriter, status int, accountID int64, msg string) {
	s.fails.add(deliverFail{At: time.Now(), AccountID: accountID, Code: codeBadRequest, Reason: msg})
	badRequest(w, status, msg)
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
	// 上限 3600 s 是 __oailb 的整个寿命（3900 s）量级，再大就永远筛不出候选。
	minRemainingFloor = 30 * time.Second
	minRemainingCap   = 3600 * time.Second

	// wait 的钳位上限。30 s 是「一次 HTTP 请求等得起」的量级；
	// 真正长的等待（冷却是几小时）**永远不等**，见 waitableCode。
	maxWait = 30 * time.Second
)

// parseCookieRequest 是 /cookie 的信任边界：查完长度和范围，往后各层直接信。
//
// 第二个返回值是调用方愿意等多久（wait），它只活在 HTTP 这一层，不进调度。
func parseCookieRequest(r *http.Request) (sched.Request, time.Duration, error) {
	q := r.URL.Query()
	req := sched.Request{Force: truthy(q.Get("force"))}

	if gw := q.Get("gateway"); gw != "" {
		if !validName(gw) {
			return req, 0, errors.New("gateway 名字不合法")
		}
		req.Gateway = gw
	}

	// 认不出的名字静默忽略：调用方那本账可能记着池子早就忘掉的网关，
	// 为此拒掉整个请求等于让它去猜池子的状态。
	ex, err := parseList(q.Get("exclude"), "exclude")
	if err != nil {
		return req, 0, err
	}
	req.Exclude = ex
	vs, err := parseList(strings.ToLower(q.Get("exclude_versions")), "exclude_versions")
	if err != nil {
		return req, 0, err
	}
	req.ExcludeVersions = vs

	if v := q.Get("min_remaining"); v != "" {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return req, 0, errors.New("min_remaining 不是整数秒")
		}
		req.MinRemaining = clampDur(time.Duration(n)*time.Second, minRemainingFloor, minRemainingCap)
	}

	wait := time.Duration(0)
	if v := q.Get("wait"); v != "" {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return req, 0, errors.New("wait 不是整数秒")
		}
		wait = clampDur(time.Duration(n)*time.Second, 0, maxWait)
	}
	return req, wait, nil
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
		return codeAllCooling, "你的槽位在所有候选网关上都还在冷却里", cool.Wait
	case errors.Is(err, sched.ErrDegraded):
		return codeDegraded, "交付前的满血验证判了降级，不发降智的票", types.FullWindow
	case errors.Is(err, sched.ErrUpstreamRejected):
		return codeUpstreamRejected, "上游否决了池子的这个账号", 0
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
// **只有这一个**：all_cooling 是几小时量级，让一次 HTTP 请求挂在那儿等它纯属把
// 连接耗光；rate_limited 同理（滑窗最长一小时）；upstream_rejected 等多久都一样。
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

// serveCookie 是 /cookie 三条身份路径共用的尾段：取票 → 成了就回，没成按码回话。
//
// 两个账号参数是**两件事**，刻意分开：acctID 是记账用的（卡片、日志，上传者该在
// 自己页面上看见的那一行），consumerID 是**真正在要票的那一行**（跨账号/匿名是占位行）。
// 合成一个的后果是静默的：轮询会按上传者的冷却去判，而发现扫描会把上传者排除掉。
//
// get 收 Request 而不是闭包捕获它：重跑那一次必须用**同一份**请求参数，
// 否则等到的票和筛选条件可能对不上。
func (s *Server) serveCookie(w http.ResponseWriter, r *http.Request, acctID, consumerID int64,
	req sched.Request, wait time.Duration, get func(sched.Request) (types.Delivery, error)) {
	d, err := get(req)
	if err != nil && wait > 0 {
		if code, _, _ := s.classify(err); waitableCode(code) &&
			s.waitForCandidate(r.Context(), wait, consumerID, req) {
			d, err = get(req)
		}
	}
	if err != nil {
		s.noteMinRemainingTooHigh(consumerID, req)
		s.deliverFailed(w, err, acctID, consumerID, req.Gateway)
		return
	}
	writeJSON(w, http.StatusOK, delivered(d))
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
		badRequest(w, http.StatusBadRequest, "请求体太大")
		return
	}
	var in struct {
		CookieVersion string `json:"cookie_version"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		badRequest(w, http.StatusBadRequest, "请求体不是合法 JSON")
		return
	}
	version := strings.ToLower(strings.TrimSpace(in.CookieVersion))
	if !validCookieVersion(version) {
		badRequest(w, http.StatusBadRequest,
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

// noteRelease 把归还也记进那张卡片。
//
// 它不是「失败」，但运营要看的读数（还成功了几个槽位、谁在还、还是不是总被拒）
// 和取票失败是同一个问题的两面，分两张表只会让人少看一张。
func (s *Server) noteRelease(accountID int64, restored int, note string) {
	s.opt.Log.Info("归还槽位", "account", accountID, "restored", restored, "note", note)
	s.fails.add(deliverFail{At: time.Now(), AccountID: accountID,
		Code: "release", Reason: note})
}
