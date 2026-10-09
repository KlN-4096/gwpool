package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gwpool/internal/sched"
)

// Strict consumer parameter validation and shared authentication/access errors.
const (
	codeNoGateway    = "no_gateway"    // 池子里一个网关都没有（冷启动）
	codeNoLivePair   = "no_live_pair"  // 指定网关没有有效票
	codeRateLimited  = "rate_limited"  // Key/公共接入策略限额，不是成员取票额度
	codePublicClosed = "public_closed" // 匿名：此刻不对外开放
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
)

// parseCookieRequest 是 /cookie 的信任边界：查完长度和范围，往后各层直接信。
func parseCookieRequest(r *http.Request) (sched.Request, error) {
	q := r.URL.Query()
	req := sched.Request{Gateway: q.Get("gateway")}
	for _, retired := range []string{"force", "wait", "count"} {
		if q.Has(retired) {
			return req, errors.New("取票不支持 force、wait 或 count；请升级到最新版 Sub2api，由客户端选择与重试")
		}
	}
	if !validName(req.Gateway) {
		return req, errors.New("取票必须指定合法的 gateway；请升级到最新版 Sub2api")
	}

	// 认不出的名字静默忽略：调用方那本账可能记着池子早就忘掉的网关，
	// 为此拒掉整个请求等于让它去猜池子的状态。
	ex, err := parseList(q.Get("exclude"), "exclude")
	if err != nil {
		return req, err
	}
	req.Exclude = ex
	vs, err := parseList(strings.ToLower(q.Get("exclude_versions")), "exclude_versions")
	if err != nil {
		return req, err
	}
	req.ExcludeVersions = vs

	if v := q.Get("min_remaining"); v != "" {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return req, errors.New("min_remaining 不是整数秒")
		}
		n = max(int(minRemainingFloor/time.Second), min(n, int(minRemainingCap/time.Second)))
		req.MinRemaining = time.Duration(n) * time.Second
	}
	return req, nil
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

// ---------- 失败分类 ----------

// ---------- 等票 ----------
