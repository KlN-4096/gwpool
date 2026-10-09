package httpapi

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"gwpool/internal/sched"
)

const codeConsumerRateLimited = "consumer_rate_limited"
const codeUpgradeRequired = "upgrade_required"

// This is a tombstone, not a compatibility implementation. It performs no
// account lookup, selection, minting, waiting, release or consumer renewal.
func (s *Server) handleUpgradeRequired(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Upgrade", "gwpool-client-dispatch")
	writeFail(w, http.StatusUpgradeRequired, apiError{
		Code:    codeUpgradeRequired,
		Message: "消费者归还和续期接口已停用，请升级到最新版本 Sub2api，通过 /cookie 明确点名取票、/gateways 获取目录",
	})
}

func validConsumerRef(ref string) bool {
	return ref != "" && len(ref) <= 128 &&
		!strings.ContainsFunc(ref, func(r rune) bool { return r < 0x20 || r == 0x7f })
}

// External consumers choose one gateway. There is deliberately no wait,
// fallback selection, batch acquisition or per-consumer delivery lease.
func (s *Server) serveExternalCookie(w http.ResponseWriter, r *http.Request, member string, req sched.Request, authed bool) {
	if req.Gateway == "" {
		s.badRequest(w, http.StatusBadRequest, "取票必须指定 gateway；请升级到最新版 Sub2api")
		return
	}
	for _, name := range []string{"force", "wait", "count"} {
		if r.URL.Query().Has(name) {
			s.badRequest(w, http.StatusBadRequest, "取票不支持 force、wait 或 count；请升级到最新版 Sub2api，由客户端选择与重试")
			return
		}
	}
	delivery, err := s.pool.DeliverAnonymous(r.Context(), member, req, authed)
	if err != nil {
		var limited *sched.ConsumerRateLimit
		switch {
		case errors.As(err, &limited):
			writeFail(w, http.StatusTooManyRequests, apiError{
				Code: codeConsumerRateLimited, Message: "外部账号取票过于频繁，请按 Retry-After 重试",
				RetryAfterSeconds: retrySeconds(limited.RetryAfter),
			})
		case errors.Is(err, sched.ErrExternalRequest):
			s.badRequest(w, http.StatusBadRequest, "外部取票需要有效 account 和 gateway")
		case errors.Is(err, sched.ErrPublicClosed):
			s.publicClosed(w, "池子此刻不对外开放")
		case errors.Is(err, sched.ErrNoFullSlot):
			// No discovery scan or consumer scheduling is triggered here.
			code, message := codeNoLivePair, "指定网关没有可交付的有效票，请刷新目录后重试"
			if len(s.st.Gateways()) == 0 {
				code, message = codeNoGateway, "池子当前没有网关，请稍后重试"
			}
			s.fails.add(deliverFail{At: time.Now(), Gateway: req.Gateway, Code: code, Reason: message})
			s.opt.Log.Info("external stock unavailable", "gateway", req.Gateway, "code", code)
			writeFail(w, http.StatusServiceUnavailable, apiError{
				Code: code, Message: message,
			})
		default:
			s.opt.Log.Error("external delivery failed", "gateway", req.Gateway, "error", err)
			writeFail(w, http.StatusInternalServerError, apiError{Code: "internal_error", Message: "取票失败，请稍后重试"})
		}
		return
	}
	if counter, ok := r.Context().Value(keyTicketCounterContext{}).(*int); ok {
		*counter++
	}
	response := s.delivered(delivery)
	writeJSON(w, http.StatusOK, response)
}
