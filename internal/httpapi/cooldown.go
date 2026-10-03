package httpapi

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"gwpool/internal/cooldown"
	"gwpool/internal/types"
)

const cooldownBodyMax = 2048

func validCooldownTag(tag string) bool {
	if len(tag) != 64 {
		return false
	}
	_, err := hex.DecodeString(tag)
	return err == nil
}

// POST /cooldown/report 只收已鉴权消费端的匿名观察，不改 Pair/Slot 的任何质量或路由状态。
func (s *Server) handleCooldownReport(w http.ResponseWriter, r *http.Request) {
	caller, ok := s.resolveCaller(r)
	if !ok {
		s.unauthorized(w, "冷却上报需要有效的 consumer key")
		return
	}
	var in struct {
		ID             string `json:"id"`
		AccountTag     string `json:"account_tag"`
		Gateway        string `json:"gateway"`
		WindowSeconds  int    `json:"window_seconds"`
		ElapsedSeconds int    `json:"elapsed_seconds"`
		Result         string `json:"result"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, cooldownBodyMax))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&in); err != nil {
		s.badRequest(w, http.StatusBadRequest, "冷却样本格式错误")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		s.badRequest(w, http.StatusBadRequest, "只接受一个冷却样本")
		return
	}
	in.ID, in.AccountTag = strings.ToLower(in.ID), strings.ToLower(in.AccountTag)
	_, knownGateway := s.st.Gateways()[in.Gateway]
	if !validCooldownTag(in.ID) || !validCooldownTag(in.AccountTag) || !knownGateway ||
		in.WindowSeconds < cooldown.MinSeconds || in.WindowSeconds > cooldown.MaxSeconds ||
		in.ElapsedSeconds < max(1, in.WindowSeconds-cooldown.GraceSeconds) ||
		in.ElapsedSeconds > cooldown.MaxElapsed ||
		(in.Result != "full" && in.Result != "degraded") {
		s.badRequest(w, http.StatusBadRequest, "冷却样本字段无效")
		return
	}
	report := types.CooldownReport{
		ID: in.ID, AccountTag: in.AccountTag, Gateway: in.Gateway,
		WindowSeconds: in.WindowSeconds, ElapsedSeconds: in.ElapsedSeconds, Result: in.Result,
		ReporterID: caller.acct.ID, ReceivedAt: time.Now().UTC(),
	}
	added, err := s.st.AddCooldownReport(report)
	if errors.Is(err, types.ErrCooldownReportConflict) {
		s.badRequest(w, http.StatusConflict, "样本 ID 已用于不同内容")
		return
	}
	if err != nil {
		s.opt.Log.Error("冷却样本写盘失败", "err", err)
		http.Error(w, "冷却样本暂存失败", http.StatusInternalServerError)
		return
	}
	var recommendation *types.CooldownRecommendation
	for _, stats := range cooldown.Summarize(s.st.CooldownReports(), time.Now()) {
		if stats.Gateway == in.Gateway {
			if rec := cooldown.ForAccount(stats, caller.acct.ID, in.AccountTag); rec.Seconds > 0 {
				recommendation = &rec
			}
			break
		}
	}
	writeJSON(w, http.StatusOK, struct {
		OK             bool                          `json:"ok"`
		Added          bool                          `json:"added"`
		Recommendation *types.CooldownRecommendation `json:"recommendation,omitempty"`
	}{true, added, recommendation})
}

func (s *Server) cooldownRecommendations(r *http.Request, now time.Time) map[string]types.CooldownRecommendation {
	tag := strings.ToLower(r.Header.Get("X-Gwpool-Account-Tag"))
	if !validCooldownTag(tag) {
		return nil // 老客户端没有匿名标识，不额外暴露个人统计。
	}
	caller, ok := s.resolveCaller(r)
	if !ok {
		return nil
	}
	out := map[string]types.CooldownRecommendation{}
	for _, stats := range cooldown.Summarize(s.st.CooldownReports(), now) {
		if rec := cooldown.ForAccount(stats, caller.acct.ID, tag); rec.Seconds > 0 {
			out[stats.Gateway] = rec
		}
	}
	return out
}
