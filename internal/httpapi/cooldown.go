package httpapi

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gwpool/internal/cooldown"
	"gwpool/internal/types"
)

const (
	cooldownBodyMax        = 2048
	cooldownBatchMax       = 32
	cooldownBatchBodyMax   = 64 * 1024
	cooldownObservedHeader = "X-Gwpool-Observed-At"
)

type cooldownObservation struct {
	ID             string    `json:"id"`
	AccountTag     string    `json:"account_tag"`
	Gateway        string    `json:"gateway"`
	WindowSeconds  int       `json:"window_seconds"`
	ElapsedSeconds int       `json:"elapsed_seconds"`
	Result         string    `json:"result"`
	ObservedAt     time.Time `json:"observed_at,omitempty"`
}

type cooldownAcknowledgement struct {
	ID             string                        `json:"id"`
	Status         int                           `json:"status"`
	Added          bool                          `json:"added"`
	Recommendation *types.CooldownRecommendation `json:"recommendation,omitempty"`
}

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
	var in cooldownObservation
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
	if at := r.Header.Get(cooldownObservedHeader); at != "" && in.ObservedAt.IsZero() {
		var err error
		in.ObservedAt, err = time.Parse(time.RFC3339Nano, at)
		if err != nil {
			s.badRequest(w, http.StatusBadRequest, "样本观察时间无效")
			return
		}
	}
	now := time.Now().UTC()
	ack := s.saveCooldownObservation(in, caller.reporterID(), now)
	if ack.Status != http.StatusOK {
		s.badRequest(w, ack.Status, "冷却样本无效、冲突或暂存失败")
		return
	}
	ack.Recommendation = cooldownRecommendationFor(cooldown.SummarizeWithin(s.st.CooldownReports(), now, cooldownClientCeiling(r)),
		in.Gateway, caller.reporterID(), strings.ToLower(in.AccountTag))
	writeJSON(w, http.StatusOK, struct {
		OK bool `json:"ok"`
		cooldownAcknowledgement
	}{true, ack})
}

// Each item is acknowledged independently. A conflict or invalid event never
// drops another valid item; retries retain the original event ID and timestamp.
func (s *Server) handleCooldownReports(w http.ResponseWriter, r *http.Request) {
	caller, ok := s.resolveCaller(r)
	if !ok {
		s.unauthorized(w, "冷却上报需要有效的 consumer key")
		return
	}
	var in struct {
		Reports []cooldownObservation `json:"reports"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, cooldownBatchBodyMax))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&in); err != nil {
		s.badRequest(w, http.StatusBadRequest, "批量样本格式错误")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF || len(in.Reports) == 0 || len(in.Reports) > cooldownBatchMax {
		s.badRequest(w, http.StatusBadRequest, "批量样本应为1至32条")
		return
	}
	now := time.Now().UTC()
	results := make([]cooldownAcknowledgement, 0, len(in.Reports))
	for _, report := range in.Reports {
		results = append(results, s.saveCooldownObservation(report, caller.reporterID(), now))
	}
	stats := cooldown.SummarizeWithin(s.st.CooldownReports(), now, cooldownClientCeiling(r))
	for i, report := range in.Reports {
		if results[i].Status == http.StatusOK {
			results[i].Recommendation = cooldownRecommendationFor(stats, report.Gateway, caller.reporterID(), strings.ToLower(report.AccountTag))
		}
	}
	writeJSON(w, http.StatusOK, struct {
		OK      bool                      `json:"ok"`
		Results []cooldownAcknowledgement `json:"results"`
	}{true, results})
}

func (s *Server) saveCooldownObservation(in cooldownObservation, reporterID int64, now time.Time) cooldownAcknowledgement {
	in.ID, in.AccountTag = strings.ToLower(in.ID), strings.ToLower(in.AccountTag)
	ack := cooldownAcknowledgement{ID: in.ID, Status: http.StatusBadRequest}
	_, knownGateway := s.st.Gateways()[in.Gateway]
	if !validCooldownTag(in.ID) || !validCooldownTag(in.AccountTag) || !knownGateway ||
		in.WindowSeconds < cooldown.MinSeconds || in.WindowSeconds > cooldown.MaxSeconds ||
		in.ElapsedSeconds < max(1, in.WindowSeconds-cooldown.GraceSeconds) ||
		in.ElapsedSeconds > cooldown.MaxElapsed ||
		(in.Result != "full" && in.Result != "degraded") ||
		(!in.ObservedAt.IsZero() && (in.ObservedAt.Before(now.Add(-cooldown.Retention)) || in.ObservedAt.After(now.Add(time.Minute)))) {
		return ack
	}
	report := types.CooldownReport{
		ID: in.ID, AccountTag: in.AccountTag, Gateway: in.Gateway,
		WindowSeconds: in.WindowSeconds, ElapsedSeconds: in.ElapsedSeconds, Result: in.Result,
		ReporterID: reporterID, ReceivedAt: now, ObservedAt: in.ObservedAt,
	}
	added, err := s.st.AddCooldownReport(report)
	if errors.Is(err, types.ErrCooldownReportConflict) {
		ack.Status = http.StatusConflict
		return ack
	}
	if err != nil {
		s.opt.Log.Error("冷却样本写盘失败", "err", err)
		ack.Status = http.StatusInternalServerError
		return ack
	}
	ack.Status, ack.Added = http.StatusOK, added
	return ack
}

func cooldownRecommendationFor(all []types.CooldownStats, gateway string, reporterID int64, tag string) *types.CooldownRecommendation {
	for _, stats := range all {
		if stats.Gateway == gateway {
			if rec := cooldown.ForAccount(stats, reporterID, tag); rec.Seconds > 0 {
				return &rec
			}
			break
		}
	}
	return nil
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
	for _, stats := range cooldown.SummarizeWithin(s.st.CooldownReports(), now, cooldownClientCeiling(r)) {
		if rec := cooldown.ForAccount(stats, caller.reporterID(), tag); rec.Seconds > 0 {
			out[stats.Gateway] = rec
		}
	}
	return out
}

func cooldownClientCeiling(r *http.Request) int {
	ceiling, err := strconv.Atoi(r.Header.Get("X-Gwpool-Cooldown-Max-Seconds"))
	if err != nil || ceiling < cooldown.LegacyMaxSeconds {
		return cooldown.LegacyMaxSeconds
	}
	return min(ceiling, cooldown.MaxSeconds)
}
