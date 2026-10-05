package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"gwpool/internal/contact"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

const contactBodyMax = 4096

func validContactToken(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, c := range value {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', strings.ContainsRune("._:/-", c):
		default:
			return false
		}
	}
	return true
}

func validContactObservation(in types.ContactObservation, now time.Time) bool {
	return validCooldownTag(in.ID) && validCooldownTag(in.AccountTag) &&
		validContactToken(in.Model) && validContactToken(in.Criterion) &&
		(in.Source == "foreground" || in.Source == "background" || in.Source == "business") &&
		(in.First == "tracked_first" || in.First == "repeat" || in.First == "unknown") &&
		(in.Outcome == "full" || in.Outcome == "refreshed" || in.Outcome == "unknown") &&
		!in.At.Before(now.Add(-contact.Retention)) && !in.At.After(now.Add(5*time.Minute)) &&
		in.ElapsedSeconds >= 0 && in.ElapsedSeconds <= contact.MaxElapsedSeconds &&
		(in.GapKnown || in.ElapsedSeconds == 0) &&
		(in.First != "tracked_first" || !in.GapKnown) &&
		in.FullWindowMS >= 0 && in.FullWindowMS <= contact.MaxWindowMS &&
		((!in.WindowFinal && in.FullWindowMS == 0) || (in.WindowFinal && in.FullWindowMS > 0 && in.Outcome == "full"))
}

func (s *Server) handleContactReport(w http.ResponseWriter, r *http.Request) {
	caller, ok := s.resolveCaller(r)
	if !ok || caller.admin {
		s.unauthorized(w, "接触上报需要有效的 consumer key")
		return
	}
	var in types.ContactObservation
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, contactBodyMax))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&in); err != nil {
		s.badRequest(w, http.StatusBadRequest, "接触样本格式错误")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		s.badRequest(w, http.StatusBadRequest, "只接受一个接触样本")
		return
	}
	in.ID, in.AccountTag = strings.ToLower(in.ID), strings.ToLower(in.AccountTag)
	now := time.Now().UTC()
	_, known := s.st.Gateways()[in.Gateway]
	if !known || !validContactObservation(in, now) {
		s.badRequest(w, http.StatusBadRequest, "接触样本字段无效")
		return
	}
	added, err := s.st.AddContactReport(types.ContactReport{ContactObservation: in, ReporterID: caller.reporterID(), ReceivedAt: now})
	if errors.Is(err, types.ErrContactReportConflict) {
		s.badRequest(w, http.StatusConflict, "接触 ID 已用于不同内容")
		return
	}
	if err != nil {
		s.opt.Log.Error("接触样本写盘失败", "err", err)
		http.Error(w, "接触样本暂存失败", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "added": added})
}

func (s *Server) contactStats(r *http.Request, now time.Time) map[string][]types.ContactStats {
	tag := strings.ToLower(r.Header.Get("X-Gwpool-Account-Tag"))
	if !validCooldownTag(tag) {
		return nil
	}
	caller, ok := s.resolveCaller(r)
	if !ok || caller.admin {
		return nil
	}
	if _, ok := s.st.(interface{ Revisions() store.Revisions }); ok {
		byConsumer, _ := s.contactData(now)
		return byConsumer[reportConsumer{caller.reporterID(), tag}]
	}
	var reports []types.ContactReport
	for _, report := range s.st.ContactReports() {
		if report.ReporterID == caller.reporterID() && report.AccountTag == tag {
			reports = append(reports, report)
		}
	}
	out := map[string][]types.ContactStats{}
	for _, row := range contact.Summarize(reports, now) {
		out[row.Gateway] = append(out[row.Gateway], row)
	}
	return out
}

func (s *Server) encodedContactStats(r *http.Request, now time.Time) map[string]json.RawMessage {
	tag := strings.ToLower(r.Header.Get("X-Gwpool-Account-Tag"))
	if !validCooldownTag(tag) {
		return nil
	}
	caller, ok := s.resolveCaller(r)
	if !ok || caller.admin {
		return nil
	}
	if _, ok := s.st.(interface{ Revisions() store.Revisions }); !ok {
		rows := s.contactStats(r, now)
		out := make(map[string]json.RawMessage, len(rows))
		for gateway, stats := range rows {
			out[gateway], _ = json.Marshal(stats)
		}
		return out
	}
	_, _, encoded := s.contactDataEncoded(now)
	return encoded[reportConsumer{caller.reporterID(), tag}]
}
