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
	now := time.Now().UTC()
	added, status := s.saveContactObservation(in, caller.reporterID(), now)
	switch status {
	case http.StatusBadRequest:
		s.badRequest(w, status, "接触样本字段无效")
	case http.StatusConflict:
		s.badRequest(w, http.StatusConflict, "接触 ID 已用于不同内容")
	case http.StatusInternalServerError:
		http.Error(w, "接触样本暂存失败", http.StatusInternalServerError)
	default:
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "added": added})
	}
}

func (s *Server) saveContactObservation(in types.ContactObservation, reporterID int64, now time.Time) (bool, int) {
	in.ID, in.AccountTag = strings.ToLower(in.ID), strings.ToLower(in.AccountTag)
	if !s.hasGateway(in.Gateway) || !validContactObservation(in, now) {
		return false, http.StatusBadRequest
	}
	added, err := s.st.AddContactReport(types.ContactReport{ContactObservation: in, ReporterID: reporterID, ReceivedAt: now})
	if errors.Is(err, types.ErrContactReportConflict) {
		return false, http.StatusConflict
	}
	if err != nil {
		s.opt.Log.Error("接触样本写盘失败", "err", err)
		return false, http.StatusInternalServerError
	}
	return added, http.StatusOK
}

func (s *Server) handleContactReports(w http.ResponseWriter, r *http.Request) {
	caller, ok := s.resolveCaller(r)
	if !ok || caller.admin {
		s.unauthorized(w, "接触上报需要有效的 consumer key")
		return
	}
	items, ok := s.readReportBatch(w, r)
	if !ok {
		return
	}
	type acknowledgement struct {
		ID     string `json:"id"`
		Status int    `json:"status"`
	}
	results := make([]acknowledgement, len(items))
	now := time.Now().UTC()
	for i, item := range items {
		results[i] = acknowledgement{ID: item.id, Status: http.StatusBadRequest}
		var report types.ContactObservation
		if decodeReportItem(item.raw, contactBodyMax, &report) {
			_, results[i].Status = s.saveContactObservation(report, caller.reporterID(), now)
		}
	}
	writeJSON(w, http.StatusOK, struct {
		OK      bool              `json:"ok"`
		Results []acknowledgement `json:"results"`
	}{true, results})
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
	return s.contactSnapshot(now).encodedFor(reportConsumer{caller.reporterID(), tag})
}

func (s *Server) catalogContacts(r *http.Request, model string, now time.Time) (map[string]json.RawMessage, map[string]types.GatewayPriority) {
	tag := strings.ToLower(r.Header.Get("X-Gwpool-Account-Tag"))
	var consumer reportConsumer
	hasConsumer := false
	if validCooldownTag(tag) {
		if caller, ok := s.resolveCaller(r); ok && !caller.admin {
			consumer, hasConsumer = reportConsumer{caller.reporterID(), tag}, true
		}
	}
	if !hasConsumer && model == "" {
		return nil, nil
	}
	// Both fields come from one immutable snapshot, rather than independently
	// locking/rechecking a possibly changing revision twice in this request.
	data := s.contactSnapshot(now)
	var contacts map[string]json.RawMessage
	if hasConsumer {
		contacts = data.encodedFor(consumer)
	}
	return contacts, data.priorities[model]
}
