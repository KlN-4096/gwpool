package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func (f *fakeStore) CooldownReports() []types.CooldownReport {
	return append([]types.CooldownReport(nil), f.reports...)
}

func (f *fakeStore) AddCooldownReport(report types.CooldownReport) (bool, error) {
	if f.putErr != nil {
		return false, f.putErr
	}
	for _, prev := range f.reports {
		if prev.ReporterID == report.ReporterID && prev.ID == report.ID {
			if !prev.SameEvent(report) {
				return false, types.ErrCooldownReportConflict
			}
			return false, nil
		}
	}
	f.reports = append(f.reports, report)
	return true, nil
}

func TestCooldownReportAuthenticationValidationAndIdempotency(t *testing.T) {
	_, handler, _, st, _ := rig(t, types.ModePool)
	st.gws["unified-142"] = types.Gateway{Name: "unified-142"}
	valid := map[string]any{
		"id": strings.Repeat("1", 64), "account_tag": strings.Repeat("2", 64), "gateway": "unified-142",
		"window_seconds": 3600, "elapsed_seconds": 3601, "result": "full",
	}
	send := func(key string, body any) int {
		raw, _ := json.Marshal(body)
		return do(handler, authed(httptest.NewRequest(http.MethodPost, "/cooldown/report", strings.NewReader(string(raw))), key)).Code
	}
	if code := send("", valid); code != http.StatusUnauthorized {
		t.Fatalf("匿名上报未拒绝：%d", code)
	}
	for range 2 {
		if code := send("akey", valid); code != http.StatusOK {
			t.Fatalf("合法上报失败：%d", code)
		}
	}
	if len(st.reports) != 1 || st.reports[0].ReceivedAt.IsZero() {
		t.Fatal("重放样本不幂等或没有服务端时间")
	}
	valid["result"] = "degraded"
	if code := send("akey", valid); code != http.StatusConflict {
		t.Fatalf("同ID不同内容应返回409：%d", code)
	}
	valid["result"] = "full"
	for _, field := range []string{"window_seconds", "elapsed_seconds", "result", "account_tag", "gateway"} {
		bad := map[string]any{}
		for k, v := range valid {
			bad[k] = v
		}
		bad[field] = "invalid"
		if code := send("akey", bad); code != http.StatusBadRequest {
			t.Fatalf("%s 没过边界校验：%d", field, code)
		}
	}
	valid["cookie"] = "must-not-be-collected"
	if code := send("akey", valid); code != http.StatusBadRequest {
		t.Fatalf("含未约定敏感字段的上报未拒绝：%d", code)
	}
	if len(st.pairs) != 1 {
		t.Fatal("上报改动了票池")
	}
}

func TestCooldownBatchAcknowledgesEachItemAndPreservesEventTime(t *testing.T) {
	_, handler, _, st, _ := rig(t, types.ModePool)
	st.gws["unified-142"] = types.Gateway{Name: "unified-142"}
	at := time.Now().UTC().Add(-6 * time.Hour)
	valid := map[string]any{
		"id": strings.Repeat("1", 64), "account_tag": strings.Repeat("2", 64), "gateway": "unified-142",
		"window_seconds": 7200, "elapsed_seconds": 7200, "result": "full", "observed_at": at,
	}
	raw, _ := json.Marshal(map[string]any{"reports": []any{valid, map[string]any{"id": "bad"}}})
	r := authed(httptest.NewRequest(http.MethodPost, "/cooldown/reports", strings.NewReader(string(raw))), "akey")
	w := do(handler, r)
	if w.Code != http.StatusOK {
		t.Fatalf("batch endpoint missing or failed: %d %s", w.Code, w.Body.String())
	}
	var response struct {
		Results []struct {
			ID     string `json:"id"`
			Status int    `json:"status"`
		} `json:"results"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || len(response.Results) != 2 ||
		response.Results[0].Status != 200 || response.Results[1].Status != 400 || len(st.reports) != 1 {
		t.Fatalf("partial acknowledgement lost good events: %s", w.Body.String())
	}
	stored, _ := json.Marshal(st.reports[0])
	if !strings.Contains(string(stored), at.Format(time.RFC3339Nano)) {
		t.Fatalf("batch arrival cannot replace event time: %s", stored)
	}
	replay := do(handler, authed(httptest.NewRequest(http.MethodPost, "/cooldown/reports", strings.NewReader(string(raw))), "akey"))
	if replay.Code != http.StatusOK || len(st.reports) != 1 {
		t.Fatal("batch retries are not idempotent")
	}
	valid["observed_at"] = at.Add(time.Second)
	changed, _ := json.Marshal(map[string]any{"reports": []any{valid}})
	conflict := do(handler, authed(httptest.NewRequest(http.MethodPost, "/cooldown/reports", strings.NewReader(string(changed))), "akey"))
	if !strings.Contains(conflict.Body.String(), `"status":409`) {
		t.Fatal("event timestamp must stay frozen for one ID")
	}
	for _, test := range []struct {
		body, key string
		status    int
	}{
		{string(raw), "", http.StatusUnauthorized},
		{`{"reports":[]}`, "akey", http.StatusBadRequest},
		{`{"reports":[{"token":"must-not-collect"}]}`, "akey", http.StatusBadRequest},
		{`{"reports":[]} {}`, "akey", http.StatusBadRequest},
	} {
		w := do(handler, authed(httptest.NewRequest(http.MethodPost, "/cooldown/reports", strings.NewReader(test.body)), test.key))
		if w.Code != test.status {
			t.Fatalf("invalid batch accepted: %d want %d", w.Code, test.status)
		}
	}
}

func TestCooldownRecommendationIsAccountScopedAndAdminVisible(t *testing.T) {
	_, handler, _, st, _ := rig(t, types.ModePool)
	tag := strings.Repeat("2", 64)
	st.gws["unified-142"] = types.Gateway{Name: "unified-142"}
	st.pairs["unified-142"] = types.Pair{Gateway: "unified-142", Cookie: liveCookie, ExpiresAt: time.Now().Add(time.Hour)}
	for range 2 {
		st.reports = append(st.reports, types.CooldownReport{
			AccountTag: tag, Gateway: "unified-142", WindowSeconds: 7200, ElapsedSeconds: 7201,
			Result: "full", ReceivedAt: time.Now(),
		})
	}
	// 管理 key 的报告者命名空间为 0；另一个匿名账号标签没有个人推荐。
	recs := func(accountTag string) string {
		r := authed(httptest.NewRequest(http.MethodGet, "/gateways?account=1", nil), "akey")
		r.Header.Set("X-Gwpool-Account-Tag", accountTag)
		w := do(handler, r)
		if w.Code != 200 {
			t.Fatalf("清单失败：%d", w.Code)
		}
		return w.Body.String()
	}
	if !strings.Contains(recs(tag), `"seconds":7200`) || strings.Contains(recs(strings.Repeat("3", 64)), `"cooldown"`) {
		t.Fatal("个人推荐没有按匿名账号隔离")
	}
	page := do(handler, authed(httptest.NewRequest(http.MethodGet, "/admin/", nil), "akey")).Body.String()
	if !strings.Contains(page, "消费端冷却统计") || !strings.Contains(page, "建议 7200 秒") {
		t.Fatal("管理页缺少统计与样本建议")
	}
}
