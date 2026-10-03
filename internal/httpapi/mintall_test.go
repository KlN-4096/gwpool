package httpapi

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"gwpool/internal/sched"
	"gwpool/internal/types"
)

type fakeMintAllQueue struct {
	calls  int
	status sched.MintAllStatus
}

func (q *fakeMintAllQueue) Confirm(total, queued int, _ ...string) (sched.MintAllStatus, bool) {
	if total != q.status.Total || queued != q.status.Queued {
		return q.status, false
	}
	q.calls++
	return q.status, true
}

func (q *fakeMintAllQueue) Enqueue(...string) sched.MintAllStatus { q.calls++; return q.status }
func (q *fakeMintAllQueue) Preview(...string) sched.MintAllStatus { return q.status }
func (q *fakeMintAllQueue) Status() sched.MintAllStatus           { return q.status }

func TestAdminMintAllRequiresAuthenticationAndSameOrigin(t *testing.T) {
	s, h, _, _, _ := rig(t, types.ModePool)
	q := &fakeMintAllQueue{}
	s.opt.MintAll = q
	for _, tc := range []struct {
		key, origin string
		code        int
	}{
		{"", "", http.StatusUnauthorized},
		{"wrong", "", http.StatusUnauthorized},
		{"akey", "https://evil.example", http.StatusForbidden},
		{"akey", "", http.StatusSeeOther},
	} {
		r := authed(httptest.NewRequest(http.MethodPost, "/admin/mint-all",
			strings.NewReader("action=start&region=europe&total=0&queued=0")), tc.key)
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("Origin", tc.origin)
		w := do(h, r)
		if w.Code != tc.code {
			t.Fatalf("key/origin 边界错误：got=%d want=%d", w.Code, tc.code)
		}
	}
	if q.calls != 1 {
		t.Fatalf("未授权请求触发了排队：%d", q.calls)
	}
	do(h, authed(httptest.NewRequest(http.MethodGet, "/admin/mint-all", nil), "akey"))
	if q.calls != 1 {
		t.Fatal("GET 请求也触发了排队")
	}
	s.opt.MintAll = nil
	if w := do(h, authed(httptest.NewRequest(http.MethodPost, "/admin/mint-all", nil), "akey")); w.Code != 503 {
		t.Fatalf("未接入队列时应明确失败：%d", w.Code)
	}
}

func TestAdminShowsMintAllProgressAndEscapesResults(t *testing.T) {
	s, h, _, _, _ := rig(t, types.ModePool)
	s.opt.MintAll = &fakeMintAllQueue{status: sched.MintAllStatus{
		Active: true, Total: 18, Queued: 13, Running: 1, Succeeded: 2, Failed: 1, Skipped: 1,
		Started: time.Now().UTC(), SkipReasons: map[string]int{"区域配额已满": 1},
		Recent: []sched.MintAllResult{{AccountID: 1, Region: "east-asia", Status: "failed", Reason: "<script>alert(1)</script>"}},
	}}
	body := do(h, authed(httptest.NewRequest(http.MethodGet, "/admin/", nil), "akey")).Body.String()
	for _, want := range []string{"按范围打票进行中", "排队 13", "成功 2", "区域配额已满"} {
		if !strings.Contains(body, want) {
			t.Fatalf("管理页缺少 %q", want)
		}
	}
	if strings.Contains(body, "<script>alert(1)</script>") || strings.Contains(body, secretToken) {
		t.Fatal("管理页未转义结果或泄漏凭据")
	}
}

func TestAdminMintAllNeedsExplicitScopeAndPreviewConfirmation(t *testing.T) {
	s, h, _, _, _ := rig(t, types.ModePool)
	q := &fakeMintAllQueue{status: sched.MintAllStatus{Total: 2, Queued: 2, Accounts: 2, Regions: 1}}
	s.opt.MintAll = q
	post := func(form url.Values) *httptest.ResponseRecorder {
		r := authed(httptest.NewRequest(http.MethodPost, "/admin/mint-all", strings.NewReader(form.Encode())), "akey")
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return do(h, r)
	}
	for _, form := range []url.Values{
		{}, {"action": {"preview"}}, {"action": {"start"}, "region": {"europe"}},
		{"action": {"preview"}, "region": {"not-a-region"}},
	} {
		if w := post(form); w.Code != http.StatusBadRequest {
			t.Fatalf("unsafe form accepted: code=%d", w.Code)
		}
	}
	form := url.Values{"action": {"preview"}, "region": {"europe"}}
	if w := post(form); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "确认执行本范围") || q.calls != 0 {
		t.Fatal("preview must render confirmation without enqueueing")
	}
	form.Set("action", "start")
	form.Set("total", "2")
	form.Set("queued", "1")
	if w := post(form); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "已变化") || q.calls != 0 {
		t.Fatal("stale preview must require reconfirmation")
	}
	form.Set("queued", "2")
	if w := post(form); w.Code != http.StatusSeeOther || q.calls != 1 {
		t.Fatal("confirmed range was not queued")
	}
}
