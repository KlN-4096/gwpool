package httpapi

import (
	"net/http"
	"net/http/httptest"
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

func (q *fakeMintAllQueue) Enqueue() sched.MintAllStatus { q.calls++; return q.status }
func (q *fakeMintAllQueue) Status() sched.MintAllStatus  { return q.status }

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
		r := authed(httptest.NewRequest(http.MethodPost, "/admin/mint-all", nil), tc.key)
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
	for _, want := range []string{"全量打票进行中", `action="/admin/mint-all"`, "排队 13", "成功 2", "区域配额已满"} {
		if !strings.Contains(body, want) {
			t.Fatalf("管理页缺少 %q", want)
		}
	}
	if strings.Contains(body, "<script>alert(1)</script>") || strings.Contains(body, secretToken) {
		t.Fatal("管理页未转义结果或泄漏凭据")
	}
}
