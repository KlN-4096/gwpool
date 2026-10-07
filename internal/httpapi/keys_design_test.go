package httpapi

import (
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestManagedKeysDetailLinksKeepListFilters(t *testing.T) {
	_, h, st, _, _ := managedKeyRig(t)
	createManagedKeys(t, st, 1, 0, 0)
	w := do(h, authed(httptest.NewRequest("GET", "/admin/keys?source=admin&sort=rpm", nil), "offline-admin"))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	want := `href="/admin/keys?key=admin%3A1&amp;sort=rpm&amp;source=admin"`
	if !strings.Contains(w.Body.String(), want) {
		t.Fatal("detail link loses filters")
	}
	detail := do(h, authed(httptest.NewRequest("GET", "/admin/keys?key=admin%3A1&source=admin&sort=rpm", nil), "offline-admin"))
	if !strings.Contains(detail.Body.String(), `data-list-url="/admin/keys?sort=rpm&amp;source=admin"`) ||
		!strings.Contains(detail.Body.String(), `id="key-detail"`) || !strings.Contains(detail.Body.String(), `data-key-auto`) {
		t.Fatal("direct drawer missing filtered return")
	}
	createManagedKeys(t, st, 50, 0, 0)
	paged := do(h, authed(httptest.NewRequest("GET", "/admin/keys?key=admin%3A1&source=admin&sort=rpm", nil), "offline-admin"))
	if !strings.Contains(paged.Body.String(), `href="/admin/keys?page=2&amp;sort=rpm&amp;source=admin"`) {
		t.Fatal("pagination retained the closed detail key")
	}
}

func TestManagedKeysDesignPreview(t *testing.T) {
	dir := os.Getenv("GWPOOL_KEYS_DESIGN_DIR")
	if dir == "" {
		t.Skip("opt-in offline design fixture")
	}
	s, h, st, _, _ := managedKeyRig(t)
	keys := createManagedKeys(t, st, 6, 120, 4)
	names := []string{"团队接入 · 主应用", "自动化工作流", "开发环境", "临时协作", "测试接入", "旧项目归档"}
	now := time.Now().UTC()
	metrics := map[string]types.KeyUsage{}
	for i, key := range keys {
		label := names[i]
		note := "仅用于离线界面预览，不是真实业务数据。"
		if err := st.UpdateAccessKeys([]string{key.Record.ID}, types.AccessKeyPatch{Action: "edit", Label: &label, Note: &note}); err != nil {
			t.Fatal(err)
		}
		total := uint64(32412 - i*4783)
		metrics[key.Record.ID] = types.KeyUsage{StartedAt: now.Add(-7 * 24 * time.Hour), LastAt: now.Add(-time.Duration(i*17+2) * time.Minute),
			Totals:    types.KeyCounters{Requests: total, OK: total - 48, ClientErrors: 36, ServerErrors: 12, Denied: 9, Tickets: total / 4, Bytes: total * 256, DurationMS: total * 18, Latency: [12]uint64{0, 0, 0, total}, Endpoints: map[string]uint64{"/gateways": total * 3 / 4, "/cookie": total / 4}},
			RecentIPs: []string{"127.0.0.1"}, AccountHashes: []string{"offline-a", "offline-b"},
			Hours: []types.KeyHour{{At: now.Truncate(time.Hour), Counters: types.KeyCounters{Requests: 184, OK: 181, ClientErrors: 2, ServerErrors: 1, Tickets: 45}}}}
	}
	if err := st.UpdateAccessKeys([]string{keys[3].Record.ID}, types.AccessKeyPatch{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateAccessKeys([]string{keys[5].Record.ID}, types.AccessKeyPatch{Action: "revoke"}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveAccessKeyUsage(metrics); err != nil {
		t.Fatal(err)
	}
	s.initKeyMetrics()
	for i, key := range keys {
		e := s.keyMetrics.entries[key.Record.ID]
		e.recent[now.Unix()%keyRecentSeconds] = keySecond{at: now.Unix(), count: uint64(12 - i*2)}
	}
	paths := map[string]string{
		"list.html":    "/admin/keys",
		"detail.html":  "/admin/keys?key=" + url.QueryEscape(keys[0].Record.ID),
		"detail2.html": "/admin/keys?key=" + url.QueryEscape(keys[1].Record.ID),
		"upload.html":  "/admin/keys?key=upload%3A1&source=upload",
		"empty.html":   "/admin/keys?q=no-match",
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for name, path := range paths {
		w := do(h, authed(httptest.NewRequest("GET", path, nil), "offline-admin"))
		if w.Code != 200 {
			t.Fatal(name, w.Code)
		}
		if err := os.WriteFile(filepath.Join(dir, name), w.Body.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/admin/keys", nil)
	s.renderKeys(w, r, keys[0].Key+"\n"+keys[1].Key+"\n"+keys[2].Key, "已生成 3 把独立 Key")
	if w.Code != 200 {
		t.Fatal("generated fixture", w.Code)
	}
	if err := os.WriteFile(filepath.Join(dir, "created.html"), w.Body.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
}
