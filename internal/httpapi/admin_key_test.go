package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"gwpool/internal/sched"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

func adminKeyCreationForm(t *testing.T, h http.Handler) url.Values {
	t.Helper()
	page := do(h, authed(httptest.NewRequest("GET", "/admin/keys?source=admin", nil), "offline-admin"))
	match := regexp.MustCompile(`name="creation_token" value="([^"]+)"`).FindStringSubmatch(page.Body.String())
	if page.Code != 200 || len(match) != 2 {
		t.Fatal("admin creation form unavailable")
	}
	return url.Values{"source": {"admin"}, "action": {"create"},
		"creation_token": {match[1]}, "key_type": {"admin_key"}, "count": {"1"}, "rpm": {"0"}, "concurrency": {"0"}}
}

func createAdminKeyFromPanel(t *testing.T, h http.Handler) string {
	t.Helper()
	created := keyAdminForm(h, adminKeyCreationForm(t, h))
	export := regexp.MustCompile(`(?s)id="created-keys"[^>]*>(.*?)</textarea>`).FindStringSubmatch(created.Body.String())
	if created.Code != 200 || len(export) != 2 || strings.Contains(export[1], "\n") {
		t.Fatal("AdminKey generation failed")
	}
	return export[1]
}

func TestAdminKeyCreationFromPanelPersistsExplicitPrivilege(t *testing.T) {
	_, h, st, _, _ := managedKeyRig(t)
	secret := createAdminKeyFromPanel(t, h)
	key, ok := st.ResolveIssuedAccessKey(secret)
	body, err := json.Marshal(key)
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(body, &metadata); err != nil {
		t.Fatal(err)
	}
	if !ok || metadata["admin_key"] != true {
		t.Fatal("AdminKey was silently created as an ordinary limited consumer key")
	}
}

func TestAdminKeyBypassesMemberLimitThroughAuthenticatedHTTP(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	now := time.Now().UTC()
	if _, err := st.UpsertGateway("unified-142", 0, types.Exit{}); err != nil {
		t.Fatal(err)
	}
	if err := st.PutPair(types.Pair{Gateway: "unified-142", Cookie: liveCookie, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	pool := sched.New(st, nil, sched.Options{Mode: types.ModePool, Now: func() time.Time { return now }})
	srv, err := New(pool, st, nil, Options{AdminKey: "offline-admin", ConsumerKey: "offline-root"})
	if err != nil {
		t.Fatal(err)
	}
	h := srv.Handler()
	secret := createAdminKeyFromPanel(t, h)
	for range 20 {
		w := do(h, authed(httptest.NewRequest("GET", "/cookie?account=member&gateway=unified-142", nil), secret))
		if w.Code != 200 {
			t.Fatalf("AdminKey was throttled despite valid stock: status=%d", w.Code)
		}
	}
	key, _ := st.ResolveIssuedAccessKey(secret)
	stats, _ := srv.keyMetrics.snapshot(time.Now())
	if stats[key.ID].Usage.Totals.Requests != 20 || stats[key.ID].Usage.Totals.Tickets != 20 ||
		stats[key.ID].Active != 0 || len(st.ExternalUsage()) != 1 {
		t.Fatal("AdminKey bypassed telemetry along with limits")
	}
	ordinary := createManagedKeys(t, st, 1, 0, 0)[0]
	path := "/cookie?account=member&gateway=unified-142"
	for range 12 {
		if w := do(h, authed(httptest.NewRequest("GET", path, nil), ordinary.Key)); w.Code != 200 {
			t.Fatal("AdminKey traffic consumed ordinary member allowance")
		}
	}
	w := do(h, authed(httptest.NewRequest("GET", path+"&admin_key=true&key_type=admin_key", nil), ordinary.Key))
	if w.Code != 429 {
		t.Fatal("query parameters forged AdminKey privilege")
	}
	if w := do(h, authed(httptest.NewRequest("GET", path, nil), secret)); w.Code != 200 {
		t.Fatal("exhausted ordinary member allowance blocked AdminKey")
	}
	for _, action := range []string{"disable", "enable", "revoke"} {
		if err := st.UpdateAccessKeys([]string{key.ID}, types.AccessKeyPatch{Action: action}); err != nil {
			t.Fatal(err)
		}
		want := 401
		if action == "enable" {
			want = 200
		}
		if w := do(h, authed(httptest.NewRequest("GET", path, nil), secret)); w.Code != want {
			t.Fatalf("AdminKey %s policy not applied immediately: %d", action, w.Code)
		}
	}
}

func TestAdminKeyRemainsConsumerOnlyAndCannotBePromotedByEdit(t *testing.T) {
	_, h, st, p, _ := managedKeyRig(t)
	secret := createAdminKeyFromPanel(t, h)
	key, _ := st.ResolveIssuedAccessKey(secret)
	for _, path := range []string{"/admin/", "/admin/keys", "/admin/security"} {
		if w := do(h, authed(httptest.NewRequest("GET", path, nil), secret)); w.Code != 401 {
			t.Fatal("AdminKey gained administrator authorization", path, w.Code)
		}
	}
	for _, path := range []string{"/cookie", "/cookie?account=member", "/cookie?account=member&gateway=x&force=1"} {
		if w := do(h, authed(httptest.NewRequest("GET", path, nil), secret)); w.Code != 400 {
			t.Fatal("AdminKey bypassed consumer protocol", path, w.Code)
		}
	}
	path := "/cookie?account=member&gateway=unified-142"
	w := do(h, authed(httptest.NewRequest("GET", path, nil), secret))
	if w.Code != 200 || len(p.reqs) != 1 || !p.reqs[0].AdminKey || !strings.Contains(p.anon[0], "authed=true") {
		t.Fatal("trusted AdminKey capability failed to reach delivery")
	}
	plain := createManagedKeys(t, st, 1, 0, 0)[0]
	for _, flag := range []string{"key_type", "admin_key"} {
		w := keyAdminForm(h, url.Values{"action": {"edit"}, "source": {"admin"}, "ids": {plain.Record.ID}, flag: {"admin_key"}})
		if w.Code != 400 {
			t.Fatal("edit attempted to change immutable key type", w.Code)
		}
	}
	if ordinary, _ := st.AccessKey(plain.Record.ID); ordinary.IsAdminKey() {
		t.Fatal("ordinary key promoted through edit")
	}
	form := adminKeyCreationForm(t, h)
	for _, secret := range []string{secret, plain.Key, ""} {
		r := authed(httptest.NewRequest("POST", "http://pool.test/admin/keys", strings.NewReader(form.Encode())), secret)
		r.Header.Set("Origin", "http://pool.test")
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if w := do(h, r); w.Code != 401 {
			t.Fatal("consumer created another AdminKey")
		}
	}
	form.Set("key_type", "arbitrary")
	if w := keyAdminForm(h, form); w.Code != 400 {
		t.Fatal("unknown kind accepted")
	}
	form.Set("key_type", "admin_key")
	r := authed(httptest.NewRequest("POST", "http://pool.test/admin/keys", strings.NewReader(form.Encode())), "offline-admin")
	r.Header.Set("Origin", "https://elsewhere.invalid")
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if w := do(h, r); w.Code != 403 {
		t.Fatal("cross-origin AdminKey creation accepted")
	}
	if current, _ := st.AccessKey(key.ID); !current.IsAdminKey() || !current.Usable() || len(st.AccessKeys()) != 2 {
		t.Fatal("denied mutations changed keys")
	}
}

func TestAdminKeySkipsIndependentAdmissionButKeepsInflightMetrics(t *testing.T) {
	s, _, _, _, _ := managedKeyRig(t)
	key := types.AccessKey{ID: "admin:999", Source: types.AccessKeyAdmin, AdminKey: true, RPM: 1, Concurrency: 1}
	now := time.Now()
	const requests = 16
	for range requests {
		if allowed, _ := s.keyMetrics.begin(key, true, now); !allowed {
			t.Fatal("AdminKey obeyed independent admission")
		}
	}
	if len(s.keyMetrics.admissions) != 0 {
		t.Fatal("AdminKey entered an independent admission queue")
	}
	stats, _ := s.keyMetrics.snapshot(now)
	if stats[key.ID].Active != requests {
		t.Fatal("AdminKey inflight traffic not recorded")
	}
	for range requests {
		s.keyMetrics.finish(key.ID, httptest.NewRequest("GET", "/gateways?account=member", nil), now, 200, 0, 0, true)
	}
	if allowed, _ := s.keyMetrics.begin(key, false, now); allowed {
		t.Fatal("AdminKey flag bypassed failed authorization")
	}
	stats, _ = s.keyMetrics.snapshot(now)
	if stats[key.ID].Active != 0 || stats[key.ID].Usage.Totals.Requests != requests {
		t.Fatal("AdminKey inflight count leaked")
	}
	for _, source := range []string{types.AccessKeyUpload, types.AccessKeyLegacy, ""} {
		key.Source = source
		if key.IsAdminKey() {
			t.Fatal("non-issued source acquired AdminKey privilege")
		}
	}
}

func TestAdminKeyPanelLabelsLimitsNoJSAndReplay(t *testing.T) {
	s, h, st, _, _ := managedKeyRig(t)
	form := adminKeyCreationForm(t, h)
	// No-JS submission may still contain ordinary limit fields. AdminKey
	// always stores zero limits, independent of those disabled UI controls.
	form.Set("rpm", "7")
	form.Set("concurrency", "2")
	created := keyAdminForm(h, form)
	if created.Code != 200 || len(st.AccessKeys()) != 1 {
		t.Fatal("no-JS AdminKey creation failed")
	}
	key := st.AccessKeys()[0]
	if !key.IsAdminKey() || key.RPM != 0 || key.Concurrency != 0 ||
		!strings.Contains(created.Body.String(), "已生成 1 把 AdminKey") {
		t.Fatal("AdminKey kind or fixed no-limit policy lost")
	}
	form.Set("key_type", "regular")
	if w := keyAdminForm(h, form); w.Code != 409 || len(st.AccessKeys()) != 1 {
		t.Fatal("changing kind replayed generation")
	}
	page := do(h, authed(httptest.NewRequest("GET", "/admin/keys?source=admin", nil), "offline-admin"))
	if !strings.Contains(page.Body.String(), `name="key_type"`) ||
		!strings.Contains(page.Body.String(), `value="admin_key">AdminKey`) ||
		!strings.Contains(page.Body.String(), "AdminKey · 直接准入") {
		t.Fatal("AdminKey creation or list policy not visible")
	}
	detail := do(h, authed(httptest.NewRequest("GET", "/admin/keys?key="+url.QueryEscape(key.ID), nil), "offline-admin"))
	panel := regexp.MustCompile(`(?s)<section id="key-detail".*?</section>`).FindString(detail.Body.String())
	if !strings.Contains(panel, "免 Key 与成员消费限额") || strings.Contains(panel, `name="rpm"`) ||
		strings.Contains(panel, `name="concurrency"`) {
		t.Fatal("AdminKey detail offers contradictory limits")
	}
	if dir := os.Getenv("GWPOOL_ADMIN_KEY_PREVIEW_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		for name, body := range map[string][]byte{"index.html": page.Body.Bytes(), "detail.html": detail.Body.Bytes(), "created.html": created.Body.Bytes()} {
			if err := os.WriteFile(filepath.Join(dir, name), body, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	_ = s
}
