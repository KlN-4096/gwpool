package httpapi

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"gwpool/internal/config"
)

func coveragePost(h http.Handler, form url.Values) *httptest.ResponseRecorder {
	r := authed(httptest.NewRequest(http.MethodPost, "/admin/coverage", strings.NewReader(form.Encode())), "akey")
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return do(h, r)
}

func TestCoverageAdminAddActualExitAndEditSeparateGroupWithoutProbing(t *testing.T) {
	h, srv, path, calls := proxyRig(t)
	form := url.Values{"action": {"region"}, "key": {"africa-south"}, "name": {"南非"},
		"country": {"ZA"}, "city": {"Cape Town"}, "enabled": {"on"}}
	w := coveragePost(h, form)
	if w.Code != http.StatusSeeOther || !strings.Contains(w.Header().Get("Location"), "note=") {
		t.Fatalf("add exit failed: %d %s", w.Code, w.Header().Get("Location"))
	}
	cfg := srv.opt.Live.Get()
	region, ok := cfg.RegionByKey("africa-south")
	if !ok || !region.Enabled || region.Country != "ZA" || region.City != "Cape Town" {
		t.Fatal("new exit was not published to runtime")
	}
	loaded, err := config.Load(path)
	if err != nil || len(loaded.RegionDefinitions()) != 10 {
		t.Fatalf("restart lost added exit: %v", err)
	}
	for _, secret := range []string{"p-secret", "w-secret"} {
		if !strings.Contains(fileText(t, path), secret) {
			t.Fatal("config patch destroyed a pre-existing secret")
		}
	}
	if len(calls.probed) != 0 || len(calls.added) != 0 {
		t.Fatal("adding an exploration definition must not trigger a live proxy probe")
	}
	group := url.Values{"action": {"group"}, "key": {"africa-south"}, "name": {"南部覆盖目标"},
		"exit_keys": {"africa-south", "south-america"}, "targets": {"unified-201\nunified-215"}, "complete": {"on"}}
	w = coveragePost(h, group)
	if !strings.Contains(w.Header().Get("Location"), "note=") {
		t.Fatal("group update failed")
	}
	cfg = srv.opt.Live.Get()
	region, _ = cfg.RegionByKey("africa-south")
	if region.Name != "南非" || region.Country != "ZA" {
		t.Fatal("display grouping changed the actual exit")
	}
	// 停用保留稳定定义，后续调度可读到禁用态。
	form.Del("enabled")
	coveragePost(h, form)
	region, _ = srv.opt.Live.Get().RegionByKey("africa-south")
	if region.Enabled {
		t.Fatal("exit disable was not published")
	}
	before := fileText(t, path)
	form.Set("city", "Johannesburg")
	w = coveragePost(h, form)
	if !strings.Contains(w.Header().Get("Location"), "error=") || before != fileText(t, path) {
		t.Fatal("same ID accepted a different physical exit")
	}
}

func TestCoverageAdminAuthCSRFSizeAndRendering(t *testing.T) {
	h, _, path, _ := proxyRig(t)
	before := fileText(t, path)
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		if w := do(h, httptest.NewRequest(method, "/admin/coverage", nil)); w.Code != http.StatusUnauthorized {
			t.Fatalf("unauthorized %s accepted: %d", method, w.Code)
		}
	}
	r := authed(httptest.NewRequest(http.MethodPost, "/admin/coverage", nil), "akey")
	r.Header.Set("Origin", "https://evil.invalid")
	if w := do(h, r); w.Code != http.StatusForbidden {
		t.Fatalf("cross-site write accepted: %d", w.Code)
	}
	r = authed(httptest.NewRequest(http.MethodPost, "/admin/coverage", strings.NewReader(strings.Repeat("x", coverageFormLimit+1))), "akey")
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if w := do(h, r); w.Code != http.StatusBadRequest {
		t.Fatalf("oversized form accepted: %d", w.Code)
	}
	if fileText(t, path) != before {
		t.Fatal("rejected requests modified config")
	}
	w := do(h, authed(httptest.NewRequest(http.MethodGet, "/admin/coverage?error=%3Cscript%3Ebad%3C%2Fscript%3E", nil), "akey"))
	body := w.Body.String()
	if w.Code != http.StatusOK || !strings.Contains(body, "201 个域名") ||
		!strings.Contains(body, "范围未确认") || !strings.Contains(body, "新增实际探索出口") ||
		!strings.Contains(body, `id="t-datacenter-coverage"`) || !strings.Contains(body, "San Antonio") ||
		!strings.Contains(body, "加入全球国家候选（249") {
		t.Fatalf("coverage page missing its core states: %d", w.Code)
	}
	if strings.Contains(body, "<script>bad") || strings.Contains(body, "p-secret") ||
		strings.Contains(body, secretToken) || strings.Contains(body, "OAILB-SECRET") {
		t.Fatal("coverage page leaked unsafe content")
	}
}

func TestCoverageGlobalCountriesSaveOnceWithoutProbing(t *testing.T) {
	h, srv, path, calls := proxyRig(t)
	form := url.Values{"action": {"global-countries"}}
	for range 2 {
		w := coveragePost(h, form)
		if w.Code != http.StatusSeeOther || !strings.Contains(w.Header().Get("Location"), "note=") {
			t.Fatal("global countries rejected")
		}
	}
	loaded, err := config.Load(path)
	if err != nil || len(loaded.RegionDefinitions()) != 209 || len(srv.opt.Live.Get().RegionDefinitions()) != 209 {
		t.Fatalf("global country definitions missing after reload: %v", err)
	}
	if len(calls.probed) != 0 || len(calls.added) != 0 {
		t.Fatal("config expansion started proxy probes")
	}
	if !strings.Contains(fileText(t, path), "p-secret") {
		t.Fatal("config expansion destroyed existing secrets")
	}
}

func TestCoverageGroupsRejectUnknownExitDuplicateTargetAndEmptyComplete(t *testing.T) {
	h, _, path, _ := proxyRig(t)
	before := fileText(t, path)
	for _, form := range []url.Values{
		{"action": {"group"}, "key": {"x"}, "name": {"X"}, "exit_keys": {"unknown-id"}, "targets": {"unified-1"}},
		{"action": {"group"}, "key": {"x"}, "name": {"X"}, "exit_keys": {"us-east"}, "targets": {"unified-1 unified-1"}},
		{"action": {"group"}, "key": {"x"}, "name": {"X"}, "exit_keys": {"us-east"}, "complete": {"on"}},
	} {
		w := coveragePost(h, form)
		if !strings.Contains(w.Header().Get("Location"), "error=") || fileText(t, path) != before {
			t.Fatal("invalid target definition persisted")
		}
	}
}
