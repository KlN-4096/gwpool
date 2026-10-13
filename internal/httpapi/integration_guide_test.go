package httpapi

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"gwpool/internal/config"
	"gwpool/internal/types"
	"gwpool/web"
)

func TestIntegrationGuideIsSharedWithoutPrivatePageAccess(t *testing.T) {
	for _, baseURL := range []string{"", "https://pool.example.com"} {
		t.Run(baseURL, func(t *testing.T) {
			srv, handler, _, st, _ := rig(t, types.ModePool)
			cfg := config.Defaults()
			cfg.PublicBaseURL = baseURL
			srv.opt.Live = config.NewLive("unused-fixture-config", cfg)
			var first string
			for _, path := range []string{"/", "/a/vt-1", "/u/" + ownerTok1} {
				w := do(handler, httptest.NewRequest(http.MethodGet, path, nil))
				if w.Code != http.StatusOK {
					t.Fatalf("%s: %d", path, w.Code)
				}
				body := w.Body.String()
				const marker = `<div class="card" data-integration-guide>`
				if strings.Count(body, marker) != 1 {
					t.Fatalf("%s must render exactly one guide", path)
				}
				_, guide, _ := strings.Cut(body, marker)
				guide, _, _ = strings.Cut(guide, "</section>")
				guide = strings.TrimSpace(guide)
				if first == "" {
					first = guide
				} else if guide != first {
					t.Fatalf("%s rendered a different protocol guide", path)
				}
				for _, want := range []string{
					"HTTP 端点速查", "Authorization: Bearer", "chatgpt_account_id/chatgpt_user_id",
					"池端只校验格式", "整个 URL 必须加引号",
					"12次/秒、444次/分钟", "外层200不代表全部成功",
					"批内重复ID", "原升级提示", "Retry-After",
				} {
					if !strings.Contains(guide, want) {
						t.Errorf("%s missing %q", path, want)
					}
				}
				for _, private := range []string{secretToken, liveCookie, ownerTok1,
					srv.OwnerKeyFor(st.owners[0]), srv.ConsumerKeyFor(st.accounts[0])} {
					if strings.Contains(guide, private) {
						t.Fatalf("%s guide disclosed a private value", path)
					}
				}
			}
		})
	}
}

func TestIntegrationGuideListsEachEndpointOnce(t *testing.T) {
	raw, err := web.Templates.ReadFile("templates/integration-guide.html")
	if err != nil {
		t.Fatal(err)
	}
	guide := string(raw)
	endpoints := []string{
		"GET /gateways", "GET /cookie",
		"POST /contact/report", "POST /cooldown/report",
		"POST /contact/reports", "POST /cooldown/reports",
		"POST /release", "POST /pair/renew",
	}
	for _, endpoint := range endpoints {
		if strings.Count(guide, "<td><code>"+endpoint+"</code></td>") != 1 {
			t.Errorf("expected exactly one row for %s", endpoint)
		}
	}
	rows := regexp.MustCompile(`(?s)<tr>(.*?)</tr>`).FindAllStringSubmatch(guide, -1)
	if len(rows) != len(endpoints)+1 {
		t.Fatalf("expected one header and %d endpoint rows, got %d rows", len(endpoints), len(rows))
	}
	for _, row := range rows[1:] {
		if strings.Count(row[1], "<td>") != 3 {
			t.Error("each endpoint needs endpoint, usage and limits columns")
		}
	}
	if strings.Contains(guide, "<pre") || strings.Contains(guide, "<details") {
		t.Fatal("keep the quick reference free of long tutorials and example blocks")
	}
}

func TestIntegrationErrorsKeepSub2APIUpgradeGuidance(t *testing.T) {
	_, handler, _, _, _ := rig(t, types.ModePool)
	for _, path := range []string{
		"/cookie?gateway=unified-142", "/gateways",
		"/cookie?account=workspace-abc/user-xyz",
		"/cookie?account=workspace-abc/user-xyz&gateway=unified-142&force=false",
	} {
		w := do(handler, authed(httptest.NewRequest(http.MethodGet, path, nil), "ckey"))
		if w.Code != http.StatusBadRequest || decodeFail(t, w).Error.Code != codeBadRequest ||
			!strings.Contains(w.Body.String(), "请升级到最新版 Sub2api") {
			t.Fatalf("upgrade guidance missing for %s: %d %s", path, w.Code, w.Body.String())
		}
	}
	// The guide must not claim that the pool validates a real upstream identity.
	w := do(handler, authed(httptest.NewRequest(http.MethodGet, "/cookie?account=1&gateway=unified-142", nil), "ckey"))
	if w.Code != http.StatusOK {
		t.Fatal("copy-only change tightened the existing account validator")
	}
}
