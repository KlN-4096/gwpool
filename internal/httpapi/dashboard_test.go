package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/sched"
	"gwpool/internal/types"
)

func TestDashboardEffectiveAccountsExcludeUnprovenExpiredAndDisabled(t *testing.T) {
	_, handler, pool, st, _ := rig(t, types.ModePool)
	now := time.Now()
	st.accounts = nil
	st.mints = map[string]types.RegionMint{}
	for id := int64(1); id <= 6; id++ {
		token := fakeToken("account", "free", now.Add(time.Hour))
		if id == 3 {
			token = fakeToken("account", "free", now.Add(-time.Minute))
		}
		if id == 6 {
			token = "malformed-secret-token"
		}
		st.accounts = append(st.accounts, types.Account{ID: id, Enabled: id != 4,
			Creds: types.Creds{AccessToken: token, AccountID: "account"}})
		if id != 5 {
			st.mints[string(rune('a'+id))] = types.RegionMint{AccountID: id, Count: 1}
		}
	}
	st.accounts = append(st.accounts, types.Account{ID: 7, Enabled: true,
		Creds: types.Creds{AccountID: "anon:outside"}})
	pool.holds = map[int64]time.Duration{2: time.Hour}
	for _, path := range []string{"/", "/admin/"} {
		body := do(handler, authed(httptest.NewRequest("GET", path, nil), "akey")).Body.String()
		for _, want := range []string{`data-stat="effective">2</b>`, `data-stat="hosted">6</b>`,
			`待验证 1`, `已停用 1`, `凭据异常 2`, `其中上游暂限 1`} {
			if !strings.Contains(body, want) {
				t.Errorf("%s missing %s", path, want)
			}
		}
		if strings.Contains(body, "malformed-secret-token") {
			t.Fatal("credential leaked")
		}
	}
	if pool.calls != 0 {
		t.Fatal("view triggered delivery")
	}
}

func TestDashboardSupplyDoesNotInferMintingFromCookieExpiry(t *testing.T) {
	_, handler, _, st, _ := rig(t, types.ModePool)
	st.samples = nil // Long-lived donor cookies are not proof of recent requests.
	body := do(handler, httptest.NewRequest("GET", "/", nil)).Body.String()
	for _, bad := range []string{"在涨", "在降", "铸入速度", "到期是唯一", "最近 10 分钟铸入"} {
		if strings.Contains(body, bad) {
			t.Errorf("unsupported supply inference: %s", bad)
		}
	}
}

func TestDashboardGatewayReadyAndShortAreDifferent(t *testing.T) {
	_, handler, _, st, _ := rig(t, types.ModePool)
	st.gws["unified-short"] = types.Gateway{Name: "unified-short"}
	st.pairs["unified-short"] = types.Pair{Gateway: "unified-short", Cookie: liveCookie,
		ExpiresAt: time.Now().Add(time.Minute), CflbExpiresAt: time.Now().Add(time.Minute)}
	body := do(handler, authed(httptest.NewRequest("GET", "/admin/", nil), "akey")).Body.String()
	for _, want := range []string{`data-facet="ready"`, `data-facet="short"`, "寿命不足"} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
}

func TestDashboardEvidenceAndSamplesRemainSeparate(t *testing.T) {
	now := time.Now()
	stats := supplySnapshot(map[string]types.Gateway{"gw": {}},
		map[string]types.Pair{"gw": {ExpiresAt: now.Add(types.DeliverFloor)}}, []types.Sample{
			{At: now.Add(-time.Second), Status: 200, Gateway: "gw"},
			{At: now, Status: 200, Gateway: "gw", Err: "unconfirmed"},
			{At: now, Status: 0, Gateway: "target"},
			{At: now.Add(time.Minute), Status: 200, Gateway: "future"},
			{At: now.Add(-time.Hour), Status: 200, Gateway: "old"},
		}, now, 3)
	if stats.Ready != 1 || stats.Expiring != 1 || stats.Attempts != 3 || stats.Landed != 1 || stats.Gap != 2 {
		t.Fatalf("mixed supply evidence: %+v", stats)
	}
	account := types.Account{Enabled: true, Creds: types.Creds{AccountID: "account",
		AccessToken: fakeToken("account", "free", time.Unix(0, 0))}}
	if got := accountHealthState(account, true, now); got != "effective" {
		t.Fatalf("no-exp historical evidence rejected: %s", got)
	}
	account.Creds.AccountID = "conflicting"
	if got := accountHealthState(account, true, now); got != "invalid" {
		t.Fatal("identity conflict counted effective")
	}
}

func TestDashboardGatewayStatsDoNotMixGatewaysOrDenominators(t *testing.T) {
	rows := []adminGatewayRow{{Name: "gw-a"}, {Name: "gw-b"}}
	attachGatewayStats(rows, []types.ContactStats{
		{Gateway: "gw-a", Model: "luna", Full: 2, Refreshed: 1, Unknown: 3},
		{Gateway: "gw-a", Model: "astra", Full: 4},
		{Gateway: "gw-b", Model: "luna", Unknown: 1},
	}, []types.CooldownStats{{Gateway: "gw-a", Successes: 50, Failures: 10,
		Recommendation: types.CooldownRecommendation{Seconds: 7200}}})
	if rows[0].Full != 6 || rows[0].Unknown != 3 || len(rows[0].Contacts) != 2 ||
		rows[0].Cooldown.Successes != 50 || rows[1].Full != 0 || rows[1].Cooldown.Samples != 0 {
		t.Fatal("mixed statistics")
	}
}

// Render a synthetic, credential-free fixture for optional local visual QA.
// It also checks that regrouping the form preserves all submitted settings.
func TestDashboardSettingsFixture(t *testing.T) {
	srv, handler, pool, st, _ := rig(t, types.ModePool)
	cfg := config.Defaults()
	cfg.Models, cfg.Model = []string{"gpt-6-luna", "gpt-6-astra"}, "gpt-6-luna"
	cfg.SpareTarget, cfg.MinPairs = 20, 90
	cfg.RenewPairs = true
	srv.opt.Live = config.NewLive("fixture/config.json", cfg)
	st.accounts[0].Creds.AccessToken = fakeToken(st.accounts[0].Creds.AccountID, "pro", time.Now().Add(time.Hour))
	st.accounts[1].Creds.AccessToken = fakeToken(st.accounts[1].Creds.AccountID, "plus", time.Now().Add(time.Hour))
	pool.target = 106
	st.accounts[0].Label = "fixture-long-account-for-preview-with-many-characters@example.invalid"
	basePair := st.pairs["unified-142"]
	for i := 1; i <= 32; i++ {
		name := fmt.Sprintf("unified-%d", i)
		st.gws[name] = types.Gateway{Name: name, FirstSeenBy: 1, FirstSeenAt: time.Now().Add(-time.Duration(i) * time.Hour)}
		pair := basePair
		pair.Gateway = name
		st.pairs[name] = pair
		st.samples = append(st.samples, types.Sample{AccountID: 1, Gateway: name, Status: 200,
			Observed: types.Observed{Country: "JP", City: fmt.Sprintf("Preview City %d with a long district name", i)}})
	}
	st.contacts = []types.ContactReport{{ReporterID: 1, ContactObservation: types.ContactObservation{
		Gateway: "unified-142", Model: "gpt-6-luna", Criterion: "echo-v1", Source: "foreground",
		First: "repeat", GapKnown: true, ElapsedSeconds: 7200, Outcome: "full", At: time.Now(),
		FullWindowMS: 100000, WindowFinal: true,
	}}}
	st.reports = []types.CooldownReport{{ReporterID: 1, Gateway: "unified-142",
		Result: "full", WindowSeconds: 7200, ElapsedSeconds: 7200, ReceivedAt: time.Now()}}
	admin := do(handler, authed(httptest.NewRequest("GET", "/admin/", nil), "akey"))
	if admin.Code != 200 {
		t.Fatal(admin.Body.String())
	}
	for _, field := range []string{"models", "public_base_url", "models_extra", "upstream_per_region_hour",
		"upstream_per_account_hour", "slot_cooldown_minutes", "max_tries_per_deliver", "spare_target",
		"min_pairs", "min_accounts", "public_enabled", "renew_pairs", "verify_full_at_deliver",
		"public_requests_per_minute", "public_max_concurrent", "public_max_queued", "public_queue_wait_ms"} {
		if !strings.Contains(admin.Body.String(), `name="`+field+`"`) {
			t.Errorf("setting lost: %s", field)
		}
	}
	values := url.Values{"models": cfg.Models, "spare_target": {"20"}, "min_pairs": {"90"},
		"renew_pairs": {"1"}, "upstream_per_region_hour": {"2"}, "upstream_per_account_hour": {"50"},
		"slot_cooldown_minutes": {"240"}, "max_tries_per_deliver": {"2"}, "min_accounts": {"2"}}
	request := httptest.NewRequest(http.MethodPost, "/admin/config", strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := request.ParseForm(); err != nil {
		t.Fatal(err)
	}
	changes, err := configChanges(request, cfg)
	if err != nil || changes["spare_target"] != 20 || changes["min_pairs"] != 90 ||
		changes["renew_pairs"] != true || changes["slot_cooldown_minutes"] != 240 ||
		changes["upstream_per_account_hour"] != 50 || changes["verify_full_at_deliver"] != false {
		t.Fatalf("grouped form changed settings: %v %v", changes, err)
	}
	if dir := os.Getenv("GWPOOL_PREVIEW_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		srv.opt.MintAll = &fakeMintAllQueue{status: sched.MintAllStatus{Active: true, Total: 36, Queued: 30, Running: 2, Succeeded: 3, Failed: 1, Started: time.Now().Add(-time.Minute), Expires: time.Now().Add(14 * time.Minute)}}
		pages := map[string]string{"admin.html": admin.Body.String(),
			"index.html":    do(handler, httptest.NewRequest("GET", "/", nil)).Body.String(),
			"owner.html":    do(handler, ownerCookieOf(httptest.NewRequest("GET", "/u/"+ownerTok1, nil), ownerTok1)).Body.String(),
			"account.html":  do(handler, httptest.NewRequest("GET", "/a/vt-1", nil)).Body.String(),
			"coverage.html": do(handler, authed(httptest.NewRequest("GET", "/admin/coverage", nil), "akey")).Body.String(),
			"mint.html":     do(handler, authed(httptest.NewRequest("GET", "/admin/mint-all", nil), "akey")).Body.String(),
		}
		st.accounts[1].Enabled = false
		st.accounts[1].DisabledReason = "铸票 HTTP 401：fixture"
		pages["cleanup.html"] = do(handler, authed(httptest.NewRequest("GET", "/admin/accounts/cleanup-401", nil), "akey")).Body.String()
		srv.opt.SiteKey = "fixture-only"
		pages["gate.html"] = do(handler, httptest.NewRequest("GET", "/", nil)).Body.String()
		for name, body := range pages {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}
