package httpapi

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestOwnerMintStatisticsUseSuccessfulSamplesWithoutConsumerSlots(t *testing.T) {
	_, handler, pool, st, _ := rig(t, types.ModePool)
	st.slots = map[string]types.Slot{}
	st.samples = []types.Sample{{
		AccountID: 1, At: time.Now().Add(-5 * time.Minute), Status: 200,
		Gateway: "unified-142", Purpose: types.PurposeInventory,
		Route: &types.RouteSample{Result: "productive"},
	}}
	body := do(handler, httptest.NewRequest("GET", "/u/"+ownerTok1, nil)).Body.String()
	for _, want := range []string{
		"<b>1</b><span>铸票尝试</span>",
		"成功获票 <strong>1 次</strong>",
		"<b>5 分钟前</b><span>最近成功铸票</span>",
		"最近成功铸票 <strong>5 分钟前</strong>",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("owner page missing %q", want)
		}
	}
	if strings.Contains(body, "还没取到过票") || strings.Contains(body, "最近一次取票") {
		t.Fatal("owner page still reports legacy consumer delivery")
	}
	if pool.calls != 0 {
		t.Fatal("rendering mint statistics triggered a ticket request")
	}
	admin := do(handler, authed(httptest.NewRequest("GET", "/admin/", nil), "akey")).Body.String()
	_, admin, _ = strings.Cut(admin, `id="tab-accounts"`)
	admin, _, _ = strings.Cut(admin, `id="tab-external"`)
	for _, want := range []string{`<th>最近成功铸票</th>`, `class="l2 a-minted">1</td>`, "5 分钟前"} {
		if !strings.Contains(admin, want) {
			t.Errorf("shared admin account card missing %q", want)
		}
	}
	if strings.Contains(admin, "还没取到过") {
		t.Fatal("admin account card still reports legacy consumer delivery")
	}
}

func TestOwnerMintStatisticsClassifyEvidence(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name      string
		sample    types.Sample
		attempts  int
		minted    int
		timestamp bool
	}{
		{"legacy success", types.Sample{Status: 200, Gateway: "g"}, 1, 1, true},
		{"no landing", types.Sample{Status: 200}, 1, 0, false},
		{"blank landing", types.Sample{Status: 200, Gateway: " "}, 1, 0, false},
		{"rejected with cookie", types.Sample{Status: 401, Gateway: "g"}, 1, 0, false},
		{"legacy error", types.Sample{Status: 200, Gateway: "g", Err: "ambiguous"}, 1, 0, false},
		{"verification", types.Sample{Status: 200, Gateway: "g", Purpose: types.PurposeVerify}, 0, 0, false},
		{"unknown purpose", types.Sample{Status: 200, Gateway: "g", Purpose: "other"}, 0, 0, false},
		{"productive drift", types.Sample{Status: 200, Gateway: "g", Err: "target drift",
			Purpose: types.PurposeRenew, Route: &types.RouteSample{Result: "productive", Target: "other"}}, 1, 1, true},
		{"duplicate landing", types.Sample{Status: 200, Gateway: "g", Purpose: types.PurposeInventory,
			Route: &types.RouteSample{Result: "no_gain"}}, 1, 1, true},
		{"persistence failure", types.Sample{Status: 200, Gateway: "g",
			Route: &types.RouteSample{Result: "persistence_error"}}, 1, 0, false},
		{"invalid response", types.Sample{Status: 200, Gateway: "g",
			Route: &types.RouteSample{Result: "invalid_response_error"}}, 1, 0, false},
		{"unknown route result", types.Sample{Status: 200, Gateway: "g", Route: &types.RouteSample{}}, 1, 0, false},
		{"future record", types.Sample{Status: 200, Gateway: "g", At: now.Add(time.Hour)}, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sample := tc.sample
			sample.AccountID = 1
			if sample.At.IsZero() {
				sample.At = now.Add(-time.Minute)
			}
			got := summarizeAccountMints([]types.Sample{sample}, now)[1]
			if got.attempts != tc.attempts || got.minted != tc.minted || !got.lastAt.IsZero() != tc.timestamp {
				t.Fatalf("stats = %+v, want attempts=%d minted=%d timestamp=%v", got, tc.attempts, tc.minted, tc.timestamp)
			}
		})
	}
}

func TestOwnerMintStatisticsMissingTimeIsNotNoSuccess(t *testing.T) {
	_, handler, _, st, _ := rig(t, types.ModePool)
	st.samples = []types.Sample{{AccountID: 1, Status: 200, Gateway: "g"}}
	body := do(handler, httptest.NewRequest("GET", "/u/"+ownerTok1, nil)).Body.String()
	if !strings.Contains(body, "成功获票 <strong>1 次</strong>") ||
		!strings.Contains(body, "成功时间未记录") || strings.Contains(body, "暂无成功铸票记录") {
		t.Fatal("missing timestamp was confused with no successful mint")
	}
}

func TestOwnerMintStatisticsFixture(t *testing.T) {
	dir := os.Getenv("GWPOOL_OWNER_MINT_PREVIEW_DIR")
	if dir == "" {
		t.Skip("opt-in offline preview")
	}
	_, handler, _, st, _ := rig(t, types.ModePool)
	now := time.Now()
	st.accounts[0].Label = "sample-very-long-account-name+134@example.invalid"
	st.accounts = append(st.accounts,
		types.Account{ID: 4, OwnerID: 1, Label: "failed-only@example.invalid", ViewToken: "vt-4", Enabled: true},
		types.Account{ID: 5, OwnerID: 1, Label: "disabled@example.invalid", ViewToken: "vt-5"},
		types.Account{ID: 6, OwnerID: 1, Label: "time-unknown@example.invalid", ViewToken: "vt-6", Enabled: true},
	)
	st.samples = []types.Sample{
		{AccountID: 1, At: now.Add(-5 * time.Minute), Status: 200, Gateway: "unified-142"},
		{AccountID: 4, At: now, Status: 429},
		{AccountID: 5, At: now, Status: 401},
		{AccountID: 6, Status: 200, Gateway: "unified-142"},
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{"owner": "/u/" + ownerTok1, "admin": "/admin/"} {
		response := do(handler, authed(httptest.NewRequest("GET", path, nil), "akey"))
		if response.Code != http.StatusOK {
			t.Fatalf("%s: HTTP %d", name, response.Code)
		}
		if err := os.WriteFile(filepath.Join(dir, name+".html"), response.Body.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
