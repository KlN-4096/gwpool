package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestReportBatchesMixedMembersAndItemValidation(t *testing.T) {
	for _, kind := range []string{"cooldown", "contact"} {
		t.Run(kind, func(t *testing.T) {
			srv, handler, _, st, _ := rig(t, types.ModePool)
			key := srv.ConsumerKeyFor(st.accounts[0])
			gateway := "unified-142"
			st.gws[gateway] = types.Gateway{Name: gateway}
			at := time.Now().UTC().Add(-time.Hour)
			reports := make([]map[string]any, 4)
			for i := range reports {
				reports[i] = map[string]any{"id": fmt.Sprintf("%064x", i+1),
					"account_tag": fmt.Sprintf("%064x", i+10), "gateway": gateway}
				if kind == "cooldown" {
					reports[i]["window_seconds"], reports[i]["elapsed_seconds"] = 7200, 7200
					reports[i]["result"], reports[i]["observed_at"] = "full", at
				} else {
					reports[i]["model"], reports[i]["criterion"] = "model-a", "state-echo-v1"
					reports[i]["source"], reports[i]["first"] = "foreground", "repeat"
					reports[i]["outcome"], reports[i]["at"] = "full", at
				}
			}
			// Type/unknown-field failures belong to their item, not the envelope.
			reports[1]["gateway"] = 17
			reports[2]["unexpected"] = true
			raw, _ := json.Marshal(map[string]any{"reports": reports})
			send := func(raw []byte) *httptest.ResponseRecorder {
				return do(handler, authed(httptest.NewRequest("POST", "/"+kind+"/reports",
					strings.NewReader(string(raw))), key))
			}
			w := send(raw)
			var reply struct {
				OK      bool `json:"ok"`
				Results []struct {
					ID     string `json:"id"`
					Status int    `json:"status"`
				} `json:"results"`
			}
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &reply) != nil || !reply.OK || len(reply.Results) != 4 {
				t.Fatalf("mixed batch failed: %d %s", w.Code, w.Body.String())
			}
			for i, want := range []int{200, 400, 400, 200} {
				if reply.Results[i].ID != reports[i]["id"] || reply.Results[i].Status != want {
					t.Fatalf("item %d: %+v", i, reply.Results[i])
				}
			}
			if w.Body.Len() > 64*1024 {
				t.Fatal("oversized acknowledgement")
			}
			if kind == "cooldown" {
				if len(st.reports) != 2 || st.reports[0].AccountTag == st.reports[1].AccountTag ||
					!st.reports[0].ObservedAt.Equal(at) {
					t.Fatal("lost member or observed_at")
				}
			} else if len(st.contacts) != 2 || st.contacts[0].AccountTag == st.contacts[1].AccountTag {
				t.Fatal("lost member")
			}
			if replay := send(raw); replay.Code != 200 {
				t.Fatal("batch retry failed")
			}
			for _, body := range []string{`{`, `{}`, `{"reports":[]}`, `{"reports":null}`, `{"reports":[{}]} {}`} {
				if got := send([]byte(body)); got.Code != 400 {
					t.Fatalf("invalid envelope: %d %s", got.Code, body)
				}
			}
		})
	}
}

func TestReportBatchLimitFailureAndContactMerge(t *testing.T) {
	for _, kind := range []string{"cooldown", "contact"} {
		srv, handler, _, st, _ := rig(t, types.ModePool)
		key := srv.ConsumerKeyFor(st.accounts[0])
		reports := make([]map[string]any, reportBatchMax)
		for i := range reports {
			reports[i] = map[string]any{"id": fmt.Sprintf("%064x", i+1), "account_tag": fmt.Sprintf("%064x", i+100),
				"gateway": "unified-142"}
			if kind == "contact" {
				for k, v := range map[string]any{"model": "m", "criterion": "state-echo-v1", "source": "foreground",
					"first": "repeat", "at": time.Now().UTC(), "outcome": "full"} {
					reports[i][k] = v
				}
			} else {
				reports[i]["window_seconds"], reports[i]["elapsed_seconds"], reports[i]["result"] = 3600, 3600, "full"
			}
		}
		send := func(reports []map[string]any) *httptest.ResponseRecorder {
			body, _ := json.Marshal(map[string]any{"reports": reports})
			return do(handler, authed(httptest.NewRequest("POST", "/"+kind+"/reports", strings.NewReader(string(body))), key))
		}
		if w := send(append(reports, map[string]any{"id": "extra"})); w.Code != 400 {
			t.Fatalf("33 reports: %d", w.Code)
		}
		if w := send(reports); w.Code != 200 || strings.Count(w.Body.String(), `"status":200`) != reportBatchMax ||
			w.Body.Len() > reportBatchBodyMax {
			t.Fatalf("%s maximum batch: %d %s", kind, w.Code, w.Body.String())
		}
		if kind == "contact" {
			reports[0]["window_final"], reports[0]["full_window_ms"] = true, 1000
			if w := send(reports[:1]); !strings.Contains(w.Body.String(), `"status":200`) ||
				len(st.contacts) != reportBatchMax || !st.contacts[0].WindowFinal {
				t.Fatal("window-final update lost or double counted", w.Body.String())
			}
			reports[0]["outcome"] = "refreshed"
			reports[0]["window_final"], reports[0]["full_window_ms"] = false, 0
		} else {
			reports[0]["result"] = "degraded"
		}
		if w := send(reports[:1]); w.Code != 200 || !strings.Contains(w.Body.String(), `"status":409`) {
			t.Fatal("conflict not item-scoped", w.Body.String())
		}
		st.putErr = errors.New("injected persistence failure")
		if w := send(reports[1:2]); w.Code != 200 || !strings.Contains(w.Body.String(), `"status":500`) {
			t.Fatal("storage failure not item-scoped", w.Body.String())
		}
	}
}

func TestCooldownBatchRecommendationsFollowEachMember(t *testing.T) {
	srv, handler, _, st, _ := rig(t, types.ModePool)
	key := srv.ConsumerKeyFor(st.accounts[0])
	reports := make([]cooldownObservation, 4)
	for i := range reports {
		member := i / 2
		reports[i] = cooldownObservation{ID: fmt.Sprintf("%064x", i+1),
			AccountTag: fmt.Sprintf("%064x", member+100), Gateway: "unified-142",
			WindowSeconds: 3600 * (member + 1), ElapsedSeconds: 3600 * (member + 1),
			Result: "full", ObservedAt: time.Now().UTC()}
	}
	body, _ := json.Marshal(map[string]any{"reports": reports})
	w := do(handler, authed(httptest.NewRequest("POST", "/cooldown/reports", strings.NewReader(string(body))), key))
	var reply struct{ Results []cooldownAcknowledgement }
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &reply) != nil || len(reply.Results) != 4 {
		t.Fatal(w.Code, w.Body.String())
	}
	for i, ack := range reply.Results {
		if ack.Recommendation == nil || ack.Recommendation.Seconds != reports[i].WindowSeconds {
			t.Fatalf("member %d got another member's recommendation: %+v", i, ack)
		}
	}
}

func TestGatewayCatalogSharingContract(t *testing.T) {
	srv, handler, pool, st, _ := rig(t, types.ModePool)
	key := srv.ConsumerKeyFor(st.accounts[0])
	tag := strings.Repeat("a", 64)
	now := time.Now().UTC()
	expires := now.Add(time.Hour)
	st.pairs["unified-142"] = types.Pair{Gateway: "unified-142", Cookie: liveCookie, ExpiresAt: expires}
	reporter := st.accounts[0].ID
	for i := range 5 {
		st.contacts = append(st.contacts, types.ContactReport{ReporterID: reporter, ReceivedAt: now,
			ContactObservation: types.ContactObservation{ID: fmt.Sprintf("%064x", i+1),
				AccountTag: tag, Gateway: "unified-142", At: now, Model: "model-a",
				Source: "foreground", Criterion: "state-echo-v1", First: "repeat", Outcome: "full"}})
	}
	for range 2 {
		st.reports = append(st.reports, types.CooldownReport{ReporterID: reporter, AccountTag: tag,
			Gateway: "unified-142", ReceivedAt: now, WindowSeconds: 3600, ElapsedSeconds: 3600, Result: "full"})
	}
	read := func(account, model, accountTag string) gatewayCatalogResponse {
		r := authed(httptest.NewRequest("GET", "/gateways?account="+account+"&model="+model, nil), key)
		r.Header.Set("X-Gwpool-Account-Tag", accountTag)
		w := do(handler, r)
		var out gatewayCatalogResponse
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil || len(out.Gateways) != 1 {
			t.Fatal("directory failed", w.Code, w.Body.String())
		}
		for i := range out.Gateways {
			if out.Gateways[i].ValidForS != int(expires.Sub(out.SnapshotAt).Seconds()) {
				t.Fatal("TTL is not solely inventory expiry minus snapshot time")
			}
			out.Gateways[i].ValidForS = 0 // account for elapsed wall clock in sequential requests
		}
		return out
	}
	first := read("member-a", "model-a", tag)
	second := read("member-b", "model-a", strings.Repeat("b", 64))
	if first.Account != "member-a" || second.Account != "member-b" {
		t.Fatal("account echo changed")
	}
	a, b := first.Gateways[0], second.Gateways[0]
	if a.Cooldown == nil || len(a.Contacts) == 0 || a.Priority == nil || b.Cooldown != nil || len(b.Contacts) != 0 {
		t.Fatal("fixture or member statistics isolation failed")
	}
	a.Cooldown, b.Cooldown, a.Contacts, b.Contacts = nil, nil, nil, nil
	if !reflect.DeepEqual(a, b) {
		t.Fatal("shared directory fields depend on member", a, b)
	}
	otherModel := read("member-c", "model-b", tag)
	if !reflect.DeepEqual(first.Gateways[0].Cooldown, otherModel.Gateways[0].Cooldown) ||
		string(first.Gateways[0].Contacts) != string(otherModel.Gateways[0].Contacts) {
		t.Fatal("personal statistics depend on model or account")
	}
	if out := read("%20member-a%20", "model-a", tag); out.Account != "member-a" {
		t.Fatal("existing trim contract changed")
	}
	if len(pool.anon) != 0 || len(pool.delivered) != 0 {
		t.Fatal("directory query invoked delivery/demand path")
	}
	if w := do(handler, authed(httptest.NewRequest("GET", "/gateways", nil), key)); w.Code != 400 {
		t.Fatal("missing account accepted")
	}
}

func TestReportBatchDuplicateIDsRejectBeforeWrites(t *testing.T) {
	for _, kind := range []string{"cooldown", "contact"} {
		srv, handler, _, st, _ := rig(t, types.ModePool)
		key := srv.ConsumerKeyFor(st.accounts[0])
		for _, id := range []string{strings.Repeat("a", 64), "bad", ""} {
			body := fmt.Sprintf(`{"reports":[{"id":%q},{"id":%q}]}`, id, strings.ToUpper(id))
			w := do(handler, authed(httptest.NewRequest("POST", "/"+kind+"/reports", strings.NewReader(body)), key))
			if w.Code != 400 || len(st.reports) != 0 || len(st.contacts) != 0 {
				t.Fatalf("%s duplicate envelope accepted: %d %s", kind, w.Code, w.Body.String())
			}
		}
	}
}

func TestReportBatchResponseBudgetRejectsBeforeWrites(t *testing.T) {
	for _, kind := range []string{"contact", "cooldown"} {
		for _, id := range []string{strings.Repeat("<", 12000), strings.Repeat("a", 64500)} {
			srv, handler, _, st, _ := rig(t, types.ModePool)
			key := srv.ConsumerKeyFor(st.accounts[0])
			// A valid first item must not commit before a later oversized ID.
			first := fmt.Sprintf(`{"id":%q,"account_tag":%q,"gateway":"unified-142",`,
				strings.Repeat("1", 64), strings.Repeat("2", 64))
			if kind == "cooldown" {
				first += `"window_seconds":3600,"elapsed_seconds":3600,"result":"full"}`
			} else {
				first += fmt.Sprintf(`"model":"m","criterion":"c","source":"foreground","first":"repeat","outcome":"full","at":%q}`,
					time.Now().UTC().Format(time.RFC3339Nano))
			}
			// Use raw '<' rather than Marshal, otherwise the input already exceeds the read limit.
			body := `{"reports":[` + first + `,{"id":"` + id + `"}]}`
			if len(body) > reportBatchBodyMax {
				t.Fatal("fixture exceeds request body limit")
			}
			w := do(handler, authed(httptest.NewRequest("POST", "/"+kind+"/reports", strings.NewReader(body)), key))
			if w.Code != 400 || w.Body.Len() > reportBatchResponseMax || len(st.contacts)+len(st.reports) != 0 {
				t.Fatal("response budget not checked before writes", w.Code, w.Body.Len())
			}
		}
	}
}
