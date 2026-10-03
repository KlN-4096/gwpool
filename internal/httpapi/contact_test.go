package httpapi

import (
	"encoding/json"
	"gwpool/internal/types"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func (f *fakeStore) ContactReports() []types.ContactReport {
	return append([]types.ContactReport(nil), f.contacts...)
}
func (f *fakeStore) AddContactReport(r types.ContactReport) (bool, error) {
	if f.putErr != nil {
		return false, f.putErr
	}
	for i, old := range f.contacts {
		if old.ReporterID == r.ReporterID && old.ID == r.ID {
			merged, err := old.Merge(r)
			if err == nil {
				f.contacts[i] = merged
			}
			return false, err
		}
	}
	f.contacts = append(f.contacts, r)
	return true, nil
}

func TestContactReportValidationAndIsolation(t *testing.T) {
	srv, handler, _, st, _ := rig(t, types.ModePool)
	key := srv.ConsumerKeyFor(st.accounts[0])
	st.gws["unified-142"] = types.Gateway{Name: "unified-142"}
	valid := types.ContactObservation{ID: strings.Repeat("1", 64), AccountTag: strings.Repeat("2", 64),
		Gateway: "unified-142", Model: "model-a", Criterion: "echo-v1", Source: "foreground", First: "repeat",
		At: time.Now().UTC(), GapKnown: true, ElapsedSeconds: 3601, Outcome: "unknown"}
	send := func(key string, body any) int {
		raw, _ := json.Marshal(body)
		return do(handler, authed(httptest.NewRequest(http.MethodPost, "/contact/report", strings.NewReader(string(raw))), key)).Code
	}
	if send("", valid) != http.StatusUnauthorized {
		t.Fatal("accepted anonymous report")
	}
	for _, key := range []string{"akey", "ckey"} {
		if send(key, valid) != http.StatusUnauthorized {
			t.Fatal("accepted shared administrator identity as a consumer reporter")
		}
	}
	for range 2 {
		if code := send(key, valid); code != http.StatusOK {
			t.Fatal(code)
		}
	}
	if len(st.contacts) != 1 || len(st.pairs) != 1 {
		t.Fatal("double count or changed pool")
	}
	bad := valid
	bad.Outcome = "full"
	if code := send(key, bad); code != http.StatusConflict {
		t.Fatal("changed initial outcome", code)
	}
	for _, mutate := range []func(*types.ContactObservation){
		func(r *types.ContactObservation) { r.First = "forever-first" },
		func(r *types.ContactObservation) { r.Source = "untrusted" },
		func(r *types.ContactObservation) { r.Model = "bad\nmodel" },
		func(r *types.ContactObservation) { r.At = r.At.Add(-8 * 24 * time.Hour) },
		func(r *types.ContactObservation) { r.GapKnown = false },
		func(r *types.ContactObservation) { r.WindowFinal = true; r.FullWindowMS = 1 },
	} {
		bad = valid
		mutate(&bad)
		if code := send(key, bad); code != http.StatusBadRequest {
			t.Fatal(bad, code)
		}
	}
	if code := send(key, map[string]any{"cookie": "do-not-collect"}); code != http.StatusBadRequest {
		t.Fatal(code)
	}
}
