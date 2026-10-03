package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gwpool/internal/store"
	"gwpool/internal/types"
)

func TestOwnerKeySurvivesCredentialChangesButRequiresOneActiveCredential(t *testing.T) {
	srv, handler, _, st, _ := rig(t, types.ModePool)
	owner := st.owners[0]
	key := srv.OwnerKeyFor(owner)
	if key == "" || key == owner.Token || key == srv.OwnerKeyFor(st.owners[1]) {
		t.Fatal("owner keys must be independent stable machine credentials")
	}
	request := func() *http.Request { return authed(httptest.NewRequest("GET", "/gateways", nil), key) }
	first, ok := srv.resolveCaller(request())
	if !ok || first.acct.ID != 1 || first.reporterID() != -owner.ID {
		t.Fatal("owner key did not resolve")
	}
	st.accounts[0].Enabled = false
	if _, ok := srv.resolveCaller(request()); ok {
		t.Fatal("owner with no active credentials kept authenticated access")
	}
	st.accounts = append(st.accounts, types.Account{ID: 3, OwnerID: owner.ID, Enabled: true,
		Creds: types.Creds{AccessToken: "replacement", AccountID: "replacement-upstream"}})
	next, ok := srv.resolveCaller(request())
	if !ok || next.acct.ID != 3 || next.reporterID() != first.reporterID() || srv.OwnerKeyFor(owner) != key {
		t.Fatal("credential replacement changed account key or reporter")
	}
	st.accounts[2].Creds.AccessToken = fakeToken("replacement-upstream", "pro", time.Now().Add(-time.Hour))
	if _, ok := srv.resolveCaller(request()); ok {
		t.Fatal("known expired credential activated owner key")
	}
	st.accounts[2].Creds.AccessToken = fakeToken("replacement-upstream", "pro", time.Now().Add(time.Hour))
	if w := do(handler, request()); w.Code != http.StatusOK {
		t.Fatal("owner key could not use gateway API", w.Code)
	}
	if w := do(handler, authed(httptest.NewRequest("GET", "/admin/", nil), key)); w.Code != http.StatusUnauthorized {
		t.Fatal("owner key obtained administrator access")
	}
}

func TestOwnerKeyHiddenLegacyKeysStillWorkAndNewCredentialKeysDoNotExist(t *testing.T) {
	srv, handler, _, st, _ := rig(t, types.ModePool)
	legacy := srv.ConsumerKeyFor(st.accounts[0])
	if legacy == "" {
		t.Fatal("fixture must have a grandfathered key")
	}
	if _, ok := srv.resolveCaller(authed(httptest.NewRequest("GET", "/cookie", nil), legacy)); !ok {
		t.Fatal("existing credential key revoked")
	}
	for _, path := range []string{"/a/vt-1", "/u/" + ownerTok1, "/admin/"} {
		body := do(handler, authed(httptest.NewRequest("GET", path, nil), "akey")).Body.String()
		if strings.Contains(body, legacy) {
			t.Fatal("legacy key redistributed in page", path)
		}
	}
	fresh, err := st.AddAccount(types.Account{OwnerID: 1, Enabled: true,
		Creds: types.Creds{AccountID: "new-account", AccessToken: "new-token"}, LegacyConsumerKey: true})
	if err != nil || srv.ConsumerKeyFor(fresh) != "" {
		t.Fatal("new credential obtained a credential-level key")
	}
}

func TestOwnerKeyContactReporterAndTargetStayIndependent(t *testing.T) {
	srv, handler, _, st, _ := rig(t, types.ModePool)
	key := srv.OwnerKeyFor(st.owners[0])
	st.accounts = append(st.accounts, types.Account{ID: 3, OwnerID: 1, Enabled: true,
		Creds: types.Creds{AccessToken: "alternate", AccountID: "alternate"}})
	claim := func(ref string) caller {
		t.Helper()
		c, ok := srv.resolveCaller(authed(httptest.NewRequest("GET", "/gateways?account="+ref, nil), key))
		if !ok {
			t.Fatal("owner authentication failed")
		}
		return c
	}
	if claim("alternate").acct.ID != 3 || claim("acct-a").acct.ID != 1 ||
		claim("alternate").reporterID() != claim("acct-a").reporterID() {
		t.Fatal("requested consumer or reporter identity was conflated")
	}
	observation := types.ContactObservation{ID: strings.Repeat("1", 64), AccountTag: strings.Repeat("2", 64),
		Gateway: "unified-142", Model: "astra", Criterion: "state-echo-v1", Source: "foreground",
		First: "repeat", At: time.Now().UTC(), GapKnown: true, ElapsedSeconds: 3600, Outcome: "full"}
	body, _ := json.Marshal(observation)
	r := authed(httptest.NewRequest("POST", "/contact/report", strings.NewReader(string(body))), key)
	if w := do(handler, r); w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body.String())
	}
	st.accounts[0].Enabled = false
	r = authed(httptest.NewRequest("GET", "/gateways", nil), key)
	r.Header.Set("X-Gwpool-Account-Tag", observation.AccountTag)
	if len(srv.contactStats(r, time.Now())["unified-142"]) != 1 {
		t.Fatal("rotating owner backing credential lost its report")
	}
	r.Header.Set("Authorization", "Bearer "+srv.OwnerKeyFor(st.owners[1]))
	if len(srv.contactStats(r, time.Now())) != 0 {
		t.Fatal("another owner obtained personal contact stats")
	}
}

func TestOwnerKeyWorksAfterRealStoreRestart(t *testing.T) {
	dir := t.TempDir()
	state, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := state.AddOwner("offline owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.AddAccount(types.Account{OwnerID: owner.ID, Enabled: true,
		Creds: types.Creds{AccessToken: fakeToken("account", "pro", time.Now().Add(time.Hour)), AccountID: "account"}}); err != nil {
		t.Fatal(err)
	}
	options := Options{ConsumerKey: "offline-root", AdminKey: "offline-admin"}
	before, err := New(&fakePool{mode: types.ModePool}, state, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	key := before.OwnerKeyFor(owner)
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	after, err := New(&fakePool{mode: types.ModePool}, reopened, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	if key != after.OwnerKeyFor(reopened.Owners()[0]) {
		t.Fatal("owner key changed on restart")
	}
	response := do(after.Handler(), authed(httptest.NewRequest("GET", "/gateways", nil), key))
	if response.Code != http.StatusOK {
		t.Fatal("saved owner key failed HTTP authentication after restart", response.Code)
	}
}
