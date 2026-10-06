package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func (f *fakeStore) DeleteUnauthorizedAccounts(candidates []types.AccountCleanupCandidate) (int, error) {
	n := 0
	for _, c := range candidates {
		for _, a := range f.accounts {
			if a.ID == c.ID && types.UnauthorizedCredential(a) && types.CleanupCandidate(a) == c {
				if err := f.DeleteAccount(a.ID); err != nil {
					return 0, err
				}
				n++
				break
			}
		}
	}
	return n, nil
}

func TestCleanupPreviewAuthorizationAndRecheck(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModePool)
	st.accounts[0].Enabled = false
	st.accounts[0].DisabledReason = "铸票 HTTP 401：expired"
	get := func(auth bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/admin/accounts/cleanup-401", nil)
		if auth {
			r = authed(r, "akey")
		}
		return do(h, r)
	}
	if get(false).Code != 401 {
		t.Fatal("unauthenticated preview")
	}
	preview := get(true)
	if preview.Code != 200 {
		t.Fatal(preview.Body.String())
	}
	re := regexp.MustCompile(`name="preview" value="([^"]+)"`)
	m := re.FindStringSubmatch(preview.Body.String())
	if len(m) != 2 {
		t.Fatal("missing signed preview")
	}
	for _, secret := range []string{st.accounts[0].Creds.AccessToken, st.accounts[0].ViewToken} {
		if secret != "" && strings.Contains(preview.Body.String(), secret) {
			t.Fatal("preview leaks credentials")
		}
	}
	post := func(token, origin string, auth bool) *httptest.ResponseRecorder {
		form := url.Values{"preview": {token}, "confirm": {"yes"}}
		r := httptest.NewRequest(http.MethodPost, "/admin/accounts/cleanup-401", strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		if auth {
			r = authed(r, "akey")
		}
		return do(h, r)
	}
	if post(m[1], "", false).Code != 401 {
		t.Fatal("unauthenticated deletion")
	}
	if post(m[1], "https://other.invalid", true).Code != 403 {
		t.Fatal("cross origin deletion")
	}
	if post(m[1]+"bad", "", true).Code != 400 {
		t.Fatal("tampered preview accepted")
	}
	payload, _ := json.Marshal(cleanupManifest{Expires: time.Now().Add(-time.Minute).Unix(), Candidates: []types.AccountCleanupCandidate{types.CleanupCandidate(st.accounts[0])}})
	p := base64.RawURLEncoding.EncodeToString(payload)
	expired := p + "." + base64.RawURLEncoding.EncodeToString(srv.cleanupSignature(p))
	if post(expired, "", true).Code != 400 {
		t.Fatal("expired preview accepted")
	}
	st.accounts[0].Enabled = true // Recovery after preview must win.
	if r := post(m[1], "", true); r.Code != 303 || !strings.Contains(r.Header().Get("Location"), "deleted=0&skipped=1") {
		t.Fatal("recovered account not skipped")
	}
	st.accounts[0].Enabled = false
	r := post(m[1], "", true)
	if r.Code != 303 || !strings.Contains(r.Header().Get("Location"), "deleted=1&skipped=0") {
		t.Fatalf("cleanup failed %d %s", r.Code, r.Body.String())
	}
	if len(st.deleted) != 1 {
		t.Fatal("wrong deletion count")
	}
}

func TestAdminUIBoundaries(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)
	admin := do(h, authed(httptest.NewRequest("GET", "/admin/", nil), "akey")).Body.String()
	for _, expected := range []string{"admin-sidebar", "201 历史目录", "需要关注", "cleanup-401"} {
		if !strings.Contains(admin, expected) {
			t.Errorf("admin missing %s", expected)
		}
	}
	for _, path := range []string{"/", "/a/vt-1", "/u/" + ownerTok1} {
		response := do(h, httptest.NewRequest("GET", path, nil))
		if response.Code != 200 {
			t.Fatalf("%s: %d", path, response.Code)
		}
		body := response.Body.String()
		for _, unexpected := range []string{`<aside class="admin-sidebar"`, `class="card directory-progress"`, `href="/admin/accounts/cleanup-401"`} {
			if strings.Contains(body, unexpected) {
				t.Errorf("%s leaked %s", path, unexpected)
			}
		}
	}
}
