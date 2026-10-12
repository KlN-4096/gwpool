package httpapi

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"

	"gwpool/internal/store"
	"gwpool/internal/types"
)

const adminTestOrigin = "https://pool.test"

type adminTestBrowser struct {
	h       http.Handler
	cookies map[string]*http.Cookie
}

func (b *adminTestBrowser) request(method, path, contentType string, body []byte) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, adminTestOrigin+path, bytes.NewReader(body))
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	if method == http.MethodGet {
		r.Header.Set("Accept", "text/html")
		r.Header.Set("Sec-Fetch-Mode", "navigate")
	} else {
		r.Header.Set("Origin", adminTestOrigin)
		r.Header.Set("Content-Type", contentType)
	}
	for _, c := range b.cookies {
		r.AddCookie(c)
	}
	w := do(b.h, r)
	for _, c := range w.Result().Cookies() {
		if c.MaxAge < 0 {
			delete(b.cookies, c.Name)
		} else {
			b.cookies[c.Name] = c
		}
	}
	return w
}

func (b *adminTestBrowser) postForm(path string, values url.Values) *httptest.ResponseRecorder {
	return b.request(http.MethodPost, path, "application/x-www-form-urlencoded", []byte(values.Encode()))
}

func newAdminTestServer(t *testing.T, st Store) (*Server, *adminTestBrowser) {
	t.Helper()
	s, err := New(&fakePool{mode: types.ModePool}, st, &fakeScanner{}, Options{
		AdminKey: "akey", PublicBaseURL: adminTestOrigin,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, &adminTestBrowser{h: s.Handler(), cookies: map[string]*http.Cookie{}}
}

func adminRealRig(t *testing.T) (*Server, *store.Store, *adminTestBrowser) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s, b := newAdminTestServer(t, st)
	return s, st, b
}

func adminPasswordLogin(t *testing.T, b *adminTestBrowser) {
	t.Helper()
	w := b.postForm("/admin/login", url.Values{"password": {"akey"}, "next": {"/admin/security"}})
	if w.Code != http.StatusSeeOther || b.cookies[adminCookie] == nil {
		t.Fatalf("password login: %d %s", w.Code, w.Body.String())
	}
}

// This offline authenticator uses a real P-256 key and CBOR attestation/assertion
// data. It exercises the production WebAuthn verifier, not a mocked success hook.
type adminTestAuthenticator struct {
	key      *ecdsa.PrivateKey
	id, user []byte
	counter  uint32
}

func newAdminTestAuthenticator(t *testing.T) *adminTestAuthenticator {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id := make([]byte, 32)
	if _, err := rand.Read(id); err != nil {
		t.Fatal(err)
	}
	return &adminTestAuthenticator{key: key, id: id}
}

func adminJSONBytes(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func adminChallenge(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("begin ceremony: %d %s", w.Code, w.Body.String())
	}
	var response struct {
		PublicKey map[string]any `json:"publicKey"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.PublicKey == nil {
		t.Fatal("missing WebAuthn options")
	}
	return response.PublicKey
}

func (a *adminTestAuthenticator) registration(t *testing.T, opts map[string]any, origin, rp string, uv bool) []byte {
	t.Helper()
	encode := base64.RawURLEncoding.EncodeToString
	user, err := base64.RawURLEncoding.DecodeString(opts["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	a.user = user
	client := adminJSONBytes(t, map[string]any{
		"type": "webauthn.create", "challenge": opts["challenge"], "origin": origin, "crossOrigin": false,
	})
	key, err := cbor.Marshal(map[int]any{
		1: 2, 3: -7, -1: 1,
		-2: a.key.X.FillBytes(make([]byte, 32)), -3: a.key.Y.FillBytes(make([]byte, 32)),
	})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(rp))
	auth := append([]byte{}, hash[:]...)
	flags := byte(0x41) // UP + attested credential data.
	if uv {
		flags |= 0x04
	}
	auth = append(auth, flags, 0, 0, 0, 0)
	auth = append(auth, make([]byte, 16)...)
	auth = binary.BigEndian.AppendUint16(auth, uint16(len(a.id)))
	auth = append(auth, a.id...)
	auth = append(auth, key...)
	attestation, err := cbor.Marshal(map[string]any{"fmt": "none", "authData": auth, "attStmt": map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	return adminJSONBytes(t, map[string]any{
		"id": encode(a.id), "rawId": encode(a.id), "type": "public-key",
		"response":               map[string]any{"clientDataJSON": encode(client), "attestationObject": encode(attestation)},
		"clientExtensionResults": map[string]any{"credProps": map[string]any{"rk": true}},
	})
}

func (a *adminTestAuthenticator) assertion(t *testing.T, opts map[string]any, origin, rp string, uv, validSignature bool) []byte {
	t.Helper()
	encode := base64.RawURLEncoding.EncodeToString
	client := adminJSONBytes(t, map[string]any{
		"type": "webauthn.get", "challenge": opts["challenge"], "origin": origin, "crossOrigin": false,
	})
	hash := sha256.Sum256([]byte(rp))
	auth := append([]byte{}, hash[:]...)
	flags := byte(0x01)
	if uv {
		flags |= 0x04
	}
	auth = append(auth, flags)
	a.counter++
	auth = binary.BigEndian.AppendUint32(auth, a.counter)
	clientHash := sha256.Sum256(client)
	signed := append(append([]byte{}, auth...), clientHash[:]...)
	digest := sha256.Sum256(signed)
	signature, err := ecdsa.SignASN1(rand.Reader, a.key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	if !validSignature {
		signature[len(signature)-1] ^= 1
	}
	return adminJSONBytes(t, map[string]any{
		"id": encode(a.id), "rawId": encode(a.id), "type": "public-key",
		"response": map[string]any{
			"clientDataJSON": encode(client), "authenticatorData": encode(auth),
			"signature": encode(signature), "userHandle": encode(a.user),
		},
	})
}

func adminRegisterPasskey(t *testing.T, b *adminTestBrowser, a *adminTestAuthenticator) {
	t.Helper()
	begin := b.request(http.MethodPost, "/admin/passkeys/register/begin", "application/json", []byte(`{"name":"测试电脑"}`))
	opts := adminChallenge(t, begin)
	selection := opts["authenticatorSelection"].(map[string]any)
	if selection["userVerification"] != "required" || selection["residentKey"] != "required" {
		t.Fatal("registration weakened user verification or resident-key requirement")
	}
	body := a.registration(t, opts, adminTestOrigin, "pool.test", true)
	w := b.request(http.MethodPost, "/admin/passkeys/register/finish", "application/json", body)
	if w.Code != http.StatusOK {
		t.Fatalf("register: %d %s", w.Code, w.Body.String())
	}
}

func adminPasskeyLogin(t *testing.T, b *adminTestBrowser, a *adminTestAuthenticator) {
	t.Helper()
	opts := adminChallenge(t, b.request(http.MethodPost, "/admin/passkeys/login/begin", "application/json", []byte(`{}`)))
	if opts["userVerification"] != "required" {
		t.Fatal("login must require user verification")
	}
	body := a.assertion(t, opts, adminTestOrigin, "pool.test", true, true)
	if w := b.request(http.MethodPost, "/admin/passkeys/login/finish", "application/json", body); w.Code != http.StatusOK {
		t.Fatalf("passkey login: %d %s", w.Code, w.Body.String())
	}
}

func TestAdminPasskeyRegisterLoginRestartLogoutAndDelete(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s, b := newAdminTestServer(t, st)
	adminPasswordLogin(t, b)
	a := newAdminTestAuthenticator(t)
	adminRegisterPasskey(t, b, a)
	if w := b.postForm("/admin/logout", nil); w.Code != http.StatusSeeOther {
		t.Fatalf("logout: %d", w.Code)
	}
	adminPasskeyLogin(t, b, a)
	session := b.cookies[adminCookie]
	if session.MaxAge != int(types.AdminSessionLifetime/time.Second) || !session.HttpOnly || !session.Secure {
		t.Fatal("passkey did not establish the same persistent protected session")
	}
	state := st.AdminAuthState()
	if _, ok := state.Sessions[session.Value]; ok {
		t.Fatal("session stored by raw bearer token")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s, freshBrowser := newAdminTestServer(t, st)
	freshBrowser.cookies[adminCookie] = session
	b = freshBrowser
	if w := b.request(http.MethodGet, "/admin/security", "", nil); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), "测试电脑") {
		t.Fatalf("restart lost passkey/session: %d", w.Code)
	}
	id := base64.RawURLEncoding.EncodeToString(a.id)
	if w := b.postForm("/admin/passkeys/delete", url.Values{"id": {id}}); w.Code != http.StatusSeeOther {
		t.Fatalf("delete passkey: %d %s", w.Code, w.Body.String())
	}
	if len(st.AdminAuthState().Passkeys) != 0 || len(st.AdminAuthState().Sessions) != 0 {
		t.Fatal("deleted passkey left a valid derived session")
	}
	if w := b.request(http.MethodGet, "/admin/", "", nil); w.Code != http.StatusSeeOther {
		t.Fatal("deleted credential session still authorized")
	}
	adminPasswordLogin(t, b) // Removing the last passkey does not remove recovery login.
	old := b.cookies[adminCookie]
	if w := b.postForm("/admin/logout", nil); w.Code != http.StatusSeeOther {
		t.Fatal("logout failed")
	}
	b.cookies[adminCookie] = old
	if _, ok := s.adminSession(func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/admin/", nil)
		r.AddCookie(old)
		return r
	}()); ok {
		t.Fatal("a copied logged-out cookie was accepted")
	}
}

func TestAdminPasskeyVerificationRejectsInvalidAssertions(t *testing.T) {
	for _, tc := range []struct {
		name, origin, rp string
		uv, signature    bool
	}{
		{"wrong-origin", "https://evil.test", "pool.test", true, true},
		{"wrong-rp", adminTestOrigin, "evil.test", true, true},
		{"no-user-verification", adminTestOrigin, "pool.test", false, true},
		{"bad-signature", adminTestOrigin, "pool.test", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, st, b := adminRealRig(t)
			adminPasswordLogin(t, b)
			a := newAdminTestAuthenticator(t)
			adminRegisterPasskey(t, b, a)
			b.postForm("/admin/logout", nil)
			opts := adminChallenge(t, b.request(http.MethodPost, "/admin/passkeys/login/begin", "application/json", []byte(`{}`)))
			body := a.assertion(t, opts, tc.origin, tc.rp, tc.uv, tc.signature)
			w := b.request(http.MethodPost, "/admin/passkeys/login/finish", "application/json", body)
			if w.Code != http.StatusUnauthorized || b.cookies[adminCookie] != nil || len(st.AdminAuthState().Sessions) != 0 {
				t.Fatalf("invalid assertion accepted: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestAdminPasskeyCeremonyIsBoundedSingleUseAndBrowserBound(t *testing.T) {
	s, _, b := adminRealRig(t)
	adminPasswordLogin(t, b)
	a := newAdminTestAuthenticator(t)
	adminRegisterPasskey(t, b, a)
	b.postForm("/admin/logout", nil)
	opts := adminChallenge(t, b.request(http.MethodPost, "/admin/passkeys/login/begin", "application/json", []byte(`{}`)))
	cookie := b.cookies[adminCeremonyCookie]
	body := a.assertion(t, opts, adminTestOrigin, "pool.test", true, true)
	other := &adminTestBrowser{h: b.h, cookies: map[string]*http.Cookie{}}
	if w := other.request(http.MethodPost, "/admin/passkeys/login/finish", "application/json", body); w.Code == http.StatusOK {
		t.Fatal("another browser consumed the challenge")
	}
	if w := b.request(http.MethodPost, "/admin/passkeys/login/finish", "application/json", body); w.Code != http.StatusOK {
		t.Fatalf("original browser: %d %s", w.Code, w.Body.String())
	}
	b.cookies[adminCeremonyCookie] = cookie
	if w := b.request(http.MethodPost, "/admin/passkeys/login/finish", "application/json", body); w.Code == http.StatusOK {
		t.Fatal("challenge replay succeeded")
	}
	opts = adminChallenge(t, b.request(http.MethodPost, "/admin/passkeys/login/begin", "application/json", []byte(`{}`)))
	body = a.assertion(t, opts, adminTestOrigin, "pool.test", true, true)
	now := time.Now()
	s.admin.now = func() time.Time { return now.Add(adminCeremonyLifetime + time.Second) }
	if w := b.request(http.MethodPost, "/admin/passkeys/login/finish", "application/json", body); w.Code == http.StatusOK {
		t.Fatal("expired challenge succeeded")
	}
}

func TestAdminRegistrationRequiresFreshIdentityAndVerifiedUser(t *testing.T) {
	s, st, b := adminRealRig(t)
	if w := b.request(http.MethodPost, "/admin/passkeys/register/begin", "application/json", []byte(`{}`)); w.Code != http.StatusUnauthorized {
		t.Fatal("anonymous registration permitted")
	}
	adminPasswordLogin(t, b)
	opts := adminChallenge(t, b.request(http.MethodPost, "/admin/passkeys/register/begin", "application/json", []byte(`{}`)))
	a := newAdminTestAuthenticator(t)
	body := a.registration(t, opts, adminTestOrigin, "pool.test", false)
	if w := b.request(http.MethodPost, "/admin/passkeys/register/finish", "application/json", body); w.Code == http.StatusOK {
		t.Fatal("registration without user verification accepted")
	}
	if len(st.AdminAuthState().Passkeys) != 0 {
		t.Fatal("failed registration persisted")
	}
	now := time.Now()
	s.admin.now = func() time.Time { return now.Add(adminFreshLifetime + time.Second) }
	if w := b.request(http.MethodPost, "/admin/passkeys/register/begin", "application/json", []byte(`{}`)); w.Code != http.StatusUnauthorized {
		t.Fatal("old remembered session could add a passkey without reauthentication")
	}
}

type adminFailingStore struct {
	*store.Store
	fail bool
}

func (s *adminFailingStore) CommitAdminAuthState(expected uint64, state types.AdminAuthState) error {
	if s.fail {
		return errors.New("injected authentication storage failure")
	}
	return s.Store.CommitAdminAuthState(expected, state)
}

func TestAdminPersistenceFailureDoesNotIssueSession(t *testing.T) {
	_, st, _ := adminRealRig(t)
	failing := &adminFailingStore{Store: st, fail: true}
	_, b := newAdminTestServer(t, failing)
	w := b.postForm("/admin/login", url.Values{"password": {"akey"}})
	if w.Code != http.StatusServiceUnavailable || b.cookies[adminCookie] != nil {
		t.Fatal("password login issued a cookie before durable commit")
	}
	failing.fail = false
	adminPasswordLogin(t, b)
	a := newAdminTestAuthenticator(t)
	adminRegisterPasskey(t, b, a)
	b.postForm("/admin/logout", nil)
	opts := adminChallenge(t, b.request(http.MethodPost, "/admin/passkeys/login/begin", "application/json", []byte(`{}`)))
	body := a.assertion(t, opts, adminTestOrigin, "pool.test", true, true)
	failing.fail = true
	w = b.request(http.MethodPost, "/admin/passkeys/login/finish", "application/json", body)
	if w.Code != http.StatusServiceUnavailable || b.cookies[adminCookie] != nil || len(st.AdminAuthState().Sessions) != 0 {
		t.Fatal("passkey login issued a cookie before durable commit")
	}
}

type adminSlowBody struct {
	reader  io.Reader
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *adminSlowBody) Read(p []byte) (int, error) {
	b.once.Do(func() {
		close(b.started)
		<-b.release
	})
	return b.reader.Read(p)
}

func (*adminSlowBody) Close() error { return nil }

func adminRequestWithCookies(path string, cookies map[string]*http.Cookie, body io.Reader) *http.Request {
	r := httptest.NewRequest(http.MethodPost, adminTestOrigin+path, body)
	r.Header.Set("Origin", adminTestOrigin)
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	r.Header.Set("Content-Type", "application/json")
	for _, c := range cookies {
		r.AddCookie(c)
	}
	return r
}

func TestAdminSlowPasskeyBodyDoesNotBlockLogoutOrResurrectSession(t *testing.T) {
	_, st, b := adminRealRig(t)
	adminPasswordLogin(t, b)
	a := newAdminTestAuthenticator(t)
	adminRegisterPasskey(t, b, a)
	opts := adminChallenge(t, b.request(http.MethodPost, "/admin/passkeys/login/begin", "application/json", []byte(`{}`)))
	body := a.assertion(t, opts, adminTestOrigin, "pool.test", true, true)
	slow := &adminSlowBody{reader: bytes.NewReader(body), started: make(chan struct{}), release: make(chan struct{})}
	loginRequest := adminRequestWithCookies("/admin/passkeys/login/finish", b.cookies, slow)
	loginDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { loginDone <- do(b.h, loginRequest) }()
	<-slow.started
	logoutRequest := adminRequestWithCookies("/admin/logout", b.cookies, nil)
	logoutDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { logoutDone <- do(b.h, logoutRequest) }()
	var logout *httptest.ResponseRecorder
	select {
	case logout = <-logoutDone:
	case <-time.After(300 * time.Millisecond):
		t.Error("anonymous slow body blocks the authentication mutation lock")
	}
	close(slow.release)
	login := <-loginDone
	if logout == nil {
		logout = <-logoutDone
	}
	if logout.Code != http.StatusSeeOther {
		t.Fatalf("logout: %d", logout.Code)
	}
	if login.Code == http.StatusOK || len(st.AdminAuthState().Sessions) != 0 {
		t.Error("in-flight passkey login resurrected a session after logout")
	}
}

func TestAdminLogoutRevokesCompletedLoginWithLateCookie(t *testing.T) {
	_, st, b := adminRealRig(t)
	adminPasswordLogin(t, b)
	a := newAdminTestAuthenticator(t)
	adminRegisterPasskey(t, b, a)
	opts := adminChallenge(t, b.request(http.MethodPost, "/admin/passkeys/login/begin", "application/json", []byte(`{}`)))
	// Logout was sent before the parallel login's new Cookie reached the browser.
	logout := adminRequestWithCookies("/admin/logout", b.cookies, nil)
	body := a.assertion(t, opts, adminTestOrigin, "pool.test", true, true)
	if w := b.request(http.MethodPost, "/admin/passkeys/login/finish", "application/json", body); w.Code != http.StatusOK {
		t.Fatalf("login: %d", w.Code)
	}
	if w := do(b.h, logout); w.Code != http.StatusSeeOther {
		t.Fatal("logout failed")
	}
	if len(st.AdminAuthState().Sessions) != 0 {
		t.Fatal("late login Cookie remains valid after logout")
	}
}
