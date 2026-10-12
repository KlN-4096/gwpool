package httpapi

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/webauthn"

	"gwpool/internal/types"
)

type adminWebAuthnUser struct {
	id          []byte
	credentials []webauthn.Credential
}

func (u adminWebAuthnUser) WebAuthnID() []byte                         { return u.id }
func (u adminWebAuthnUser) WebAuthnName() string                       { return "gwpool-admin" }
func (u adminWebAuthnUser) WebAuthnDisplayName() string                { return "gwpool 管理员" }
func (u adminWebAuthnUser) WebAuthnCredentials() []webauthn.Credential { return u.credentials }

type adminCeremony struct {
	kind        string
	name        string
	realm       string
	sessionHash string
	expires     time.Time
	session     webauthn.SessionData
	verifier    *webauthn.WebAuthn
}

func (s *Server) adminWebAuthn() (*webauthn.WebAuthn, error) {
	origin := s.adminOrigin()
	if origin == "" || s.opt.AdminKey == "" {
		return nil, errors.New("Passkey requires a configured HTTPS public URL and administrator key")
	}
	u, err := url.Parse(origin)
	if err != nil {
		return nil, err
	}
	timeout := webauthn.TimeoutConfig{Enforce: true, Timeout: adminCeremonyLifetime, TimeoutUVD: adminCeremonyLifetime}
	return webauthn.New(&webauthn.Config{
		RPID: u.Hostname(), RPDisplayName: "gwpool 管理员", RPOrigins: []string{origin},
		AttestationPreference: protocol.PreferNoAttestation,
		AuthenticatorSelection: protocol.AuthenticatorSelection{
			ResidentKey:      protocol.ResidentKeyRequirementRequired,
			UserVerification: protocol.VerificationRequired,
		},
		Timeouts: webauthn.TimeoutsConfig{Login: timeout, Registration: timeout},
	})
}

func (s *Server) adminUser(state types.AdminAuthState) (adminWebAuthnUser, error) {
	realm := s.adminRealm()
	id := sha256.Sum256([]byte("gwpool-admin-webauthn-user\x00" + realm))
	user := adminWebAuthnUser{id: id[:]}
	for id, key := range state.Passkeys {
		if key.Realm != realm {
			continue
		}
		var credential webauthn.Credential
		if err := json.Unmarshal(key.Credential, &credential); err != nil ||
			base64.RawURLEncoding.EncodeToString(credential.ID) != id || len(credential.PublicKey) == 0 {
			return adminWebAuthnUser{}, errors.New("stored passkey is invalid")
		}
		user.credentials = append(user.credentials, credential)
	}
	return user, nil
}

func adminJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func adminFailure(w http.ResponseWriter, status int, message string) {
	adminJSON(w, status, map[string]string{"error": message})
}

func (s *Server) adminRequireFresh(w http.ResponseWriter, r *http.Request) bool {
	if s.adminFresh(r) {
		return true
	}
	adminFailure(w, http.StatusUnauthorized, "请先重新验证管理员身份，再管理 Passkey（验证后5分钟内有效）。")
	return false
}

func (s *Server) adminStartCeremony(w http.ResponseWriter, r *http.Request, ceremony adminCeremony, options any) {
	token, err := newAdminToken()
	if err != nil {
		adminFailure(w, http.StatusInternalServerError, "无法创建验证请求")
		return
	}
	s.admin.mu.Lock()
	now := s.admin.now()
	if s.admin.pending == nil {
		s.admin.pending = map[string]adminCeremony{}
	}
	for id, pending := range s.admin.pending {
		if !pending.expires.After(now) {
			delete(s.admin.pending, id)
		}
	}
	delete(s.admin.pending, adminCookieHash(r, adminCeremonyCookie))
	if len(s.admin.pending) >= adminMaxCeremonies {
		s.admin.mu.Unlock()
		adminFailure(w, http.StatusTooManyRequests, "验证请求繁忙，请稍后重试")
		return
	}
	ceremony.realm, ceremony.expires = s.adminRealm(), now.Add(adminCeremonyLifetime)
	s.admin.pending[adminTokenHash(token)] = ceremony
	s.admin.mu.Unlock()
	s.adminSetCookie(w, r, adminCeremonyCookie, token, adminCeremonyLifetime)
	adminJSON(w, http.StatusOK, options)
}

// Called with admin.mu held through verification and commit. A ceremony belongs
// to one browser, is single-use even on failure, and cannot race past logout.
func (s *Server) adminConsumeCeremony(w http.ResponseWriter, r *http.Request, kind string) (adminCeremony, bool) {
	id := adminCookieHash(r, adminCeremonyCookie)
	ceremony, ok := s.admin.pending[id]
	delete(s.admin.pending, id)
	s.adminSetCookie(w, r, adminCeremonyCookie, "", -time.Second)
	if !ok || ceremony.kind != kind || ceremony.realm != s.adminRealm() || !ceremony.expires.After(s.admin.now()) {
		adminFailure(w, http.StatusBadRequest, "验证请求已过期或已使用，请重新开始")
		return adminCeremony{}, false
	}
	return ceremony, true
}

func (s *Server) handleAdminRegisterBegin(w http.ResponseWriter, r *http.Request) {
	if !s.adminRequireFresh(w, r) || !s.adminAllowAttempt(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, adminMaxRequestBytes)
	var input struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		adminFailure(w, http.StatusBadRequest, "Passkey 名称无效")
		return
	}
	input.Name = strings.TrimSpace(input.Name)
	if input.Name == "" {
		input.Name = "我的通行密钥"
	}
	if len(input.Name) > 128 || !utf8.ValidString(input.Name) || strings.IndexFunc(input.Name, unicode.IsControl) >= 0 {
		adminFailure(w, http.StatusBadRequest, "Passkey 名称过长或含控制字符")
		return
	}
	verifier, err := s.adminWebAuthn()
	if err != nil {
		adminFailure(w, http.StatusServiceUnavailable, "请先配置固定 HTTPS 公共地址")
		return
	}
	user, err := s.adminUser(s.st.AdminAuthState())
	if err != nil {
		adminFailure(w, http.StatusServiceUnavailable, "无法读取 Passkey")
		return
	}
	if len(user.credentials) >= types.MaxAdminPasskeys {
		adminFailure(w, http.StatusConflict, "Passkey 已达上限，请先移除不再使用的密钥")
		return
	}
	options, session, err := verifier.BeginRegistration(user,
		webauthn.WithExclusions(webauthn.Credentials(user.credentials).CredentialDescriptors()))
	if err != nil {
		adminFailure(w, http.StatusInternalServerError, "无法创建 Passkey 注册请求")
		return
	}
	s.adminStartCeremony(w, r, adminCeremony{
		kind: "register", name: input.Name, sessionHash: adminCookieHash(r, adminCookie),
		session: *session, verifier: verifier,
	}, options)
}

func (s *Server) handleAdminRegisterFinish(w http.ResponseWriter, r *http.Request) {
	parsed, parseErr := protocol.ParseCredentialCreationResponseBody(r.Body)
	s.admin.mu.Lock()
	defer s.admin.mu.Unlock()
	ceremony, ok := s.adminConsumeCeremony(w, r, "register")
	if !ok {
		return
	}
	if parseErr != nil {
		adminFailure(w, http.StatusBadRequest, "Passkey 响应无效，请重新开始")
		return
	}
	if ceremony.sessionHash != adminCookieHash(r, adminCookie) {
		adminFailure(w, http.StatusUnauthorized, "管理员登录已变化，请重新绑定")
		return
	}
	// Recheck under the mutation lock so concurrent logout/deletion cannot race
	// authorization with the durable registration commit.
	if !s.adminFresh(r) {
		adminFailure(w, http.StatusUnauthorized, "管理员登录已失效")
		return
	}
	state := s.adminPrepareState()
	user, err := s.adminUser(state)
	if err != nil {
		adminFailure(w, http.StatusServiceUnavailable, "无法读取 Passkey")
		return
	}
	credential, err := ceremony.verifier.CreateCredential(user, ceremony.session, parsed)
	if err != nil {
		adminFailure(w, http.StatusBadRequest, "Passkey 验证失败，请重新开始")
		return
	}
	id := base64.RawURLEncoding.EncodeToString(credential.ID)
	if _, exists := state.Passkeys[id]; exists || len(state.Passkeys) >= types.MaxAdminPasskeys {
		adminFailure(w, http.StatusConflict, "Passkey 已存在或数量已达上限")
		return
	}
	data, err := json.Marshal(credential)
	if err != nil {
		adminFailure(w, http.StatusInternalServerError, "无法保存 Passkey")
		return
	}
	state.Passkeys[id] = types.AdminPasskey{
		Realm: ceremony.realm, Name: ceremony.name, Credential: data, CreatedAt: s.admin.now().UTC(),
	}
	if err := s.st.CommitAdminAuthState(state.Revision, state); err != nil {
		s.opt.Log.Error("admin passkey registration persistence failed")
		adminFailure(w, http.StatusServiceUnavailable, "Passkey 未保存，请重试")
		return
	}
	adminJSON(w, http.StatusOK, map[string]string{"redirect": "/admin/security"})
}

func (s *Server) handleAdminPasskeyLoginBegin(w http.ResponseWriter, r *http.Request) {
	if !s.adminAllowAttempt(w, r) {
		return
	}
	verifier, err := s.adminWebAuthn()
	if err != nil {
		adminFailure(w, http.StatusServiceUnavailable, "Passkey 暂不可用，请使用管理员密钥")
		return
	}
	user, err := s.adminUser(s.st.AdminAuthState())
	if err != nil {
		adminFailure(w, http.StatusServiceUnavailable, "无法读取 Passkey")
		return
	}
	if len(user.credentials) == 0 {
		adminFailure(w, http.StatusConflict, "尚未绑定 Passkey，请先用管理员密钥登录")
		return
	}
	options, session, err := verifier.BeginLogin(user)
	if err != nil {
		adminFailure(w, http.StatusInternalServerError, "无法创建 Passkey 登录请求")
		return
	}
	s.adminStartCeremony(w, r, adminCeremony{kind: "login", session: *session, verifier: verifier}, options)
}

func (s *Server) handleAdminPasskeyLoginFinish(w http.ResponseWriter, r *http.Request) {
	parsed, parseErr := protocol.ParseCredentialRequestResponseBody(r.Body)
	s.admin.mu.Lock()
	defer s.admin.mu.Unlock()
	ceremony, ok := s.adminConsumeCeremony(w, r, "login")
	if !ok {
		return
	}
	if parseErr != nil {
		adminFailure(w, http.StatusBadRequest, "Passkey 响应无效，请重新开始")
		return
	}
	state := s.adminPrepareState()
	user, err := s.adminUser(state)
	if err != nil {
		adminFailure(w, http.StatusServiceUnavailable, "无法读取 Passkey")
		return
	}
	credential, err := ceremony.verifier.ValidateLogin(user, ceremony.session, parsed)
	if err != nil || credential.Authenticator.CloneWarning {
		adminFailure(w, http.StatusUnauthorized, "Passkey 验证失败，请重试或使用管理员密钥")
		return
	}
	id := base64.RawURLEncoding.EncodeToString(credential.ID)
	key, exists := state.Passkeys[id]
	if !exists || key.Realm != ceremony.realm {
		adminFailure(w, http.StatusUnauthorized, "Passkey 已被移除")
		return
	}
	key.Credential, err = json.Marshal(credential)
	if err != nil {
		adminFailure(w, http.StatusInternalServerError, "无法更新 Passkey")
		return
	}
	key.LastUsedAt = s.admin.now().UTC()
	state.Passkeys[id] = key
	token, err := s.adminIssueSession(&state, r, id)
	if err == nil {
		err = s.st.CommitAdminAuthState(state.Revision, state)
	}
	if err != nil {
		s.opt.Log.Error("admin passkey login persistence failed")
		adminFailure(w, http.StatusServiceUnavailable, "无法保存登录状态，请重试")
		return
	}
	s.adminSetCookie(w, r, adminCookie, token, types.AdminSessionLifetime)
	adminJSON(w, http.StatusOK, map[string]string{"redirect": "/admin/"})
}

type adminPasskeyRow struct {
	ID, Name, CreatedAt, LastUsedAt string
}

type adminSecurityView struct {
	base
	Passkeys []adminPasskeyRow
	Origin   string
	Fresh    bool
}

func (s *Server) handleAdminSecurity(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	view := adminSecurityView{
		base:   base{Title: "登录与安全", Mode: s.pool.Mode(), Admin: true, AdminPage: "security"},
		Origin: s.adminOrigin(), Fresh: s.adminFresh(r),
	}
	for id, key := range s.st.AdminAuthState().Passkeys {
		if key.Realm != s.adminRealm() {
			continue
		}
		last := "尚未使用"
		if !key.LastUsedAt.IsZero() {
			last = key.LastUsedAt.UTC().Format(time.DateTime) + " UTC"
		}
		view.Passkeys = append(view.Passkeys, adminPasskeyRow{
			ID: id, Name: key.Name, CreatedAt: key.CreatedAt.UTC().Format(time.DateTime) + " UTC", LastUsedAt: last,
		})
	}
	sort.Slice(view.Passkeys, func(i, j int) bool { return view.Passkeys[i].CreatedAt < view.Passkeys[j].CreatedAt })
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tplAdminSecurity.ExecuteTemplate(w, "layout", view); err != nil {
		s.opt.Log.Error("render admin security failed")
	}
}

func (s *Server) handleAdminPasskeyDelete(w http.ResponseWriter, r *http.Request) {
	if !s.adminRequireFresh(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, adminMaxRequestBytes)
	if err := r.ParseForm(); err != nil {
		adminFailure(w, http.StatusBadRequest, "无效的 Passkey")
		return
	}
	id := r.PostForm.Get("id")
	s.admin.mu.Lock()
	defer s.admin.mu.Unlock()
	if !s.adminFresh(r) {
		adminFailure(w, http.StatusUnauthorized, "管理员登录已失效")
		return
	}
	state := s.adminPrepareState()
	key, exists := state.Passkeys[id]
	if !exists || key.Realm != s.adminRealm() {
		adminFailure(w, http.StatusNotFound, "Passkey 不存在")
		return
	}
	delete(state.Passkeys, id)
	for hash, session := range state.Sessions {
		if session.PasskeyID == id {
			delete(state.Sessions, hash)
		}
	}
	if err := s.st.CommitAdminAuthState(state.Revision, state); err != nil {
		adminFailure(w, http.StatusServiceUnavailable, "移除未保存，请重试")
		return
	}
	http.Redirect(w, r, "/admin/security", http.StatusSeeOther)
}
