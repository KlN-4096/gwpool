package httpapi

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"gwpool/internal/types"
)

const (
	adminCookie           = "gwp_admin"
	adminCeremonyCookie   = "gwp_admin_ceremony"
	adminFreshLifetime    = 5 * time.Minute
	adminCeremonyLifetime = 5 * time.Minute
	adminMaxCeremonies    = 64
	adminMaxRequestBytes  = 64 << 10
	adminLoginRateWindow  = time.Minute
	adminLoginRateLimit   = 12
	adminLoginRateClients = 256
	adminLoginGlobalLimit = 120
	adminBodyTimeout      = 10 * time.Second
)

type adminRate struct {
	start time.Time
	count int
}

type adminAuthentication struct {
	mu      sync.Mutex
	now     func() time.Time
	pending map[string]adminCeremony
	rates   map[string]adminRate
	global  adminRate
}

func (s *Server) adminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /admin/login", s.handleAdminLoginPage)
	mux.Handle("POST /admin/login", s.adminSameOrigin(http.HandlerFunc(s.handleAdminPasswordLogin)))
	mux.Handle("POST /admin/logout", s.adminSameOrigin(http.HandlerFunc(s.handleAdminLogout)))
	mux.HandleFunc("GET /admin/security", s.handleAdminSecurity)
	mux.Handle("POST /admin/passkeys/register/begin", s.adminSameOrigin(http.HandlerFunc(s.handleAdminRegisterBegin)))
	mux.Handle("POST /admin/passkeys/register/finish", s.adminSameOrigin(http.HandlerFunc(s.handleAdminRegisterFinish)))
	mux.Handle("POST /admin/passkeys/login/begin", s.adminSameOrigin(http.HandlerFunc(s.handleAdminPasskeyLoginBegin)))
	mux.Handle("POST /admin/passkeys/login/finish", s.adminSameOrigin(http.HandlerFunc(s.handleAdminPasskeyLoginFinish)))
	mux.Handle("POST /admin/passkeys/delete", s.adminSameOrigin(http.HandlerFunc(s.handleAdminPasskeyDelete)))
}

// An exact configured origin, never a value chosen by Host/Forwarded headers.
// Localhost HTTP is reserved for local development and offline UI tests.
func (s *Server) adminOrigin() string {
	u, err := url.Parse(s.publicBaseURL())
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		u.Hostname() == "" || (u.Path != "" && u.Path != "/") {
		return ""
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && u.Hostname() == "localhost") {
		return ""
	}
	return u.Scheme + "://" + strings.ToLower(u.Host)
}

func (s *Server) adminRealm() string {
	if s.opt.AdminKey == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(s.opt.AdminKey))
	mac.Write([]byte("gwpool-admin-auth-v1\x00" + s.adminOrigin()))
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *Server) adminSameOrigin(next http.Handler) http.Handler {
	return s.sameOrigin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			expected := s.adminOrigin()
			if expected == "" {
				scheme := "http"
				if forwardedHTTPS(r) {
					scheme = "https"
				}
				expected = scheme + "://" + strings.ToLower(r.Host)
			}
			if !strings.EqualFold(origin, expected) {
				http.Error(w, "请从本站管理员页面操作", http.StatusForbidden)
				return
			}
		}
		// Never read a network body while holding the authentication mutex.
		// Restrict this timeout to the small auth forms, not uploads/model traffic.
		if r.Body != nil {
			controller := http.NewResponseController(w)
			if err := controller.SetReadDeadline(time.Now().Add(adminBodyTimeout)); err != nil &&
				!errors.Is(err, http.ErrNotSupported) {
				http.Error(w, "无法读取验证请求", http.StatusServiceUnavailable)
				return
			}
			body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, adminMaxRequestBytes))
			_ = controller.SetReadDeadline(time.Time{})
			if err != nil {
				if strings.HasSuffix(r.URL.Path, "/finish") {
					s.admin.mu.Lock()
					delete(s.admin.pending, adminCookieHash(r, adminCeremonyCookie))
					s.admin.mu.Unlock()
					s.adminSetCookie(w, r, adminCeremonyCookie, "", -time.Second)
				}
				http.Error(w, "验证请求过大或读取超时，请重试", http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		next.ServeHTTP(w, r)
	}))
}

func adminBrowserRequest(r *http.Request) bool {
	return r.Header.Get("Sec-Fetch-Mode") != "" || r.Header.Get("Sec-Fetch-Site") != "" ||
		strings.Contains(r.Header.Get("Accept"), "text/html")
}

func (s *Server) adminHeaderAuthorized(r *http.Request) bool {
	if got := bearer(r); got != "" {
		return keyOK(s.opt.AdminKey, got)
	}
	// Browsers must use the revocable cookie, not a cached Basic credential that
	// would silently log them back in immediately after logout.
	if !adminBrowserRequest(r) {
		if _, pass, ok := r.BasicAuth(); ok {
			return keyOK(s.opt.AdminKey, pass)
		}
	}
	return false
}

func adminTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func adminCookieHash(r *http.Request, name string) string {
	c, err := r.Cookie(name)
	if err != nil {
		return ""
	}
	b, err := base64.RawURLEncoding.DecodeString(c.Value)
	if err != nil || len(b) != 32 {
		return ""
	}
	return adminTokenHash(c.Value)
}

func (s *Server) adminSession(r *http.Request) (types.AdminSession, bool) {
	realm := s.adminRealm()
	state := s.st.AdminAuthState()
	session, ok := state.Sessions[adminCookieHash(r, adminCookie)]
	now := s.admin.now()
	if !ok || realm == "" || session.Realm != realm || session.CreatedAt.After(now) ||
		!session.ExpiresAt.After(now) || session.ExpiresAt.Sub(session.CreatedAt) > types.AdminSessionLifetime {
		return types.AdminSession{}, false
	}
	if session.PasskeyID != "" {
		key, exists := state.Passkeys[session.PasskeyID]
		if !exists || key.Realm != realm {
			return types.AdminSession{}, false
		}
	}
	return session, true
}

func (s *Server) authAdmin(w http.ResponseWriter, r *http.Request) bool {
	if s.adminHeaderAuthorized(r) {
		return true
	}
	// An explicitly supplied wrong API key must not fall back to a browser identity.
	if bearer(r) == "" {
		if _, ok := s.adminSession(r); ok {
			return true
		}
	}
	if r.Method == http.MethodGet && adminBrowserRequest(r) {
		http.Redirect(w, r, "/admin/login?next="+url.QueryEscape(safeAdminNext(r.URL.Path)), http.StatusSeeOther)
		return false
	}
	if !adminBrowserRequest(r) {
		w.Header().Set("WWW-Authenticate", `Basic realm="gwpool admin", charset="UTF-8"`)
	}
	http.Error(w, "请先登录管理员", http.StatusUnauthorized)
	return false
}

func (s *Server) adminFresh(r *http.Request) bool {
	if s.adminHeaderAuthorized(r) {
		return true
	}
	if bearer(r) != "" {
		return false
	}
	session, ok := s.adminSession(r)
	return ok && s.admin.now().Sub(session.CreatedAt) <= adminFreshLifetime
}

func safeAdminNext(value string) string {
	u, err := url.Parse(value)
	if err != nil || u.IsAbs() || u.Host != "" || strings.ContainsAny(value, "\\\r\n") ||
		!strings.HasPrefix(u.Path, "/admin/") || u.Path == "/admin/login" || u.Path == "/admin/logout" {
		return "/admin/"
	}
	// Never copy arbitrary login query parameters into redirect targets.
	u.RawQuery, u.ForceQuery = "", false
	return u.String()
}

func (s *Server) adminSetCookie(w http.ResponseWriter, r *http.Request, name, token string, lifetime time.Duration) {
	age := int(lifetime / time.Second)
	expiry := s.admin.now().Add(lifetime)
	if lifetime < 0 {
		age, expiry = -1, time.Unix(1, 0)
	}
	http.SetCookie(w, &http.Cookie{
		Name: name, Value: token, Path: "/admin/", HttpOnly: true,
		Secure:   forwardedHTTPS(r) || strings.HasPrefix(s.adminOrigin(), "https://"),
		SameSite: http.SameSiteLaxMode, MaxAge: age, Expires: expiry,
	})
}

func newAdminToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// Called under admin.mu; all changes stay private until the one WAL commit.
func (s *Server) adminPrepareState() types.AdminAuthState {
	state := s.st.AdminAuthState()
	if state.Passkeys == nil {
		state.Passkeys = map[string]types.AdminPasskey{}
	}
	if state.Sessions == nil {
		state.Sessions = map[string]types.AdminSession{}
	}
	now, realm := s.admin.now(), s.adminRealm()
	for id, key := range state.Passkeys {
		if key.Realm != realm {
			delete(state.Passkeys, id)
		}
	}
	for id, session := range state.Sessions {
		if session.Realm != realm || !session.ExpiresAt.After(now) {
			delete(state.Sessions, id)
		}
	}
	return state
}

func (s *Server) adminIssueSession(state *types.AdminAuthState, r *http.Request, passkeyID string) (string, error) {
	realm := s.adminRealm()
	if realm == "" {
		return "", errors.New("administrator is not configured")
	}
	token, err := newAdminToken()
	if err != nil {
		return "", err
	}
	delete(state.Sessions, adminCookieHash(r, adminCookie))
	if len(state.Sessions) >= types.MaxAdminSessions {
		var oldest string
		for id, session := range state.Sessions {
			if oldest == "" || session.CreatedAt.Before(state.Sessions[oldest].CreatedAt) {
				oldest = id
			}
		}
		delete(state.Sessions, oldest)
	}
	now := s.admin.now().UTC()
	ceremonyHash := ""
	if passkeyID != "" {
		ceremonyHash = adminCookieHash(r, adminCeremonyCookie)
	}
	state.Sessions[adminTokenHash(token)] = types.AdminSession{
		Realm: realm, PasskeyID: passkeyID, CeremonyHash: ceremonyHash,
		CreatedAt: now, ExpiresAt: now.Add(types.AdminSessionLifetime),
	}
	return token, nil
}

func (s *Server) adminAllowAttempt(w http.ResponseWriter, r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if peer := net.ParseIP(host); s.opt.AdminTrustLoopbackProxy && peer != nil && peer.IsLoopback() {
		// The deployed Caddy explicitly overwrites X-Real-IP with client_ip.
		// Reject lists/malformed values; never trust this header from external peers.
		if client := net.ParseIP(strings.TrimSpace(r.Header.Get("X-Real-IP"))); client != nil {
			host = client.String()
		}
	}
	s.admin.mu.Lock()
	defer s.admin.mu.Unlock()
	now := s.admin.now()
	if now.Sub(s.admin.global.start) >= adminLoginRateWindow || s.admin.global.start.After(now) {
		s.admin.global = adminRate{start: now}
	}
	if s.admin.rates == nil {
		s.admin.rates = map[string]adminRate{}
	}
	for id, rate := range s.admin.rates {
		if now.Sub(rate.start) >= adminLoginRateWindow || rate.start.After(now) {
			delete(s.admin.rates, id)
		}
	}
	rate, exists := s.admin.rates[host]
	if rate.count >= adminLoginRateLimit || s.admin.global.count >= adminLoginGlobalLimit ||
		(!exists && len(s.admin.rates) >= adminLoginRateClients) {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "登录尝试过于频繁，请一分钟后重试", http.StatusTooManyRequests)
		return false
	}
	if !exists {
		rate.start = now
	}
	rate.count++
	s.admin.global.count++
	s.admin.rates[host] = rate
	return true
}

type adminLoginView struct {
	base
	Next          string
	Error         string
	PasskeyReady  bool
	PasskeyReason string
}

func (s *Server) renderAdminLogin(w http.ResponseWriter, r *http.Request, status int, message string) {
	realm := s.adminRealm()
	ready := false
	for _, key := range s.st.AdminAuthState().Passkeys {
		ready = ready || key.Realm == realm
	}
	reason := "首次使用请用管理员密钥登录，然后在「登录与安全」绑定 Passkey。"
	if s.adminOrigin() == "" {
		ready = false
		reason = "尚未配置有效的 HTTPS 公共地址，暂不可使用 Passkey；管理员密钥仍可登录。"
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := s.tplAdminLogin.ExecuteTemplate(w, "layout", adminLoginView{
		base: base{Title: "管理员登录", Restricted: true}, Next: safeAdminNext(r.FormValue("next")),
		Error: message, PasskeyReady: ready, PasskeyReason: reason,
	}); err != nil {
		s.opt.Log.Error("render admin login failed")
	}
}

func (s *Server) handleAdminLoginPage(w http.ResponseWriter, r *http.Request) {
	s.renderAdminLogin(w, r, http.StatusOK, "")
}

func (s *Server) handleAdminPasswordLogin(w http.ResponseWriter, r *http.Request) {
	if !s.adminAllowAttempt(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, adminMaxRequestBytes)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "登录表单无效", http.StatusBadRequest)
		return
	}
	if !keyOK(s.opt.AdminKey, r.PostForm.Get("password")) {
		s.renderAdminLogin(w, r, http.StatusUnauthorized, "管理员密钥不正确。")
		return
	}
	s.admin.mu.Lock()
	state := s.adminPrepareState()
	token, err := s.adminIssueSession(&state, r, "")
	if err == nil {
		err = s.st.CommitAdminAuthState(state.Revision, state)
	}
	s.admin.mu.Unlock()
	if err != nil {
		s.opt.Log.Error("admin login persistence failed")
		http.Error(w, "无法保存登录状态，请稍后重试", http.StatusServiceUnavailable)
		return
	}
	s.adminSetCookie(w, r, adminCookie, token, types.AdminSessionLifetime)
	http.Redirect(w, r, safeAdminNext(r.PostForm.Get("next")), http.StatusSeeOther)
}

func (s *Server) handleAdminLogout(w http.ResponseWriter, r *http.Request) {
	s.admin.mu.Lock()
	state := s.adminPrepareState()
	id := adminCookieHash(r, adminCookie)
	ceremony := adminCookieHash(r, adminCeremonyCookie)
	var err error
	changed := false
	for hash, session := range state.Sessions {
		if hash == id || (ceremony != "" && session.CeremonyHash == ceremony) {
			delete(state.Sessions, hash)
			changed = true
		}
	}
	if changed {
		err = s.st.CommitAdminAuthState(state.Revision, state)
	}
	delete(s.admin.pending, ceremony)
	s.admin.mu.Unlock()
	if err != nil {
		http.Error(w, "退出未保存，请重试", http.StatusServiceUnavailable)
		return
	}
	s.adminSetCookie(w, r, adminCookie, "", -time.Second)
	s.adminSetCookie(w, r, adminCeremonyCookie, "", -time.Second)
	http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
}
