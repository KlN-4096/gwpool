package reauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"

	"gwpool/internal/oauth"
)

const (
	authOrigin       = "https://auth.openai.com"
	clientID         = "app_EMoamEEZ73f0CkXaXp7hrann"
	callbackURL      = "http://localhost:1455/auth/callback"
	maxResponseBytes = 512 << 10
	maxSteps         = 24
	requestTimeout   = 30 * time.Second
)

type Choice struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type Challenge struct {
	Kind    string   `json:"kind"` // email_code, totp_code, workspace, organization
	Choices []Choice `json:"choices,omitempty"`
}

type Hooks struct {
	Progress func(stage string)
	Prompt   func(context.Context, Challenge) (string, error)
}

// Error deliberately contains no upstream body, URL, identity, or credentials.
type Error struct {
	Code   string
	Status int
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("%s (HTTP %d)", e.Code, e.Status)
	}
	return e.Code
}

type Client struct {
	http *http.Client
}

// New creates a private cookie jar and disables automatic redirects. Redirects
// are checked before every request; neither cookies nor credentials can leave
// the authentication origin. The OAuth callback is parsed, never fetched.
func New(transport http.RoundTripper) (*Client, error) {
	if transport == nil {
		return nil, errors.New("登录必须显式配置动态出口")
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	return &Client{http: &http.Client{
		Transport: transport, Jar: jar, Timeout: requestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

type flow struct {
	c                *Client
	hooks            Hooks
	state, verifier  string
	device, referer  string
	email, password  string
	secret           string
	didPassword      bool
	didEmail, didMFA bool
}

type page struct {
	kind, next string
	data       map[string]any
}

func (c *Client) Login(ctx context.Context, in Input, hooks Hooks) (oauth.Tokens, error) {
	if err := in.Validate(); err != nil {
		return oauth.Tokens{}, err
	}
	state, err := randomToken()
	if err != nil {
		return oauth.Tokens{}, err
	}
	verifier, err := randomToken()
	if err != nil {
		return oauth.Tokens{}, err
	}
	device, err := randomToken()
	if err != nil {
		return oauth.Tokens{}, err
	}
	f := &flow{c: c, hooks: hooks, state: state, verifier: verifier, device: device,
		referer: authOrigin + "/log-in", email: in.Email, password: in.Password, secret: in.TOTP}
	defer func() {
		f.email, f.password, f.secret, f.verifier = "", "", "", ""
		c.http.CloseIdleConnections()
		// Do not keep session cookies after completion or a failed attempt.
		c.http.Jar, _ = cookiejar.New(nil)
	}()
	origin, _ := url.Parse(authOrigin)
	c.http.Jar.SetCookies(origin, []*http.Cookie{{Name: "oai-did", Value: device, Path: "/", Secure: true}})
	challenge := sha256.Sum256([]byte(verifier))
	q := url.Values{
		"client_id": {clientID}, "response_type": {"code"}, "redirect_uri": {callbackURL},
		"scope": {"openid profile email offline_access"}, "state": {state},
		"code_challenge": {base64.RawURLEncoding.EncodeToString(challenge[:])}, "code_challenge_method": {"S256"},
	}
	f.progress("connect")
	p, err := f.request(ctx, "GET", authOrigin+"/oauth/authorize?"+q.Encode(), nil)
	for step := 0; err == nil && p.next != "" && step < maxSteps; step++ {
		// The initial session has no user identity yet. Never exchange an
		// unsolicited code, or accept a session belonging to a previous login.
		if _, ok := f.callback(p.next); ok {
			return oauth.Tokens{}, &Error{Code: "unexpected_callback"}
		}
		p, err = f.request(ctx, "GET", p.next, nil)
	}
	if err != nil {
		return oauth.Tokens{}, err
	}
	if p.next != "" {
		return oauth.Tokens{}, &Error{Code: "too_many_steps"}
	}
	f.progress("identify")
	p, err = f.request(ctx, "POST", authOrigin+"/api/accounts/authorize/continue",
		map[string]any{"username": map[string]string{"kind": "email", "value": f.email}})
	for step := 0; err == nil && step < maxSteps; step++ {
		if p.next != "" {
			if code, ok := f.callback(p.next); ok {
				if code == "" {
					return oauth.Tokens{}, &Error{Code: "invalid_callback"}
				}
				return f.exchange(ctx, code)
			}
		}
		switch {
		case p.is("password", "login_password"):
			if f.didPassword {
				return oauth.Tokens{}, &Error{Code: "password_rejected"}
			}
			f.progress("password")
			f.didPassword = true
			p, err = f.request(ctx, "POST", authOrigin+"/api/accounts/password/verify",
				map[string]string{"password": f.password})
			f.password = ""
		case p.is("email_otp_verification", "email_otp", "email-verification"):
			if f.didEmail {
				return oauth.Tokens{}, &Error{Code: "email_code_rejected"}
			}
			f.didEmail = true
			code, promptErr := f.prompt(ctx, Challenge{Kind: "email_code"})
			if promptErr != nil {
				return oauth.Tokens{}, promptErr
			}
			if !validCode(code) {
				return oauth.Tokens{}, &Error{Code: "invalid_code"}
			}
			f.progress("verify_email")
			p, err = f.request(ctx, "POST", authOrigin+"/api/accounts/email-otp/validate",
				map[string]string{"code": code})
		case p.is("mfa_challenge", "mfa-challenge"):
			p, err = f.mfa(ctx, p)
		case p.is("consent", "workspace", "workspace_select"):
			p, err = f.selectWorkspace(ctx)
		case p.is("organization_select"):
			p, err = f.selectOrganization(ctx, p)
		case p.is("add_phone", "phone", "phone_verification", "captcha", "sso", "signup"):
			return oauth.Tokens{}, &Error{Code: "unsupported_challenge"}
		default:
			if p.next == "" {
				return oauth.Tokens{}, &Error{Code: "unsupported_login_step"}
			}
			p, err = f.request(ctx, "GET", p.next, nil)
		}
	}
	if err == nil {
		err = &Error{Code: "too_many_steps"}
	}
	return oauth.Tokens{}, err
}

func randomToken() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", errors.New("无法生成登录会话")
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func (f *flow) progress(stage string) {
	if f.hooks.Progress != nil {
		f.hooks.Progress(stage)
	}
}

func (f *flow) prompt(ctx context.Context, c Challenge) (string, error) {
	if f.hooks.Prompt == nil {
		return "", &Error{Code: "input_required"}
	}
	return f.hooks.Prompt(ctx, c)
}

func validCode(s string) bool {
	if len(s) != 6 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func authURL(raw string) (*url.URL, error) {
	base, _ := url.Parse(authOrigin)
	u, err := base.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host != "auth.openai.com" ||
		u.User != nil || u.Fragment != "" {
		return nil, &Error{Code: "unsupported_redirect"}
	}
	return u, nil
}

func (f *flow) callback(raw string) (code string, isCallback bool) {
	u, err := url.Parse(raw)
	if err != nil || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1") {
		return "", false
	}
	// Recognized callback hosts must fail closed, never become a GET request.
	q, qerr := url.ParseQuery(u.RawQuery)
	if qerr != nil || u.Scheme != "http" || u.Port() != "1455" || u.Path != "/auth/callback" ||
		u.User != nil || u.Fragment != "" || len(q["state"]) != 1 || q.Get("state") != f.state ||
		len(q["code"]) != 1 || q.Get("code") == "" || q.Get("error") != "" {
		return "", true
	}
	return q.Get("code"), true
}

func (f *flow) request(ctx context.Context, method, rawURL string, data any) (page, error) {
	u, err := authURL(rawURL)
	if err != nil {
		return page{}, err
	}
	var body io.Reader
	if data != nil {
		b, marshalErr := json.Marshal(data)
		if marshalErr != nil {
			return page{}, &Error{Code: "request_encoding"}
		}
		body = strings.NewReader(string(b))
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return page{}, &Error{Code: "invalid_request"}
	}
	req.Header.Set("Accept", "application/json, text/html;q=0.8")
	req.Header.Set("User-Agent", "gwpool/reauth")
	req.Header.Set("Referer", f.referer)
	req.Header.Set("oai-device-id", f.device)
	if data != nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", authOrigin)
	}
	resp, err := f.c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return page{}, ctx.Err()
		}
		return page{}, networkError(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(b) > maxResponseBytes {
		return page{}, &Error{Code: "invalid_response", Status: resp.StatusCode}
	}
	if resp.Header.Get("cf-mitigated") == "challenge" ||
		strings.Contains(string(b), "cf-chl-") || strings.Contains(string(b), "challenges.cloudflare.com") {
		return page{}, &Error{Code: "browser_challenge", Status: resp.StatusCode}
	}
	if resp.StatusCode >= 400 {
		code := "upstream_rejected"
		switch resp.StatusCode {
		case http.StatusTooManyRequests:
			code = "rate_limited"
		case http.StatusUnauthorized:
			code = "login_rejected"
		case http.StatusForbidden:
			code = "login_forbidden"
		}
		return page{}, &Error{Code: code, Status: resp.StatusCode}
	}
	p := page{}
	_ = json.Unmarshal(b, &p.data)
	if p.data != nil {
		p.kind = field(p.data, "page", "type")
		if p.kind == "" {
			p.kind = field(p.data, "page_type")
		}
		for _, path := range [][]string{{"continue_url"}, {"url"}, {"page", "payload", "url"}} {
			if p.next = field(p.data, path...); p.next != "" {
				break
			}
		}
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		p.next = resp.Header.Get("Location")
		if p.next == "" {
			return page{}, &Error{Code: "invalid_redirect"}
		}
	}
	if p.next != "" {
		next, parseErr := u.Parse(p.next)
		if parseErr != nil {
			return page{}, &Error{Code: "invalid_redirect"}
		}
		p.next = next.String()
		// Validate before making *any* subsequent action, including a password
		// submission suggested by the same untrusted response.
		if _, callback := f.callback(p.next); !callback {
			if _, err := authURL(p.next); err != nil {
				return page{}, err
			}
		}
	}
	f.referer = u.String()
	if p.kind == "" {
		// GET pages may be HTML; only recognize exact known paths. Never scrape
		// scripts or infer a factor ID from a URL.
		switch strings.TrimRight(u.Path, "/") {
		case "/log-in/password":
			p.kind = "password"
		case "/email-verification":
			p.kind = "email_otp"
		case "/mfa-challenge":
			p.kind = "mfa_challenge"
		case "/sign-in-with-chatgpt/codex/consent":
			p.kind = "consent"
		}
	}
	return p, nil
}

func field(m map[string]any, path ...string) string {
	var v any = m
	for _, key := range path {
		child, ok := v.(map[string]any)
		if !ok {
			return ""
		}
		v = child[key]
	}
	s, _ := v.(string)
	return s
}

func (p page) is(kinds ...string) bool {
	for _, kind := range kinds {
		if p.kind == kind {
			return true
		}
	}
	return false
}

func (f *flow) exchange(ctx context.Context, code string) (oauth.Tokens, error) {
	f.progress("exchange")
	form := url.Values{
		"grant_type": {"authorization_code"}, "client_id": {clientID}, "redirect_uri": {callbackURL},
		"code": {code}, "code_verifier": {f.verifier},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, authOrigin+"/oauth/token", strings.NewReader(form.Encode()))
	if err != nil {
		return oauth.Tokens{}, &Error{Code: "invalid_request"}
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := f.c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return oauth.Tokens{}, ctx.Err()
		}
		return oauth.Tokens{}, networkError(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return oauth.Tokens{}, &Error{Code: "token_exchange_failed", Status: resp.StatusCode}
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	var tokens oauth.Tokens
	if err != nil || len(b) > maxResponseBytes || json.Unmarshal(b, &tokens) != nil ||
		tokens.AccessToken == "" || tokens.RefreshToken == "" {
		return oauth.Tokens{}, &Error{Code: "incomplete_tokens"}
	}
	claims, err := oauth.ClaimsFor(tokens, "")
	if err != nil || claims.AccountID == "" || claims.Expires.IsZero() || !claims.Expires.After(time.Now()) {
		return oauth.Tokens{}, &Error{Code: "invalid_token_identity"}
	}
	if claims.Email != "" && !strings.EqualFold(strings.TrimSpace(claims.Email), f.email) {
		return oauth.Tokens{}, &Error{Code: "identity_mismatch"}
	}
	return tokens, nil
}

func networkError(err error) error {
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return &Error{Code: "network_timeout"}
	}
	return &Error{Code: "network"}
}
