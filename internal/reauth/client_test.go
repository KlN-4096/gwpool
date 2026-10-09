package reauth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type fixture struct {
	t                *testing.T
	mode             string
	calls            []string
	state, challenge string
	mutateCallback   func(url.Values)
	tokenBody        string
}

func fakeToken(email, account string) string {
	b, _ := json.Marshal(map[string]any{"exp": time.Now().Add(time.Hour).Unix(), "email": email,
		"https://api.openai.com/auth": map[string]string{"chatgpt_account_id": account, "chatgpt_plan_type": "free"}})
	return "eyJhbGciOiJub25lIn0." + base64.RawURLEncoding.EncodeToString(b) + ".c2ln"
}

func (fx *fixture) response(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}
}

func (fx *fixture) callback() string {
	q := url.Values{"code": {"fixture-code"}, "state": {fx.state}}
	if fx.mutateCallback != nil {
		fx.mutateCallback(q)
	}
	b, _ := json.Marshal(map[string]string{"continue_url": callbackURL + "?" + q.Encode()})
	return string(b)
}

func (fx *fixture) transport(r *http.Request) (*http.Response, error) {
	fx.t.Helper()
	if r.URL.Scheme != "https" || r.URL.Host != "auth.openai.com" {
		fx.t.Fatalf("unexpected outbound destination: %s", r.URL.Host)
	}
	fx.calls = append(fx.calls, r.Method+" "+r.URL.Path)
	switch r.URL.Path {
	case "/oauth/authorize":
		q := r.URL.Query()
		fx.state, fx.challenge = q.Get("state"), q.Get("code_challenge")
		if q.Get("code_challenge_method") != "S256" || q.Get("scope") != "openid profile email offline_access" {
			fx.t.Fatal("missing PKCE/scopes")
		}
		resp := fx.response(r, 302, "")
		resp.Header.Set("Location", "/log-in")
		resp.Header.Set("Set-Cookie", "fixture_session=bound; Secure; Path=/")
		return resp, nil
	case "/log-in":
		return fx.response(r, 200, "<html>login</html>"), nil
	case "/api/accounts/authorize/continue":
		if c, err := r.Cookie("fixture_session"); err != nil || c.Value != "bound" {
			fx.t.Fatal("lost isolated cookie session")
		}
		if fx.mode == "external" {
			return fx.response(r, 200, `{"page":{"type":"password"},"continue_url":"https://evil.invalid/password"}`), nil
		}
		if fx.mode == "challenge" {
			resp := fx.response(r, 403, "untrusted secret response")
			resp.Header.Set("cf-mitigated", "challenge")
			return resp, nil
		}
		if fx.mode == "phone" {
			return fx.response(r, 200, `{"page":{"type":"add_phone"}}`), nil
		}
		return fx.response(r, 200, `{"page":{"type":"password"}}`), nil
	case "/api/accounts/password/verify":
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["password"] != " fake password " {
			fx.t.Fatal("password was trimmed or changed")
		}
		switch fx.mode {
		case "totp":
			return fx.response(r, 200, `{"page":{"type":"mfa_challenge","payload":{"mfa_factors":[{"id":"factor-a","type":"totp"}]}}}`), nil
		case "email":
			return fx.response(r, 200, `{"page":{"type":"email_otp_verification"}}`), nil
		case "workspace", "organization":
			return fx.response(r, 200, `{"page":{"type":"consent"}}`), nil
		}
		return fx.response(r, 200, fx.callback()), nil
	case "/api/accounts/mfa/issue_challenge":
		return fx.response(r, 204, ""), nil
	case "/api/accounts/mfa/verify":
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["id"] != "factor-a" || body["type"] != "totp" || !validCode(body["code"]) {
			fx.t.Fatal("invalid MFA request")
		}
		return fx.response(r, 200, fx.callback()), nil
	case "/api/accounts/email-otp/validate":
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["code"] != "123456" {
			fx.t.Fatal("did not submit the explicit email code")
		}
		return fx.response(r, 200, fx.callback()), nil
	case "/api/accounts/client_auth_session_dump":
		return fx.response(r, 200, `{"client_auth_session":{"workspaces":[{"id":"a","name":"A"},{"id":"b","name":"B"}]}}`), nil
	case "/api/accounts/workspace/select":
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["workspace_id"] != "b" {
			fx.t.Fatal("arbitrarily selected first workspace")
		}
		if fx.mode == "organization" {
			return fx.response(r, 200, `{"page":{"type":"organization_select"},"continue_url":"/organization","data":{"orgs":[{"id":"org-a","name":"A","projects":[{"id":"p-a","name":"A"},{"id":"p-b","name":"B"}]}]}}`), nil
		}
		return fx.response(r, 200, fx.callback()), nil
	case "/api/accounts/organization/select":
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["org_id"] != "org-a" || body["project_id"] != "p-b" {
			fx.t.Fatal("organization/project not selected explicitly")
		}
		return fx.response(r, 200, fx.callback()), nil
	case "/oauth/token":
		if err := r.ParseForm(); err != nil {
			fx.t.Fatal(err)
		}
		challenge := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		if base64.RawURLEncoding.EncodeToString(challenge[:]) != fx.challenge ||
			r.Form.Get("code") != "fixture-code" || r.Form.Get("redirect_uri") != callbackURL {
			fx.t.Fatal("PKCE token exchange mismatch")
		}
		body := fx.tokenBody
		if body == "" {
			b, _ := json.Marshal(map[string]string{"access_token": fakeToken("test@example.com", "account-a"),
				"refresh_token": "rt.fixture-never-valid"})
			body = string(b)
		}
		return fx.response(r, 200, body), nil
	}
	fx.t.Fatalf("unexpected request %s", r.URL.Path)
	return nil, nil
}

func TestLoginFlows(t *testing.T) {
	for _, mode := range []string{"password", "totp", "email", "workspace", "organization"} {
		t.Run(mode, func(t *testing.T) {
			fx := &fixture{t: t, mode: mode}
			client, err := New(roundTripFunc(fx.transport))
			if err != nil {
				t.Fatal(err)
			}
			input := Input{Email: "test@example.com", Password: " fake password "}
			if mode == "totp" {
				input.TOTP = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
			}
			var stages, prompts []string
			tokens, err := client.Login(context.Background(), input, Hooks{
				Progress: func(stage string) { stages = append(stages, stage) },
				Prompt: func(_ context.Context, challenge Challenge) (string, error) {
					prompts = append(prompts, challenge.Kind)
					if challenge.Kind == "workspace" {
						return "b", nil
					}
					if challenge.Kind == "project" {
						return "p-b", nil
					}
					return "123456", nil
				},
			})
			if err != nil || tokens.RefreshToken == "" {
				t.Fatalf("login failed: %v", err)
			}
			if len(stages) < 4 || stages[0] != "connect" || stages[len(stages)-1] != "exchange" {
				t.Fatalf("progress not grounded in real steps: %v", stages)
			}
			if mode == "email" && !reflect.DeepEqual(prompts, []string{"email_code"}) {
				t.Fatal(prompts)
			}
			origin, _ := url.Parse(authOrigin)
			if len(client.http.Jar.Cookies(origin)) != 0 {
				t.Fatal("session cookies retained after completion")
			}
		})
	}
}

func TestLoginFailsClosed(t *testing.T) {
	for _, tc := range []struct{ mode, code string }{
		{"external", "unsupported_redirect"}, {"challenge", "browser_challenge"}, {"phone", "unsupported_challenge"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			fx := &fixture{t: t, mode: tc.mode}
			client, _ := New(roundTripFunc(fx.transport))
			_, err := client.Login(context.Background(), Input{Email: "test@example.com", Password: " fake password "}, Hooks{})
			if err == nil || !strings.Contains(err.Error(), tc.code) || strings.Contains(err.Error(), "untrusted secret") {
				t.Fatalf("unsafe error: %v", err)
			}
			for _, call := range fx.calls {
				if strings.Contains(call, "password/verify") || strings.Contains(call, "oauth/token") {
					t.Fatalf("continued after unsupported challenge: %s", call)
				}
			}
		})
	}
}

func TestInvalidCallbackNeverExchanged(t *testing.T) {
	for _, change := range []func(url.Values){
		func(q url.Values) { q.Set("state", "wrong-state") },
		func(q url.Values) { q.Add("state", "duplicate") },
		func(q url.Values) { q.Del("code") },
		func(q url.Values) { q.Set("error", "denied") },
	} {
		fx := &fixture{t: t, mutateCallback: change}
		client, _ := New(roundTripFunc(fx.transport))
		_, err := client.Login(context.Background(), Input{Email: "test@example.com", Password: " fake password "}, Hooks{})
		if err == nil || err.Error() != "invalid_callback" {
			t.Fatal(err)
		}
		for _, call := range fx.calls {
			if call == "POST /oauth/token" {
				t.Fatal("exchanged invalid callback")
			}
		}
	}
}

func TestLoginValidatesTokens(t *testing.T) {
	for _, body := range []string{
		`{"access_token":"x"}`,
		fmt.Sprintf(`{"access_token":%q,"refresh_token":"rt.fixture"}`, fakeToken("someone@example.com", "other")),
		fmt.Sprintf(`{"access_token":%q,"refresh_token":"rt.fixture"}`, fakeToken("test@example.com", "")),
	} {
		fx := &fixture{t: t, tokenBody: body}
		client, _ := New(roundTripFunc(fx.transport))
		tokens, err := client.Login(context.Background(), Input{Email: "test@example.com", Password: " fake password "}, Hooks{})
		if err == nil || tokens.AccessToken != "" {
			t.Fatal("accepted incomplete/mismatched identity")
		}
	}
}

func TestTOTPAndInput(t *testing.T) {
	const seed = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ" // RFC 6238 public test vector.
	code, err := totp(seed, time.Unix(59, 0))
	if err != nil || code != "287082" {
		t.Fatalf("RFC vector failed: %s %v", code, err)
	}
	for _, secret := range []string{"", seed, "otpauth://totp/test?secret=" + seed} {
		input := Input{Email: "test@example.com", Password: " password ", TOTP: secret}
		if err := input.Validate(); err != nil || input.Password != " password " {
			t.Fatal(err)
		}
		if strings.Contains(fmt.Sprintf("%+v %#v", input, input), "password") {
			t.Fatal("diagnostic string leaked secret")
		}
	}
	for _, secret := range []string{"123456", "bad", "otpauth://hotp/test?secret=" + seed, "otpauth://totp/test?secret=" + seed + "&digits=8"} {
		input := Input{Email: "test@example.com", Password: "p", TOTP: secret}
		if input.Validate() == nil {
			t.Fatal("accepted invalid or unsupported TOTP profile")
		}
	}
}

func TestPromptCancellation(t *testing.T) {
	fx := &fixture{t: t, mode: "email"}
	client, _ := New(roundTripFunc(fx.transport))
	ctx, cancel := context.WithCancel(context.Background())
	_, err := client.Login(ctx, Input{Email: "test@example.com", Password: " fake password "}, Hooks{
		Prompt: func(ctx context.Context, _ Challenge) (string, error) {
			cancel()
			<-ctx.Done()
			return "", ctx.Err()
		},
	})
	if err != context.Canceled {
		t.Fatal(err)
	}
}
