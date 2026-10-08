// Package oauth 管 Codex 凭据本身：读它说了什么，以及用 refresh_token 换新的。
//
// 为什么不在 mint 包里：铸票要经住宅出口（换 IP 是它存在的理由），而换票是
// 纯凭据管理，直连就行——走住宅出口只会给它加一成的失败率。
package oauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// clientID 是 Codex CLI 的官方 OAuth 客户端。换票必须用它：
	// refresh_token 是按 client 签发的，换个 client_id 直接 400。
	clientID = "app_EMoamEEZ73f0CkXaXp7hrann"
	tokenURL = "https://auth.openai.com/oauth/token"
	// refreshScopes 刻意**不含** offline_access：带上它上游会轮换 refresh_token，
	// 而我们存的那张就作废了。照 Codex 客户端自己换票时的 scope 来。
	refreshScopes = "openid profile email"

	refreshTimeout = 30 * time.Second
)

// Claims 是 access_token 里我们关心的那几项。
type Claims struct {
	AccountID string
	Plan      string // free / plus / pro / ...
	Email     string // 上传时没填备注就拿它当名字
	Expires   time.Time
}

// Tokens is the token endpoint response. IDToken supplies identity claims when
// the access token omits them; it is never used as the upstream bearer token.
type Tokens struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token"`
}

func (Tokens) String() string   { return "Tokens{***}" }
func (Tokens) GoString() string { return "Tokens{***}" }

// Parse 读 access_token 的 payload。
//
// **不验签**：签名只有上游能验，这里要的不是「这票是真的」而是「这票说它是谁」。
// 贴错凭据是人为失误，不是攻击；真伪由上游第一次请求裁决。
func Parse(accessToken string) (Claims, error) {
	parts := strings.Split(accessToken, ".")
	if len(parts) != 3 {
		return Claims{}, errors.New("access_token 不是 JWT（应该是三段）")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Claims{}, errors.New("access_token 的 payload 解不开")
	}
	var c struct {
		Exp   int64  `json:"exp"`
		Email string `json:"email"`
		Auth  struct {
			AccountID string `json:"chatgpt_account_id"`
			Plan      string `json:"chatgpt_plan_type"`
		} `json:"https://api.openai.com/auth"`
		Profile json.RawMessage `json:"https://api.openai.com/profile"`
	}
	if err := json.Unmarshal(payload, &c); err != nil {
		return Claims{}, errors.New("access_token 的 payload 不是 JSON")
	}
	out := Claims{AccountID: c.Auth.AccountID, Plan: c.Auth.Plan}
	out.Email = c.Email
	// 邮箱只拿来起名字：profile 长得不对就当没有，别让整张票因此读不出来
	// （读不出来的票，后台续票会跳过它）。
	var prof struct {
		Email string `json:"email"`
	}
	if json.Unmarshal(c.Profile, &prof) == nil && prof.Email != "" {
		out.Email = prof.Email
	}
	if c.Exp > 0 {
		out.Expires = time.Unix(c.Exp, 0)
	}
	return out, nil
}

// ClaimsFor reconciles all available identities. The ID token can supply
// identity/profile fields, but only the access token supplies Expires.
// Empty access is supported while an RT-only upload is awaiting refresh.
func ClaimsFor(tokens Tokens, expectedAccount string) (Claims, error) {
	out := Claims{AccountID: expectedAccount}
	mergeID := func(id string) error {
		if out.AccountID != "" && id != "" && out.AccountID != id {
			return errors.New("凭据中的 account_id 不一致：请确认各项凭据属于同一账号")
		}
		if id != "" {
			out.AccountID = id
		}
		return nil
	}
	if tokens.AccessToken != "" {
		if len(tokens.AccessToken) < 20 || strings.ContainsAny(tokens.AccessToken, " \t\r\n") {
			return Claims{}, errors.New("access_token 形状不对")
		}
		access, err := Parse(tokens.AccessToken)
		if err != nil {
			return Claims{}, err
		}
		if err := mergeID(access.AccountID); err != nil {
			return Claims{}, err
		}
		out.Expires, out.Plan, out.Email = access.Expires, access.Plan, access.Email
	}
	if tokens.IDToken != "" {
		id, err := Parse(tokens.IDToken)
		if err != nil {
			return Claims{}, errors.New("id_token 格式不正确")
		}
		if err := mergeID(id.AccountID); err != nil {
			return Claims{}, err
		}
		if out.Plan == "" {
			out.Plan = id.Plan
		}
		if out.Email == "" {
			out.Email = id.Email
		}
	}
	return out, nil
}

// Refresh 用 refresh_token 换一张新的 access_token。
//
// 返回的 refresh 是上游**如果**轮换了就给新的，没轮换就是空串——调用方拿空串
// 当「继续用原来那张」，别把它写成空值覆盖掉手里那张（那就再也换不了票了）。
//
// hc 为 nil 时用默认客户端直连。
// RefreshError carries only the allowlisted OAuth category, never the upstream body.
type RefreshError struct {
	Status int
	Code   string
}

func (e *RefreshError) Error() string {
	return fmt.Sprintf("换票回 HTTP %d：%s", e.Status, e.Code)
}

func Refresh(ctx context.Context, hc *http.Client, refreshToken string) (Tokens, error) {
	refreshToken = strings.TrimSpace(refreshToken)
	if refreshToken == "" {
		return Tokens{}, errors.New("refresh_token 是空的")
	}
	if hc == nil {
		hc = &http.Client{Timeout: refreshTimeout}
	}
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
		"client_id":     {clientID},
		"scope":         {refreshScopes},
	}
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL,
		strings.NewReader(form.Encode()))
	if err != nil {
		return Tokens{}, fmt.Errorf("建换票请求: %w", err)
	}
	req.Header.Set("content-type", "application/x-www-form-urlencoded")
	req.Header.Set("accept", "application/json")

	resp, err := hc.Do(req)
	if err != nil {
		return Tokens{}, fmt.Errorf("换票请求失败: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode/100 != 2 {
		// Never echo the upstream body: error descriptions may contain
		// credentials or identifying fields, and callers also log this error.
		return Tokens{}, &RefreshError{Status: resp.StatusCode, Code: refreshErrorCode(body)}
	}
	var out Tokens
	if err := json.Unmarshal(body, &out); err != nil {
		return Tokens{}, errors.New("换票响应格式不正确")
	}
	if out.AccessToken == "" {
		return Tokens{}, errors.New("换票成功但没给 access_token")
	}
	return out, nil
}

func refreshErrorCode(body []byte) string {
	var response struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(body, &response) != nil {
		return "upstream_error"
	}
	var code string
	if json.Unmarshal(response.Error, &code) != nil {
		var detail struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(response.Error, &detail) == nil {
			code = detail.Code
		}
	}
	switch code {
	case "invalid_grant", "invalid_request", "invalid_client", "unauthorized_client",
		"access_denied", "unsupported_grant_type", "temporarily_unavailable",
		"server_error", "rate_limit_exceeded", "refresh_token_reused",
		"refresh_token_expired", "refresh_token_invalid":
		return code
	default:
		return "upstream_error"
	}
}
