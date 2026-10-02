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
		Exp  int64 `json:"exp"`
		Auth struct {
			AccountID string `json:"chatgpt_account_id"`
			Plan      string `json:"chatgpt_plan_type"`
		} `json:"https://api.openai.com/auth"`
		Profile json.RawMessage `json:"https://api.openai.com/profile"`
	}
	if err := json.Unmarshal(payload, &c); err != nil {
		return Claims{}, errors.New("access_token 的 payload 不是 JSON")
	}
	out := Claims{AccountID: c.Auth.AccountID, Plan: c.Auth.Plan}
	// 邮箱只拿来起名字：profile 长得不对就当没有，别让整张票因此读不出来
	// （读不出来的票，后台续票会跳过它）。
	var prof struct {
		Email string `json:"email"`
	}
	if json.Unmarshal(c.Profile, &prof) == nil {
		out.Email = prof.Email
	}
	if c.Exp > 0 {
		out.Expires = time.Unix(c.Exp, 0)
	}
	return out, nil
}

// Refresh 用 refresh_token 换一张新的 access_token。
//
// 返回的 refresh 是上游**如果**轮换了就给新的，没轮换就是空串——调用方拿空串
// 当「继续用原来那张」，别把它写成空值覆盖掉手里那张（那就再也换不了票了）。
//
// hc 为 nil 时用默认客户端直连。
func Refresh(ctx context.Context, hc *http.Client, refreshToken string) (access, refresh string, err error) {
	refreshToken = strings.TrimSpace(refreshToken)
	if refreshToken == "" {
		return "", "", errors.New("refresh_token 是空的")
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
		return "", "", fmt.Errorf("建换票请求: %w", err)
	}
	req.Header.Set("content-type", "application/x-www-form-urlencoded")
	req.Header.Set("accept", "application/json")

	resp, err := hc.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("换票请求失败: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode/100 != 2 {
		// 带上上游原话：换票失败最常见的原因是 refresh_token 本身被撤销了，
		// 而那句话是唯一能把它和「网络抖了一下」分开的东西。
		return "", "", fmt.Errorf("换票回 HTTP %d：%s",
			resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", "", fmt.Errorf("换票响应不是 JSON: %w", err)
	}
	if out.AccessToken == "" {
		return "", "", errors.New("换票成功但没给 access_token")
	}
	return out.AccessToken, out.RefreshToken, nil
}
