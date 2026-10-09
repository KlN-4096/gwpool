package oauth

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// LocalExpiredReason is recorded only when the locally declared AT deadline
// passed. Unknown/malformed deadlines are not evidence of expiry.
const LocalExpiredReason = "access_token 已过期"

const UpstreamExpiredReason = `铸票 HTTP 401：{"error":{"code":"token_expired"}}`

// Not recognized by ExpiredReason: the newly issued AT was rejected as well.
// Do not immediately restart forced recovery from a periodic background tick.
const ExpiredAfterRefreshReason = `过期凭据刷新后仍失败（HTTP 401：{"error":{"code":"token_expired"}}）`

const ExpiredRefreshRejectedReason = "过期凭据的 refresh_token 已失效，需重新上传凭据"

var ErrAccessExpired = errors.New(LocalExpiredReason)

func RefreshTokenRejected(err error) bool {
	var failure *RefreshError
	if !errors.As(err, &failure) || (failure.Status != http.StatusBadRequest && failure.Status != http.StatusUnauthorized) {
		return false
	}
	switch failure.Code {
	case "invalid_grant", "refresh_token_reused", "refresh_token_expired", "refresh_token_invalid":
		return true
	}
	return false
}

func AccessExpired(access string, now time.Time) bool {
	claims, err := Parse(access)
	return err == nil && !claims.Expires.IsZero() && !claims.Expires.After(now)
}

// TokenExpired requires the upstream's exact code, not a substring in a human
// message or a generic 401. Revoked/invalidated tokens are deliberately excluded.
func TokenExpired(status int, body string) bool {
	if status != http.StatusUnauthorized {
		return false
	}
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	return json.Unmarshal([]byte(body), &response) == nil && response.Error.Code == "token_expired"
}

// ExpiredReason also recognizes previously persisted scheduler/scanner errors.
// Only these known wrappers may trigger recovery before the JWT deadline.
func ExpiredReason(reason string) bool {
	if reason == LocalExpiredReason {
		return true
	}
	if body, ok := strings.CutPrefix(reason, "铸票 HTTP 401："); ok {
		return TokenExpired(http.StatusUnauthorized, body)
	}
	if body, ok := strings.CutPrefix(reason, "上游否决了这个账号（HTTP 401："); ok &&
		strings.HasSuffix(body, "）") {
		return TokenExpired(http.StatusUnauthorized, strings.TrimSuffix(body, "）"))
	}
	return false
}
