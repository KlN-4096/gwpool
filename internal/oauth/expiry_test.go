package oauth

import (
	"net/http"
	"testing"
)

func TestTokenExpiredRequiresExactUpstreamCode(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		want       bool
	}{
		{"expired", `{"error":{"code":"token_expired"}}`, 401, true},
		{"different status", `{"error":{"code":"token_expired"}}`, 400, false},
		{"revoked", `{"error":{"code":"token_revoked","message":"token_expired"}}`, 401, false},
		{"message only", `{"error":{"message":"token_expired"}}`, 401, false},
		{"wrong type", `{"error":"token_expired"}`, 401, false},
		{"malformed", `{"error":{"code":"token_expired"}`, 401, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := TokenExpired(tc.status, tc.body); got != tc.want {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
		})
	}
	if !ExpiredReason(LocalExpiredReason) || !ExpiredReason(UpstreamExpiredReason) ||
		!ExpiredReason(`上游否决了这个账号（HTTP 401：{"error":{"code":"token_expired"}}）`) {
		t.Fatal("known expiry reasons were not recognized")
	}
	for _, reason := range []string{"", "token_expired", ExpiredAfterRefreshReason,
		`铸票 HTTP 401：{"error":{"code":"token_revoked"}}`,
		`铸票 HTTP 401：{"error":{"message":"token_expired"}}`,
		`铸票 HTTP 400：{"error":{"code":"token_expired"}}`} {
		if ExpiredReason(reason) {
			t.Fatalf("unconfirmed/previously retried reason recovered: %s", reason)
		}
	}
	if TokenExpired(http.StatusUnauthorized, "") {
		t.Fatal("empty 401 is not expiry")
	}
}
