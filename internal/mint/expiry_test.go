package mint

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"gwpool/internal/oauth"
	"gwpool/internal/types"
)

func TestVerifyFullReportsOnlyExplicitTokenExpiry(t *testing.T) {
	for _, code := range []string{"token_expired", "token_revoked", "token_invalidated", ""} {
		t.Run(code, func(t *testing.T) {
			client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":{"code":"` + code + `"}}`))
			})
			v, err := client.VerifyFull(context.Background(), types.Creds{AccountID: "fake", AccessToken: "fake"},
				types.Regions[0].Exit, "fake-sid", "fake-pair", "fake-ticket")
			if code == "token_expired" {
				if !errors.Is(err, oauth.ErrAccessExpired) || v == nil || v.Status != 401 || v.Full {
					t.Fatalf("expiry not distinguishable: verdict=%v err=%v", v, err)
				}
			} else if v != nil || err != nil {
				t.Fatalf("unrelated 401 changed: verdict=%v err=%v", v, err)
			}
		})
	}
}
