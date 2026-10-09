package authrefresh

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"gwpool/internal/oauth"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

type fakeTransport func(*http.Request) (*http.Response, error)

func (f fakeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testAccess(account string, exp time.Time) string {
	payload := fmt.Sprintf(`{"exp":%d,"https://api.openai.com/auth":{"chatgpt_account_id":%q}}`, exp.Unix(), account)
	return "e30." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".fake"
}

func TestRefreshLockedValidatesAndPreservesConcurrentState(t *testing.T) {
	for _, mode := range []string{"rotate", "manual pause", "new upload", "wrong identity", "expired reply", "RT rejected", "cancel after response"} {
		t.Run(mode, func(t *testing.T) {
			st, err := store.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			a, err := st.AddAccount(types.Account{Enabled: true, Creds: types.Creds{
				AccountID: "fake-account", AccessToken: testAccess("fake-account", time.Now().Add(-time.Minute)),
				RefreshToken: "fake-rt"}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			newAccount, expiry := "fake-account", time.Now().Add(time.Hour)
			if mode == "wrong identity" {
				newAccount = "other"
			}
			if mode == "expired reply" {
				expiry = time.Now().Add(-time.Minute)
			}
			next := testAccess(newAccount, expiry)
			body, _ := json.Marshal(oauth.Tokens{AccessToken: next, RefreshToken: "rotated-fake-rt"})
			previous := http.DefaultTransport
			calls := 0
			http.DefaultTransport = fakeTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if err := r.ParseForm(); err != nil || r.Form.Get("refresh_token") != "fake-rt" {
					t.Error("refresh did not use current RT")
				}
				switch mode {
				case "manual pause":
					err = st.SetAccountEnabled(a.ID, false)
				case "new upload":
					err = st.SetTokens(a.ID, "uploaded", "uploaded-rt")
				case "cancel after response":
					cancel()
				}
				if err != nil {
					t.Error(err)
				}
				status, data := http.StatusOK, string(body)
				if mode == "RT rejected" {
					status, data = 400, `{"error":"invalid_grant"}`
				}
				return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(data))}, nil
			})
			t.Cleanup(func() { http.DefaultTransport = previous })
			unlock := st.LockAccount(a.ID)
			_, gotErr := RefreshLocked(ctx, st, a, true)
			unlock()
			got, _ := st.Account(a.ID)
			if calls != 1 {
				t.Fatalf("refresh requests=%d", calls)
			}
			switch mode {
			case "wrong identity", "expired reply", "RT rejected":
				if gotErr == nil || got.Creds != a.Creds {
					t.Fatalf("invalid refresh reply accepted: err=%v", gotErr)
				}
			case "new upload":
				if !errors.Is(gotErr, store.ErrCredentialsChanged) || got.Creds.AccessToken != "uploaded" {
					t.Fatalf("new upload overwritten: %v", gotErr)
				}
			default:
				if gotErr != nil || got.Creds.AccessToken != next || got.Creds.RefreshToken != "rotated-fake-rt" {
					t.Fatalf("valid rotation lost: %v", gotErr)
				}
				if mode == "manual pause" && (got.Enabled || got.DisabledReason != "") {
					t.Fatal("refresh reopened manual pause")
				}
			}
		})
	}
}
