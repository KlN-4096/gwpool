package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"gwpool/internal/oauth"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

func TestRefreshWaitsForAccountLockAndRereadsCredentials(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	a, err := st.AddAccount(types.Account{
		Enabled: true,
		Creds:   types.Creds{AccountID: "test-account", AccessToken: "old-invalid-token", RefreshToken: "test-refresh"},
	})
	if err != nil {
		t.Fatal(err)
	}
	unlock := st.LockAccount(a.ID)
	done := make(chan struct{})
	go func() {
		defer close(done)
		refreshDue(context.Background(), st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	select {
	case <-done:
		unlock()
		t.Fatal("刷新绕过了铸票持有的账号锁")
	case <-time.After(20 * time.Millisecond):
	}
	// 锁内换成远期凭据。刷新必须重读，不能拿等待前的旧快照调用真实 OAuth。
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, time.Now().Add(7*24*time.Hour).Unix())))
	token := "e30." + payload + ".sig"
	if err := st.SetTokens(a.ID, token, ""); err != nil {
		unlock()
		t.Fatal(err)
	}
	unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("刷新没有在重读未到期凭据后结束")
	}
}

type refreshTransport func(*http.Request) (*http.Response, error)

func (f refreshTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func refreshTestToken(account string, expiry time.Time) string {
	payload, _ := json.Marshal(map[string]any{
		"exp": expiry.Unix(), "https://api.openai.com/auth": map[string]string{"chatgpt_account_id": account},
	})
	return "e30." + base64.RawURLEncoding.EncodeToString(payload) + ".fake"
}

func TestBackgroundRefreshRejectsInvalidIdentityAndExpiredAccess(t *testing.T) {
	for _, tc := range []struct {
		name, accessAccount, idAccount string
		expiry                         time.Time
	}{
		{"different access identity", "acct-b", "", time.Now().Add(time.Hour)},
		{"different id identity", "", "acct-b", time.Now().Add(time.Hour)},
		{"expired access", "acct-a", "", time.Now().Add(-time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = st.Close() }()
			original := refreshTestToken("acct-a", time.Now().Add(time.Hour))
			account, err := st.AddAccount(types.Account{Enabled: true,
				Creds: types.Creds{AccountID: "acct-a", AccessToken: original, RefreshToken: "fake-refresh"}})
			if err != nil {
				t.Fatal(err)
			}
			tokens := oauth.Tokens{AccessToken: refreshTestToken(tc.accessAccount, tc.expiry)}
			if tc.idAccount != "" {
				tokens.IDToken = refreshTestToken(tc.idAccount, time.Now().Add(time.Hour))
			}
			body, _ := json.Marshal(tokens)
			previous := http.DefaultTransport
			http.DefaultTransport = refreshTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{},
					Body: io.NopCloser(strings.NewReader(string(body)))}, nil
			})
			t.Cleanup(func() { http.DefaultTransport = previous })
			refreshAccount(context.Background(), st, slog.New(slog.NewTextHandler(io.Discard, nil)),
				account.ID, time.Now().Add(24*time.Hour))
			got, _ := st.Account(account.ID)
			if got.Creds.AccessToken != original {
				t.Fatal("后台刷新把不同账号或过期凭据写入了现有账号")
			}
		})
	}
}

func TestBackgroundRefreshRecoversExplicitExpiryBeforeJWTDeadline(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		wantCalls    int
	}{
		{"mint expired", `铸票 HTTP 401：{"error":{"code":"token_expired"}}`, 1},
		{"scan expired", `上游否决了这个账号（HTTP 401：{"error":{"code":"token_expired"}}）`, 1},
		{"revoked unchanged", `铸票 HTTP 401：{"error":{"code":"token_revoked"}}`, 0},
		{"manual unchanged", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = st.Close() }()
			a, err := st.AddAccount(types.Account{Enabled: true,
				Creds: types.Creds{AccountID: "acct-a",
					AccessToken:  refreshTestToken("acct-a", time.Now().Add(7*24*time.Hour)),
					RefreshToken: "fake-refresh"}})
			if err != nil {
				t.Fatal(err)
			}
			if tc.reason == "" {
				err = st.SetAccountEnabled(a.ID, false)
			} else {
				err = st.DisableAccount(a.ID, tc.reason)
			}
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			next := refreshTestToken("acct-a", time.Now().Add(10*24*time.Hour))
			body, _ := json.Marshal(oauth.Tokens{AccessToken: next, RefreshToken: "rotated-fake-refresh"})
			previous := http.DefaultTransport
			http.DefaultTransport = refreshTransport(func(*http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{},
					Body: io.NopCloser(strings.NewReader(string(body)))}, nil
			})
			t.Cleanup(func() { http.DefaultTransport = previous })
			refreshDue(context.Background(), st, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if calls != tc.wantCalls {
				t.Fatalf("refresh calls=%d, want=%d", calls, tc.wantCalls)
			}
			got, _ := st.Account(a.ID)
			if tc.wantCalls == 1 {
				if !got.Enabled || got.DisabledReason != "" || got.Creds.AccessToken != next ||
					got.Creds.RefreshToken != "rotated-fake-refresh" {
					t.Fatal("explicit expired account was not recovered with rotated credentials")
				}
			} else if got.Enabled || got.Creds != a.Creds {
				t.Fatal("unrelated rejection/manual state changed")
			}
		})
	}
}

func TestBackgroundExpiryRecoveryStopsAfterRefreshTokenRejected(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	a, err := st.AddAccount(types.Account{Enabled: true, Creds: types.Creds{
		AccountID: "acct-a", AccessToken: refreshTestToken("acct-a", time.Now().Add(-time.Minute)),
		RefreshToken: "fake-refresh"}})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	previous := http.DefaultTransport
	http.DefaultTransport = refreshTransport(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 400, Header: http.Header{},
			Body: io.NopCloser(strings.NewReader(`{"error":"refresh_token_reused"}`))}, nil
	})
	t.Cleanup(func() { http.DefaultTransport = previous })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	refreshDue(context.Background(), st, log)
	refreshDue(context.Background(), st, log)
	got, _ := st.Account(a.ID)
	if calls != 1 || got.Enabled || got.DisabledReason != oauth.ExpiredRefreshRejectedReason {
		t.Fatalf("permanent RT failure retried: calls=%d enabled=%v", calls, got.Enabled)
	}
}
