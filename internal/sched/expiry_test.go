package sched

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/mint"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

func expiryToken(at time.Time) string {
	payload := fmt.Sprintf(`{"exp":%d,"https://api.openai.com/auth":{"chatgpt_account_id":"expiry-account"}}`, at.Unix())
	return "e30." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".fake"
}

func TestMintExpiredCredentialRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, code                      string
		localExpired, noRT, refreshFail bool
		repeat401                       bool
		limit, wantCalls, wantRefresh   int
		wantEnabled                     bool
	}{
		{name: "explicit expiry before JWT deadline", code: "token_expired", limit: 2, wantCalls: 2, wantRefresh: 1, wantEnabled: true},
		{name: "local expiry", localExpired: true, limit: 2, wantCalls: 1, wantRefresh: 1, wantEnabled: true},
		{name: "local and upstream expiry share one recovery", localExpired: true, code: "token_expired", limit: 2, wantCalls: 1, wantRefresh: 1},
		{name: "second expiry stops", code: "token_expired", repeat401: true, limit: 3, wantCalls: 2, wantRefresh: 1},
		{name: "refresh failure stops", code: "token_expired", refreshFail: true, limit: 2, wantCalls: 1, wantRefresh: 1},
		{name: "no RT", code: "token_expired", noRT: true, limit: 2, wantCalls: 1},
		{name: "local expiry without RT costs no request", localExpired: true, noRT: true, limit: 2},
		{name: "retry obeys quota", code: "token_expired", limit: 1, wantCalls: 1, wantRefresh: 1, wantEnabled: true},
		{name: "revoked unchanged", code: "token_revoked", limit: 2, wantCalls: 1},
		{name: "invalidated unchanged", code: "token_invalidated", limit: 2, wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			exp := time.Now().Add(7 * 24 * time.Hour)
			if tc.localExpired {
				exp = time.Now().Add(-time.Minute)
			}
			refreshToken := "fake-refresh"
			if tc.noRT {
				refreshToken = ""
			}
			a, err := st.AddAccount(types.Account{Enabled: true, Creds: types.Creds{
				AccountID: "expiry-account", AccessToken: expiryToken(exp), RefreshToken: refreshToken}})
			if err != nil {
				t.Fatal(err)
			}
			calls, refreshes := 0, 0
			fresh := expiryToken(time.Now().Add(10 * 24 * time.Hour))
			var sent []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/geo" {
					_, _ = w.Write([]byte(`{"ip":"203.0.113.1","country":"US"}`))
					return
				}
				calls++
				sent = append(sent, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
				if tc.code != "" && (calls == 1 || tc.repeat401) {
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = fmt.Fprintf(w, `{"error":{"code":%q}}`, tc.code)
					return
				}
				payload := fmt.Sprintf(`{"host":"chatgpt-prod.edge.unified-7.internal","exp":%d}`, time.Now().Add(time.Hour).Unix())
				w.Header().Add("Set-Cookie", "__oailb=e30."+base64.RawURLEncoding.EncodeToString([]byte(payload))+".fake; Path=/")
				w.Header().Add("Set-Cookie", "__cflb=fake-route; Max-Age=3600; Path=/")
				w.Header().Set("x-codex-turn-state", "fake-ticket")
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(server.Close)
			m := &mint.Client{Upstream: server.URL, GeoURL: server.URL + "/geo", HTTP: server.Client(),
				Proxies:  []config.Proxy{{Kind: config.KindNovproxy, Host: "unused.invalid:1", User: "fake", Pass: "fake", Enabled: true}},
				Attempts: st, UpstreamPerRegionHour: tc.limit, UpstreamPerAccountHour: 50}
			s := New(st, m, Options{RefreshExpired: func(_ context.Context, expected types.Account) (types.Account, error) {
				refreshes++
				if tc.refreshFail {
					return types.Account{}, errors.New("offline refresh failure")
				}
				return st.CommitRefreshedTokens(expected, fresh, "rotated-fake-refresh", false)
			}})
			_, gotErr := backgroundMint(context.Background(), s, a.ID, "")
			got, _ := st.Account(a.ID)
			if calls != tc.wantCalls || refreshes != tc.wantRefresh || got.Enabled != tc.wantEnabled {
				t.Fatalf("calls=%d refreshes=%d enabled=%v err=%v", calls, refreshes, got.Enabled, gotErr)
			}
			if gotUsage := st.AttemptUsage(a.Creds.AccountID, time.Now())["US"]; gotUsage != tc.wantCalls {
				t.Fatalf("attempt budget=%d, upstream calls=%d", gotUsage, tc.wantCalls)
			}
			if tc.name == "retry obeys quota" && !errors.Is(gotErr, types.ErrRateLimited) {
				t.Fatalf("retry did not report quota: %v", gotErr)
			}
			if tc.wantRefresh == 1 && !tc.refreshFail {
				if got.Creds.AccessToken != fresh || got.Creds.RefreshToken != "rotated-fake-refresh" {
					t.Fatal("refreshed credentials not retained")
				}
				if tc.wantEnabled && tc.wantCalls == 2 && sent[1] != fresh {
					t.Fatal("retry used stale access token")
				}
				if tc.localExpired && len(sent) > 0 && sent[0] != fresh {
					t.Fatal("local expiry was sent before refresh")
				}
			}
			samples, err := st.Samples()
			if err != nil || len(samples) != tc.wantCalls {
				t.Fatalf("samples=%d want=%d err=%v", len(samples), tc.wantCalls, err)
			}
			if tc.code != "" && len(samples) > 0 && samples[0].Status != 401 {
				t.Fatal("first failed HTTP sample was lost")
			}
		})
	}
}

func TestConcurrentExpiredMintsRefreshOnceAndUseCurrentToken(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	a, err := st.AddAccount(types.Account{Enabled: true, Creds: types.Creds{
		AccountID: "expiry-account", AccessToken: expiryToken(time.Now().Add(-time.Minute)),
		RefreshToken: "fake-rt"}})
	if err != nil {
		t.Fatal(err)
	}
	clk := newClock()
	m := newMinter(clk)
	landBare(m, "unified-7")
	calls := 0 // protected by the account lock
	fresh := expiryToken(time.Now().Add(10 * 24 * time.Hour))
	s := New(st, m, Options{RefreshExpired: func(_ context.Context, expected types.Account) (types.Account, error) {
		calls++
		time.Sleep(10 * time.Millisecond)
		return st.CommitRefreshedTokens(expected, fresh, "rotated-rt", false)
	}})
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := backgroundMint(context.Background(), s, a.ID, "")
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("concurrent stale account snapshots refreshed %d times", calls)
	}
}
