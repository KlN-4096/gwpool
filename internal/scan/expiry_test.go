package scan

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	"gwpool/internal/store"
	"gwpool/internal/types"
)

type expiryMinter struct {
	*fakeMinter
	store      *store.Store
	limit      int
	code       string
	repeat401  bool
	retryError error
	access     []string
}

func (m *expiryMinter) BeginAttempt(ctx context.Context, c types.Creds, e types.Exit) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return ctx, err
	}
	return ctx, m.store.ReserveAttempt(c.AccountID, e.Region, m.limit, 50, time.Now())
}

func (m *expiryMinter) Mint(ctx context.Context, c types.Creds, e types.Exit, sid, pair string, exp time.Time) (types.MintResult, error) {
	m.access = append(m.access, c.AccessToken)
	if m.code != "" && (len(m.access) == 1 || m.repeat401) {
		return types.MintResult{Status: 401, ErrBody: fmt.Sprintf(`{"error":{"code":%q}}`, m.code)}, errors.New("missing landing")
	}
	if m.retryError != nil {
		return types.MintResult{}, m.retryError
	}
	return m.fakeMinter.Mint(ctx, c, e, sid, pair, exp)
}

func scanExpiryToken(exp time.Time) string {
	return "e30." + base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, exp.Unix()))) + ".fake"
}

func TestScanExpiredCredentialRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, code                    string
		local, noRT, fail, repeat     bool
		transportAfterRetry           bool
		limit, wantCalls, wantRefresh int
		enabled                       bool
	}{
		{name: "expired", code: "token_expired", limit: 2, wantCalls: 2, wantRefresh: 1, enabled: true},
		{name: "local", local: true, limit: 2, wantCalls: 1, wantRefresh: 1, enabled: true},
		{name: "local and upstream", local: true, code: "token_expired", limit: 3, wantCalls: 1, wantRefresh: 1},
		{name: "repeat expired", code: "token_expired", repeat: true, limit: 3, wantCalls: 2, wantRefresh: 1},
		{name: "failed refresh", code: "token_expired", fail: true, limit: 3, wantCalls: 1, wantRefresh: 1},
		{name: "no RT", code: "token_expired", noRT: true, limit: 3, wantCalls: 1},
		{name: "expired no RT", local: true, noRT: true, limit: 3},
		{name: "quota", code: "token_expired", limit: 1, wantCalls: 1, wantRefresh: 1, enabled: true},
		{name: "transport after retry", code: "token_expired", transportAfterRetry: true, limit: 3, wantCalls: 2, wantRefresh: 1, enabled: true},
		{name: "other 401", code: "token_revoked", limit: 3, wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, st := newScanner(t, newFake())
			a, _ := st.Account(acct.ID)
			exp := time.Now().Add(7 * 24 * time.Hour)
			if tc.local {
				exp = time.Now().Add(-time.Minute)
			}
			a.Creds.AccessToken = scanExpiryToken(exp)
			if !tc.noRT {
				a.Creds.RefreshToken = "fake-refresh"
			}
			a, err := st.AddAccount(a)
			if err != nil {
				t.Fatal(err)
			}
			m := &expiryMinter{fakeMinter: newFake(), store: st, limit: tc.limit, code: tc.code, repeat401: tc.repeat}
			if tc.transportAfterRetry {
				m.retryError = errors.New("offline transport failure")
			}
			sc.Mint, sc.MaxTries = m, 3
			calls := 0
			fresh := scanExpiryToken(time.Now().Add(10 * 24 * time.Hour))
			sc.RefreshExpired = func(_ context.Context, old types.Account) (types.Account, error) {
				calls++
				if tc.fail {
					return types.Account{}, errors.New("offline refresh failure")
				}
				return st.CommitRefreshedTokens(old, fresh, "rotated", false)
			}
			_, scanErr := sc.ScanRegion(context.Background(), a, types.Regions[0])
			got, _ := st.Account(a.ID)
			if len(m.access) != tc.wantCalls || calls != tc.wantRefresh || got.Enabled != tc.enabled {
				t.Fatalf("requests=%d refresh=%d enabled=%v err=%v", len(m.access), calls, got.Enabled, scanErr)
			}
			if used := st.AttemptUsage(a.Creds.AccountID, time.Now())["US"]; used != tc.wantCalls {
				t.Fatalf("budget=%d want=%d", used, tc.wantCalls)
			}
			if tc.name == "quota" && !errors.Is(scanErr, types.ErrRateLimited) {
				t.Fatalf("retry quota missing: %v", scanErr)
			}
			if tc.wantRefresh == 1 && !tc.fail {
				if tc.wantCalls == 2 && m.access[1] != fresh || tc.local && tc.wantCalls == 1 && m.access[0] != fresh {
					t.Fatal("request used stale token after refresh")
				}
			}
			sm, err := st.Samples()
			if err != nil || len(sm) != tc.wantCalls {
				t.Fatalf("samples=%d want=%d err=%v", len(sm), tc.wantCalls, err)
			}
		})
	}
}
