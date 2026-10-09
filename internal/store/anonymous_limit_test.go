package store

import (
	"fmt"
	"testing"

	"gwpool/internal/types"
)

func TestAnonAccountLimitBoundary400(t *testing.T) {
	const expectedLimit = 400
	if maxAnonAccounts != expectedLimit {
		t.Fatalf("anonymous account limit = %d, want %d", maxAnonAccounts, expectedLimit)
	}
	s := open(t, t.TempDir())
	accounts := make([]types.Account, expectedLimit-1)
	for i := range accounts {
		accounts[i] = types.Account{
			ID:    int64(i + 1),
			Creds: types.Creds{AccountID: fmt.Sprintf("anon:existing-%d", i)},
		}
	}
	s.mu.Lock()
	s.st.Accounts = accounts
	s.mu.Unlock()

	if _, err := s.AddAccount(types.Account{Creds: types.Creds{AccountID: "anon:last"}}); err != nil {
		t.Fatalf("400th anonymous account rejected: %v", err)
	}
	if _, err := s.AddAccount(types.Account{Creds: types.Creds{AccountID: "anon:overflow"}}); err == nil {
		t.Fatal("401st anonymous account should be rejected")
	}
	if _, err := s.AddAccount(types.Account{Creds: types.Creds{AccountID: "anon:existing-0"}}); err != nil {
		t.Fatalf("existing anonymous account rejected at capacity: %v", err)
	}
	if _, err := s.AddAccount(types.Account{Creds: types.Creds{AccountID: "hosted", AccessToken: "test-token"}}); err != nil {
		t.Fatalf("anonymous capacity blocked a hosted account: %v", err)
	}
}
