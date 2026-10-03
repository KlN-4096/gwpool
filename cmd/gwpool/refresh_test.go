package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

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
