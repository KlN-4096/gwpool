package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gwpool/internal/oauth"
	"gwpool/internal/types"
)

func uploadIdentity(t *testing.T, h http.Handler, payload any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	return do(h, req)
}

func TestUploadResolvesAccountFromIDToken(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "top-level", true: "auth-json"}[nested], func(t *testing.T) {
			_, h, _, st, _ := rig(t, types.ModeSingle)
			tokens := map[string]string{
				"access_token": fakeToken("", "pro", time.Now().Add(time.Hour)),
				"id_token":     fakeToken("acct-id-token", "pro", time.Now().Add(time.Hour)),
			}
			var payload any = tokens
			if nested {
				payload = map[string]any{"tokens": tokens}
			}
			w := uploadIdentity(t, h, payload)
			if w.Code != http.StatusOK || len(st.added) != 1 || st.added[0].Creds.AccountID != "acct-id-token" {
				t.Fatalf("ID token 中的账号 ID 未用于上传：status=%d, added=%d", w.Code, len(st.added))
			}
		})
	}
}

func TestUploadRejectsConflictingTokenIdentities(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModeSingle)
	w := uploadIdentity(t, h, map[string]string{
		"access_token": fakeToken("acct-a", "pro", time.Now().Add(time.Hour)),
		"id_token":     fakeToken("acct-b", "pro", time.Now().Add(time.Hour)),
	})
	if w.Code != http.StatusBadRequest || len(st.added) != 0 {
		t.Fatalf("AT/ID token 归属不一致仍被接受：status=%d, added=%d", w.Code, len(st.added))
	}
}

func TestUploadRejectsAccountChangeAfterRefresh(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModeSingle)
	srv.opt.Refresh = func(context.Context, string) (oauth.Tokens, error) {
		return oauth.Tokens{AccessToken: fakeToken("acct-b", "pro", time.Now().Add(time.Hour))}, nil
	}
	w := uploadIdentity(t, h, map[string]string{
		"access_token":  fakeToken("acct-a", "pro", time.Now().Add(-time.Hour)),
		"refresh_token": "rt-" + strings.Repeat("x", 40),
	})
	if w.Code != http.StatusBadRequest || len(st.added) != 0 {
		t.Fatalf("刷新后账号改变仍被接受：status=%d, added=%d", w.Code, len(st.added))
	}
}

func TestUploadRefreshRetainsIDTokenIdentityAndRotatedRT(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModeSingle)
	fresh := fakeToken("", "pro", time.Now().Add(time.Hour))
	rotated := "rt-" + strings.Repeat("y", 40)
	calls := 0
	srv.opt.Refresh = func(context.Context, string) (oauth.Tokens, error) {
		calls++
		return oauth.Tokens{
			AccessToken: fresh, RefreshToken: rotated,
			IDToken: fakeToken("acct-refreshed-id", "pro", time.Now().Add(24*time.Hour)),
		}, nil
	}
	w := uploadIdentity(t, h, map[string]string{"refresh_token": "rt-" + strings.Repeat("x", 40)})
	if w.Code != http.StatusOK || len(st.added) != 1 || calls != 1 {
		t.Fatalf("RT 刷新身份回退失败：status=%d, added=%d, refreshes=%d", w.Code, len(st.added), calls)
	}
	got := st.added[0].Creds
	if got.AccessToken != fresh || got.RefreshToken != rotated || got.AccountID != "acct-refreshed-id" {
		t.Fatal("刷新结果未完整保存，或 ID token 被误用为 access token")
	}
}

func TestUploadIDTokenCannotExtendExpiredAccess(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModeSingle)
	w := uploadIdentity(t, h, map[string]string{
		"access_token": fakeToken("", "pro", time.Now().Add(-time.Minute)),
		"id_token":     fakeToken("acct-a", "pro", time.Now().Add(24*time.Hour)),
	})
	if w.Code != http.StatusBadRequest || len(st.added) != 0 {
		t.Fatal("ID token 的长寿命掩盖了 access token 过期")
	}
}

func TestUploadStillMissingIdentityIsActionableAndRefreshesOnlyOnce(t *testing.T) {
	srv, h, _, st, _ := rig(t, types.ModeSingle)
	calls := 0
	srv.opt.Refresh = func(context.Context, string) (oauth.Tokens, error) {
		calls++
		return oauth.Tokens{AccessToken: fakeToken("", "pro", time.Now().Add(time.Hour))}, nil
	}
	w := uploadIdentity(t, h, map[string]string{"refresh_token": "rt-" + strings.Repeat("x", 40)})
	if w.Code != http.StatusBadRequest || len(st.added) != 0 || calls != 1 ||
		!strings.Contains(w.Body.String(), "完整 auth.json") {
		t.Fatalf("缺身份时未有界失败：status=%d, added=%d, refreshes=%d", w.Code, len(st.added), calls)
	}
}

func TestUploadRTAndInputIDTokenReconcileWithRefresh(t *testing.T) {
	for _, newID := range []string{"", "acct-b"} {
		t.Run("fresh-id="+newID, func(t *testing.T) {
			srv, h, _, st, _ := rig(t, types.ModeSingle)
			srv.opt.Refresh = func(context.Context, string) (oauth.Tokens, error) {
				return oauth.Tokens{AccessToken: fakeToken(newID, "pro", time.Now().Add(time.Hour))}, nil
			}
			w := uploadIdentity(t, h, map[string]string{
				"refresh_token": "rt-" + strings.Repeat("x", 40),
				"id_token":      fakeToken("acct-a", "pro", time.Now().Add(time.Hour)),
			})
			if newID == "" {
				if w.Code != http.StatusOK || len(st.added) != 1 || st.added[0].Creds.AccountID != "acct-a" {
					t.Fatal("刷新前输入的 ID token 身份被丢弃")
				}
			} else if w.Code != http.StatusBadRequest || len(st.added) != 0 {
				t.Fatal("RT 与输入 ID token 的身份冲突被忽略")
			}
		})
	}
}
