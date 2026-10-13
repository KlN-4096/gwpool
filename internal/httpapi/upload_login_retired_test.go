package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"gwpool/internal/oauth"
)

func TestUploadRejectsRetiredLoginBeforeSideEffects(t *testing.T) {
	for _, endpoint := range []string{"/upload/jobs", "/upload"} {
		for name, fields := range map[string]url.Values{
			"login-mode":        {"mode": {"login"}},
			"password-fields":   {"email": {"offline@example.invalid"}, "password": {"offline-password"}},
			"empty-password":    {"password": {""}},
			"totp-field":        {"totp": {"offline-totp"}},
			"batch-field":       {"login_batch": {"offline@example.invalid----offline-password"}},
			"duplicate-mode":    {"mode": {"credentials", "login"}},
			"unrecognized-mode": {"mode": {"other"}},
		} {
			t.Run(endpoint+"/"+name, func(t *testing.T) {
				var refreshes atomic.Int32
				s, st, h := uploadJobRig(t, Options{Refresh: func(context.Context, string) (oauth.Tokens, error) {
					refreshes.Add(1)
					return fakeUploadTokens(), nil
				}})
				form := credentialForm()
				for key, values := range fields {
					form[key] = values
				}
				r := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(form.Encode()))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				w := do(h, r)
				if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "邮箱密码上传已停用") {
					t.Fatalf("retired login request accepted: status=%d", w.Code)
				}
				if strings.Contains(w.Body.String(), "offline-") || len(w.Result().Cookies()) != 0 {
					t.Fatal("rejected login echoed input or changed browser identity")
				}
				s.uploadJobs.mu.Lock()
				jobs := len(s.uploadJobs.jobs)
				s.uploadJobs.mu.Unlock()
				if jobs != 0 || refreshes.Load() != 0 || len(st.Accounts()) != 0 || len(st.Owners()) != 0 {
					t.Fatal("retired login request created work or changed account state")
				}
			})
		}
	}
}
