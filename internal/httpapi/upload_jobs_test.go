package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gwpool/internal/oauth"
	"gwpool/internal/reauth"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

func uploadJobRig(t *testing.T, options Options) (*Server, *store.Store, http.Handler) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(&fakePool{mode: types.ModePool}, st, &fakeScanner{}, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.ShutdownUploads(ctx); err != nil {
			t.Error(err)
		}
		_ = st.Close()
	})
	return s, st, s.Handler()
}

func startUploadJob(t *testing.T, h http.Handler, values url.Values) map[string]string {
	t.Helper()
	r := httptest.NewRequest("POST", "/upload/jobs", strings.NewReader(values.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := do(h, r)
	if w.Code != http.StatusAccepted {
		t.Fatalf("job not admitted: %d %s", w.Code, w.Body.String())
	}
	var out struct {
		ID, Key string
	}
	if json.Unmarshal(w.Body.Bytes(), &out) != nil || out.ID == "" || out.Key == "" {
		t.Fatal("missing private capability")
	}
	return map[string]string{"id": out.ID, "key": out.Key}
}

func jobRequest(h http.Handler, job map[string]string, method, suffix, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/upload/jobs/"+job["id"]+suffix, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+job["key"])
	r.Header.Set("Content-Type", "application/json")
	return do(h, r)
}

func waitUploadJob(t *testing.T, h http.Handler, job map[string]string, want string) uploadJobView {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		w := jobRequest(h, job, "GET", "", "")
		var view uploadJobView
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &view) != nil {
			t.Fatalf("invalid job response: %d", w.Code)
		}
		if view.State == want || view.Stage == want {
			return view
		}
		if view.State == "failed" || view.State == "completed" || view.State == "canceled" {
			t.Fatalf("unexpected terminal state: %s %s", view.State, view.Message)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("job did not reach expected state", want)
	return uploadJobView{}
}

func fakeLoginTokens() oauth.Tokens {
	return oauth.Tokens{AccessToken: fakeToken("login-fixture", "free", time.Now().Add(time.Hour)),
		RefreshToken: "rt.offline-login-never-valid"}
}

func loginForm() url.Values {
	return url.Values{"mode": {"login"}, "email": {"test@example.com"}, "password": {"offline-password"}}
}

func promptedLogin(ctx context.Context, _ reauth.Input, hooks reauth.Hooks) (oauth.Tokens, error) {
	hooks.Progress("password")
	_, err := hooks.Prompt(ctx, reauth.Challenge{Kind: "email_code"})
	if err != nil {
		return oauth.Tokens{}, err
	}
	hooks.Progress("exchange")
	return fakeLoginTokens(), nil
}

func answerPrompt(t *testing.T, h http.Handler, job map[string]string, prompt uint64) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"prompt_id": prompt, "value": "123456"})
	w := jobRequest(h, job, "POST", "/input", string(body))
	if w.Code != http.StatusAccepted {
		t.Fatalf("input rejected: %d %s", w.Code, w.Body.String())
	}
}

func TestUploadJobsKeepSecretsPrivateAndBindPrompts(t *testing.T) {
	_, st, h := uploadJobRig(t, Options{Login: promptedLogin})
	job := startUploadJob(t, h, loginForm())
	view := waitUploadJob(t, h, job, "waiting")
	if len(st.Owners()) != 0 {
		t.Fatal("allocated an owner before successful login")
	}
	w := jobRequest(h, map[string]string{"id": job["id"], "key": "wrong"}, "GET", "", "")
	if w.Code != http.StatusNotFound {
		t.Fatal("job ID alone grants access")
	}
	w = jobRequest(h, job, "GET", "", "")
	for _, secret := range []string{"offline-password", "test@example.com", "rt.offline", job["key"]} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("job status disclosed a secret")
		}
	}
	if w = jobRequest(h, job, "POST", "/input", `{"prompt_id":999,"value":"123456"}`); w.Code != http.StatusConflict {
		t.Fatal("stale prompt accepted")
	}
	if w = jobRequest(h, job, "POST", "/input", `{"prompt_id":1,"value":"not-a-code"}`); w.Code != http.StatusBadRequest {
		t.Fatal("invalid code accepted")
	}
	answerPrompt(t, h, job, view.PromptID)
	view = waitUploadJob(t, h, job, "completed")
	if len(st.Accounts()) != 1 || view.Receipt["owner_token"] == "" || view.Receipt["status"] != "created" {
		t.Fatal("missing successful owner/account receipt")
	}
	w = jobRequest(h, job, "GET", "", "")
	if strings.Contains(w.Body.String(), fakeLoginTokens().RefreshToken) || len(w.Result().Cookies()) == 0 {
		t.Fatal("secret leaked or explicitly established owner not remembered")
	}
}

func TestUploadJobsExposeActualRTProgress(t *testing.T) {
	release := make(chan struct{})
	_, st, h := uploadJobRig(t, Options{Refresh: func(ctx context.Context, _ string) (oauth.Tokens, error) {
		select {
		case <-release:
			return fakeLoginTokens(), nil
		case <-ctx.Done():
			return oauth.Tokens{}, ctx.Err()
		}
	}})
	job := startUploadJob(t, h, url.Values{"mode": {"credentials"}, "kind": {"refresh"}, "creds": {"rt.offline-input-never-valid"}})
	view := waitUploadJob(t, h, job, "refresh")
	if len(st.Accounts()) != 0 || view.Stage != "refresh" {
		t.Fatal("progress claimed storage before refresh")
	}
	close(release)
	view = waitUploadJob(t, h, job, "completed")
	if strings.Join(view.Stages, ",") != "queued,validate,refresh,store,done" {
		t.Fatal("missing actual stage history", view.Stages)
	}
}

func TestUploadJobsDoNotPlantPageOwnerCookie(t *testing.T) {
	_, st, h := uploadJobRig(t, Options{Login: func(context.Context, reauth.Input, reauth.Hooks) (oauth.Tokens, error) {
		return fakeLoginTokens(), nil
	}})
	owner, _ := st.AddOwner("fixture")
	form := loginForm()
	form.Set("page_owner", owner.Token)
	job := startUploadJob(t, h, form)
	waitUploadJob(t, h, job, "completed")
	if w := jobRequest(h, job, "GET", "", ""); len(w.Result().Cookies()) != 0 {
		t.Fatal("page-owner import planted browser identity")
	}
	if st.Accounts()[0].OwnerID != owner.ID {
		t.Fatal("did not attach to explicit owner")
	}
}

func TestUploadJobsRejectChangesDuringOTPWait(t *testing.T) {
	for _, change := range []string{"pause", "delete", "credentials"} {
		t.Run(change, func(t *testing.T) {
			_, st, h := uploadJobRig(t, Options{Login: promptedLogin})
			owner, _ := st.AddOwner("fixture")
			a, _ := st.AddAccount(types.Account{OwnerID: owner.ID, Enabled: true,
				Creds: types.Creds{AccountID: "login-fixture", AccessToken: "old", RefreshToken: "old-rt"}})
			form := loginForm()
			form.Set("owner_token", owner.Token)
			job := startUploadJob(t, h, form)
			view := waitUploadJob(t, h, job, "waiting")
			switch change {
			case "pause":
				_ = st.SetAccountEnabled(a.ID, false)
			case "delete":
				_ = st.DeleteAccount(a.ID)
			case "credentials":
				_ = st.SetTokens(a.ID, "newer", "newer-rt")
			}
			answerPrompt(t, h, job, view.PromptID)
			view = waitUploadJob(t, h, job, "failed")
			if view.Receipt != nil || !strings.Contains(view.Message, "没有覆盖") {
				t.Fatal("concurrent mutation was lost", view.State, view.Message)
			}
		})
	}
}

func TestUploadJobsAdmissionCancellationAndCSRF(t *testing.T) {
	_, st, h := uploadJobRig(t, Options{Login: promptedLogin})
	var jobs []map[string]string
	for range maxClientUploads {
		job := startUploadJob(t, h, loginForm())
		waitUploadJob(t, h, job, "waiting")
		jobs = append(jobs, job)
	}
	r := httptest.NewRequest("POST", "/upload/jobs", strings.NewReader(loginForm().Encode()))
	if w := do(h, r); w.Code != http.StatusTooManyRequests {
		t.Fatal("per-client admission not bounded")
	}
	r = httptest.NewRequest("POST", "/upload/jobs/"+jobs[0]["id"]+"/input", strings.NewReader(`{"prompt_id":1,"value":"123456"}`))
	r.Header.Set("Authorization", "Bearer "+jobs[0]["key"])
	r.Header.Set("Origin", "https://attacker.invalid")
	if w := do(h, r); w.Code != http.StatusForbidden {
		t.Fatal("cross-origin prompt submission allowed")
	}
	for _, job := range jobs {
		if w := jobRequest(h, job, "POST", "/cancel", "{}"); w.Code != http.StatusAccepted {
			t.Fatal("cancel failed")
		}
		waitUploadJob(t, h, job, "canceled")
	}
	if len(st.Accounts()) != 0 || len(st.Owners()) != 0 {
		t.Fatal("canceled OTP waits wrote accounts/owners")
	}
}

type delayedImportStore struct {
	Store
	actual  *store.Store
	entered chan struct{}
	release chan struct{}
}

func (s *delayedImportStore) AddAccountIfVersion(ctx context.Context, a types.Account, id int64, revision uint64) (types.Account, error) {
	close(s.entered)
	<-s.release
	return s.actual.AddAccountIfVersion(ctx, a, id, revision)
}

func TestUploadJobsMissingLiveFailsWithoutNetworkOrPanic(t *testing.T) {
	_, st, h := uploadJobRig(t, Options{})
	job := startUploadJob(t, h, loginForm())
	view := waitUploadJob(t, h, job, "failed")
	if !strings.Contains(view.Message, "动态登录出口") || len(st.Accounts()) != 0 {
		t.Fatal("nil Live should be an explicit unavailable transport", view.Message)
	}
}

func TestUploadWorkspaceFixture(t *testing.T) {
	dir := os.Getenv("GWPOOL_UPLOAD_PREVIEW_DIR")
	if dir == "" {
		t.Skip("optional offline UI fixture")
	}
	_, _, h := uploadJobRig(t, Options{})
	w := do(h, httptest.NewRequest("GET", "/", nil))
	if w.Code != http.StatusOK {
		t.Fatal("preview did not render")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.html"), w.Body.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestUploadJobsCancellationBeforeStoreCommit(t *testing.T) {
	s, st, _ := uploadJobRig(t, Options{Login: func(context.Context, reauth.Input, reauth.Hooks) (oauth.Tokens, error) {
		return fakeLoginTokens(), nil
	}})
	delayed := &delayedImportStore{Store: st, actual: st, entered: make(chan struct{}), release: make(chan struct{})}
	s.st = delayed
	h := s.Handler()
	job := startUploadJob(t, h, loginForm())
	select {
	case <-delayed.entered:
	case <-time.After(time.Second):
		t.Fatal("commit not reached")
	}
	s.uploadJobs.mu.Lock()
	s.uploadJobs.jobs[job["id"]].cancel() // Models deadline/shutdown while waiting for Store's mutex.
	s.uploadJobs.mu.Unlock()
	close(delayed.release)
	waitUploadJob(t, h, job, "canceled")
	if len(st.Accounts()) != 0 {
		t.Fatal("expired task committed after waiting for Store")
	}
}
