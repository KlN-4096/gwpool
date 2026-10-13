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

func fakeUploadTokens() oauth.Tokens {
	return oauth.Tokens{AccessToken: fakeToken("upload-fixture", "free", time.Now().Add(time.Hour)),
		RefreshToken: "rt.offline-upload-never-valid"}
}

func credentialForm() url.Values {
	return url.Values{"mode": {"credentials"}, "kind": {"refresh"}, "creds": {"rt.offline-upload-input"}}
}

func heldUploadRefresh(release <-chan struct{}) func(context.Context, string) (oauth.Tokens, error) {
	return func(ctx context.Context, _ string) (oauth.Tokens, error) {
		select {
		case <-release:
			return fakeUploadTokens(), nil
		case <-ctx.Done():
			return oauth.Tokens{}, ctx.Err()
		}
	}
}

func TestUploadJobsKeepSecretsPrivateAndBindCapabilities(t *testing.T) {
	release := make(chan struct{})
	_, st, h := uploadJobRig(t, Options{Refresh: heldUploadRefresh(release)})
	job := startUploadJob(t, h, credentialForm())
	waitUploadJob(t, h, job, "refresh")
	if len(st.Owners()) != 0 {
		t.Fatal("allocated an owner before successful refresh")
	}
	w := jobRequest(h, map[string]string{"id": job["id"], "key": "wrong"}, "GET", "", "")
	if w.Code != http.StatusNotFound {
		t.Fatal("job ID alone grants access")
	}
	w = jobRequest(h, job, "GET", "", "")
	for _, secret := range []string{"rt.offline", job["key"]} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("job status disclosed a secret")
		}
	}
	if w = jobRequest(h, job, "POST", "/input", `{"prompt_id":1,"value":"123456"}`); w.Code != http.StatusNotFound {
		t.Fatal("retired verification endpoint still registered")
	}
	close(release)
	view := waitUploadJob(t, h, job, "completed")
	if len(st.Accounts()) != 1 || view.Receipt["owner_token"] == "" || view.Receipt["status"] != "created" {
		t.Fatal("missing successful owner/account receipt")
	}
	w = jobRequest(h, job, "GET", "", "")
	if strings.Contains(w.Body.String(), fakeUploadTokens().RefreshToken) || len(w.Result().Cookies()) == 0 {
		t.Fatal("secret leaked or explicitly established owner not remembered")
	}
}

func TestUploadJobsExposeActualRTProgress(t *testing.T) {
	release := make(chan struct{})
	_, st, h := uploadJobRig(t, Options{Refresh: func(ctx context.Context, _ string) (oauth.Tokens, error) {
		select {
		case <-release:
			return fakeUploadTokens(), nil
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
	_, st, h := uploadJobRig(t, Options{Refresh: func(context.Context, string) (oauth.Tokens, error) {
		return fakeUploadTokens(), nil
	}})
	owner, _ := st.AddOwner("fixture")
	form := credentialForm()
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

func TestUploadJobsRejectChangesDuringRefresh(t *testing.T) {
	for _, change := range []string{"pause", "delete", "credentials"} {
		t.Run(change, func(t *testing.T) {
			release := make(chan struct{})
			_, st, h := uploadJobRig(t, Options{Refresh: heldUploadRefresh(release)})
			owner, _ := st.AddOwner("fixture")
			a, _ := st.AddAccount(types.Account{OwnerID: owner.ID, Enabled: true,
				Creds: types.Creds{AccountID: "upload-fixture", AccessToken: "old", RefreshToken: "old-rt"}})
			form := credentialForm()
			form.Set("owner_token", owner.Token)
			job := startUploadJob(t, h, form)
			waitUploadJob(t, h, job, "refresh")
			switch change {
			case "pause":
				_ = st.SetAccountEnabled(a.ID, false)
			case "delete":
				_ = st.DeleteAccount(a.ID)
			case "credentials":
				_ = st.SetTokens(a.ID, "newer", "newer-rt")
			}
			close(release)
			view := waitUploadJob(t, h, job, "failed")
			if view.Receipt != nil || !strings.Contains(view.Message, "没有覆盖") {
				t.Fatal("concurrent mutation was lost", view.State, view.Message)
			}
		})
	}
}

func TestUploadJobsAdmissionCancellationAndCSRF(t *testing.T) {
	_, st, h := uploadJobRig(t, Options{Refresh: heldUploadRefresh(make(chan struct{}))})
	var jobs []map[string]string
	for range maxClientUploads {
		job := startUploadJob(t, h, credentialForm())
		waitUploadJob(t, h, job, "refresh")
		jobs = append(jobs, job)
	}
	r := httptest.NewRequest("POST", "/upload/jobs", strings.NewReader(credentialForm().Encode()))
	if w := do(h, r); w.Code != http.StatusTooManyRequests {
		t.Fatal("per-client admission not bounded")
	}
	r = httptest.NewRequest("POST", "/upload/jobs/"+jobs[0]["id"]+"/cancel", nil)
	r.Header.Set("Authorization", "Bearer "+jobs[0]["key"])
	r.Header.Set("Origin", "https://attacker.invalid")
	if w := do(h, r); w.Code != http.StatusForbidden {
		t.Fatal("cross-origin cancellation allowed")
	}
	for _, job := range jobs {
		if w := jobRequest(h, job, "POST", "/cancel", "{}"); w.Code != http.StatusAccepted {
			t.Fatal("cancel failed")
		}
		waitUploadJob(t, h, job, "canceled")
	}
	if len(st.Accounts()) != 0 || len(st.Owners()) != 0 {
		t.Fatal("canceled refreshes wrote accounts/owners")
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
	s, st, _ := uploadJobRig(t, Options{Refresh: func(context.Context, string) (oauth.Tokens, error) {
		return fakeUploadTokens(), nil
	}})
	delayed := &delayedImportStore{Store: st, actual: st, entered: make(chan struct{}), release: make(chan struct{})}
	s.st = delayed
	h := s.Handler()
	job := startUploadJob(t, h, credentialForm())
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
