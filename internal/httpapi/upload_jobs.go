package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"gwpool/internal/store"
	"gwpool/internal/types"
)

const (
	uploadJobTTL        = 10 * time.Minute
	uploadResultTTL     = 10 * time.Minute
	maxUploadJobs       = 256
	maxActiveUploads    = 8
	maxClientUploads    = 3
	maxUploadJobPayload = 128 << 10
)

type uploadJobManager struct {
	mu       sync.Mutex
	jobs     map[string]*uploadJob
	stopping bool
	wg       sync.WaitGroup
}

type accountVersion struct {
	id       int64
	revision uint64
}

type uploadJobContextKey struct{}

type uploadJob struct {
	mu        sync.Mutex
	id, key   string
	client    string
	ctx       context.Context
	cancel    context.CancelFunc
	created   time.Time
	expires   time.Time
	state     string
	stage     string
	stages    []string
	message   string
	receipt   map[string]string
	cookie    string
	committed bool
	versions  map[string]accountVersion
}

type uploadJobView struct {
	ID      string            `json:"id"`
	State   string            `json:"state"`
	Stage   string            `json:"stage"`
	Stages  []string          `json:"stages"`
	Message string            `json:"message,omitempty"`
	Expires time.Time         `json:"expires"`
	Receipt map[string]string `json:"receipt,omitempty"`
}

func (j *uploadJob) viewLocked() uploadJobView {
	return uploadJobView{ID: j.id, State: j.state, Stage: j.stage, Stages: j.stages, Message: j.message,
		Expires: j.expires, Receipt: j.receipt}
}

func (j *uploadJob) progress(stage string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.stage = stage
	if len(j.stages) < 32 && (len(j.stages) == 0 || j.stages[len(j.stages)-1] != stage) {
		j.stages = append(j.stages, stage)
	}
}

func uploadProgress(ctx context.Context, stage string) {
	if job, ok := ctx.Value(uploadJobContextKey{}).(*uploadJob); ok {
		job.progress(stage)
	}
}

type conditionalAccountStore interface {
	AddAccountIfVersion(context.Context, types.Account, int64, uint64) (types.Account, error)
}

func uploadConflict(err error) bool { return errors.Is(err, store.ErrCredentialsChanged) }

func (s *Server) addUploadedAccount(ctx context.Context, a types.Account) (types.Account, error) {
	j, async := ctx.Value(uploadJobContextKey{}).(*uploadJob)
	if !async {
		return s.st.AddAccount(a)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return types.Account{}, err
	}
	version := j.versions[a.Creds.AccountID]
	out, err := s.st.(conditionalAccountStore).AddAccountIfVersion(ctx, a, version.id, version.revision)
	if err == nil && out.OwnerID == a.OwnerID {
		j.committed = true
	}
	return out, err
}

func nonce() (string, error) {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func rejectRetiredUploadLogin(w http.ResponseWriter, form url.Values) bool {
	modes := form["mode"]
	retired := len(modes) > 1 || len(modes) == 1 && modes[0] != "" && modes[0] != "credentials"
	for _, field := range []string{"email", "password", "totp", "login_email", "login_password", "login_totp", "login_batch"} {
		if _, present := form[field]; present {
			retired = true
		}
	}
	if retired {
		http.Error(w, "邮箱密码上传已停用，请使用 Token 或 JSON 凭据导入", http.StatusBadRequest)
	}
	return retired
}

func (s *Server) handleUploadJobStart(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.st.(conditionalAccountStore); !ok {
		http.Error(w, "此实例未接入异步上传存储", http.StatusServiceUnavailable)
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxUploadJobPayload))
	if err != nil {
		http.Error(w, "上传内容超过限制", http.StatusRequestEntityTooLarge)
		return
	}
	form, err := url.ParseQuery(string(raw))
	if err != nil {
		http.Error(w, "上传表单格式错误", http.StatusBadRequest)
		return
	}
	if rejectRetiredUploadLogin(w, form) {
		return
	}
	if form.Get("mode") != "credentials" {
		http.Error(w, "仅支持 Token 或 JSON 凭据上传", http.StatusBadRequest)
		return
	}
	// Freeze attribution before the async task starts. A later cookie change or
	// another tab cannot reassign an in-flight import.
	owner, hadOwner := s.ownerFromCookie(r)
	fromPage := strings.TrimSpace(form.Get("page_owner")) != ""
	token := strings.TrimSpace(form.Get("owner_token"))
	if fromPage {
		token = strings.TrimSpace(form.Get("page_owner"))
	}
	if token != "" {
		var ok bool
		owner, ok = s.st.OwnerByToken(token)
		if !ok {
			http.Error(w, "认不出这个账号总览令牌，留空就新建一个", http.StatusBadRequest)
			return
		}
	}
	form.Set("owner_token", token)
	if owner.ID != 0 && token == "" {
		form.Set("owner_token", owner.Token)
	}
	if len(form.Get("creds")) > 64<<10 || strings.TrimSpace(form.Get("creds")) == "" {
		http.Error(w, "单份凭据为空或超过 64 KB", http.StatusBadRequest)
		return
	}
	// Check permission before spending any upstream RT request.
	id, err := nonce()
	if err != nil {
		http.Error(w, "无法创建上传任务", http.StatusInternalServerError)
		return
	}
	key, err := nonce()
	if err != nil {
		http.Error(w, "无法创建上传任务", http.StatusInternalServerError)
		return
	}
	client, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		client = r.RemoteAddr
	}
	if owner.ID != 0 {
		// A bearer owner is stable across proxy/NAT addresses. The key is
		// used only for admission, never printed or returned in job status.
		client = "owner:" + owner.Token
	}
	j := &uploadJob{id: id, key: key, client: client, created: time.Now(),
		state: "running", stage: "queued", stages: []string{"queued"}, versions: make(map[string]accountVersion)}
	for _, a := range s.st.Accounts() {
		if a.OwnerID == owner.ID || a.OwnerID == 0 {
			j.versions[a.Creds.AccountID] = accountVersion{a.ID, a.CredentialRevision}
		}
	}
	m := &s.uploadJobs
	m.mu.Lock()
	if m.jobs == nil {
		m.jobs = make(map[string]*uploadJob)
	}
	active, sameClient := 0, 0
	for _, job := range m.jobs {
		job.mu.Lock()
		running := job.state == "running"
		if running {
			active++
			if job.client == client {
				sameClient++
			}
		}
		job.mu.Unlock()
	}
	if m.stopping || len(m.jobs) >= maxUploadJobs || active >= maxActiveUploads || sameClient >= maxClientUploads {
		m.mu.Unlock()
		http.Error(w, "上传任务繁忙，请等当前任务完成后再提交；本项尚未开始", http.StatusTooManyRequests)
		return
	}
	j.ctx, j.cancel = context.WithTimeout(context.Background(), uploadJobTTL)
	j.expires, _ = j.ctx.Deadline()
	m.jobs[id] = j
	m.wg.Add(1)
	m.mu.Unlock()
	// The response exposes a separate capability only once. It is not the
	// public job ID, never a URL parameter/cookie/localStorage entry.
	writeJSON(w, http.StatusAccepted, map[string]any{"id": id, "key": key, "expires": j.expires})
	headers := r.Header.Clone()
	// Cookie restoration is decided using the initiating attribution. Page
	// links may attach an account, but must never plant an owner identity.
	allowCookie := !fromPage && (!hadOwner || func() bool {
		cur, _ := s.ownerFromCookie(r)
		return cur.ID == owner.ID
	}())
	go func() {
		defer m.wg.Done()
		defer j.cancel()
		s.runUploadJob(j, form, headers, allowCookie)
		time.AfterFunc(uploadResultTTL, func() {
			m.mu.Lock()
			delete(m.jobs, j.id)
			m.mu.Unlock()
		})
	}()
}

// uploadResponse adapts the existing upload boundary, preserving its parsing,
// attribution, diagnostics, and receipt contract rather than duplicating them.
type uploadResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (w *uploadResponse) Header() http.Header { return w.header }
func (w *uploadResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *uploadResponse) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(b)
}

func (s *Server) runUploadJob(j *uploadJob, form url.Values, headers http.Header, allowCookie bool) {
	ctx := context.WithValue(j.ctx, uploadJobContextKey{}, j)
	var jobErr error
	var receipt map[string]string
	var cookie string
	defer func() {
		for key := range form {
			delete(form, key)
		}
		j.mu.Lock()
		defer j.mu.Unlock()
		j.versions = nil
		j.client = ""
		j.expires = time.Now().Add(uploadResultTTL)
		if jobErr != nil {
			j.state = "failed"
			j.message = uploadJobError(jobErr)
			if errors.Is(jobErr, context.Canceled) {
				j.state = "canceled"
			}
			return
		}
		j.state, j.stage, j.receipt, j.cookie = "completed", "done", receipt, cookie
		j.stages = append(j.stages, "done")
	}()
	if err := ctx.Err(); err != nil {
		jobErr = err
		return
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "/upload", strings.NewReader(form.Encode()))
	if err != nil {
		jobErr = errors.New("无法建立入库请求")
		return
	}
	request.Header = headers
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")
	response := &uploadResponse{header: make(http.Header)}
	s.handleUpload(response, request)
	if response.status != http.StatusOK {
		if ctx.Err() != nil {
			jobErr = ctx.Err()
			return
		}
		// Existing upload responses are already sanitized. Preserve uncertainty
		// for storage failures.
		message := strings.TrimSpace(response.body.String())
		if len(message) > 500 {
			message = "上传失败，请查看账号总览后再操作"
		}
		if response.status >= 500 {
			message += "；结果未确认，请先检查账号总览，不要直接重复刷新"
		}
		jobErr = errors.New(message)
		return
	}
	if err := json.Unmarshal(response.body.Bytes(), &receipt); err != nil {
		jobErr = errors.New("上传回执未确认，请先检查账号总览")
		return
	}
	if allowCookie {
		cookie = receipt["owner_token"]
	}
}

func uploadJobError(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "任务已取消；不会自动重试"
	case errors.Is(err, context.DeadlineExceeded):
		return "任务已超时；不会自动重试"
	}
	return err.Error()
}

func (s *Server) findUploadJob(w http.ResponseWriter, r *http.Request) *uploadJob {
	m := &s.uploadJobs
	m.mu.Lock()
	j := m.jobs[r.PathValue("id")]
	m.mu.Unlock()
	if j == nil || !keyOK(j.key, bearer(r)) {
		http.Error(w, "上传任务不存在或已过期", http.StatusNotFound)
		return nil
	}
	return j
}

func (s *Server) handleUploadJobStatus(w http.ResponseWriter, r *http.Request) {
	j := s.findUploadJob(w, r)
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.cookie != "" {
		cur, had := s.ownerFromCookie(r)
		if !had || cur.Token == j.cookie {
			s.setOwnerCookie(w, r, j.cookie)
		}
	}
	writeJSON(w, http.StatusOK, j.viewLocked())
}

func (s *Server) handleUploadJobCancel(w http.ResponseWriter, r *http.Request) {
	j := s.findUploadJob(w, r)
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.committed || j.state == "completed" {
		http.Error(w, "凭据已经入库，取消不能撤销，请查看账号总览", http.StatusConflict)
		return
	}
	j.cancel()
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

// ShutdownUploads cancels in-flight imports before the Store closes.
func (s *Server) ShutdownUploads(ctx context.Context) error {
	m := &s.uploadJobs
	m.mu.Lock()
	m.stopping = true
	for _, job := range m.jobs {
		job.cancel()
	}
	m.mu.Unlock()
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
