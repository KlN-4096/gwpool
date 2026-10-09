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
	"strconv"
	"strings"
	"sync"
	"time"

	"gwpool/internal/mint"
	"gwpool/internal/oauth"
	"gwpool/internal/reauth"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

const (
	uploadJobTTL        = 10 * time.Minute
	uploadInputTTL      = 5 * time.Minute
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
	challenge *reauth.Challenge
	promptID  uint64
	submitted bool
	answer    chan string
	receipt   map[string]string
	cookie    string
	committed bool
	versions  map[string]accountVersion
}

type uploadJobView struct {
	ID        string            `json:"id"`
	State     string            `json:"state"`
	Stage     string            `json:"stage"`
	Stages    []string          `json:"stages"`
	Message   string            `json:"message,omitempty"`
	Expires   time.Time         `json:"expires"`
	Challenge *reauth.Challenge `json:"challenge,omitempty"`
	PromptID  uint64            `json:"prompt_id,omitempty"`
	Submitted bool              `json:"submitted"`
	Receipt   map[string]string `json:"receipt,omitempty"`
}

func (j *uploadJob) viewLocked() uploadJobView {
	return uploadJobView{ID: j.id, State: j.state, Stage: j.stage, Stages: j.stages, Message: j.message,
		Expires: j.expires, Challenge: j.challenge, PromptID: j.promptID, Submitted: j.submitted, Receipt: j.receipt}
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

func (j *uploadJob) prompt(ctx context.Context, challenge reauth.Challenge) (string, error) {
	j.mu.Lock()
	j.state, j.challenge = "waiting", &challenge
	j.promptID++
	j.submitted = false
	j.expires = time.Now().Add(uploadInputTTL)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(j.expires) {
		j.expires = deadline
	}
	deadline := j.expires
	j.mu.Unlock()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	var answer string
	var err error
	select {
	case answer = <-j.answer:
	case <-ctx.Done():
		err = ctx.Err()
	case <-timer.C:
		err = context.DeadlineExceeded
	}
	j.mu.Lock()
	j.state, j.challenge, j.submitted = "running", nil, false
	j.expires, _ = ctx.Deadline()
	j.mu.Unlock()
	return answer, err
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
	mode := form.Get("mode")
	if mode != "login" && mode != "credentials" {
		http.Error(w, "请选择上传方式", http.StatusBadRequest)
		return
	}
	// Freeze attribution before the async task starts. A later cookie change or
	// another tab cannot reassign an in-flight login.
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
	var input reauth.Input
	if mode == "login" {
		input = reauth.Input{Email: form.Get("email"), Password: form.Get("password"), TOTP: form.Get("totp")}
		if err := input.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else if len(form.Get("creds")) > 64<<10 || strings.TrimSpace(form.Get("creds")) == "" {
		http.Error(w, "单份凭据为空或超过 64 KB", http.StatusBadRequest)
		return
	}
	form.Del("email")
	form.Del("password")
	form.Del("totp")
	// Check permission before spending any upstream login or RT request.
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
		state: "running", stage: "queued", stages: []string{"queued"}, answer: make(chan string, 1), versions: make(map[string]accountVersion)}
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
		running := job.state == "running" || job.state == "waiting"
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
		s.runUploadJob(j, input, mode, form, headers, allowCookie)
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

func (s *Server) runUploadJob(j *uploadJob, input reauth.Input, mode string, form url.Values, headers http.Header, allowCookie bool) {
	ctx := context.WithValue(j.ctx, uploadJobContextKey{}, j)
	var jobErr error
	var receipt map[string]string
	var cookie string
	defer func() {
		input = reauth.Input{}
		for key := range form {
			delete(form, key)
		}
		j.mu.Lock()
		defer j.mu.Unlock()
		j.versions, j.challenge = nil, nil
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
	if mode == "login" {
		var tokens oauth.Tokens
		hooks := reauth.Hooks{Progress: j.progress, Prompt: j.prompt}
		if s.opt.Login != nil {
			tokens, jobErr = s.opt.Login(ctx, input, hooks)
		} else {
			transport, err := mint.LoginTransport(s.opt.Live.Get())
			if err != nil {
				jobErr = &reauth.Error{Code: "no_dynamic_proxy"}
				return
			}
			client, err := reauth.New(transport)
			if err != nil {
				jobErr = &reauth.Error{Code: "session_failed"}
				return
			}
			tokens, jobErr = client.Login(ctx, input, hooks)
		}
		input = reauth.Input{}
		if jobErr != nil {
			return
		}
		raw, err := json.Marshal(tokens)
		if err != nil {
			jobErr = errors.New("凭据编码失败")
			return
		}
		form.Set("creds", string(raw))
		form.Set("kind", "auto")
	}
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
		// Existing upload responses are already sanitized. Never use upstream
		// login bodies as errors. Preserve uncertainty for storage failures.
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
		return "任务或验证码等待已超时；不会自动重试"
	}
	var login *reauth.Error
	if errors.As(err, &login) {
		messages := map[string]string{
			"no_dynamic_proxy":       "没有可用的动态登录出口",
			"browser_challenge":      "上游要求人机验证，已停止；未绕过或自动重试",
			"unsupported_challenge":  "上游要求手机、人机或其他不支持的验证，已停止",
			"unsupported_mfa":        "上游要求的验证器类型不受支持",
			"network":                "登录出口或上游连接失败",
			"network_timeout":        "登录出口或上游连接超时",
			"rate_limited":           "上游限流，已停止；请勿连续重试",
			"login_rejected":         "上游拒绝登录，请检查账号信息",
			"login_forbidden":        "上游禁止此登录请求，已停止",
			"upstream_rejected":      "上游拒绝当前登录步骤",
			"invalid_callback":       "授权回调校验失败，未交换凭据",
			"unsupported_redirect":   "上游转向不支持的登录地址，已停止",
			"invalid_token_identity": "返回凭据的身份或期限无效，未入库",
			"identity_mismatch":      "返回凭据与提交邮箱不一致，未入库",
			"incomplete_tokens":      "上游没有返回完整的 AT 和 RT，未入库",
			"token_exchange_failed":  "授权码换取 RT 失败",
		}
		message := messages[login.Code]
		if message == "" {
			message = "登录流程未能完成，已停止；不会自动重试"
		}
		if login.Status != 0 {
			message += "（上游 HTTP " + strconv.Itoa(login.Status) + "）"
		}
		return message
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

func (s *Server) handleUploadJobInput(w http.ResponseWriter, r *http.Request) {
	j := s.findUploadJob(w, r)
	if j == nil {
		return
	}
	var answer struct {
		PromptID uint64 `json:"prompt_id"`
		Value    string `json:"value"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&answer); err != nil {
		http.Error(w, "补充信息格式错误", http.StatusBadRequest)
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.state != "waiting" || j.challenge == nil || j.promptID != answer.PromptID || j.submitted ||
		!time.Now().Before(j.expires) || j.ctx.Err() != nil {
		http.Error(w, "此验证步骤已提交、取消或过期，请刷新任务状态", http.StatusConflict)
		return
	}
	value := strings.TrimSpace(answer.Value)
	valid := false
	if j.challenge.Kind == "email_code" || j.challenge.Kind == "totp_code" {
		valid = len(value) == 6 && strings.Trim(value, "0123456789") == ""
	} else {
		for _, choice := range j.challenge.Choices {
			valid = valid || value == choice.ID
		}
	}
	if !valid {
		http.Error(w, "请填写六位验证码或选择当前可用选项", http.StatusBadRequest)
		return
	}
	j.submitted = true
	j.answer <- value
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
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

// ShutdownUploads cancels network requests and OTP waits before the Store closes.
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
