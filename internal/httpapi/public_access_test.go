package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/sched"
	"gwpool/internal/types"
)

var publicEndpoints = []struct{ method, path string }{
	{"GET", "/cookie?account=external&gateway=unified-142"},
	{"GET", "/gateways?account=external"},
	{"POST", "/cooldown/report"},
	{"POST", "/cooldown/reports"},
	{"POST", "/contact/report"},
}

func TestPublicAccessRequiresValidKeyWhenOpen(t *testing.T) {
	_, h, p, _, _ := rig(t, types.ModePool)
	p.public = sched.PublicState{Enabled: true, Open: true}
	for _, endpoint := range publicEndpoints {
		for _, key := range []string{"", "incorrect-key"} {
			w := do(h, authed(httptest.NewRequest(endpoint.method, endpoint.path, nil), key))
			if w.Code != http.StatusUnauthorized {
				t.Errorf("%s with invalid/missing key: got %d, want 401", endpoint.path, w.Code)
			}
		}
	}
}

func TestPublicAccessSwitchThresholdAndPermission(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	key := srv.PublicKey()
	if key == "" || key == srv.opt.ConsumerKey || key == srv.opt.AdminKey || key == srv.ConsumerKeyFor(st.accounts[0]) {
		t.Fatal("public credential not independent")
	}
	if key != srv.PublicKey() {
		t.Fatal("public credential not stable")
	}
	for _, endpoint := range publicEndpoints {
		for _, state := range []sched.PublicState{{}, {Enabled: true}} {
			p.public = state
			w := do(h, authed(httptest.NewRequest(endpoint.method, endpoint.path, nil), key))
			want := http.StatusUnauthorized
			if state.Enabled {
				want = http.StatusServiceUnavailable
			}
			if w.Code != want {
				t.Fatalf("%s gate: got %d want %d", endpoint.path, w.Code, want)
			}
		}
	}
	p.public = sched.PublicState{Enabled: true, Open: true}
	if w := do(h, authed(httptest.NewRequest("GET", "/admin/", nil), key)); w.Code != http.StatusUnauthorized {
		t.Fatal("public key granted admin access")
	}
	for _, path := range []string{"/cookie", "/gateways"} {
		if w := do(h, authed(httptest.NewRequest("GET", path, nil), key)); w.Code != http.StatusBadRequest {
			t.Fatalf("missing account accepted: %s %d", path, w.Code)
		}
	}
	// Claiming a hosted account's ID must still use the untrusted external path.
	ref := st.accounts[0].Creds.AccountID
	if w := do(h, authed(httptest.NewRequest("GET", "/cookie?gateway=unified-142&account="+url.QueryEscape(ref), nil), key)); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if len(p.delivered) != 0 || len(p.anon) != 1 {
		t.Fatal("public key borrowed hosted credentials")
	}
	for _, path := range []string{"/release", "/pair/renew"} {
		w := do(h, authed(httptest.NewRequest("POST", path+"?account="+url.QueryEscape(ref), strings.NewReader(`{}`)), key))
		if w.Code != http.StatusUpgradeRequired {
			t.Fatalf("retired endpoint remained active: %s %d", path, w.Code)
		}
	}
}

func TestPublicAccessReportsAndAccountIsolation(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	p.public = sched.PublicState{Enabled: true, Open: true}
	// Keep this test about protocol, not pacing.
	cfg := config.Defaults()
	cfg.PublicWhenSurplus.MaxConcurrent = 50
	srv.opt.Live = config.NewLive("", cfg)
	for i, tag := range []string{strings.Repeat("a", 64), strings.Repeat("b", 64)} {
		event := strings.Repeat(string(rune('1'+i)), 64)
		c := types.ContactObservation{ID: event, AccountTag: tag, Gateway: "unified-142",
			Model: "gpt-6-luna", Criterion: "state-echo-v1", Source: "foreground",
			First: "repeat", At: time.Now().UTC(), Outcome: "full"}
		cd := cooldownObservation{ID: event, AccountTag: tag, Gateway: "unified-142",
			WindowSeconds: 3600, ElapsedSeconds: 3600, Result: "full"}
		for _, request := range []struct {
			path string
			body any
		}{
			{"/contact/report", c},
			{"/cooldown/report", cd},
			{"/cooldown/reports", map[string]any{"reports": []cooldownObservation{cd}}},
		} {
			raw, err := json.Marshal(request.body)
			if err != nil {
				t.Fatal(err)
			}
			w := do(h, authed(httptest.NewRequest("POST", request.path, strings.NewReader(string(raw))), srv.PublicKey()))
			if w.Code != 200 {
				t.Fatalf("%s %d %s", request.path, w.Code, w.Body.String())
			}
		}
		r := authed(httptest.NewRequest("GET", "/gateways?account=external", nil), srv.PublicKey())
		r.Header.Set("X-Gwpool-Account-Tag", tag)
		if w := do(h, r); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		stats := srv.contactStats(r, time.Now())
		if len(stats["unified-142"]) != 1 || stats["unified-142"][0].Full != 1 {
			t.Fatal("account tags were combined", stats)
		}
	}
	if len(st.contacts) != 2 || len(st.CooldownReports()) != 2 {
		t.Fatal("missing reports or duplicate counted")
	}
	for _, r := range st.contacts {
		if r.ReporterID != publicReporterID {
			t.Fatal("public caller counted as an owner")
		}
	}
	r := authed(httptest.NewRequest("GET", "/", nil), srv.ConsumerKeyFor(st.accounts[0]))
	r.Header.Set("X-Gwpool-Account-Tag", strings.Repeat("a", 64))
	if len(srv.contactStats(r, time.Now())) != 0 {
		t.Fatal("public reports leaked into ordinary reporter's personal stats")
	}
}

func TestPublicAccessBudgetSharedAndOrdinaryKeyBypass(t *testing.T) {
	srv, h, p, st, _ := rig(t, types.ModePool)
	p.public = sched.PublicState{Enabled: true, Open: true}
	cfg := config.Defaults()
	cfg.PublicWhenSurplus.RequestsPerMinute = 1
	cfg.PublicWhenSurplus.QueueWaitMS = 0
	srv.opt.Live = config.NewLive("", cfg)
	for i, ref := range []string{"external-a", "external-b"} {
		w := do(h, authed(httptest.NewRequest("GET", "/gateways?account="+ref, nil), srv.PublicKey()))
		if i == 0 && w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		if i == 1 {
			fail := decodeFail(t, w)
			if w.Code != 503 || fail.Error.Code != codeRateLimited || fail.Error.RetryAfterSeconds < 1 || w.Header().Get("Retry-After") == "" {
				t.Fatal("no structured shared-budget rejection", w.Code, fail)
			}
		}
	}
	p.public = sched.PublicState{}
	w := do(h, authed(httptest.NewRequest("GET", "/cookie?account=member&gateway=unified-142", nil), srv.ConsumerKeyFor(st.accounts[0])))
	if w.Code != 200 {
		t.Fatal("ordinary key affected by public gate/budget", w.Code)
	}
}

func TestPublicAccessPageKeyOnlyWhenEnabled(t *testing.T) {
	srv, h, p, _, _ := rig(t, types.ModePool)
	cfg := config.Defaults()
	cfg.PublicBaseURL = "https://pool.example.invalid"
	srv.opt.Live = config.NewLive("", cfg)
	for _, state := range []sched.PublicState{{}, {Enabled: true}, {Enabled: true, Open: true}} {
		p.public = state
		body := do(h, httptest.NewRequest("GET", "/", nil)).Body.String()
		if strings.Contains(body, srv.PublicKey()) != state.Enabled {
			t.Fatal("key visibility differs from switch")
		}
		if state.Enabled {
			start := strings.Index(body, "<h2>公共 Key 接入</h2>")
			if start < 0 {
				t.Fatal("public connection card missing")
			}
			card := body[start:]
			if end := strings.Index(card, "</section>"); end >= 0 {
				card = card[:end]
			}
			if strings.Contains(strings.ToLower(card), "sub2") || strings.Contains(card, "二开") {
				t.Fatal("public connection card names a specific client")
			}
			for _, want := range []string{cfg.PublicBaseURL, "720 次/分钟", "并发 10", "排队 50", "1000 毫秒", "data-copy=\"#public-key\""} {
				if !strings.Contains(body, want) {
					t.Error("missing public connection setting", want)
				}
			}
		}
		if strings.Contains(body, ">"+srv.opt.ConsumerKey+"<") || strings.Contains(body, ">"+srv.opt.AdminKey+"<") {
			t.Fatal("root key published")
		}
		if dir := os.Getenv("GWPOOL_PUBLIC_PREVIEW_DIR"); dir != "" {
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			name := "public-off.html"
			if state.Enabled {
				name = "public-closed.html"
				if state.Open {
					name = "public-open.html"
				}
			}
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestPublicAccessAdminConfigSaveReloadAndValidation(t *testing.T) {
	srv, h, _, _, _ := rig(t, types.ModePool)
	path := filepath.Join(t.TempDir(), "config.json")
	raw := `{"consumer_key":"test-root-secret","proxies":[{"name":"test","kind":"novproxy","host":"proxy.example.invalid:1","user":"test","pass":"test","enabled":true}]}`
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	srv.opt.Live = config.NewLive(path, cfg)
	form := url.Values{"models": {"gpt-6-luna"}, "public_enabled": {"1"}, "min_accounts": {"3"},
		"public_requests_per_minute": {"600"}, "public_max_concurrent": {"8"},
		"public_max_queued": {"25"}, "public_queue_wait_ms": {"750"}}
	send := func() {
		r := authed(httptest.NewRequest("POST", "/admin/config", strings.NewReader(form.Encode())), "akey")
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := do(h, r)
		if w.Code != http.StatusSeeOther {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	send()
	want := config.PublicWhenSurplus{Enabled: true, MinAccounts: 3,
		RequestsPerMinute: 600, MaxConcurrent: 8, MaxQueued: 25, QueueWaitMS: 750}
	if got := srv.publicLimits(); got != want {
		t.Fatal("form did not hot reload", got)
	}
	disk, err := config.Load(path)
	if err != nil || disk.PublicWhenSurplus != want || disk.ConsumerKey.Reveal() != "test-root-secret" {
		t.Fatal("limits not persisted or root secret damaged", err)
	}
	for _, field := range []string{"public_requests_per_minute", "public_max_concurrent", "public_max_queued", "public_queue_wait_ms"} {
		old := form.Get(field)
		form.Set(field, "-1")
		send()
		if srv.publicLimits() != want {
			t.Fatal("invalid limit changed active settings", field)
		}
		form.Set(field, old)
	}
	// Older forms do not silently replace custom limits with defaults.
	for _, field := range []string{"public_requests_per_minute", "public_max_concurrent", "public_max_queued", "public_queue_wait_ms"} {
		form.Del(field)
	}
	send()
	if srv.publicLimits() != want {
		t.Fatal("omitted limits overwritten")
	}
}

func TestUploadRecommendsFreeAccounts(t *testing.T) {
	_, h, _, _, _ := rig(t, types.ModePool)
	for _, path := range []string{"/", "/u/" + ownerTok1} {
		w := do(h, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || strings.Count(w.Body.String(), "推荐上传 Free 账号。") != 1 {
			t.Fatalf("%s: missing or duplicated upload recommendation", path)
		}
	}
}

func TestPublicAdmissionRollingMinuteAndPacing(t *testing.T) {
	now := time.Now()
	limits := config.Defaults().PublicWhenSurplus
	a := publicAdmission{}
	if wait := a.retryLocked(now, limits); wait != 0 || a.tokens != 10 {
		t.Fatal("wrong initial burst", wait, a.tokens)
	}
	a.tokens = 0
	if wait := a.retryLocked(now, limits); wait < 83*time.Millisecond || wait > 84*time.Millisecond {
		t.Fatal("wrong 720/min pacing", wait)
	}
	limits.RequestsPerMinute = 2
	a.recent = []time.Time{now.Add(-time.Second), now}
	if wait := a.retryLocked(now, limits); wait != 59*time.Second {
		t.Fatal("rolling cap bypassed", wait)
	}
	limits.RequestsPerMinute = 1
	if wait := a.retryLocked(now, limits); wait != time.Minute {
		t.Fatal("lowered cap did not retain recent usage", wait)
	}
	if wait := a.retryLocked(now.Add(time.Minute), limits); wait != 0 || len(a.recent) != 0 {
		t.Fatal("minute boundary not released", wait)
	}
}

func waitPublicQueue(t *testing.T, a *publicAdmission, want int) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		a.mu.Lock()
		n := len(a.waiters)
		a.mu.Unlock()
		if n == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("public queue did not reach expected length", want)
}

func TestPublicAdmissionQueueFIFOOverflowCancelAndRelease(t *testing.T) {
	limits := config.Defaults().PublicWhenSurplus
	limits.MaxConcurrent, limits.MaxQueued = 1, 2
	limits.RequestsPerMinute = config.MaxPublicRequestsPerMinute
	current := func() config.PublicWhenSurplus { return limits }
	a := publicAdmission{}
	release, _, err := a.acquire(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelled := make(chan error, 1)
	go func() {
		done, _, err := a.acquire(ctx, current)
		if done != nil {
			done()
		}
		cancelled <- err
	}()
	waitPublicQueue(t, &a, 1)
	granted := make(chan func(), 1)
	errs := make(chan error, 1)
	go func() {
		done, _, err := a.acquire(context.Background(), current)
		granted <- done
		errs <- err
	}()
	waitPublicQueue(t, &a, 2)
	if done, retry, err := a.acquire(context.Background(), current); err == nil || done != nil || retry <= 0 {
		t.Fatal("queue overflow accepted")
	}
	cancel()
	if err := <-cancelled; !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled waiter admitted", err)
	}
	waitPublicQueue(t, &a, 1)
	select {
	case <-granted:
		t.Fatal("concurrency slot bypassed")
	default:
	}
	release()
	done := <-granted
	if err := <-errs; err != nil || done == nil {
		t.Fatal("queued request not admitted", err)
	}
	done()
	done() // release is idempotent.
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active != 0 || len(a.waiters) != 0 || len(a.recent) != 2 {
		t.Fatal("leaked queue, slot, or charged cancelled/overflow requests")
	}
}

func TestPublicAdmissionQueueTimeoutAndCancelledBeforeEntry(t *testing.T) {
	limits := config.Defaults().PublicWhenSurplus
	limits.MaxConcurrent, limits.QueueWaitMS = 1, 20
	a := publicAdmission{}
	current := func() config.PublicWhenSurplus { return limits }
	release, _, err := a.acquire(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if done, _, err := a.acquire(context.Background(), current); err == nil || done != nil {
		t.Fatal("queue timeout not enforced")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if done, _, err := a.acquire(ctx, current); !errors.Is(err, context.Canceled) || done != nil {
		t.Fatal("already cancelled request admitted")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.waiters) != 0 || len(a.recent) != 1 {
		t.Fatal("timed out requests remained queued or charged")
	}
}

func TestPublicAdmissionFIFOAndHotLimitChanges(t *testing.T) {
	limits := config.Defaults().PublicWhenSurplus
	limits.MaxConcurrent, limits.MaxQueued = 1, 2
	limits.RequestsPerMinute = config.MaxPublicRequestsPerMinute
	var live atomic.Pointer[config.PublicWhenSurplus]
	live.Store(&limits)
	current := func() config.PublicWhenSurplus { return *live.Load() }
	a := publicAdmission{}
	release, _, err := a.acquire(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	first, second := make(chan func(), 1), make(chan func(), 1)
	errs := make(chan error, 2)
	for i, ch := range []chan func(){first, second} {
		go func(out chan func()) {
			done, _, err := a.acquire(context.Background(), current)
			errs <- err
			out <- done
		}(ch)
		waitPublicQueue(t, &a, i+1)
	}
	release()
	done := <-first
	if err := <-errs; err != nil || done == nil {
		t.Fatal("first queued request failed", err)
	}
	defer done()
	select {
	case <-second:
		t.Fatal("second waiter overtook active first waiter")
	default:
	}
	// Hot disabling queue wait evicts the remaining waiter, not the in-flight request.
	next := limits
	next.QueueWaitMS = 0
	live.Store(&next)
	if other := <-second; other != nil {
		other()
		t.Fatal("waiter ignored new zero wait")
	}
	if err := <-errs; err == nil {
		t.Fatal("hot queue timeout was not surfaced")
	}
	done()
	waitPublicQueue(t, &a, 0)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active != 0 {
		t.Fatal("hot queue limit leaked active slots")
	}
}

func TestPublicAccessRechecksGateAfterQueue(t *testing.T) {
	srv, _, p, _, _ := rig(t, types.ModePool)
	p.public = sched.PublicState{Enabled: true, Open: true}
	cfg := config.Defaults()
	cfg.PublicWhenSurplus.MaxConcurrent = 1
	cfg.PublicWhenSurplus.RequestsPerMinute = config.MaxPublicRequestsPerMinute
	srv.opt.Live = config.NewLive("", cfg)
	release, _, err := srv.publicAdmission.acquire(context.Background(), srv.publicLimits)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var calls atomic.Int32
	h := srv.consumerAPI(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(200)
	})
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		result <- do(h, authed(httptest.NewRequest("GET", "/", nil), srv.PublicKey()))
	}()
	waitPublicQueue(t, &srv.publicAdmission, 1)
	// Only admission is running now; the release lock synchronizes this state change.
	p.public = sched.PublicState{}
	release()
	w := <-result
	if w.Code != http.StatusUnauthorized || calls.Load() != 0 {
		t.Fatal("queued caller outlived public gate", w.Code, calls.Load())
	}
}
