package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"gwpool/internal/store"
	"gwpool/internal/types"
)

func managedKeyRig(t *testing.T) (*Server, http.Handler, *store.Store, *fakePool, string) {
	t.Helper()
	_, _, pool, fake, _ := rig(t, types.ModePool)
	dir := t.TempDir()
	data, err := json.Marshal(store.State{OwnerKeysMigrated: true, Accounts: fake.accounts, Owners: fake.owners,
		Gateways: fake.gws, Pairs: fake.pairs, Slots: fake.slots, Mints: fake.mints})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(pool, st, &fakeScanner{}, Options{ConsumerKey: "offline-root", AdminKey: "offline-admin"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, s.Handler(), st, pool, dir
}

func createManagedKeys(t *testing.T, st *store.Store, count, rpm, concurrency int) []types.CreatedAccessKey {
	t.Helper()
	keys, err := st.CreateAccessKeys(count, "离线测试", "仅测试", rpm, concurrency)
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestManagedKeysIndependentAuthorityAndSevenEndpointPolicy(t *testing.T) {
	s, h, st, p, _ := managedKeyRig(t)
	key := createManagedKeys(t, st, 1, 0, 0)[0]
	for _, path := range []string{"/admin/keys", "/admin/"} {
		if w := do(h, authed(httptest.NewRequest("GET", path, nil), key.Key)); w.Code != 401 {
			t.Fatal("consumer received admin", path, w.Code)
		}
	}
	for _, path := range []string{"/cookie", "/gateways"} {
		if w := do(h, authed(httptest.NewRequest("GET", path, nil), key.Key)); w.Code != 400 {
			t.Fatal("missing account accepted", path, w.Code)
		}
	}
	// Closed public switch and a hosted reference do not change the independent path.
	ref := st.Accounts()[0].Creds.AccountID
	w := do(h, authed(httptest.NewRequest("GET", "/cookie?account="+url.QueryEscape(ref), nil), key.Key))
	if w.Code != 200 || len(p.delivered) != 0 || len(p.anon) != 1 || !strings.Contains(p.anon[0], "authed=true") {
		t.Fatal("independent key gained credentials or public gate", w.Code, p.anon, p.delivered)
	}
	for _, path := range []string{"/release", "/pair/renew"} {
		w := do(h, authed(httptest.NewRequest("POST", path+"?account="+url.QueryEscape(ref), strings.NewReader(`{}`)), key.Key))
		if w.Code != 409 {
			t.Fatal("external ownership guard missing", path, w.Code)
		}
	}
	if err := st.UpdateAccessKeys([]string{key.Record.ID}, types.AccessKeyPatch{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	for _, ep := range publicEndpoints {
		w := do(h, authed(httptest.NewRequest(ep.method, ep.path, nil), key.Key))
		if w.Code != 401 {
			t.Fatal("disabled key accepted", ep, w.Code)
		}
	}
	before, _ := s.keyMetrics.snapshot(time.Now())
	w = do(h, authed(httptest.NewRequest("GET", "/gateways?account=x", nil), key.Key+"x"))
	after, _ := s.keyMetrics.snapshot(time.Now())
	if w.Code != 401 || before[key.Record.ID].Usage.Totals.Requests != after[key.Record.ID].Usage.Totals.Requests {
		t.Fatal("invalid digest falsely attributed")
	}
	if err := st.UpdateAccessKeys([]string{key.Record.ID}, types.AccessKeyPatch{Action: "enable"}); err != nil {
		t.Fatal(err)
	}
	if w = do(h, authed(httptest.NewRequest("GET", "/gateways?account=x", nil), key.Key)); w.Code != 200 {
		t.Fatal("reenable failed", w.Code)
	}
}

func TestManagedKeysUploadAndLegacyExemptAndPolicyImmediate(t *testing.T) {
	s, h, st, _, _ := managedKeyRig(t)
	keys := []struct{ id, secret string }{
		{types.AccessKeyID(types.AccessKeyUpload, 1), s.OwnerKeyFor(st.Owners()[0])},
		{types.AccessKeyID(types.AccessKeyLegacy, 1), s.ConsumerKeyFor(st.Accounts()[0])},
	}
	for _, key := range keys {
		for i := 0; i < 5; i++ {
			if w := do(h, authed(httptest.NewRequest("GET", "/gateways", nil), key.secret)); w.Code != 200 {
				t.Fatal("automatic key throttled", w.Code)
			}
		}
		one := 1
		if err := st.UpdateAccessKeys([]string{key.id}, types.AccessKeyPatch{Action: "edit", RPM: &one}); err == nil {
			t.Fatal("automatic limits accepted")
		}
		if err := st.UpdateAccessKeys([]string{key.id}, types.AccessKeyPatch{Action: "disable"}); err != nil {
			t.Fatal(err)
		}
		if w := do(h, authed(httptest.NewRequest("GET", "/gateways", nil), key.secret)); w.Code != 401 {
			t.Fatal("automatic policy ignored")
		}
	}
	// Even accidental fields on legacy metadata cannot activate admission.
	now := time.Now()
	for _, source := range []string{types.AccessKeyUpload, types.AccessKeyLegacy} {
		key := types.AccessKey{ID: source + ":999", Source: source, RPM: 1, Concurrency: 1}
		for i := 0; i < 5; i++ {
			if admitted, _ := s.keyMetrics.begin(key, true, now); !admitted {
				t.Fatal("source exemption broken")
			}
		}
	}
}

func TestManagedKeysReportsRemainPerKeyAndPerAccount(t *testing.T) {
	s, h, st, _, _ := managedKeyRig(t)
	keys := createManagedKeys(t, st, 2, 0, 0)
	event := types.ContactObservation{ID: strings.Repeat("1", 64), AccountTag: strings.Repeat("2", 64),
		Gateway: "unified-142", Model: "astra", Criterion: "state-echo-v1", Source: "foreground", First: "repeat", At: time.Now(), Outcome: "full"}
	raw, _ := json.Marshal(event)
	for _, key := range keys {
		if w := do(h, authed(httptest.NewRequest("POST", "/contact/report", strings.NewReader(string(raw))), key.Key)); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	seen := map[int64]bool{}
	for _, report := range st.ContactReports() {
		if report.ReporterID >= -int64(types.AccessKeyMaxID) || report.ReporterID == publicReporterID || seen[report.ReporterID] {
			t.Fatal("reporter collision")
		}
		seen[report.ReporterID] = true
	}
	if len(seen) != 2 {
		t.Fatal("lost independent reports")
	}
	for _, key := range keys {
		r := authed(httptest.NewRequest("GET", "/gateways?account=x", nil), key.Key)
		r.Header.Set("X-Gwpool-Account-Tag", event.AccountTag)
		if stats := s.contactStats(r, time.Now())["unified-142"]; len(stats) != 1 || stats[0].Full != 1 {
			t.Fatal("key histories merged", stats)
		}
		r.Header.Set("X-Gwpool-Account-Tag", strings.Repeat("3", 64))
		if len(s.contactStats(r, time.Now())) != 0 {
			t.Fatal("account histories merged")
		}
	}
}

func TestManagedKeyLimitsIndependentWindowConcurrencyAndRelease(t *testing.T) {
	s, h, st, _, _ := managedKeyRig(t)
	keys := createManagedKeys(t, st, 2, 1, 1)
	request := func(secret string) *http.Request {
		return authed(httptest.NewRequest("GET", "/gateways?account=x", nil), secret)
	}
	for _, key := range keys {
		if w := do(h, request(key.Key)); w.Code != 200 {
			t.Fatal("independent initial admission", w.Code)
		}
		w := do(h, request(key.Key))
		if w.Code != 503 || decodeFail(t, w).Error.Code != codeRateLimited || w.Header().Get("Retry-After") == "" {
			t.Fatal("missing limit response", w.Code)
		}
	}
	now := time.Now()
	key := types.AccessKey{ID: "admin:999", Source: types.AccessKeyAdmin, RPM: 2, Concurrency: 1}
	if admitted, _ := s.keyMetrics.begin(key, true, now); !admitted {
		t.Fatal("first")
	}
	if admitted, _ := s.keyMetrics.begin(key, true, now); admitted {
		t.Fatal("concurrency")
	}
	s.keyMetrics.finish(key.ID, request(""), now, 200, 0, 0, true)
	if admitted, _ := s.keyMetrics.begin(key, true, now.Add(time.Second)); !admitted {
		t.Fatal("slot not released")
	}
	s.keyMetrics.finish(key.ID, request(""), now, 200, 0, 0, true)
	if admitted, _ := s.keyMetrics.begin(key, true, now.Add(2*time.Second)); admitted {
		t.Fatal("minute budget")
	}
	if admitted, _ := s.keyMetrics.begin(key, true, now.Add(time.Minute)); !admitted {
		t.Fatal("window did not expire")
	}
	// Concurrent admission through the real middleware, without fakePool mutations.
	unlimited := createManagedKeys(t, st, 1, 0, 1)[0]
	entered, release := make(chan struct{}), make(chan struct{})
	handler := s.consumerAPI(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release; w.WriteHeader(204) })
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); do(handler, request(unlimited.Key)) }()
	<-entered
	if w := do(handler, request(unlimited.Key)); w.Code != 503 {
		t.Fatal("parallel request admitted", w.Code)
	}
	close(release)
	wg.Wait()
	if w := do(s.consumerAPI(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }), request(unlimited.Key)); w.Code != 204 {
		t.Fatal("inflight slot leaked", w.Code)
	}
}

func TestManagedKeyMetricsTicketsErrorsAndPersistence(t *testing.T) {
	s, h, st, p, dir := managedKeyRig(t)
	key := createManagedKeys(t, st, 1, 0, 0)[0]
	for _, path := range []string{"/cookie?account=external&count=3", "/cookie?account=external&count=bad", "/gateways?account=external"} {
		r := authed(httptest.NewRequest("GET", path, nil), key.Key)
		r.RemoteAddr = "127.0.0.1:5000"
		r.Header.Set("X-Forwarded-For", "203.0.113.8")
		do(h, r)
	}
	if p.calls != 3 {
		t.Fatal("batch fixture failed", p.calls)
	}
	if err := st.UpdateAccessKeys([]string{key.Record.ID}, types.AccessKeyPatch{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	do(h, authed(httptest.NewRequest("GET", "/gateways?account=external", nil), key.Key))
	values, _ := s.keyMetrics.snapshot(time.Now())
	v := values[key.Record.ID]
	if v.Usage.Totals.Requests != 4 || v.Usage.Totals.Tickets != 3 || v.Usage.Totals.OK != 2 || v.Usage.Totals.ClientErrors != 2 || v.Usage.Totals.Denied != 1 || v.Active != 0 || v.RPM != 4 {
		t.Fatal("incorrect counters", v)
	}
	if len(v.Usage.AccountHashes) != 1 || len(v.Usage.Hours) != 1 || v.Usage.RecentIPs[len(v.Usage.RecentIPs)-1] != "127.0.0.1" || strings.Contains(strings.Join(v.Usage.RecentIPs, ","), "203.0.113.8") {
		t.Fatal("unsafe metadata", v.Usage)
	}
	if err := s.FlushKeyMetrics(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	next, err := New(p, reopened, &fakeScanner{}, s.opt)
	if err != nil {
		t.Fatal(err)
	}
	persisted, _ := next.keyMetrics.snapshot(time.Now())
	if persisted[key.Record.ID].Usage.Totals.Requests != 4 || persisted[key.Record.ID].RPM != 0 {
		t.Fatal("restart lost totals or retained live rate")
	}
}

func keyAdminForm(h http.Handler, values url.Values) *httptest.ResponseRecorder {
	r := authed(httptest.NewRequest("POST", "http://pool.test/admin/keys", strings.NewReader(values.Encode())), "offline-admin")
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Origin", "http://pool.test")
	return do(h, r)
}

func TestManagedKeysAdminCreationExportReplayFiltersAndCSRF(t *testing.T) {
	s, h, st, _, _ := managedKeyRig(t)
	if w := do(h, httptest.NewRequest("GET", "/admin/keys", nil)); w.Code != 401 {
		t.Fatal("public admin page")
	}
	page := do(h, authed(httptest.NewRequest("GET", "/admin/keys", nil), "offline-admin"))
	re := regexp.MustCompile(`name="creation_token" value="([^"]+)"`)
	match := re.FindStringSubmatch(page.Body.String())
	if len(match) != 2 {
		t.Fatal("missing creation form")
	}
	form := url.Values{"action": {"create"}, "creation_token": {match[1]}, "count": {"3"}, "label": {"<script>alert(1)</script>"}, "rpm": {"2"}, "concurrency": {"1"}}
	r := authed(httptest.NewRequest("POST", "http://pool.test/admin/keys", strings.NewReader(form.Encode())), "offline-admin")
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Origin", "https://evil.test")
	if w := do(h, r); w.Code != 403 {
		t.Fatal("cross-site generation accepted")
	}
	created := keyAdminForm(h, form)
	if created.Code != 200 {
		t.Fatal(created.Code, created.Body.String())
	}
	exportRE := regexp.MustCompile(`(?s)id="created-keys"[^>]*>(.*?)</textarea>`)
	result := exportRE.FindStringSubmatch(created.Body.String())
	if len(result) != 2 {
		t.Fatal("export missing")
	}
	secrets := strings.Split(result[1], "\n")
	if len(secrets) != 3 {
		t.Fatal("export not one key per line", len(secrets))
	}
	for _, secret := range secrets {
		if _, ok := st.ResolveIssuedAccessKey(secret); !ok {
			t.Fatal("extra text in export")
		}
	}
	if w := keyAdminForm(h, form); w.Code != 409 || len(st.AccessKeys()) != 3 {
		t.Fatal("replayed generation duplicated keys", w.Code)
	}
	variant := strings.Split(form.Get("creation_token"), ".")
	variant[2] = strings.ToUpper(variant[2])
	form.Set("creation_token", strings.Join(variant, "."))
	if w := keyAdminForm(h, form); w.Code != 409 || len(st.AccessKeys()) != 3 {
		t.Fatal("equivalent signature bypassed replay protection", w.Code)
	}
	list := do(h, authed(httptest.NewRequest("GET", "/admin/keys?source=admin&q=script&sort=rpm", nil), "offline-admin"))
	if strings.Contains(list.Body.String(), "<script>alert(1)</script>") {
		t.Fatal("stored XSS")
	}
	for _, secret := range secrets {
		if strings.Contains(list.Body.String(), secret) {
			t.Fatal("secret re-exposed")
		}
	}
	if !strings.Contains(list.Body.String(), "筛选命中 3 / 7") {
		t.Fatal("source filter missing")
	}
	id := st.AccessKeys()[0].ID
	detail := do(h, authed(httptest.NewRequest("GET", "/admin/keys?key="+url.QueryEscape(id), nil), "offline-admin"))
	if detail.Code != 200 || !strings.Contains(detail.Body.String(), "P95") {
		t.Fatal("detail failed")
	}
	if w := keyAdminForm(h, url.Values{"action": {"revoke"}, "ids": {id}}); w.Code != 400 {
		t.Fatal("revocation without confirmation")
	}
	if w := keyAdminForm(h, url.Values{"action": {"revoke"}, "ids": {id}, "confirm_revoke": {"yes"}}); w.Code != 303 {
		t.Fatal("confirmed revoke failed")
	}
	if w := keyAdminForm(h, url.Values{"action": {"enable"}, "ids": {id}}); w.Code != 409 {
		t.Fatal("revoked key reenabled")
	}
	if len(st.Accounts()) != 2 {
		t.Fatal("key revoke changed uploaded credentials")
	}
	if dir := os.Getenv("GWPOOL_KEYS_PREVIEW_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		for name, body := range map[string]string{"keys.html": list.Body.String(), "created.html": created.Body.String(), "detail.html": detail.Body.String()} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	_ = s
}

type failedKeyWriter struct{ header http.Header }

func (w *failedKeyWriter) Header() http.Header { return w.header }
func (w *failedKeyWriter) WriteHeader(int)     {}
func (w *failedKeyWriter) Write([]byte) (int, error) {
	return 0, errors.New("offline disconnected writer")
}

func TestManagedKeyMetricsWriteFailureDoesNotCountTickets(t *testing.T) {
	s, h, st, _, _ := managedKeyRig(t)
	key := createManagedKeys(t, st, 1, 0, 0)[0]
	h.ServeHTTP(&failedKeyWriter{header: make(http.Header)}, authed(httptest.NewRequest("GET", "/cookie?account=x&count=3", nil), key.Key))
	metrics, _ := s.keyMetrics.snapshot(time.Now())
	if got := metrics[key.Record.ID].Usage.Totals; got.Requests != 1 || got.Tickets != 0 {
		t.Fatal("failed response counted as delivered", got)
	}
}

func TestManagedKeyMetricsCapacityDoesNotChangeAdmission(t *testing.T) {
	s, _, _, _, _ := managedKeyRig(t)
	// Shared empty entries avoid allocating 100 MB of irrelevant test buckets.
	empty := &keyMetricEntry{}
	for i := 0; i < keyMetricsCapacity; i++ {
		s.keyMetrics.entries[fmt.Sprintf("upload:%d", i+100)] = empty
	}
	now := time.Now()
	unlimited := types.AccessKey{ID: "admin:100", Source: types.AccessKeyAdmin}
	limited := types.AccessKey{ID: "admin:101", Source: types.AccessKeyAdmin, RPM: 2, Concurrency: 1}
	if ok, _ := s.keyMetrics.begin(unlimited, true, now); !ok {
		t.Fatal("telemetry cap rejected unlimited key")
	}
	if ok, _ := s.keyMetrics.begin(limited, true, now); !ok {
		t.Fatal("telemetry cap rejected initial admission")
	}
	if ok, _ := s.keyMetrics.begin(limited, true, now); ok {
		t.Fatal("telemetry cap bypassed concurrency")
	}
	r := httptest.NewRequest("GET", "/gateways", nil)
	s.keyMetrics.finish(limited.ID, r, now, 200, 0, 0, true)
	if ok, _ := s.keyMetrics.begin(limited, true, now); !ok {
		t.Fatal("telemetry cap leaked active slot")
	}
	s.keyMetrics.finish(limited.ID, r, now, 200, 0, 0, true)
	if ok, _ := s.keyMetrics.begin(limited, true, now); ok {
		t.Fatal("telemetry cap bypassed rpm")
	}
	if !s.keyMetrics.capacityExceeded {
		t.Fatal("missing visible telemetry warning")
	}
}

type keyFailingMetricsStore struct {
	*store.Store
	fail             bool
	entered, release chan struct{}
}

func (st *keyFailingMetricsStore) SaveAccessKeyUsage(values map[string]types.KeyUsage) error {
	if st.fail {
		return errors.New("offline save failure")
	}
	if st.entered != nil {
		close(st.entered)
		<-st.release
	}
	return st.Store.SaveAccessKeyUsage(values)
}

func TestManagedKeyMetricsFailedAndConcurrentFlushRetainsNewData(t *testing.T) {
	s, _, st, _, _ := managedKeyRig(t)
	key := createManagedKeys(t, st, 1, 0, 0)[0]
	wrapped := &keyFailingMetricsStore{Store: st, fail: true}
	s.st = wrapped
	h := s.Handler()
	request := func() { do(h, authed(httptest.NewRequest("GET", "/gateways?account=x", nil), key.Key)) }
	request()
	if err := s.FlushKeyMetrics(); err == nil {
		t.Fatal("failed save hidden")
	}
	if _, failed := s.keyMetrics.snapshot(time.Now()); !failed {
		t.Fatal("save error not visible")
	}
	wrapped.fail = false
	wrapped.entered, wrapped.release = make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- s.FlushKeyMetrics() }()
	<-wrapped.entered
	request() // A request completing during disk I/O must remain dirty.
	close(wrapped.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	wrapped.entered = nil
	if err := s.FlushKeyMetrics(); err != nil {
		t.Fatal(err)
	}
	if got := st.AccessKeyUsage()[key.Record.ID].Totals.Requests; got != 2 {
		t.Fatal("new counters cleared by old flush", got)
	}
	if _, failed := s.keyMetrics.snapshot(time.Now()); failed {
		t.Fatal("save error did not clear after retry")
	}
}

func TestManagedKeysServerPaginationSelectionAndUploadEdit(t *testing.T) {
	_, h, st, _, _ := managedKeyRig(t)
	createManagedKeys(t, st, 65, 0, 0)
	body := do(h, authed(httptest.NewRequest("GET", "/admin/keys?source=admin&page=2", nil), "offline-admin")).Body.String()
	if strings.Count(body, `name="ids" value=`) != 15 || !strings.Contains(body, "第 2 / 2 页") {
		t.Fatal("server pagination invalid")
	}
	if w := keyAdminForm(h, url.Values{"action": {"disable"}, "ids": {"admin:1", "admin:99999"}}); w.Code != 409 {
		t.Fatal("mixed selection accepted")
	}
	if key, _ := st.AccessKey("admin:1"); !key.Usable() {
		t.Fatal("partial batch update")
	}
	if w := keyAdminForm(h, url.Values{"action": {"edit"}, "ids": {"upload:1"}, "label": {"上传来源备注"}, "note": {"safe"}, "rpm": {"1"}}); w.Code != 409 {
		t.Fatal("upload limit form accepted")
	}
	if w := keyAdminForm(h, url.Values{"action": {"edit"}, "ids": {"upload:1"}, "label": {"上传来源备注"}, "note": {"safe"}}); w.Code != 303 {
		t.Fatal("upload metadata edit failed", w.Code)
	}
}

func TestManagedKeyWorksWithNoUploadedCredentialsOrRootConsumerKey(t *testing.T) {
	_, _, _, pool, _ := managedKeyRig(t)
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	}()
	key := createManagedKeys(t, st, 1, 0, 0)[0]
	s, err := New(pool, st, &fakeScanner{}, Options{AdminKey: "offline-admin"})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/cookie?account=external", "/gateways?account=external"} {
		if w := do(s.Handler(), authed(httptest.NewRequest("GET", path, nil), key.Key)); w.Code != 200 {
			t.Fatal("independent key needs uploaded credentials", path, w.Code)
		}
	}
	if len(st.Accounts()) != 0 || len(st.Owners()) != 0 {
		t.Fatal("created fake owner/credential")
	}
}
