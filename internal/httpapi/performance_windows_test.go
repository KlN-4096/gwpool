//go:build windows

package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"gwpool/internal/config"
	"gwpool/internal/sched"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

const (
	perfWorkers           = 32
	perfMaxRounds         = 12 // do not benchmark by violating the member rate limit
	perfRequiredInventory = 1
)

// No real minter is constructed. Unexpected outbound calls hit the nil embedded
// interface and fail the test instead of acquiring a network client.
type offlinePerfMinter struct{ sched.Minter }

func (offlinePerfMinter) SupportsDynamicExit(types.Exit) bool { return true }
func (offlinePerfMinter) Rate(string) (map[string]int, int)   { return nil, 50 }

type perfIOCounters struct {
	ReadOperations, WriteOperations, OtherOperations uint64
	ReadBytes, WriteBytes, OtherBytes                uint64
}

type perfResources struct {
	CPU100ns uint64
	IO       perfIOCounters
}

func readPerfResources(t *testing.T) perfResources {
	t.Helper()
	kernel := syscall.NewLazyDLL("kernel32.dll")
	handle, err := syscall.GetCurrentProcess()
	if err != nil {
		t.Fatal(err)
	}
	var created, exited, kernelTime, userTime syscall.Filetime
	result, _, callErr := kernel.NewProc("GetProcessTimes").Call(uintptr(handle),
		uintptr(unsafe.Pointer(&created)), uintptr(unsafe.Pointer(&exited)),
		uintptr(unsafe.Pointer(&kernelTime)), uintptr(unsafe.Pointer(&userTime)))
	if result == 0 {
		t.Fatal(callErr)
	}
	count := func(v syscall.Filetime) uint64 { return uint64(v.HighDateTime)<<32 | uint64(v.LowDateTime) }
	out := perfResources{CPU100ns: count(kernelTime) + count(userTime)}
	result, _, callErr = kernel.NewProc("GetProcessIoCounters").Call(uintptr(handle), uintptr(unsafe.Pointer(&out.IO)))
	if result == 0 {
		t.Fatal(callErr)
	}
	return out
}

func perfShiftJWT(token string, delta time.Duration) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return token
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return token
	}
	var claims map[string]any
	if json.Unmarshal(b, &claims) != nil {
		return token
	}
	if exp, ok := claims["exp"].(float64); ok && exp > 0 {
		claims["exp"] = exp + float64(int64(delta/time.Second))
	}
	b, _ = json.Marshal(claims)
	parts[1] = base64.RawURLEncoding.EncodeToString(b)
	return strings.Join(parts, ".")
}

// Keep original ages, relative TTLs and all rows; never empty the historical
// tables to obtain a fast result. The shift is identical for every timestamp.
func perfSnapshot(t *testing.T, path string) (store.State, map[string]any, time.Duration) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(path, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(path, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	captured, err := time.Parse(time.RFC3339Nano, fmt.Sprint(manifest["captured_at"]))
	if err != nil {
		t.Fatal(err)
	}
	delta := time.Since(captured)
	var data any
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatal(err)
	}
	var shift func(any) any
	shift = func(value any) any {
		switch v := value.(type) {
		case map[string]any:
			for key, item := range v {
				v[key] = shift(item)
			}
		case []any:
			for i, item := range v {
				v[i] = shift(item)
			}
		case string:
			if at, err := time.Parse(time.RFC3339Nano, v); err == nil && !at.IsZero() {
				return at.Add(delta).Format(time.RFC3339Nano)
			}
		}
		return value
	}
	raw, err = json.Marshal(shift(data))
	if err != nil {
		t.Fatal(err)
	}
	var state store.State
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	for i := range state.Accounts {
		state.Accounts[i].Creds.AccessToken = perfShiftJWT(state.Accounts[i].Creds.AccessToken, delta)
	}
	versions := map[string]string{}
	for key, pair := range state.Pairs {
		old := types.CookieVersion(pair.Cookie)
		parts := strings.Split(pair.Cookie, ";")
		for i, part := range parts {
			name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
			if ok && name == "__oailb" {
				parts[i] = " " + name + "=" + perfShiftJWT(value, delta)
			}
		}
		pair.Cookie = strings.TrimSpace(strings.Join(parts, ";"))
		state.Pairs[key] = pair
		versions[old] = types.CookieVersion(pair.Cookie)
	}
	for key, slot := range state.Slots {
		if version, ok := versions[slot.LastDeliverVersion]; ok {
			slot.LastDeliverVersion = version
		}
		if version, ok := versions[slot.PrevDeliverVersion]; ok {
			slot.PrevDeliverVersion = version
		}
		state.Slots[key] = slot
	}
	if len(raw) < 8<<20 || len(state.ContactReports) < 8000 || len(state.Slots) < 12000 {
		t.Fatal("snapshot is not the expected full production-sized state")
	}
	return state, manifest, delta
}

type perfConsumer struct{ Key, Account, Tag, Gateway string }

type perfServerReady struct {
	URL                    string
	Consumers              []perfConsumer
	ConsumerIDs            []int64
	HistoricalTagConsumers int
	StateBytes             int
	ShiftSeconds           float64
	FixtureSHA256          string
	Manifest               json.RawMessage
}

// The same native test executable hosts the real server in a separate process.
// No client allocations or response decoding share its heap or resource metrics.
func TestProductionSnapshotPerformanceServer(t *testing.T) {
	if os.Getenv("GWPOOL_PERF_CHILD") != "1" {
		t.Skip("benchmark child only")
	}
	path := os.Getenv("GWPOOL_PERF_SNAPSHOT")
	state, manifest, shift := perfSnapshot(t, path)
	dir := t.TempDir()
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	cfg := config.Defaults()
	cfg.Mode = types.ModePool
	live := config.NewLive("", cfg)
	pool := sched.New(st, offlinePerfMinter{}, sched.Options{Mode: types.ModePool, Live: live})
	srv, err := New(pool, st, nil, Options{
		ConsumerKey: "offline-performance-root-not-a-production-key", Live: live,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	owners := map[int64]types.Owner{}
	for _, owner := range st.Owners() {
		owners[owner.ID] = owner
	}
	accounts := map[int64]types.Account{}
	var fallback types.Account
	for _, account := range st.Accounts() {
		accounts[account.ID] = account
		if fallback.ID == 0 && account.OwnerID > 0 && ownerCredentialActive(account, time.Now()) {
			fallback = account
		}
	}
	var consumers []perfConsumer
	reportTags := map[int64][]string{}
	tagSeen := map[int64]map[string]bool{}
	tagCursor := map[int64]int{}
	for _, report := range state.ContactReports {
		if tagSeen[report.ReporterID] == nil {
			tagSeen[report.ReporterID] = map[string]bool{}
		}
		if !tagSeen[report.ReporterID][report.AccountTag] {
			tagSeen[report.ReporterID][report.AccountTag] = true
			reportTags[report.ReporterID] = append(reportTags[report.ReporterID], report.AccountTag)
		}
	}
	for _, account := range st.Accounts() {
		if account.OwnerID > 0 && ownerCredentialActive(account, time.Now()) && len(reportTags[-account.OwnerID]) > 0 {
			fallback = account
			break
		}
	}
	if len(reportTags[-fallback.OwnerID]) == 0 {
		t.Fatal("fixture has no active owner with historical contact data")
	}
	historicalTagConsumers := 0
	var consumerIDs []int64
	for _, account := range st.Accounts() {
		if !strings.HasPrefix(account.Creds.AccountID, types.AnonPrefix) || !account.Enabled {
			continue
		}
		// Select the same stock-sufficient cohort for all scenarios. Do not
		// manufacture new stock or erase cooldown history to finish a long run.
		available := 0
		selected := ""
		now := time.Now()
		for gateway, pair := range state.Pairs {
			if pair.ExpiresAt.Sub(now) >= types.DeliverFloor {
				available++
				if selected == "" || gateway < selected {
					selected = gateway
				}
			}
		}
		if available < perfRequiredInventory {
			continue
		}
		source := accounts[account.Via]
		if source.OwnerID == 0 || !ownerCredentialActive(source, time.Now()) || len(reportTags[-source.OwnerID]) == 0 {
			source = fallback
		}
		owner, ok := owners[source.OwnerID]
		if !ok {
			continue
		}
		tagHash := sha256.Sum256([]byte(account.Creds.AccountID))
		tag := hex.EncodeToString(tagHash[:])
		if tags := reportTags[-owner.ID]; len(tags) > 0 {
			tag = tags[tagCursor[-owner.ID]%len(tags)]
			tagCursor[-owner.ID]++
			historicalTagConsumers++
		}
		consumers = append(consumers, perfConsumer{srv.OwnerKeyFor(owner),
			strings.TrimPrefix(account.Creds.AccountID, types.AnonPrefix), tag, selected})
		consumerIDs = append(consumerIDs, account.ID)
		if len(consumers) == perfWorkers {
			break
		}
	}
	if len(consumers) < perfWorkers {
		t.Fatalf("only %d eligible existing consumers", len(consumers))
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	sum := sha256.Sum256(raw)
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(os.Stdout)
	decoder := json.NewDecoder(os.Stdin)
	if err := encoder.Encode(perfServerReady{ts.URL, consumers, consumerIDs, historicalTagConsumers,
		len(raw), shift.Seconds(), hex.EncodeToString(sum[:]), manifestJSON}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"start", "stop"} {
		var command string
		if err := decoder.Decode(&command); err != nil || command != want {
			t.Fatalf("benchmark control: want %s, got %s, error %v", want, command, err)
		}
		if command == "start" {
			runtime.GC()
			if path := os.Getenv("GWPOOL_PERF_CPU_PROFILE"); path != "" {
				file, err := os.Create(path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = file.Close() }()
				if err := pprof.StartCPUProfile(file); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := encoder.Encode(readPerfResources(t)); err != nil {
			t.Fatal(err)
		}
		if command == "stop" && os.Getenv("GWPOOL_PERF_CPU_PROFILE") != "" {
			pprof.StopCPUProfile()
		}
	}
}

// Opt-in integration benchmark, not a fake-Pool benchmark. Run one scenario per
// invocation with -timeout 60s; use GWPOOL_PERF_ROUNDS to bound slow baselines.
func TestProductionSnapshotPerformance(t *testing.T) {
	if os.Getenv("GWPOOL_PERF_SNAPSHOT") == "" {
		t.Skip("set GWPOOL_PERF_SNAPSHOT to the remotely sanitized snapshot directory")
	}
	scenario := os.Getenv("GWPOOL_PERF_SCENARIO")
	if scenario != "gateways" && scenario != "single" {
		t.Fatal("GWPOOL_PERF_SCENARIO must be gateways or single")
	}
	rounds, err := strconv.Atoi(os.Getenv("GWPOOL_PERF_ROUNDS"))
	if err != nil || rounds < 1 || rounds > perfMaxRounds {
		t.Fatal("GWPOOL_PERF_ROUNDS must be 1..12")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProductionSnapshotPerformanceServer$", "-test.timeout=55s")
	command.Env = append(os.Environ(), "GWPOOL_PERF_CHILD=1")
	command.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	var childErrors bytes.Buffer
	command.Stderr = &childErrors
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	childOutput, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		_ = input.Close()
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	encoder, decoder := json.NewEncoder(input), json.NewDecoder(childOutput)
	var ready perfServerReady
	if err := decoder.Decode(&ready); err != nil {
		t.Fatalf("child startup: %v; %s", err, childErrors.String())
	}
	if len(ready.Consumers) != perfWorkers || ready.HistoricalTagConsumers != perfWorkers {
		t.Fatal("child did not supply the full historical consumer cohort")
	}
	resources := func(action string) perfResources {
		t.Helper()
		if err := encoder.Encode(action); err != nil {
			t.Fatal(err)
		}
		var result perfResources
		if err := decoder.Decode(&result); err != nil {
			t.Fatalf("child resources: %v", err)
		}
		return result
	}
	transport := &http.Transport{
		MaxIdleConns: perfWorkers, MaxIdleConnsPerHost: perfWorkers,
		DialContext: (&net.Dialer{Timeout: time.Second}).DialContext,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 50 * time.Second}
	// Establish connections with a cheap unknown route, not the measured endpoint.
	for range perfWorkers {
		response, err := client.Get(ready.URL + "/offline-benchmark-not-found")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
	type sample struct {
		ms     float64
		worker int
		body   []byte
		err    error
	}
	results := make(chan sample, perfWorkers*rounds)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for index, c := range ready.Consumers {
		wg.Add(1)
		go func(index int, c perfConsumer) {
			defer wg.Done()
			<-start
			for range rounds {
				query := url.Values{"account": {c.Account}, "model": {"gpt-6-luna"}}
				if c.Tag != "" {
					query.Set("account_tag", c.Tag)
				}
				endpoint := "/cookie"
				if scenario == "gateways" {
					endpoint = "/gateways"
				} else {
					query.Set("gateway", c.Gateway)
				}
				req, err := http.NewRequest(http.MethodGet, ready.URL+endpoint+"?"+query.Encode(), nil)
				if err != nil {
					results <- sample{err: err}
					return
				}
				req.Header.Set("Authorization", "Bearer "+c.Key)
				if c.Tag != "" {
					req.Header.Set("X-Gwpool-Account-Tag", c.Tag)
					req.Header.Set("X-Gwpool-Cooldown-Max-Seconds", strconv.Itoa(24*60*60))
				}
				at := time.Now()
				response, err := client.Do(req)
				if err != nil {
					results <- sample{err: err}
					return
				}
				body, readErr := io.ReadAll(response.Body)
				_ = response.Body.Close()
				elapsed := time.Since(at).Seconds() * 1000
				if response.StatusCode != http.StatusOK || readErr != nil {
					results <- sample{err: fmt.Errorf("status=%d read_error=%v", response.StatusCode, readErr)}
					return
				}
				// Keep the complete wire response; validate it after resource
				// sampling; this client has its own heap, separate from the server.
				results <- sample{ms: elapsed, worker: index, body: body}
			}
		}(index, c)
	}
	runtime.GC()
	before := resources("start")
	begin := time.Now()
	close(start)
	wg.Wait()
	wall := time.Since(begin)
	after := resources("stop")
	// Drain the child's trailing PASS output before Wait closes its pipe.
	_, _ = io.Copy(io.Discard, childOutput)
	waitErr := command.Wait()
	waited = true
	if waitErr != nil {
		t.Fatalf("benchmark server: %v; %s", waitErr, childErrors.String())
	}
	close(results)
	var timings []float64
	var responseBytes int
	var contactRows, priorityRows int
	for result := range results {
		if result.err != nil {
			t.Error(result.err)
			continue
		}
		if scenario == "gateways" {
			var catalog struct {
				Live     int `json:"live"`
				Gateways []struct {
					Contacts []json.RawMessage `json:"contacts"`
					Priority *json.RawMessage  `json:"priority"`
				} `json:"gateways"`
			}
			if err := json.Unmarshal(result.body, &catalog); err != nil || catalog.Live < 90 {
				t.Error("catalog lacks ready inventory or valid JSON")
				continue
			}
			contacts, priorities := 0, 0
			for _, gateway := range catalog.Gateways {
				contacts += len(gateway.Contacts)
				if gateway.Priority != nil {
					priorities++
				}
			}
			if contacts == 0 || priorities == 0 {
				t.Error("catalog did not exercise populated historical statistics")
				continue
			}
			contactRows += contacts
			priorityRows += priorities
		} else {
			var ticket deliveryResponse
			if err := json.Unmarshal(result.body, &ticket); err != nil {
				t.Error("invalid single ticket")
				continue
			}
			if ticket.Gateway != ready.Consumers[result.worker].Gateway || ticket.Cookie == "" {
				t.Error("missing ticket or unrequested gateway")
			}
		}
		timings = append(timings, result.ms)
		responseBytes += len(result.body)
	}
	if t.Failed() || len(timings) != perfWorkers*rounds {
		t.Fatalf("only %d/%d valid samples; not a passing benchmark", len(timings), perfWorkers*rounds)
	}
	sort.Float64s(timings)
	percentile := func(p float64) float64 { return timings[int(math.Ceil(float64(len(timings))*p))-1] }
	cpuSeconds := float64(after.CPU100ns-before.CPU100ns) / 1e7
	enforce := os.Getenv("GWPOOL_PERF_BASELINE") != "1"
	p95Limit, p99Limit := 50.0, 100.0
	if scenario == "gateways" {
		p95Limit, p99Limit = 10, 0
	}
	performancePassed := percentile(.95) <= p95Limit && (p99Limit == 0 || percentile(.99) <= p99Limit)
	report := map[string]any{
		"scenario": scenario, "workers": perfWorkers, "samples": len(timings),
		"historical_tag_consumers": ready.HistoricalTagConsumers,
		"consumer_local_ids":       ready.ConsumerIDs, "required_initial_inventory_per_consumer": perfRequiredInventory,
		"catalog_contact_rows_returned": contactRows, "catalog_priority_rows_returned": priorityRows,
		"performance_gate_enforced": enforce, "performance_gate_passed": performancePassed,
		"p95_limit_ms": p95Limit, "p99_limit_ms": p99Limit,
		"git_head": os.Getenv("GWPOOL_PERF_GIT_HEAD"), "source_tree_sha256": os.Getenv("GWPOOL_PERF_SOURCE_SHA256"),
		"tag_mapping_note":    "existing report tags sampled per owner; anonymous account to HMAC-tag mapping cannot be reconstructed from state, so no exact identity association is claimed",
		"latencies_ms_sorted": timings,
		"p50_ms":              percentile(.50), "p95_ms": percentile(.95), "p99_ms": percentile(.99),
		"wall_seconds": wall.Seconds(), "qps": float64(len(timings)) / wall.Seconds(),
		"cpu_seconds": cpuSeconds, "cpu_percent_one_core": cpuSeconds / wall.Seconds() * 100,
		"process_write_bytes":      after.IO.WriteBytes - before.IO.WriteBytes,
		"process_write_operations": after.IO.WriteOperations - before.IO.WriteOperations,
		"response_bytes":           responseBytes, "state_bytes": ready.StateBytes,
		"shift_seconds": ready.ShiftSeconds, "fixture_sha256": ready.FixtureSHA256,
		"source_manifest": ready.Manifest, "go_version": runtime.Version(), "logical_cpus": runtime.NumCPU(),
		"metric_scope": "32 external member references authenticated with local owner keys and populated historical tags; v2 explicit gateway, at most 12 calls/member; separate native child server and parent load generator, complete loopback HTTP latency, server CPU/IO, every body validated; repeated delivery allowed; no background loops or real minter; not comparable to the retired forced/batch protocol benchmark",
	}
	output, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if outputPath := os.Getenv("GWPOOL_PERF_OUTPUT"); outputPath != "" {
		if err := os.WriteFile(outputPath, output, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%s: N=%d p50=%.3fms p95=%.3fms p99=%.3fms QPS=%.1f CPU=%.4fs writes=%dB",
		scenario, len(timings), percentile(.50), percentile(.95), percentile(.99),
		float64(len(timings))/wall.Seconds(), cpuSeconds, after.IO.WriteBytes-before.IO.WriteBytes)
	if enforce && !performancePassed {
		t.Fatalf("performance target missed: p95=%.3fms (<=%.0f), p99=%.3fms (<=%.0f; zero means no gate)",
			percentile(.95), p95Limit, percentile(.99), p99Limit)
	}
}
