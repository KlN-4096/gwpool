//go:build linux

package httpapi

// Opt-in, production-sized, offline benchmark server. The load generator is a
// separate Python process; no client allocations/CPU enter the server counters.
import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/routes"
	"gwpool/internal/sched"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

const mixFixtureMembers = 32
const mixFixtureLifetime = 50 * time.Second

type mixFakeMinter struct {
	st      *store.Store
	sources map[bool][]types.RouteSource
	calls   atomic.Int64
}

func (m *mixFakeMinter) BeginAttempt(ctx context.Context, c types.Creds, e types.Exit) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return ctx, err
	}
	return ctx, m.st.ReserveAttempt(c.AccountID, e.Region, 2, 50, time.Now())
}
func (m *mixFakeMinter) Mint(ctx context.Context, _ types.Creds, _ types.Exit, _, _ string, _ time.Time) (types.MintResult, error) {
	m.calls.Add(1)
	select {
	case <-ctx.Done():
		return types.MintResult{}, ctx.Err()
	case <-time.After(5 * time.Millisecond):
		return types.MintResult{Status: 503, ErrBody: "offline controlled upstream failure"}, nil
	}
}
func (*mixFakeMinter) VerifyFull(context.Context, types.Creds, types.Exit, string, string, string) (*types.Verdict, error) {
	panic("consumer or background unexpectedly requested quality verification")
}
func (*mixFakeMinter) SupportsDynamicExit(types.Exit) bool { return true }
func (*mixFakeMinter) Provider(string) string              { return "novproxy" }
func (*mixFakeMinter) Geo(ctx context.Context, e types.Exit, _ string) (types.Observed, error) {
	return types.Observed{IP: "198.18.0.1", Country: e.Region, State: e.State, City: e.City}, ctx.Err()
}
func (m *mixFakeMinter) Rate(account string) (map[string]int, int) {
	return m.st.AttemptUsage(account, time.Now()), 2
}
func (m *mixFakeMinter) RouteSources(_ types.Exit, seed bool) []types.RouteSource {
	return m.sources[seed]
}
func (m *mixFakeMinter) NewSIDFor(e types.Exit, _ bool) (string, error) {
	return m.NewSIDForRoute(e, m.sources[false][0].ID, false)
}
func (m *mixFakeMinter) NewSeedSID() (string, error) {
	return m.NewSIDForRoute(types.Exit{}, m.sources[true][0].ID, true)
}
func (*mixFakeMinter) NewSIDForRoute(_ types.Exit, source string, seed bool) (string, error) {
	if seed {
		return "seed:" + source, nil
	}
	return "bare:" + source, nil
}
func (*mixFakeMinter) SeedBound(sid string) bool { return strings.HasPrefix(sid, "seed:") }
func (m *mixFakeMinter) Sticky(sid string) bool  { return !m.SeedBound(sid) }
func (m *mixFakeMinter) RouteSource(sid string) types.RouteSource {
	for _, s := range m.sources[m.SeedBound(sid)] {
		if strings.HasSuffix(sid, s.ID) {
			return s
		}
	}
	panic("unknown offline source")
}

func mixShiftJWT(token string, delta time.Duration) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return token
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return token
	}
	var body map[string]any
	if json.Unmarshal(raw, &body) != nil {
		return token
	}
	for _, field := range []string{"exp", "iat", "nbf"} {
		if old, ok := body[field].(float64); ok && old > 0 {
			body[field] = old + float64(delta/time.Second)
		}
	}
	raw, _ = json.Marshal(body)
	parts[1] = base64.RawURLEncoding.EncodeToString(raw)
	return strings.Join(parts, ".")
}

func mixShift(value any, delta time.Duration) any {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			v[key] = mixShift(item, delta)
		}
	case []any:
		for i, item := range v {
			v[i] = mixShift(item, delta)
		}
	case string:
		if at, err := time.Parse(time.RFC3339Nano, v); err == nil && !at.IsZero() {
			return at.Add(delta).Format(time.RFC3339Nano)
		}
	}
	return value
}

func mixMustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func mixDecode(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber() // reporter namespaces use int64 values near MinInt64
	return decoder.Decode(value)
}

func TestPerformanceShiftPreservesReporterAndRouteIdentity(t *testing.T) {
	raw := []byte(`{"reporter_id":-9223372036854775801,"at":"2026-10-12T08:00:00Z"}`)
	var value any
	if err := mixDecode(raw, &value); err != nil {
		t.Fatal(err)
	}
	shifted := mixMustJSON(t, mixShift(value, time.Hour))
	var row struct {
		Reporter int64     `json:"reporter_id"`
		At       time.Time `json:"at"`
	}
	if err := json.Unmarshal(shifted, &row); err != nil || row.Reporter != -9223372036854775801 || row.At.Hour() != 9 {
		t.Fatalf("large reporter ID changed: %s %v", shifted, err)
	}
}

func mixFixture(t *testing.T, source, output string) (store.State, map[string]any) {
	t.Helper()
	manifestRaw, err := os.ReadFile(filepath.Join(source, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	captured, err := time.Parse(time.RFC3339Nano, manifest["captured_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	// Integral-second shift preserves JWT integer expiry and RFC3339 offsets
	// exactly together. Every run starts at the same fixture-relative age.
	delta := time.Since(captured).Truncate(time.Second)
	raw, err := os.ReadFile(filepath.Join(source, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != manifest["state_sha256"] {
		t.Fatal("fixture state hash mismatch")
	}
	var data any
	if err := mixDecode(raw, &data); err != nil {
		t.Fatal(err)
	}
	var state store.State
	if err := json.Unmarshal(mixMustJSON(t, mixShift(data, delta)), &state); err != nil {
		t.Fatal(err)
	}
	identities := map[string]string{}
	for i := range state.Accounts {
		a := &state.Accounts[i]
		old := routes.CredentialIdentity(a.Creds)
		a.Creds.AccessToken = mixShiftJWT(a.Creds.AccessToken, delta)
		identities[old] = routes.CredentialIdentity(a.Creds)
	}
	routeStates := make(map[string]types.RouteState, len(state.Routes))
	for _, row := range state.Routes {
		if newID, ok := identities[row.Route.Identity]; ok {
			row.Route.Identity = newID
		}
		routeStates[routes.Key(row.Route)] = row
	}
	if len(routeStates) != len(state.Routes) {
		t.Fatal("shifted routes lost identity cardinality")
	}
	state.Routes = routeStates
	versions := map[string]string{}
	for key, pair := range state.Pairs {
		old := types.CookieVersion(pair.Cookie)
		parts := strings.Split(pair.Cookie, ";")
		for i, part := range parts {
			name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
			if ok && name == "__oailb" {
				parts[i] = " " + name + "=" + mixShiftJWT(value, delta)
			}
		}
		pair.Cookie = strings.TrimSpace(strings.Join(parts, ";"))
		state.Pairs[key] = pair
		versions[old] = types.CookieVersion(pair.Cookie)
	}
	for key, slot := range state.Slots {
		if v, ok := versions[slot.LastDeliverVersion]; ok {
			slot.LastDeliverVersion = v
		}
		if v, ok := versions[slot.PrevDeliverVersion]; ok {
			slot.PrevDeliverVersion = v
		}
		state.Slots[key] = slot
	}
	// Export was replayed consistently on the source. Start a new private WAL,
	// retaining every business row; do not pretend this is a production restore.
	state.JournalVersion, state.JournalSequence = 0, 0
	if err := os.WriteFile(filepath.Join(output, "state.json"), mixMustJSON(t, state), 0600); err != nil {
		t.Fatal(err)
	}
	samples, err := os.Open(filepath.Join(source, "samples.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer samples.Close()
	dst, err := os.OpenFile(filepath.Join(output, "samples.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(samples, hash))
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	lines := 0
	writer := bufio.NewWriter(dst)
	for scanner.Scan() {
		line := scanner.Bytes()
		var sample any
		if mixDecode(line, &sample) == nil {
			sample = mixShift(sample, delta)
			if obj, ok := sample.(map[string]any); ok {
				if route, ok := obj["Route"].(map[string]any); ok {
					if desc, ok := route["Route"].(map[string]any); ok {
						if old, ok := desc["Identity"].(string); ok && identities[old] != "" {
							desc["Identity"] = identities[old]
						}
						var descriptor types.RouteDescriptor
						if err := json.Unmarshal(mixMustJSON(t, desc), &descriptor); err != nil {
							t.Fatal(err)
						}
						route["ID"] = routes.Key(descriptor)
					}
				}
			}
			line = mixMustJSON(t, sample)
		}
		if _, err := writer.Write(append(append([]byte(nil), line...), '\n')); err != nil {
			t.Fatal(err)
		}
		lines++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(hash.Sum(nil)) != manifest["samples_sha256"] || lines != int(manifest["samples_lines"].(float64)) {
		t.Fatal("full sample prefix hash/count mismatch")
	}
	manifest["shift_seconds"] = int64(delta / time.Second)
	manifest["private_wal_reset"] = true
	return state, manifest
}

type mixConsumer struct {
	Key, Account, Tag, Gateway, Model string
}

func mixConsumers(t *testing.T, s *Server, state store.State) []mixConsumer {
	t.Helper()
	now := time.Now()
	keys := map[int64]string{}
	accounts := append([]types.Account(nil), state.Accounts...)
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].ID < accounts[j].ID })
	for _, owner := range state.Owners {
		for _, a := range accounts {
			if a.OwnerID == owner.ID && ownerCredentialActive(a, now) {
				keys[-owner.ID] = s.OwnerKeyFor(owner)
			}
		}
	}
	for _, a := range accounts {
		if ownerCredentialActive(a, now) && a.LegacyConsumerKey {
			keys[a.ID] = s.ConsumerKeyFor(a)
		}
	}
	// Pick stock with at least five minutes, sufficient for this <=50s run.
	gateway := ""
	for name, pair := range s.effectivePairs(now) {
		if pair.ExpiresAt.Sub(now) > 5*time.Minute && (gateway == "" || name < gateway) {
			gateway = name
		}
	}
	if gateway == "" {
		t.Fatal("fixture has no sufficiently live gateway; do not manufacture inventory")
	}
	type historical struct{ key, tag, model string }
	var histories []historical
	seen := map[string]bool{}
	for _, report := range state.ContactReports {
		key := keys[report.ReporterID]
		if key == "" || seen[key+"|"+report.AccountTag] {
			continue
		}
		seen[key+"|"+report.AccountTag] = true
		histories = append(histories, historical{key, report.AccountTag, report.Model})
	}
	sort.Slice(histories, func(i, j int) bool {
		return histories[i].key+histories[i].tag < histories[j].key+histories[j].tag
	})
	if len(histories) == 0 {
		t.Fatal("no authorized existing reporter history")
	}
	var out []mixConsumer
	for _, a := range accounts {
		if !strings.HasPrefix(a.Creds.AccountID, types.AnonPrefix) {
			continue
		}
		h := histories[len(out)%len(histories)]
		out = append(out, mixConsumer{h.key, strings.TrimPrefix(a.Creds.AccountID, types.AnonPrefix), h.tag, gateway, h.model})
		if len(out) == mixFixtureMembers {
			break
		}
	}
	if len(out) != mixFixtureMembers {
		t.Fatal("not enough existing external identities")
	}
	return out
}

func mixResource() map[string]any {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		panic(err)
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	out := map[string]any{
		"cpu_seconds":  float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6,
		"rss_peak_kib": usage.Maxrss, "allocated_bytes": mem.TotalAlloc, "heap_bytes": mem.HeapAlloc,
		"mallocs": mem.Mallocs, "gc_cycles": mem.NumGC, "gc_pause_ns": mem.PauseTotalNs,
	}
	if raw, err := os.ReadFile("/proc/self/io"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			var key string
			var value uint64
			if _, err := fmt.Sscanf(line, "%s %d", &key, &value); err == nil {
				out[strings.TrimSuffix(key, ":")] = value
			}
		}
	}
	return out
}

func TestPortableProductionBenchmark(t *testing.T) {
	source, output := os.Getenv("GWPOOL_MIX_FIXTURE"), os.Getenv("GWPOOL_MIX_OUTPUT")
	if source == "" || output == "" {
		t.Skip("explicit sanitized fixture and output required")
	}
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(output, "data")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	state, manifest := mixFixture(t, source, dir)
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	}()
	minter := &mixFakeMinter{st: st, sources: map[bool][]types.RouteSource{}}
	sourceSeen := map[string]bool{}
	for _, row := range state.Routes {
		seed := row.Route.Mode == routes.Seed
		if !sourceSeen[row.Route.Source] {
			kind := "novproxy"
			if seed {
				kind = "seed"
			}
			minter.sources[seed] = append(minter.sources[seed], types.RouteSource{ID: row.Route.Source, Kind: kind})
			sourceSeen[row.Route.Source] = true
		}
	}
	for _, seed := range []bool{false, true} {
		if len(minter.sources[seed]) == 0 {
			kind := "novproxy"
			if seed {
				kind = "seed"
			}
			minter.sources[seed] = []types.RouteSource{{ID: routes.Digest("offline", kind), Kind: kind}}
		}
		sort.Slice(minter.sources[seed], func(i, j int) bool { return minter.sources[seed][i].ID < minter.sources[seed][j].ID })
	}
	cfg := config.Defaults()
	cfg.Mode = types.ModePool
	live := config.NewLive("", cfg)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	warmInterval := time.Minute
	background := os.Getenv("GWPOOL_MIX_BACKGROUND") == "1"
	if background {
		warmInterval = 2 * time.Second // disclosed stress-only acceleration, not production cadence
	}
	pool := sched.New(st, minter, sched.Options{Mode: types.ModePool, Live: live, Log: logger, WarmInterval: warmInterval})
	server, err := New(pool, st, nil, Options{ConsumerKey: "offline-fixture-consumer", AdminKey: "offline-fixture-admin", Live: live, Log: logger})
	if err != nil {
		t.Fatal(err)
	}
	consumers := mixConsumers(t, server, state)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	joined := make(chan struct{})
	if background {
		go func() { defer close(joined); pool.Run(ctx) }()
	} else {
		close(joined)
	}
	defer cancel()
	runtime.GC()
	profile := os.Getenv("GWPOOL_MIX_PROFILE") == "1"
	if profile {
		mixWriteProfile(t, output, "allocs-before", "allocs")
		f, err := os.Create(filepath.Join(output, "cpu.pprof"))
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			t.Fatal(err)
		}
		defer f.Close()
	}
	before := mixResource()
	started := time.Now()
	ready := map[string]any{"url": ts.URL, "consumers": consumers, "pid": os.Getpid(),
		"admin_key": "offline-fixture-admin", "manifest": manifest, "before": before,
		"warm_interval_seconds": warmInterval.Seconds(), "background": background,
		"started_at": started.UTC(), "checkpoint_forced": os.Getenv("GWPOOL_MIX_CHECKPOINT") == "1"}
	if err := os.WriteFile(filepath.Join(output, "ready.json"), mixMustJSON(t, ready), 0600); err != nil {
		t.Fatal(err)
	}
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	var checkpointDuration time.Duration
	checkpointDone := false
	for range tick.C {
		if os.Getenv("GWPOOL_MIX_CHECKPOINT") == "1" && !checkpointDone && time.Since(started) >= 10*time.Second {
			checkpointDone = true
			begin := time.Now()
			if err := st.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkpointDuration = time.Since(begin)
		}
		if _, err := os.Stat(filepath.Join(output, "stop")); err == nil {
			break
		}
		if time.Since(started) > mixFixtureLifetime {
			t.Fatal("load generator did not stop benchmark within 50s")
		}
	}
	ts.Close()
	cancel()
	<-joined
	after := mixResource()
	if profile {
		pprof.StopCPUProfile()
		mixWriteProfile(t, output, "allocs-after", "allocs")
		runtime.GC()
		mixWriteProfile(t, output, "heap-after-gc", "heap")
	}
	result := map[string]any{"before": before, "after": after, "elapsed_seconds": time.Since(started).Seconds(),
		"checkpoint_seconds": checkpointDuration.Seconds(), "fake_mint_calls": minter.calls.Load(),
		"revisions": st.Revisions(), "production_network_clients": 0}
	if err := os.WriteFile(filepath.Join(output, "server.json"), mixMustJSON(t, result), 0600); err != nil {
		t.Fatal(err)
	}
}

func mixWriteProfile(t *testing.T, output, name, kind string) {
	t.Helper()
	f, err := os.Create(filepath.Join(output, name+".pprof"))
	if err != nil {
		t.Fatal(err)
	}
	err = pprof.Lookup(kind).WriteTo(f, 0)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
}
