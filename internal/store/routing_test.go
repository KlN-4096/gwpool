package store

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gwpool/internal/routing"
	"gwpool/internal/types"
)

func routingSample(account int64, at time.Time) types.Sample {
	return types.Sample{AccountID: account, At: at, Status: 200, Gateway: "unified-1",
		Requested: types.Exit{Region: "US"}, Observed: types.Observed{Country: "US"},
		Purpose: types.PurposeInventory, Provider: "webshare"}
}

func TestRoutingHistoryLoadsOnceAndTracksSuccessfulAppends(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	account, err := s.AddAccount(types.Account{Creds: types.Creds{AccountID: "fake-domain", AccessToken: "fake"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	at := account.CreatedAt.Add(time.Second)
	if err := s.AppendSample(routingSample(account.ID, at)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.samples.WriteString("invalid-json\n"); err != nil {
		t.Fatal(err)
	}
	history, err := s.RoutingObservations(at)
	rows := history.Observations
	if err != nil || len(rows) != 1 || rows[0].Donor != routing.DonorKey("fake-domain") ||
		!history.Loaded || history.SkippedLines != 1 {
		t.Fatalf("load: history=%+v err=%v", history, err)
	}
	cache := s.routingHistory
	if err := s.AppendSample(routingSample(account.ID, at.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	history, err = s.RoutingObservations(at.Add(time.Second))
	rows = history.Observations
	if err != nil || len(rows) != 2 || cache != s.routingHistory || history.Loaded {
		t.Fatalf("incremental load: n=%d err=%v", len(rows), err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendSample(routingSample(account.ID, at.Add(2*time.Second))); err == nil {
		t.Fatal("closed sample file accepted a write")
	}
	history, err = s.RoutingObservations(at.Add(2 * time.Second))
	rows = history.Observations
	if err != nil || len(rows) != 2 {
		t.Fatalf("failed append trained model: n=%d err=%v", len(rows), err)
	}
	reopened := open(t, dir)
	history, err = reopened.RoutingObservations(at.Add(2 * time.Second))
	rows = history.Observations
	if err != nil || len(rows) != 2 {
		t.Fatalf("restart disagreed with incremental history: n=%d err=%v", len(rows), err)
	}
}

func TestRoutingHistoryRejectsStaleFutureAndWrongIdentityHistory(t *testing.T) {
	s := open(t, t.TempDir())
	account, err := s.AddAccount(types.Account{Creds: types.Creds{AccountID: "fake-domain", AccessToken: "fake"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	now := account.CreatedAt.Add(8 * 24 * time.Hour)
	for _, at := range []time.Time{
		account.CreatedAt.Add(-time.Second), account.CreatedAt, now.Add(time.Second), now,
	} {
		if err := s.AppendSample(routingSample(account.ID, at)); err != nil {
			t.Fatal(err)
		}
	}
	history, err := s.RoutingObservations(now)
	rows := history.Observations
	if err != nil || len(rows) != 1 {
		t.Fatalf("unexpected eligible rows=%d err=%v", len(rows), err)
	}
	if err := s.DeleteAccount(account.ID); err != nil {
		t.Fatal(err)
	}
	history, err = s.RoutingObservations(now)
	rows = history.Observations
	if err != nil || len(rows) != 0 {
		t.Fatalf("deleted identity still trains: n=%d err=%v", len(rows), err)
	}
}

func TestRoutingHistoryIsBoundedAndLoadErrorsRemainVisible(t *testing.T) {
	cache := &routingHistoryCache{}
	for i := 0; i <= routing.HistoryLimit; i++ {
		cache.append(routingSample(int64(i+1), time.Unix(int64(i+1), 0)))
	}
	if len(cache.rows) != routing.HistoryLimit {
		t.Fatalf("history size = %d", len(cache.rows))
	}
	for _, row := range cache.rows {
		if row.account == 1 {
			t.Fatal("oldest observation was not evicted")
		}
	}
	s := open(t, t.TempDir())
	original := s.dir
	s.dir = t.TempDir() // No samples file here; leave the real file untouched.
	now := time.Now()
	if _, err := s.RoutingObservations(now); err == nil || s.routingHistory != nil {
		t.Fatal("missing history silently became a successful empty model")
	}
	if err := os.WriteFile(filepath.Join(s.dir, samplesFile), []byte{}, filePerm); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RoutingObservations(now.Add(routingLoadRetryGap / 2)); err == nil {
		t.Fatal("I/O failure retried before its backoff elapsed")
	}
	if _, err := s.RoutingObservations(now.Add(routingLoadRetryGap)); err != nil {
		t.Fatal(err)
	}
	s.dir = original
}

func TestRoutingHistoryOversizedRowsAreSkippedBeforeAndAfterRestart(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	account, err := s.AddAccount(types.Account{Creds: types.Creds{AccountID: "fake-domain", AccessToken: "fake"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	at := account.CreatedAt.Add(time.Second)
	large := routingSample(account.ID, at)
	large.Observed.ISP = strings.Repeat("x", (1<<20)+1) // Exceeds the former scanner limit.
	valid := routingSample(account.ID, at)
	for i := 0; i < 2; i++ {
		if err := s.AppendSample(large); err != nil {
			t.Fatal(err)
		}
		if err := s.AppendSample(valid); err != nil {
			t.Fatal(err)
		}
		history, err := s.RoutingObservations(at)
		if err != nil || len(history.Observations) != i+1 || history.SkippedLines != i+1 {
			t.Fatalf("oversized line blocked subsequent valid rows: %+v err=%v", history, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	history, err := reopened.RoutingObservations(at)
	if err != nil || len(history.Observations) != 2 || history.SkippedLines != 2 {
		t.Fatalf("restart differs from live cache: %+v err=%v", history, err)
	}
}

func TestRoutingHistoryReadsOnlyBoundedTail(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), samplesFile))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Truncate(maxRoutingHistoryBytes + maxRoutingLineBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("\n"); err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(f).Encode(routingSample(1, time.Now())); err != nil {
		t.Fatal(err)
	}
	stat, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	cache, err := readRoutingHistory(f, stat.Size())
	if err != nil || !cache.clipped || len(cache.rows) != 1 {
		t.Fatalf("bounded tail failed: cache=%+v err=%v", cache, err)
	}
}

func TestRoutingHistoryConcurrentLoadAndAppendDoNotLoseOrDuplicateRows(t *testing.T) {
	s := open(t, t.TempDir())
	account, err := s.AddAccount(types.Account{Creds: types.Creds{AccountID: "fake-domain", AccessToken: "fake"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	const count = 200
	at := account.CreatedAt.Add(time.Second)
	for i := 0; i < count; i++ {
		if err := s.AppendSample(routingSample(account.ID, at.Add(time.Duration(i)))); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := s.RoutingObservations(at.Add(time.Second))
		errs <- err
	}()
	go func() {
		defer wg.Done()
		for i := count; i < 2*count; i++ {
			if err := s.AppendSample(routingSample(account.ID, at.Add(time.Duration(i)))); err != nil {
				errs <- err
				return
			}
		}
		errs <- nil
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	history, err := s.RoutingObservations(at.Add(time.Second))
	if err != nil || len(history.Observations) != 2*count {
		t.Fatalf("lost/doubled rows: n=%d err=%v", len(history.Observations), err)
	}
	seen := map[time.Time]bool{}
	for _, row := range history.Observations {
		if seen[row.At] {
			t.Fatal("observation included both from file snapshot and pending buffer")
		}
		seen[row.At] = true
	}
}

type pausedRoutingSnapshot struct {
	*os.File
	started chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (f *pausedRoutingSnapshot) ReadAt(p []byte, offset int64) (int, error) {
	f.once.Do(func() {
		close(f.started)
		<-f.resume
	})
	return f.File.ReadAt(p, offset)
}

func TestRoutingHistorySnapshotPendingBoundaryAndWrap(t *testing.T) {
	s := open(t, t.TempDir())
	account, err := s.AddAccount(types.Account{Creds: types.Creds{AccountID: "fake-domain", AccessToken: "fake"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	at := account.CreatedAt.Add(time.Second)
	if err := s.AppendSample(routingSample(account.ID, at)); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(s.dir, samplesFile))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	snapshot := &pausedRoutingSnapshot{File: f, started: make(chan struct{}), resume: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(snapshot.resume) })
	done := make(chan error, 1)
	go func() {
		s.routingLoadMu.Lock()
		defer s.routingLoadMu.Unlock()
		done <- s.loadRoutingSnapshot(snapshot)
	}()
	<-snapshot.started // File size captured, pending enabled, read not finished.

	appended := make(chan error, 1)
	go func() {
		for i := 1; i <= routing.HistoryLimit+2; i++ {
			if err := s.AppendSample(routingSample(account.ID, at.Add(time.Duration(i)))); err != nil {
				appended <- err
				return
			}
		}
		appended <- nil
	}()
	select {
	case err := <-appended:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot read blocked Store writes")
	}
	s.mu.RLock()
	pendingCount := len(s.routingPending.rows)
	s.mu.RUnlock()
	if pendingCount != routing.HistoryLimit {
		t.Fatalf("did not exercise pending ring wrap: %d", pendingCount)
	}
	release.Do(func() { close(snapshot.resume) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	history, err := s.RoutingObservations(at.Add(time.Second))
	if err != nil || len(history.Observations) != routing.HistoryLimit {
		t.Fatalf("pending merge: n=%d err=%v", len(history.Observations), err)
	}
	seen := make(map[time.Time]bool, routing.HistoryLimit)
	for _, row := range history.Observations {
		if row.At.Before(at.Add(3*time.Nanosecond)) || seen[row.At] {
			t.Fatal("old/doubled snapshot row survived pending wrap")
		}
		seen[row.At] = true
	}
}
