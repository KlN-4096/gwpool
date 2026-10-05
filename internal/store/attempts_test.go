package store

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestCountryAttemptsPersistShareAliasesAndExpire(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	now := time.Now().UTC()
	for _, code := range []string{"us", "US"} {
		if err := s.ReserveAttempt("credential", code, 2, 50, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ReserveAttempt("credential", "US", 2, 50, now); !errors.Is(err, types.ErrRateLimited) {
		t.Fatalf("third attempt = %v", err)
	}
	reopened := open(t, dir)
	if got := reopened.AttemptUsage("credential", now); got["US"] != 2 {
		t.Fatalf("restart reset quota: %v", got)
	}
	rows := reopened.CountryAttempts("credential")
	rows["US"].Recent[0] = time.Time{}
	if got := reopened.AttemptUsage("credential", now); got["US"] != 2 {
		t.Fatal("snapshot changed live budget")
	}
	if err := reopened.ReserveAttempt("credential", "US", 2, 50, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := reopened.CountryAttempts("credential")["US"]; got.Count != 3 || len(got.Recent) != 1 {
		t.Fatalf("rolling expiry lost lifetime attempt count: %+v", got)
	}
}

func TestCountryAttemptsConcurrentCountryAndAccountCaps(t *testing.T) {
	s := open(t, t.TempDir())
	now := time.Now().UTC()
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.ReserveAttempt("one", "US", 2, 50, now); err == nil {
				accepted.Add(1)
			} else if !errors.Is(err, types.ErrRateLimited) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 2 {
		t.Fatalf("country accepted %d", accepted.Load())
	}
	for _, country := range []string{"JP", "DE", "BR"} {
		if err := s.ReserveAttempt("one", country, 2, 5, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ReserveAttempt("one", "AU", 2, 5, now); !errors.Is(err, types.ErrRateLimited) {
		t.Fatalf("account cap not independent: %v", err)
	}
	if err := s.ReserveAttempt("two", "AU", 2, 5, now); err != nil {
		t.Fatal("another credential inherited the cap", err)
	}
}

func TestCountryAttemptsSaveFailureDoesNotReserveInMemory(t *testing.T) {
	s := open(t, t.TempDir())
	breakSave(t, s)
	if err := s.ReserveAttempt("one", "US", 2, 50, time.Now()); err == nil {
		t.Fatal("save failure was hidden")
	}
	if len(s.CountryAttempts("one")) != 0 {
		t.Fatal("failed reservation survived in memory")
	}
}

func TestCountryAttemptsMigrationPreservesFailuresAndIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	a, err := s.AddAccount(types.Account{Creds: types.Creds{AccountID: "same-credential"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, sample := range []types.Sample{
		{AccountID: a.ID, Requested: types.Exit{Region: "US"}, Status: 200, At: now.Add(-2 * time.Hour)},
		{AccountID: a.ID, Requested: types.Exit{Region: "US", City: "Seattle"}, Status: 0, At: now.Add(-time.Minute)},
		{AccountID: a.ID, Requested: types.Exit{Region: "US"}, Status: 403, At: now},
		{AccountID: a.ID, Requested: types.Exit{Region: "JP"}, At: now.Add(time.Hour)},
	} {
		if err := s.AppendSample(sample); err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.OpenFile(filepath.Join(dir, samplesFile), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.WriteString("bad-json\n")
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal(writeErr, closeErr)
	}
	if skipped, err := s.MigrateAttempts(); err != nil || skipped != 1 {
		t.Fatalf("migration: skipped=%d err=%v", skipped, err)
	}
	if row := s.CountryAttempts("same-credential")["US"]; row.Count != 3 || len(row.Recent) != 2 {
		t.Fatalf("successful and failed samples must share country: %+v", row)
	}
	if got := s.AttemptUsage("same-credential", now); got["US"] != 2 || got["JP"] != 0 {
		t.Fatalf("migration usage %v", got)
	}
	if err := s.ReserveAttempt("same-credential", "BR", 2, 50, now); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	if _, err := reopened.MigrateAttempts(); err != nil {
		t.Fatal(err)
	}
	if got := reopened.AttemptUsage("same-credential", now); got["BR"] != 1 {
		t.Fatal("repeat migration erased new attempts")
	}
}
