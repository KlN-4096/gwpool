package sched

import (
	"context"
	"errors"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/mint"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

type durableAttemptMinter struct {
	*fakeMinter
	budget *mint.Client
}

func (m *durableAttemptMinter) BeginAttempt(ctx context.Context, cr types.Creds, exit types.Exit) (context.Context, error) {
	return m.budget.BeginAttempt(ctx, cr, exit)
}
func (m *durableAttemptMinter) Rate(account string) (map[string]int, int) {
	return m.budget.Rate(account)
}

func TestCountryAttemptsFailedBindingAdvancesInventoryAcrossRounds(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	a, err := st.AddAccount(types.Account{Enabled: true, Creds: types.Creds{AccountID: "one", AccessToken: "fake"}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Regions = []config.ExplorationRegion{
		{Key: "us", Name: "US", Country: "US", Enabled: true},
		{Key: "jp", Name: "JP", Country: "JP", Enabled: true},
	}
	live := config.NewLive("", cfg)
	clock := newClock()
	fake := newMinter(clock)
	fake.sidErr = types.ErrNoExit
	m := &durableAttemptMinter{fakeMinter: fake, budget: &mint.Client{Live: live, Attempts: st}}
	s := New(st, m, Options{Live: live, Now: clock.now, MintGap: -1, RenewGap: -1})
	s.premintBare(context.Background(), 1)
	s.premintBare(context.Background(), 1)
	attempts := st.CountryAttempts(a.Creds.AccountID)
	if attempts["US"].Count != 1 || attempts["JP"].Count != 1 {
		t.Fatalf("failed first country monopolized the next round: %+v", attempts)
	}
	rows, err := st.Samples()
	if err != nil || len(rows) != 2 || rows[0].Purpose != types.PurposeInventory || rows[0].Status != 0 {
		t.Fatalf("binding failures are missing from evidence: rows=%d err=%v", len(rows), err)
	}
	if len(st.RegionMints()) != 0 || len(fake.mints) != 0 {
		t.Fatal("failed attempts became successful mints")
	}
}

func TestCountryAttemptsUnsupportedObservedExitDoesNotSendOrDisableAccount(t *testing.T) {
	s, st, m, _ := rig(t, types.ModeSingle)
	m.geoCountry = "HK"
	a := st.Accounts()[0]
	_, err := s.mintOnce(context.Background(), a, types.Exit{Region: "US"}, "dynamic-sid", types.Pair{}, "", "")
	if !errors.Is(err, types.ErrUnsupportedCountry) || len(m.mints) != 0 || len(st.disabled) != 0 {
		t.Fatalf("unsupported observed exit leaked upstream or disabled account: %v", err)
	}
	if len(st.samples) != 1 || st.samples[0].Observed.Country != "HK" || st.samples[0].Status != 0 {
		t.Fatal("blocked exit was not recorded as a local failed attempt")
	}
}

func TestCountryAttemptsRegion403DoesNotDisableAccount(t *testing.T) {
	s, st, m, _ := rig(t, types.ModeSingle)
	m.mintStatus = 403
	a := st.Accounts()[0]
	if _, err := s.mintOnce(context.Background(), a, types.Exit{Region: "US"}, "sid", types.Pair{}, "", ""); err == nil {
		t.Fatal("403 should fail this attempt")
	}
	if len(st.disabled) != 0 {
		t.Fatal("403 alone disabled the whole account")
	}
}

type countryMissingMinter struct{ *fakeMinter }

func (m countryMissingMinter) Geo(context.Context, types.Exit, string) (types.Observed, error) {
	return types.Observed{IP: "192.0.2.1"}, nil
}

func TestCountryAttemptsUnknownActualCountryDoesNotSend(t *testing.T) {
	for _, sid := range []string{"dynamic", "seed-existing"} {
		t.Run(sid, func(t *testing.T) {
			s, st, m, _ := rig(t, types.ModeSingle)
			s.mint = countryMissingMinter{m}
			_, err := s.mintOnce(context.Background(), st.Accounts()[0], types.Exit{Region: "US"}, sid, types.Pair{}, "", "")
			if !errors.Is(err, types.ErrUnsupportedCountry) || len(m.mints) != 0 {
				t.Fatalf("unknown actual country leaked upstream: %v", err)
			}
		})
	}
}

func TestCountryAttemptsManualBatchAdvancesAfterFailedCountriesAndBudgetExpiry(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	_, err = st.AddAccount(types.Account{Enabled: true, Creds: types.Creds{AccountID: "one", AccessToken: "fake"}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.UpstreamPerAccountHour = 2
	cfg.Regions = []config.ExplorationRegion{
		{Key: "us", Country: "US", Enabled: true},
		{Key: "jp", Country: "JP", Enabled: true},
		{Key: "br", Country: "BR", Enabled: true},
	}
	live := config.NewLive("", cfg)
	m := &durableAttemptMinter{fakeMinter: newMinter(newClock()), budget: &mint.Client{Live: live, Attempts: st}}
	q := NewMintAllQueue(New(st, m, Options{Live: live}))
	first, _ := q.plan([]string{"us", "jp", "br"})
	if len(first) != 2 || first[0].region.Key != "us" || first[1].region.Key != "jp" {
		t.Fatalf("unexpected first batch: %+v", first)
	}
	// Persist failed attempts whose rolling allowance has now expired.
	for _, job := range first {
		if err := st.ReserveAttempt("one", job.region.Exit.Region, 2, 2, time.Now().Add(-2*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	next, _ := q.plan([]string{"us", "jp", "br"})
	foundBR := false
	for _, job := range next {
		foundBR = foundBR || job.region.Key == "br"
	}
	if len(next) != 2 || !foundBR {
		t.Fatalf("next batch repeated the same failed countries: %+v", next)
	}
}
