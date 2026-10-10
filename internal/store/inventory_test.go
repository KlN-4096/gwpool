package store

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"gwpool/internal/routes"
	"gwpool/internal/ticket"
	"gwpool/internal/types"
)

func inventoryPair(gateway, region, cflb string, fruit, lease time.Time) types.Pair {
	payload := fmt.Sprintf(`{"host":"x.y.%s","exp":%d}`, gateway, fruit.Unix())
	cookie := "__cflb=" + cflb + "; __oailb=e30." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".test"
	deadline := fruit
	if lease.Before(deadline) {
		deadline = lease
	}
	return types.Pair{Gateway: gateway, Cookie: cookie, RegionKey: region,
		ExpiresAt: deadline, CflbExpiresAt: lease, MintedBy: 1}
}

func inventoryRouteAttempt(now time.Time) types.RouteAttempt {
	descriptor := routes.Describe(types.Creds{AccountID: "synthetic-account"}, types.Exit{Region: "US"},
		types.RouteSource{ID: routes.Digest("synthetic-source")}, routes.Bare, "")
	return types.RouteAttempt{Route: descriptor, StartedAt: now, At: now,
		Expected: "wanted", Gateway: "actual", Status: 200}
}

func TestRouteInventoryRestartPreservesNegativeEvidenceAndGain(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	now := time.Now().UTC().Truncate(time.Second)
	attempt := inventoryRouteAttempt(now)
	pair := inventoryPair("actual", "country-us", "new", now.Add(time.Hour), now.Add(time.Hour))
	result, err := s.CommitMint(types.MintCommit{Pair: pair, Route: &attempt, At: now})
	if err != nil || !result.Gain.CoverageAdded || result.Route.LastMissed != "wanted" {
		t.Fatalf("drift and committed stock benefit not recorded independently: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	key := routes.Key(attempt.Route)
	state := reopened.RouteStates(now)[key]
	if state != result.Route || routes.Allowed(state, now, false) ||
		!routes.Allowed(state, state.NextCheck, false) {
		t.Fatal("restart cleared hold, lost gain or created a permanent blacklist")
	}
	if len(reopened.RouteStates(now.Add(routes.Retention))) != 0 {
		t.Fatal("expired route evidence remained eligible")
	}
	later := now.Add(routes.Retention)
	next := inventoryRouteAttempt(later)
	next.Route.Source = routes.Digest("other-source")
	next.Gateway, next.Expected, next.Status, next.Failure = "", "", 0, "transport"
	if _, err := reopened.CommitMint(types.MintCommit{Route: &next, At: later}); err != nil {
		t.Fatal(err)
	}
	if _, retained := reopened.st.Routes[key]; retained {
		t.Fatal("expired evidence was not pruned in the durable mutation")
	}
}

func TestRouteInventoryFailureRollsBackEvidenceWithStock(t *testing.T) {
	s := open(t, t.TempDir())
	now := time.Now().UTC().Truncate(time.Second)
	attempt := inventoryRouteAttempt(now)
	candidate := inventoryPair("actual", "country-us", "new", now.Add(time.Hour), now.Add(time.Hour))
	s.journal.file = &faultyJournal{journalIO: s.journal.file, syncFail: true}
	seq, revision := s.st.JournalSequence, s.Revisions()
	if _, err := s.CommitMint(types.MintCommit{Pair: candidate, Route: &attempt, At: now}); err == nil {
		t.Fatal("failed combined transaction acknowledged")
	}
	pairs, donors := s.TicketInventory()
	if len(pairs)+len(donors)+len(s.RouteStates(now)) != 0 ||
		s.st.JournalSequence != seq || s.Revisions() != revision {
		t.Fatal("failed route/stock transaction leaked a partial result")
	}
}

func TestRouteInventoryRejectsMalformedObservation(t *testing.T) {
	s := open(t, t.TempDir())
	now := time.Now().UTC()
	for _, field := range []string{"source", "time", "failure"} {
		attempt := inventoryRouteAttempt(now)
		switch field {
		case "source":
			attempt.Route.Source = "raw-proxy-credential"
		case "time":
			attempt.At = now.Add(-time.Second)
		case "failure":
			attempt.Failure = "raw-provider-error"
		}
		if _, err := s.CommitMint(types.MintCommit{Route: &attempt, At: attempt.At}); err == nil {
			t.Fatalf("invalid %s was accepted", field)
		}
	}
	if len(s.RouteStates(now)) != 0 {
		t.Fatal("invalid observation changed route state")
	}
}

func TestRouteInventoryPruningBoundsRecentStateAndExpiresAtBoundary(t *testing.T) {
	now := time.Now().UTC()
	states := map[string]types.RouteState{}
	for i := 0; i < routes.MaxStates+2; i++ {
		attempt := inventoryRouteAttempt(now.Add(time.Duration(i) * time.Millisecond))
		attempt.Route.Source = routes.Digest(fmt.Sprint(i))
		states[routes.Key(attempt.Route)] = routes.Advance(types.RouteState{}, attempt, types.StockGain{})
	}
	oldest := inventoryRouteAttempt(now)
	oldest.Route.Source = routes.Digest("0")
	expired := inventoryRouteAttempt(now.Add(-routes.Retention))
	states[routes.Key(expired.Route)] = routes.Advance(types.RouteState{}, expired, types.StockGain{})
	dropped := pruneRouteStates(states, now)
	if len(states) != routes.MaxStates || len(dropped) != 3 {
		t.Fatalf("state cap/expiration mismatch: remaining=%d dropped=%d", len(states), len(dropped))
	}
	if _, exists := states[routes.Key(oldest.Route)]; exists {
		t.Fatal("capacity eviction did not remove oldest evidence")
	}
	if _, exists := states[routes.Key(expired.Route)]; exists {
		t.Fatal("exact retention boundary retained stale evidence")
	}
}

func TestRouteInventoryExpiredSameKeyStartsFreshEvidence(t *testing.T) {
	s := open(t, t.TempDir())
	now := time.Now().UTC()
	attempt := inventoryRouteAttempt(now)
	var result types.MintCommitResult
	for i := 0; i < 5; i++ {
		var err error
		result, err = s.CommitMint(types.MintCommit{At: attempt.At, Route: &attempt})
		if err != nil {
			t.Fatal(err)
		}
		attempt.StartedAt, attempt.At = result.Route.NextCheck, result.Route.NextCheck
	}
	later := result.Route.LastAttempt.Add(routes.Retention)
	attempt.StartedAt, attempt.At = later, later
	attempt.Status, attempt.Gateway, attempt.Expected, attempt.Failure = 0, "", "", "transport"
	result, err := s.CommitMint(types.MintCommit{At: later, Route: &attempt})
	if err != nil {
		t.Fatal(err)
	}
	if result.Route.LowYieldStreak != 1 || result.Route.NextCheck != later.Add(routes.RetryBase) ||
		result.Route.Gateway != "" || result.Route.LastMissed != "" || result.Route.Attempts != 1 {
		t.Fatal("expired same-key commit resurrected stale mapping, miss or retry streak")
	}
}

func TestInventoryCommitSameCookieLongerDonorLeaseCannotShortenStock(t *testing.T) {
	s := open(t, t.TempDir())
	now := time.Now().UTC().Truncate(time.Second)
	old := inventoryPair("a", "region", "same-cflb", now.Add(time.Hour), now.Add(10*time.Minute))
	if err := s.PutPair(old); err != nil {
		t.Fatal(err)
	}
	candidate := inventoryPair("a", "region", "same-cflb", now.Add(12*time.Minute), now.Add(time.Hour))
	result, err := s.CommitMint(types.MintCommit{Pair: candidate, At: now})
	if err != nil {
		t.Fatal(err)
	}
	pairs, donors := s.TicketInventory()
	effective := ticket.Rehome(pairs["a"], ticket.Best(pairs, donors, now), now)
	if effective.Cookie != old.Cookie || effective.ExpiresAt != now.Add(time.Hour) ||
		result.Gain.ExtendedMS != (50*time.Minute).Milliseconds() ||
		result.BeforeExpiresAt != now.Add(10*time.Minute) || result.AfterExpiresAt != now.Add(time.Hour) {
		t.Fatalf("same-value donor renewal replaced long oailb with shorter stock: remaining=%s gain=%+v",
			effective.ExpiresAt.Sub(now), result.Gain)
	}
}

func TestInventoryCommitRetainsLongerStockAndIndependentDonor(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	now := time.Now().UTC().Truncate(time.Second)
	old := inventoryPair("a", "region", "old", now.Add(40*time.Minute), now.Add(40*time.Minute))
	other := inventoryPair("b", "region", "short", now.Add(55*time.Minute), now.Add(8*time.Minute))
	for _, pair := range []types.Pair{old, other} {
		if err := s.PutPair(pair); err != nil {
			t.Fatal(err)
		}
	}
	candidate := inventoryPair("a", "region", "new-donor", now.Add(12*time.Minute), now.Add(time.Hour))
	result, err := s.CommitMint(types.MintCommit{Pair: candidate, At: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.PairChanged || result.Pair.ExpiresAt != old.ExpiresAt || result.Gain.ExtendedMS != 0 ||
		result.Gain.DonorExtendedMS != (20*time.Minute).Milliseconds() {
		t.Fatalf("short candidate was preferred or donor gain lost: %+v", result.Gain)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir)
	pairs, donors := s.TicketInventory()
	if pairs["a"] != old || donors["region"].Cflb != "new-donor" {
		t.Fatal("stock or independent donor lost on restart")
	}
	projected := ticket.Rehome(pairs["b"], ticket.Best(pairs, donors, now), now)
	if projected.ExpiresAt != now.Add(55*time.Minute) {
		t.Fatal("rejected short ticket's donor did not rescue another ticket")
	}
	donors["region"] = types.RegionDonor{}
	_, untouched := s.TicketInventory()
	if untouched["region"].Cflb != "new-donor" {
		t.Fatal("read snapshot aliases durable donor state")
	}
}

func TestInventoryCommitComparesEffectiveIncumbentAndKeepsOldRegionDonor(t *testing.T) {
	s := open(t, t.TempDir())
	now := time.Now().UTC().Truncate(time.Second)
	old := inventoryPair("a", "old-region", "old", now.Add(time.Hour), now.Add(6*time.Minute))
	donor := inventoryPair("b", "old-region", "borrowed", now.Add(30*time.Minute), now.Add(30*time.Minute))
	for _, pair := range []types.Pair{old, donor} {
		if err := s.PutPair(pair); err != nil {
			t.Fatal(err)
		}
	}
	short := inventoryPair("a", "new-region", "new", now.Add(12*time.Minute), now.Add(12*time.Minute))
	result, err := s.CommitMint(types.MintCommit{Pair: short, At: now})
	if err != nil || result.PairChanged || result.Pair.ExpiresAt != now.Add(30*time.Minute) {
		t.Fatalf("raw expiry used instead of effective expiry: err=%v changed=%v expiry=%s",
			err, result.PairChanged, result.Pair.ExpiresAt)
	}
	long := inventoryPair("b", "new-region", "newer", now.Add(time.Hour), now.Add(time.Hour))
	if _, err := s.CommitMint(types.MintCommit{Pair: long, At: now}); err != nil {
		t.Fatal(err)
	}
	pairs, donors := s.TicketInventory()
	if donors["old-region"].Cflb != "borrowed" ||
		ticket.Rehome(pairs["a"], ticket.Best(pairs, donors, now), now).ExpiresAt != now.Add(30*time.Minute) {
		t.Fatal("replacement in a different region destroyed the old region's donor")
	}
}

func TestInventoryCommitConcurrentAndLateResultsNeverShortenStock(t *testing.T) {
	s := open(t, t.TempDir())
	now := time.Now().UTC().Truncate(time.Second)
	var wg sync.WaitGroup
	for minutes := 10; minutes <= 60; minutes++ {
		wg.Add(1)
		go func(minutes int) {
			defer wg.Done()
			expiry := now.Add(time.Duration(minutes) * time.Minute)
			pair := inventoryPair("a", "region", fmt.Sprint(minutes), expiry, expiry)
			if _, err := s.CommitMint(types.MintCommit{Pair: pair, At: now}); err != nil {
				t.Error(err)
			}
		}(minutes)
	}
	wg.Wait()
	stale := inventoryPair("a", "region", "late", now.Add(12*time.Minute), now.Add(12*time.Minute))
	result, err := s.CommitMint(types.MintCommit{Pair: stale, At: now.Add(time.Minute)})
	if err != nil || result.Pair.ExpiresAt != now.Add(time.Hour) || result.PairChanged {
		t.Fatalf("late response shortened committed stock: err=%v expiry=%s", err, result.Pair.ExpiresAt)
	}
}

func TestInventoryCommitFailureRollsBackPairDonorAndRevision(t *testing.T) {
	for _, shortWrite := range []bool{false, true} {
		t.Run(fmt.Sprint(shortWrite), func(t *testing.T) {
			s := open(t, t.TempDir())
			now := time.Now().UTC().Truncate(time.Second)
			old := inventoryPair("a", "region", "old", now.Add(10*time.Minute), now.Add(10*time.Minute))
			if err := s.PutPair(old); err != nil {
				t.Fatal(err)
			}
			beforePairs, beforeDonors := s.TicketInventory()
			revision, seq := s.Revisions(), s.st.JournalSequence
			s.journal.file = &faultyJournal{journalIO: s.journal.file, shortWrite: shortWrite, syncFail: !shortWrite}
			candidate := inventoryPair("a", "region", "new", now.Add(time.Hour), now.Add(time.Hour))
			if _, err := s.CommitMint(types.MintCommit{Pair: candidate, At: now}); err == nil {
				t.Fatal("failed inventory commit acknowledged")
			}
			afterPairs, afterDonors := s.TicketInventory()
			if !reflect.DeepEqual(beforePairs, afterPairs) || !reflect.DeepEqual(beforeDonors, afterDonors) ||
				s.Revisions() != revision || s.st.JournalSequence != seq {
				t.Fatal("failed commit leaked inventory or invalidated caches")
			}
			if _, err := s.CommitMint(types.MintCommit{Pair: candidate, At: now}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
