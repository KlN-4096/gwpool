package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gwpool/internal/routes"
	"gwpool/internal/types"
)

func TestRecoverySourcesKeepBothLandingsAndReplay(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	now := time.Now().UTC().Truncate(time.Second)
	for i, gateway := range []string{"a", "b", "a"} {
		// The last A is a late older observation; it must not refresh its age.
		at := now.Add(time.Duration(i%2) * time.Minute)
		if i == 2 {
			at = now.Add(-time.Minute)
		}
		attempt := inventoryRouteAttempt(at)
		attempt.Gateway = gateway
		if _, err := s.CommitMint(types.MintCommit{At: at, Route: &attempt}); err != nil {
			t.Fatal(err)
		}
	}
	before := s.RecoverySources(now.Add(time.Minute))
	if len(before) != 2 {
		t.Fatal("A to B drift replaced a source instead of preserving both")
	}
	for _, source := range before {
		if source.Gateway == "a" && source.LastSeen != now {
			t.Fatal("late observation regressed the source age")
		}
	}
	snapshot := cloneState(s.st)
	for key := range snapshot.Recovery {
		delete(snapshot.Recovery, key)
		delete(before, key)
	}
	if len(s.RecoverySources(now.Add(time.Minute))) != 2 {
		t.Fatal("reader/checkpoint clone aliases committed sources")
	}
	before = s.RecoverySources(now.Add(time.Minute))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir)
	if !reflect.DeepEqual(before, s.RecoverySources(now.Add(time.Minute))) {
		t.Fatal("WAL replay changed recovery sources")
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir)
	if !reflect.DeepEqual(before, s.RecoverySources(now.Add(time.Minute))) {
		t.Fatal("checkpoint restart changed recovery sources")
	}
}

func TestRecoverySourcesFailureRollsBackAllState(t *testing.T) {
	s := open(t, t.TempDir())
	now := time.Now().UTC().Truncate(time.Second)
	attempt := inventoryRouteAttempt(now)
	before, revision := cloneState(s.st), s.Revisions()
	s.journal.file = &faultyJournal{journalIO: s.journal.file, syncFail: true}
	_, err := s.CommitMint(types.MintCommit{At: now, Route: &attempt,
		Pair: inventoryPair(attempt.Gateway, "country-us", "cflb", now.Add(time.Hour), now.Add(time.Hour))})
	if err == nil {
		t.Fatal("failed WAL acknowledged")
	}
	if !reflect.DeepEqual(before, s.st) || s.Revisions() != revision {
		t.Fatal("failed commit leaked Pair/Donor/Route/Recovery/sequence/revision")
	}
}

func TestRecoverySourcesMigrateOnlyExactOriginalObservation(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC().Truncate(time.Second)
	at := now.Add(-2 * time.Hour)
	attempt := inventoryRouteAttempt(at)
	state := routes.Advance(types.RouteState{}, attempt, types.StockGain{})
	// A later failure is not a new successful observation timestamp.
	failed := attempt
	failed.StartedAt, failed.At, failed.Status, failed.Failure = now, now, 429, "upstream"
	state = routes.Advance(state, failed, types.StockGain{})
	legacy := State{Routes: map[string]types.RouteState{routes.Key(attempt.Route): state},
		Pairs: map[string]types.Pair{"unproven": {Gateway: "unproven", MintedBy: 7, RegionKey: "country-us"}},
		Mints: map[string]types.RegionMint{"legacy": {AccountID: 7, RegionKey: "country-us", Gateway: "unproven"}}}
	body, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), body, filePerm); err != nil {
		t.Fatal(err)
	}
	s := open(t, dir)
	sources := s.RecoverySources(now)
	if len(sources) != 1 {
		t.Fatal("migration dropped exact evidence or manufactured legacy Pair/Mint identities")
	}
	for _, source := range sources {
		if source.LastSeen != at || source.Route != attempt.Route || source.Gateway != attempt.Gateway {
			t.Fatal("migration invented time, identity or source")
		}
	}
	seq := s.st.JournalSequence
	if err := s.prepareRecoverySources(now); err != nil || s.st.JournalSequence != seq {
		t.Fatal("migration not idempotent")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir)
	if !reflect.DeepEqual(sources, s.RecoverySources(now)) {
		t.Fatal("migration replay refreshed the original observation")
	}
	if err := s.prepareRecoverySources(at.Add(routes.RecoveryRetention)); err != nil {
		t.Fatal(err)
	}
	if len(s.st.Recovery) != 0 {
		t.Fatal("retention boundary failed to persist removal")
	}
}

func TestRecoverySourcesRejectInvalidPersistence(t *testing.T) {
	dir := t.TempDir()
	source := types.RecoverySource{Gateway: "a", LastSeen: time.Now().UTC()}
	body, err := json.Marshal(State{Recovery: map[string]types.RecoverySource{"bad": source}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), body, filePerm); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err == nil {
		_ = s.Close()
		t.Fatal("invalid persisted source accepted")
	}
}
