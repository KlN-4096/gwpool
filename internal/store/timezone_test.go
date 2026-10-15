package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestTicketExitTimezoneSurvivesWALAndCheckpoint(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		dir := t.TempDir()
		st := open(t, dir)
		now := time.Now().UTC().Truncate(time.Second)
		pair := inventoryPair("unified-142", "east-asia", "fake-cflb", now.Add(time.Hour), now.Add(time.Hour))
		pair.MintExit = types.ExitTimezoneObservation{Timezone: "Asia/Tokyo", ObservedAt: now.Add(-time.Minute)}
		if _, err := st.CommitMint(types.MintCommit{Pair: pair, At: now}); err != nil {
			t.Fatal(err)
		}
		if checkpoint {
			if err := st.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		restored, ok := open(t, dir).Pair(pair.Gateway)
		if !ok || restored.MintExit != pair.MintExit {
			t.Fatal("durable replay lost exact ticket provenance")
		}
	}
	legacy := types.Pair{Gateway: "legacy"}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "MintExit") {
		t.Fatal("unknown provenance changed legacy snapshot shape")
	}
	if err := json.Unmarshal([]byte(`{"Gateway":"legacy","Cookie":"fake"}`), &legacy); err != nil ||
		legacy.MintExit != (types.ExitTimezoneObservation{}) {
		t.Fatal("legacy pair did not retain unknown provenance")
	}
}

func TestTicketExitTimezonePreservesIncumbentAndRollsBackWithStock(t *testing.T) {
	st := open(t, t.TempDir())
	now := time.Now().UTC().Truncate(time.Second)
	old := inventoryPair("unified-142", "east-asia", "old", now.Add(time.Hour), now.Add(40*time.Minute))
	old.MintExit = types.ExitTimezoneObservation{Timezone: "Asia/Tokyo", ObservedAt: now.Add(-time.Minute)}
	if err := st.PutPair(old); err != nil {
		t.Fatal(err)
	}
	shorter := inventoryPair(old.Gateway, old.RegionKey, "long-donor", now.Add(20*time.Minute), now.Add(2*time.Hour))
	shorter.MintExit = types.ExitTimezoneObservation{Timezone: "America/New_York", ObservedAt: now}
	result, err := st.CommitMint(types.MintCommit{Pair: shorter, At: now})
	if err != nil || result.Pair.MintExit != old.MintExit || result.Gain.DonorExtendedMS <= 0 {
		t.Fatal("donor-only candidate overwrote the retained oailb's provenance", err)
	}
	pairs, _ := st.TicketInventory()
	copy := pairs[old.Gateway]
	copy.MintExit.Timezone = "Europe/Paris"
	pairs[old.Gateway] = copy
	retained, _ := st.Pair(old.Gateway)
	if retained.MintExit != old.MintExit {
		t.Fatal("inventory read leaked mutable provenance")
	}
	candidate := inventoryPair(old.Gateway, old.RegionKey, "replacement", now.Add(3*time.Hour), now.Add(3*time.Hour))
	candidate.MintExit = shorter.MintExit
	seq, revision := st.st.JournalSequence, st.Revisions()
	st.journal.file = &faultyJournal{journalIO: st.journal.file, syncFail: true}
	if _, err := st.CommitMint(types.MintCommit{Pair: candidate, At: now}); err == nil {
		t.Fatal("failed metadata/stock transaction was acknowledged")
	}
	retained, _ = st.Pair(old.Gateway)
	if retained.MintExit != old.MintExit || st.st.JournalSequence != seq || st.Revisions() != revision {
		t.Fatal("failed stock write leaked new timezone metadata or revision")
	}
}
