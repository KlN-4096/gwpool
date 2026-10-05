package store

import (
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestSlotIndexTracksOnlyCommittedState(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	account, err := s.AddAccount(types.Account{Creds: types.Creds{AccountID: "one"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{account.ID, account.ID + 1} {
		if err := s.PutSlot(types.Slot{AccountID: id, Gateway: "g", LastVerdict: "old"}); err != nil {
			t.Fatal(err)
		}
	}
	original := s.journal.file
	s.journal.file = &faultyJournal{journalIO: original, syncFail: true}
	if err := s.UpdateSlot(account.ID, "g", func(sl *types.Slot) { sl.LastVerdict = "failed" }); err == nil {
		t.Fatal("injected persistence failure ignored")
	}
	if got := s.SlotsForAccount(account.ID); len(got) != 1 || got[0].LastVerdict != "old" {
		t.Fatal("failed change reached the slot index")
	}
	copy := s.SlotsForAccount(account.ID)
	copy[0].LastVerdict = "mutated"
	if s.SlotsForAccount(account.ID)[0].LastVerdict != "old" {
		t.Fatal("index aliases its returned rows")
	}
	if err := s.UpdateSlot(account.ID, "g", func(sl *types.Slot) { sl.LastVerdict = "new" }); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	if got := reopened.SlotsForAccount(account.ID); len(got) != 1 || got[0].LastVerdict != "new" {
		t.Fatal("index not rebuilt after journal replay")
	}
	if err := reopened.DeleteAccount(account.ID); err != nil {
		t.Fatal(err)
	}
	if len(reopened.SlotsForAccount(account.ID)) != 0 || len(reopened.SlotsForAccount(account.ID+1)) != 1 {
		t.Fatal("cascading deletion corrupted slot index")
	}
}

func TestBusiestDeliveriesCacheExpiresAndRevises(t *testing.T) {
	s := open(t, t.TempDir())
	account, err := s.AddAccount(types.Account{Enabled: true, Creds: types.Creds{AccountID: "one"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := s.PutSlot(types.Slot{AccountID: account.ID, Gateway: "g", LastDeliver: now}); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		cutoff time.Time
		want   int
	}{{now.Add(-time.Second), 1}, {now.Add(-time.Nanosecond), 1}, {now, 0}, {now.Add(-time.Second), 1}} {
		if got := s.BusiestDeliveries(check.cutoff); got != check.want {
			t.Fatalf("cutoff %v: got %d want %d", check.cutoff, got, check.want)
		}
	}
	if err := s.SetAccountEnabled(account.ID, false); err != nil {
		t.Fatal(err)
	}
	if got := s.BusiestDeliveries(now.Add(-time.Second)); got != 0 {
		t.Fatal("disabled account retained in demand cache")
	}
	if err := s.SetAccountEnabled(account.ID, true); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSlot(types.Slot{AccountID: account.ID, Gateway: "g2", LastDeliver: now}); err != nil {
		t.Fatal(err)
	}
	if got := s.BusiestDeliveries(now.Add(-time.Second)); got != 2 {
		t.Fatal("new delivery failed to invalidate demand cache")
	}
}
