package store

import (
	"fmt"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestExternalStatisticsAreIndependentAndPersisted(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	now := time.Now().UTC()
	for i := range 401 {
		if err := s.RecordExternalUsage(types.ExternalConsumerID(fmt.Sprintf("member-%d", i)), "unified-1", now, true, false); err != nil {
			t.Fatal(err)
		}
	}
	id := types.ExternalConsumerID("member-0")
	if err := s.RecordExternalUsage(id, "unified-2", now.Add(time.Second), false, true); err != nil {
		t.Fatal(err)
	}
	snapshot := s.ExternalUsage()
	delete(snapshot, id)
	if got := s.ExternalUsage()[id]; got.Requests != 2 || got.Delivered != 1 || got.Limited != 1 {
		t.Fatalf("wrong independent summary: %+v", got)
	}
	if len(s.Accounts()) != 0 || len(s.Slots()) != 0 || s.BusiestDeliveries(now.Add(-time.Hour)) != 0 {
		t.Fatal("statistics created accounts, slots or supply decisions")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	if len(reopened.ExternalUsage()) != 401 || reopened.ExternalUsage()[id].Requests != 2 {
		t.Fatal("external summaries did not survive snapshot/journal reopen")
	}
}
