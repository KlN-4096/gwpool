package store

import (
	"fmt"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestCleanupAtomicRecheckAndReplay(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	var candidates []types.AccountCleanupCandidate
	var ids []int64
	for i := 0; i < 6; i++ {
		a, err := s.AddAccount(types.Account{Creds: types.Creds{AccessToken: "fake", AccountID: fmt.Sprint(i)}, ViewToken: fmt.Sprint("view", i)})
		if err != nil {
			t.Fatal(err)
		}
		if err = s.DisableAccount(a.ID, "铸票 HTTP 401：expired"); err != nil {
			t.Fatal(err)
		}
		if err = s.PutSlot(types.Slot{AccountID: a.ID, Gateway: "g"}); err != nil {
			t.Fatal(err)
		}
		if err = s.NoteRegionMint(a.ID, "US", "g", time.Now()); err != nil {
			t.Fatal(err)
		}
		a, _ = s.AccountByViewToken(a.ViewToken)
		ids = append(ids, a.ID)
		if i < 5 {
			candidates = append(candidates, types.CleanupCandidate(a))
		}
	}
	if err := s.SetTokens(ids[1], "refreshed", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.SetTokens(ids[2], "replaced", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.DisableAccount(ids[2], "铸票 HTTP 401：expired"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAccountEnabled(ids[3], true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAccountEnabled(ids[3], false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertGateway("g", ids[0], types.Exit{Region: "US"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutPair(types.Pair{Gateway: "g", Cookie: "fake"}); err != nil {
		t.Fatal(err)
	}
	before := s.st.JournalSequence
	// One-shot sync fault must restore accounts and their dependent records.
	s.journal.file = &faultyJournal{journalIO: s.journal.file, syncFail: true}
	if n, err := s.DeleteUnauthorizedAccounts(candidates); err == nil || n != 0 {
		t.Fatal("failed commit acknowledged")
	}
	if len(s.Accounts()) != 6 || len(s.Slots()) != 6 || len(s.RegionMints()) != 6 {
		t.Fatal("failed cleanup changed state")
	}
	n, err := s.DeleteUnauthorizedAccounts(candidates)
	if err != nil || n != 2 {
		t.Fatalf("deleted=%d err=%v", n, err)
	}
	if s.st.JournalSequence != before+1 {
		t.Fatal("cleanup was not a single commit")
	}
	if len(s.Accounts()) != 4 || len(s.Slots()) != 4 || len(s.RegionMints()) != 4 {
		t.Fatal("wrong cascade")
	}
	if len(s.Pairs()) != 1 || len(s.Gateways()) != 1 {
		t.Fatal("shared history removed")
	}
	if n, err = s.DeleteUnauthorizedAccounts(candidates); err != nil || n != 0 {
		t.Fatal("replay is not idempotent")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	if len(reopened.Accounts()) != 4 || len(reopened.Slots()) != 4 || len(reopened.RegionMints()) != 4 || len(reopened.Pairs()) != 1 {
		t.Fatal("reopen differs")
	}
}

func TestCleanupWaitsForInFlightRefresh(t *testing.T) {
	s := open(t, t.TempDir())
	a, err := s.AddAccount(types.Account{Creds: types.Creds{AccessToken: "fake", AccountID: "refresh"}, ViewToken: "refresh-view"})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.DisableAccount(a.ID, "铸票 HTTP 401：expired"); err != nil {
		t.Fatal(err)
	}
	a, _ = s.AccountByViewToken(a.ViewToken)
	unlock := s.LockAccount(a.ID)
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := s.DeleteUnauthorizedAccounts([]types.AccountCleanupCandidate{types.CleanupCandidate(a)})
		done <- result{n, err}
	}()
	select {
	case <-done:
		unlock()
		t.Fatal("cleanup bypassed in-flight account lock")
	case <-time.After(20 * time.Millisecond):
	}
	if err = s.SetTokens(a.ID, "fresh", ""); err != nil {
		unlock()
		t.Fatal(err)
	}
	unlock()
	select {
	case got := <-done:
		if got.err != nil || got.n != 0 {
			t.Fatalf("refresh lost: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish")
	}
}

func TestCleanupRejectsRecoveredThenRejectedABA(t *testing.T) {
	for _, change := range []string{"enable", "upload", "refresh"} {
		t.Run(change, func(t *testing.T) {
			s := open(t, t.TempDir())
			a, err := s.AddAccount(types.Account{Creds: types.Creds{AccessToken: "fake", AccountID: "aba"}, ViewToken: "aba"})
			if err != nil {
				t.Fatal(err)
			}
			if err = s.DisableAccount(a.ID, "铸票 HTTP 401：expired"); err != nil {
				t.Fatal(err)
			}
			a, _ = s.Account(a.ID)
			before := types.CleanupCandidate(a)
			switch change {
			case "enable":
				err = s.SetAccountEnabled(a.ID, true)
			case "upload":
				_, err = s.AddAccount(a)
			case "refresh":
				err = s.SetTokens(a.ID, a.Creds.AccessToken, "")
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = s.DisableAccount(a.ID, "铸票 HTTP 401：expired"); err != nil {
				t.Fatal(err)
			}
			if n, err := s.DeleteUnauthorizedAccounts([]types.AccountCleanupCandidate{before}); err != nil || n != 0 {
				t.Fatalf("old preview deleted recovered revision: %d %v", n, err)
			}
		})
	}
}
