package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"gwpool/internal/contact"
	"gwpool/internal/types"
)

func TestJournalReplaysAllMutationKinds(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	owner, err := s.AddOwner("owner")
	if err != nil {
		t.Fatal(err)
	}
	account, err := s.AddAccount(types.Account{OwnerID: owner.ID, Enabled: true,
		Creds: types.Creds{AccessToken: "fake", AccountID: "domain"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	operations := []func() error{
		func() error { return s.SetAccountVia(account.ID, 5) },
		func() error { return s.SetAccountEnabled(account.ID, false) },
		func() error { return s.DisableAccount(account.ID, "invalid") },
		func() error { return s.SetTokens(account.ID, "fake-new", "fake-refresh") },
		func() error { _, err := s.UpsertGateway("g", account.ID, types.Exit{Region: "US"}); return err },
		func() error {
			return s.UpdateGateway("g", func(g *types.Gateway) { g.ReachableExits[0].City = "test" })
		},
		func() error {
			return s.PutPair(types.Pair{Gateway: "g", Cookie: "fake", CflbExpiresAt: now.Add(time.Hour), ExpiresAt: now.Add(time.Hour)})
		},
		func() error { return s.PutPair(types.Pair{Gateway: "removed", Cookie: "fake"}) },
		func() error { return s.DeletePair("removed") },
		func() error { return s.PutSlot(types.Slot{AccountID: account.ID, Gateway: "g"}) },
		func() error { return s.UpdateSlot(account.ID, "g", func(sl *types.Slot) { sl.LastTouch = now }) },
		func() error { return s.NoteRegionMint(account.ID, "US", "g", now) },
		func() error { _, err := s.MigrateAttempts(); return err },
		func() error { return s.ReserveAttempt("domain", "US", 2, 50, now) },
		func() error {
			_, err := s.AddContactReport(types.ContactReport{ReporterID: -owner.ID,
				ContactObservation: types.ContactObservation{ID: "contact", At: now, Outcome: "full"}})
			return err
		},
		func() error {
			_, err := s.AddCooldownReport(types.CooldownReport{ID: "cooldown", ReporterID: -owner.ID, ReceivedAt: now})
			return err
		},
	}
	for i, operation := range operations {
		if err := operation(); err != nil {
			t.Fatalf("mutation %d: %v", i, err)
		}
	}
	// Cascading deletion must preserve pair, gateway, report and attempt history.
	removed, err := s.AddAccount(types.Account{Creds: types.Creds{AccountID: "removed"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutSlot(types.Slot{AccountID: removed.ID, Gateway: "g"}); err != nil {
		t.Fatal(err)
	}
	if err := s.NoteRegionMint(removed.ID, "US", "g", now); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAccount(removed.ID); err != nil {
		t.Fatal(err)
	}
	before := cloneState(s.st)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	if !reflect.DeepEqual(before, reopened.st) {
		t.Fatal("replay differs from committed memory")
	}
	if reopened.st.NextAccountID != removed.ID {
		t.Fatal("deleted ID high water lost")
	}
}

func TestJournalTailAndCorruption(t *testing.T) {
	for _, mode := range []string{"header-tail", "body-tail", "checksum", "length", "sequence", "header"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			s := open(t, dir)
			if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "committed"}); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, journalFile)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			goodLength := len(raw)
			switch mode {
			case "header-tail":
				raw = append(raw, 1, 2, 3)
			case "body-tail":
				record := raw[journalHeaderSize:]
				raw = append(raw, record[:journalRecordHeader+2]...)
			case "checksum":
				raw[len(raw)-1] ^= 1
			case "length":
				raw[journalHeaderSize] ^= 1
			case "sequence":
				raw[8]++ // journal base now lies beyond the snapshot
			case "header":
				raw[0] ^= 1
			}
			if err := os.WriteFile(path, raw, filePerm); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dir)
			if mode != "header-tail" && mode != "body-tail" {
				if err == nil {
					_ = reopened.Close()
					t.Fatal("corrupt committed journal accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reopened.Close() }()
			if _, ok := reopened.Slot(1, "committed"); !ok {
				t.Fatal("valid prefix lost")
			}
			if info, err := os.Stat(path); err != nil || info.Size() != int64(goodLength) {
				t.Fatal("incomplete tail not durably repaired")
			}
			if err := reopened.PutSlot(types.Slot{AccountID: 1, Gateway: "after-tail"}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type faultyJournal struct {
	journalIO
	shortWrite, syncFail, truncateFail bool
	headerFail                         bool
}

func (f *faultyJournal) WriteAt(body []byte, offset int64) (int, error) {
	if f.headerFail {
		f.headerFail = false
		n, err := f.journalIO.WriteAt(body[:len(body)/2], offset)
		if err != nil {
			return n, err
		}
		return n, io.ErrShortWrite
	}
	return f.journalIO.WriteAt(body, offset)
}

func (f *faultyJournal) Write(body []byte) (int, error) {
	if f.shortWrite {
		f.shortWrite = false
		n, err := f.journalIO.Write(body[:len(body)/2])
		if err != nil {
			return n, err
		}
		return n, io.ErrShortWrite
	}
	return f.journalIO.Write(body)
}

func (f *faultyJournal) Sync() error {
	if f.syncFail {
		f.syncFail = false
		return errors.New("injected sync failure")
	}
	return f.journalIO.Sync()
}

func (f *faultyJournal) Truncate(size int64) error {
	if f.truncateFail {
		return errors.New("injected truncate failure")
	}
	return f.journalIO.Truncate(size)
}

func TestJournalFailedCommitRollsBackAndCanRetry(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(fmt.Sprint(short), func(t *testing.T) {
			dir := t.TempDir()
			s := open(t, dir)
			s.journal.file = &faultyJournal{journalIO: s.journal.file, shortWrite: short, syncFail: !short}
			seq := s.st.JournalSequence
			revision := s.Revisions()
			if err := s.ReserveAttempt("domain", "US", 2, 50, time.Now()); err == nil {
				t.Fatal("write failure acknowledged")
			}
			if len(s.CountryAttempts("domain")) != 0 || s.st.JournalSequence != seq || s.Revisions() != revision {
				t.Fatal("failed transaction became visible")
			}
			if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "success"}); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := open(t, dir)
			if len(reopened.CountryAttempts("domain")) != 0 {
				t.Fatal("retry committed failed reservation")
			}
			if _, ok := reopened.Slot(1, "success"); !ok {
				t.Fatal("successful retry lost")
			}
		})
	}
}

func TestJournalCommitHeaderFailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	s.journal.file = &faultyJournal{journalIO: s.journal.file, headerFail: true}
	if err := s.ReserveAttempt("domain", "US", 1, 50, time.Now()); err == nil {
		t.Fatal("torn commit boundary was acknowledged")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	if len(reopened.CountryAttempts("domain")) != 0 {
		t.Fatal("failed commit survived header rollback")
	}
	if err := reopened.ReserveAttempt("domain", "US", 1, 50, time.Now()); err != nil {
		t.Fatal("rollback did not restore a usable commit boundary", err)
	}
}
func TestJournalUncertainRollbackStopsFurtherWrites(t *testing.T) {
	s := open(t, t.TempDir())
	s.journal.file = &faultyJournal{journalIO: s.journal.file, syncFail: true, truncateFail: true}
	if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "uncertain"}); err == nil {
		t.Fatal("ambiguous sync acknowledged")
	}
	if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "next"}); err == nil {
		t.Fatal("write continued after rollback failure")
	}
	if _, ok := s.Slot(1, "uncertain"); ok {
		t.Fatal("failed write visible in memory")
	}
}

func TestJournalCheckpointConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 80 {
			if err := s.PutSlot(types.Slot{AccountID: int64(i + 1), Gateway: "g"}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range 12 {
			if err := s.Checkpoint(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
	before := cloneState(s.st)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	if !reflect.DeepEqual(before, reopened.st) || len(reopened.Slots()) != 80 {
		t.Fatal("checkpoint discarded newer committed mutations")
	}
}

func TestJournalSnapshotBeforeCompactionCrash(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "before"}); err != nil {
		t.Fatal(err)
	}
	oldLog, err := os.ReadFile(filepath.Join(dir, journalFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "after"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, journalFile)
	newLog, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	combined := append(oldLog, newLog[journalHeaderSize:]...)
	copy(combined, journalHeader(binary.LittleEndian.Uint64(oldLog[8:]), s.st.JournalSequence, int64(len(combined))))
	if err := os.WriteFile(path, combined, filePerm); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	if len(reopened.Slots()) != 2 {
		t.Fatal("covered records double-applied or suffix lost")
	}
}

func TestJournalRejectsLossOfAcknowledgedRecords(t *testing.T) {
	for _, mode := range []string{"last-byte", "whole-record", "empty", "missing"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			s := open(t, dir)
			start := s.journal.offset
			if err := s.ReserveAttempt("domain", "US", 1, 50, time.Now()); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, journalFile)
			switch mode {
			case "last-byte":
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(path, info.Size()-1); err != nil {
					t.Fatal(err)
				}
			case "whole-record":
				if err := os.Truncate(path, start); err != nil {
					t.Fatal(err)
				}
			case "empty":
				if err := os.Truncate(path, 0); err != nil {
					t.Fatal(err)
				}
			case "missing":
				// Move only the disposable fixture to simulate a missing WAL.
				if err := os.Rename(path, path+".missing"); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := Open(dir)
			if err == nil {
				_ = reopened.Close()
				t.Fatal("lost ACK was accepted as a fresh budget")
			}
		})
	}
}

func TestJournalReportPruningReplaysExactOrder(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	now := time.Now().UTC()
	for i := range contact.MaxReporterReports {
		s.st.ContactReports = append(s.st.ContactReports, types.ContactReport{
			ReporterID: 1, ContactObservation: types.ContactObservation{ID: fmt.Sprint(i), At: now}})
	}
	// Seed a full table once, then prove the next commit is incremental.
	if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "seed"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddContactReport(types.ContactReport{ReporterID: 1,
		ContactObservation: types.ContactObservation{ID: "new", At: now}}); err != nil {
		t.Fatal(err)
	}
	want := s.ContactReports()
	info, err := os.Stat(filepath.Join(dir, journalFile))
	if err != nil || info.Size() > 2048 {
		t.Fatal("one report rewrote the report table")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	if !reflect.DeepEqual(want, reopened.ContactReports()) || want[0].ID != "1" {
		t.Fatal("replay changed report pruning/order")
	}
}

func TestJournalWritesDoNotRewriteSnapshot(t *testing.T) {
	s := open(t, t.TempDir())
	path := filepath.Join(s.dir, stateFile)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "one"}); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("incremental mutation rewrote the snapshot")
	}
}
