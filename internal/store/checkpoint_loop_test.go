package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gwpool/internal/types"
)

func readCheckpointState(t *testing.T, dir string) State {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(dir, stateFile))
	if err != nil {
		t.Fatal(err)
	}
	var state State
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestCheckpointBackgroundIntervalAndExplicitDurability(t *testing.T) {
	dir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		synctest.Wait()
		initial := readCheckpointState(t, dir).JournalSequence
		if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "background"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if readCheckpointState(t, dir).JournalSequence != initial {
			t.Fatal("small durable mutations still rewrite the whole snapshot every minute")
		}
		time.Sleep(4*time.Minute - time.Second)
		synctest.Wait()
		if readCheckpointState(t, dir).JournalSequence != initial {
			t.Fatal("checkpoint occurred before the maximum background interval")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if readCheckpointState(t, dir).JournalSequence <= initial {
			t.Fatal("dirty state was not checkpointed at the maximum interval")
		}
		if err := s.PutSlot(types.Slot{AccountID: 2, Gateway: "explicit"}); err != nil {
			t.Fatal(err)
		}
		before := readCheckpointState(t, dir).JournalSequence
		if err := s.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if readCheckpointState(t, dir).JournalSequence <= before {
			t.Fatal("explicit checkpoint was delayed by the background policy")
		}
	})
}

func TestCheckpointUsesCompactJSONWithoutChangingState(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "compact"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(dir, stateFile))
	if err != nil {
		t.Fatal(err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, body); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, compact.Bytes()) {
		t.Fatal("checkpoint still creates a second indented whole-state encoding")
	}
	if got := readCheckpointState(t, dir); !got.OwnerKeysMigrated || len(got.Slots) != 1 {
		t.Fatal("compact snapshot changed the acknowledged state")
	}
}

func TestCheckpointByteThresholdPreemptsBackgroundInterval(t *testing.T) {
	dir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		synctest.Wait()
		before := readCheckpointState(t, dir).JournalSequence
		start := time.Now()
		if err := s.PutPair(types.Pair{Gateway: "large",
			Cookie: strings.Repeat("offline", checkpointBytes/len("offline")+1)}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if time.Since(start) >= time.Minute || readCheckpointState(t, dir).JournalSequence <= before {
			t.Fatal("size-triggered checkpoint waited for the background timer")
		}
	})
}

func TestCheckpointBackgroundFailureRetriesWithoutNewWrites(t *testing.T) {
	dir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		synctest.Wait()
		var attempts atomic.Int32
		s.checkpointIO.writeFile = func(path string, body []byte) error {
			if strings.HasSuffix(path, stateFile+".tmp") && attempts.Add(1) == 1 {
				return errors.New("offline checkpoint retry fixture")
			}
			return writeDurableFile(path, body)
		}
		if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "retry"}); err != nil {
			t.Fatal(err)
		}
		s.journal.wake <- struct{}{}
		synctest.Wait()
		if attempts.Load() != 1 {
			t.Fatal("first checkpoint attempt did not reach the injected failure")
		}
		for range 3 {
			s.journal.wake <- struct{}{}
			synctest.Wait()
		}
		time.Sleep(time.Minute - time.Second)
		synctest.Wait()
		if attempts.Load() != 1 {
			t.Fatal("failed checkpoint hot-looped instead of backing off")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if len(readCheckpointState(t, dir).Slots) != 1 {
			t.Fatal("checkpoint did not retry one minute after failure without new writes")
		}
	})
}

func TestCloseBeforeCheckpointKeepsAcknowledgedWAL(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	before := readCheckpointState(t, dir).JournalSequence
	if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "wal-only"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if readCheckpointState(t, dir).JournalSequence != before {
		t.Fatal("Close unexpectedly added a full checkpoint")
	}
	reopened := open(t, dir)
	if _, ok := reopened.Slot(1, "wal-only"); !ok {
		t.Fatal("acknowledged WAL data was lost before the next checkpoint")
	}
}
