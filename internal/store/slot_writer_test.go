package store

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gwpool/internal/types"
)

type blockingSyncJournal struct {
	journalIO
	syncs   atomic.Int32
	entered chan struct{}
	resume  chan struct{}
	failOn  int32
}

func (j *blockingSyncJournal) Sync() error {
	n := j.syncs.Add(1)
	if n == 1 {
		close(j.entered)
		<-j.resume
	}
	if n == j.failOn {
		return errors.New("injected group sync failure")
	}
	return j.journalIO.Sync()
}

func TestSlotWriterBatchesDurablyAndRollsBackAsAGroup(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "sync_failure"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s := open(t, dir)
			journal := &blockingSyncJournal{journalIO: s.journal.file,
				entered: make(chan struct{}), resume: make(chan struct{})}
			t.Cleanup(func() {
				select {
				case <-journal.resume:
				default:
					close(journal.resume)
				}
			})
			if fail {
				journal.failOn = 2
			}
			s.journal.file = journal
			first := make(chan error, 1)
			go func() { first <- s.UpdateSlot(1, "first", func(*types.Slot) {}) }()
			select {
			case <-journal.entered:
			case <-time.After(time.Second):
				t.Fatal("writer never reached durability barrier")
			}
			const pending = 32
			results := make(chan error, pending)
			for i := range pending {
				go func(id int64) {
					results <- s.UpdateSlot(id, "g", func(slot *types.Slot) { slot.LastVerdict = "new" })
				}(int64(i + 2))
			}
			deadline := time.Now().Add(time.Second)
			for len(s.slotWriter.queue) != pending && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			queued := len(s.slotWriter.queue)
			select {
			case <-first:
				t.Fatal("ACK preceded Sync")
			default:
			}
			close(journal.resume)
			if queued != pending {
				t.Fatalf("only %d requests queued behind sync", queued)
			}
			if err := <-first; err != nil {
				t.Fatal(err)
			}
			for range pending {
				err := <-results
				if (err != nil) != fail {
					t.Fatalf("group result error=%v fail=%v", err, fail)
				}
			}
			wantSyncs, wantRows := int32(2), pending+1
			if fail {
				wantSyncs, wantRows = 3, 1 // includes durable truncate rollback
			}
			if got := journal.syncs.Load(); got != wantSyncs {
				t.Fatalf("expected %d syncs for 33 operations, got %d", wantSyncs, got)
			}
			if got := len(s.Slots()); got != wantRows {
				t.Fatalf("group rollback/publish retained %d rows, want %d", got, wantRows)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := open(t, dir)
			if len(reopened.Slots()) != wantRows {
				t.Fatal("group replay differs from acknowledged state")
			}
		})
	}
}

func TestSlotWriterSameKeySerializesAndStopsCleanly(t *testing.T) {
	s := open(t, t.TempDir())
	const requests = 40
	done := make(chan error, requests)
	for range requests {
		go func() {
			done <- s.UpdateSlot(1, "g", func(slot *types.Slot) {
				slot.LastTouch = slot.LastTouch.Add(time.Second)
			})
		}()
	}
	for range requests {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	slot, _ := s.Slot(1, "g")
	if !slot.LastTouch.Equal(time.Time{}.Add(requests * time.Second)) {
		t.Fatal("same-key mutations lost an update")
	}
	if err := s.UpdateSlot(1, "g", func(slot *types.Slot) {
		slot.LastVerdict = "must roll back"
		panic("test")
	}); err == nil {
		t.Fatal("callback panic did not abort the batch")
	}
	if got, _ := s.Slot(1, "g"); got != slot {
		t.Fatal("panicking mutation became visible")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateSlot(1, "g", func(*types.Slot) {}); err == nil {
		t.Fatal("closed writer accepted a new mutation")
	}
}
