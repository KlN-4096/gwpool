package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gwpool/internal/types"
)

var errCheckpointInjected = errors.New("injected checkpoint failure")

type closeFailureJournal struct{ journalIO }

func (f closeFailureJournal) Close() error {
	_ = f.journalIO.Close()
	return errCheckpointInjected
}

func TestCheckpointFailureBoundariesPreserveAcknowledgedState(t *testing.T) {
	for _, mode := range []string{"snapshot-write", "snapshot-before-replace", "snapshot-after-replace",
		"journal-write", "journal-close", "journal-before-replace", "journal-after-replace", "journal-reopen"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			s := open(t, dir)
			if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "acknowledged"}); err != nil {
				t.Fatal(err)
			}
			s.checkpointIO.writeFile = func(path string, body []byte) error {
				if (mode == "snapshot-write" && strings.HasSuffix(path, stateFile+".tmp")) ||
					(mode == "journal-write" && strings.HasSuffix(path, journalFile+".tmp")) {
					return errCheckpointInjected
				}
				return writeDurableFile(path, body)
			}
			s.checkpointIO.replaceFile = func(from, to string) error {
				target := "snapshot"
				if filepath.Base(to) == journalFile {
					target = "journal"
				}
				if mode == target+"-before-replace" {
					return errCheckpointInjected
				}
				if err := durableReplace(from, to); err != nil {
					return err
				}
				if mode == target+"-after-replace" {
					// Same uncertainty as rename success followed by a failed
					// directory persistence barrier on POSIX.
					return errCheckpointInjected
				}
				return nil
			}
			s.checkpointIO.openFile = func(path string) (*os.File, error) {
				if mode == "journal-reopen" {
					return nil, errCheckpointInjected
				}
				return os.OpenFile(path, os.O_RDWR, filePerm)
			}
			if mode == "journal-close" {
				s.journal.file = closeFailureJournal{s.journal.file}
			}
			if err := s.Checkpoint(); !errors.Is(err, errCheckpointInjected) {
				t.Fatalf("failure seam not reached: %v", err)
			}
			uncertain := mode == "journal-close" || mode == "journal-before-replace" ||
				mode == "journal-after-replace" || mode == "journal-reopen"
			err := s.PutSlot(types.Slot{AccountID: 2, Gateway: "later"})
			if (err != nil) != uncertain {
				t.Fatalf("write after checkpoint error=%v uncertain=%v", err, uncertain)
			}
			_ = s.Close() // Close surfaces poisoned persistence where expected.
			reopened := open(t, dir)
			if _, ok := reopened.Slot(1, "acknowledged"); !ok {
				t.Fatal("checkpoint failure lost an existing ACK")
			}
			_, hasLater := reopened.Slot(2, "later")
			if hasLater == uncertain {
				t.Fatal("checkpoint failure changed subsequent ACK boundary")
			}
		})
	}
}
