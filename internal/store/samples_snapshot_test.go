package store

import (
	"encoding/json"
	"io"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestSampleSnapshotIsBoundedAndDoesNotBlockWriters(t *testing.T) {
	s := open(t, t.TempDir())
	if err := s.AppendSample(types.Sample{Gateway: "before"}); err != nil {
		t.Fatal(err)
	}
	file, size, err := s.sampleSnapshot()
	if err != nil || file == nil {
		t.Fatal("sample prefix not captured", err)
	}
	defer func() { _ = file.Close() }()
	// Leave the captured read entirely unconsumed while another writer
	// appends. Taking a sample snapshot must not keep the global lock held.
	done := make(chan error, 1)
	go func() {
		if err := s.AppendSample(types.Sample{Gateway: "after"}); err != nil {
			done <- err
			return
		}
		done <- s.PutSlot(types.Slot{AccountID: 1, Gateway: "independent-write"})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("sample reader blocked inventory or sample writes")
	}
	body, err := io.ReadAll(io.NewSectionReader(file, 0, size))
	if err != nil {
		t.Fatal(err)
	}
	var original types.Sample
	if err := json.Unmarshal(body, &original); err != nil || original.Gateway != "before" {
		t.Fatal("fixed prefix included a later append or truncated its original row", err)
	}
	if rows, err := s.Samples(); err != nil || len(rows) != 2 {
		t.Fatal("next snapshot did not observe the appended row", err)
	}
}
