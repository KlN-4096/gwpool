package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestPageSamplesIncrementalProjectionPreservesValidationAndTail(t *testing.T) {
	s := open(t, t.TempDir())
	sample := types.Sample{AccountID: 1, RegionKey: "us", Gateway: "unified-142", Status: 200,
		At: time.Now().UTC(), Err: " ", Observed: types.Observed{IP: "198.18.0.1", ISP: "fixture",
			Country: "US", State: "state", City: "city", ASN: "AS123"},
		Route:               &types.RouteSample{Result: "productive", Route: types.RouteDescriptor{Identity: "private"}},
		InventoryPrediction: &types.InventoryPrediction{Model: "prediction"}}
	if err := s.AppendSample(sample); err != nil {
		t.Fatal(err)
	}
	first, err := s.ReadPageSamples(PageSampleCursor{})
	if err != nil || !first.Reset || len(first.Rows) != 1 || first.Tail {
		t.Fatal("initial snapshot", err, first)
	}
	want := pageSample(sample)
	if !reflect.DeepEqual(first.Rows[0], want) || first.Rows[0].Err == "" ||
		first.Rows[0].Route == nil || first.Rows[0].Route.Result != sample.Route.Result ||
		first.Rows[0].Route.Route.Identity != "" || first.Rows[0].Observed.IP != "" ||
		first.Rows[0].InventoryPrediction != nil {
		t.Fatal("page projection changed evidence or retained unused payload")
	}
	sample.AccountID++
	if err := s.AppendSample(sample); err != nil {
		t.Fatal(err)
	}
	next, err := s.ReadPageSamples(first.Cursor)
	if err != nil || next.Reset || len(next.Rows) != 1 || next.Rows[0].AccountID != 2 {
		t.Fatal("append reread old history", err)
	}
	empty, err := s.ReadPageSamples(next.Cursor)
	if err != nil || empty.Reset || len(empty.Rows) != 0 {
		t.Fatal("unchanged file was reparsed", err)
	}
	// Validate the full original schema even for fields omitted from page data.
	write := func(text string) {
		t.Helper()
		if _, err := s.samples.WriteString(text); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"AccountID":3,"InventoryPrediction":{"DecidedAt":"broken-time"}}` + "\n")
	write(`{"AccountID":4,"Route":{"Gain":{"extended_ms":"bad"}}}` + "\n")
	write(`{"AccountID":5,"Route":{},"Err":""}` + "\n")
	write(`{"AccountID":6}`)
	tail, err := s.ReadPageSamples(empty.Cursor)
	if err != nil || len(tail.Rows) != 2 || !tail.Tail ||
		tail.Rows[0].AccountID != 5 || tail.Rows[0].Route == nil || tail.Rows[1].AccountID != 6 {
		t.Fatal("invalid rows/tail/nil Route semantics changed", err, tail.Rows)
	}
	write("\n" + `{"AccountID":7`)
	partial, err := s.ReadPageSamples(tail.Cursor)
	if err != nil || len(partial.Rows) != 1 || partial.Rows[0].AccountID != 6 || partial.Tail {
		t.Fatal("completed tail or partial JSON handled incorrectly", err)
	}
	write("}\n")
	completed, err := s.ReadPageSamples(partial.Cursor)
	if err != nil || len(completed.Rows) != 1 || completed.Rows[0].AccountID != 7 || completed.Tail {
		t.Fatal("partial JSON completion lost", err)
	}
}

func TestPageSamplesResetAndOversizeFailure(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	if err := s.AppendSample(types.Sample{AccountID: 10}); err != nil {
		t.Fatal(err)
	}
	first, err := s.ReadPageSamples(PageSampleCursor{})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, samplesFile)
	if err := os.WriteFile(path, []byte(`{"AccountID":1}`+"\n"), filePerm); err != nil {
		t.Fatal(err)
	}
	second, err := s.ReadPageSamples(first.Cursor)
	if err != nil || !second.Reset || len(second.Rows) != 1 || second.Rows[0].AccountID != 1 {
		t.Fatal("truncation did not rebuild", err)
	}
	if _, err := s.samples.WriteString(strings.Repeat("x", 1<<20+1) + "\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadPageSamples(second.Cursor); err == nil {
		t.Fatal("oversized line was silently accepted")
	}
}

func TestPageSamplesChangedFileAndMissingFileReset(t *testing.T) {
	original := open(t, t.TempDir())
	if err := original.AppendSample(types.Sample{AccountID: 1}); err != nil {
		t.Fatal(err)
	}
	before, err := original.ReadPageSamples(PageSampleCursor{})
	if err != nil {
		t.Fatal(err)
	}
	replacement := open(t, t.TempDir())
	if err := replacement.AppendSample(types.Sample{AccountID: 2}); err != nil {
		t.Fatal(err)
	}
	after, err := replacement.ReadPageSamples(before.Cursor)
	if err != nil || !after.Reset || len(after.Rows) != 1 || after.Rows[0].AccountID != 2 {
		t.Fatal("different file identity did not reset", err)
	}
	absent := &Store{dir: t.TempDir()}
	after, err = absent.ReadPageSamples(before.Cursor)
	if err != nil || !after.Reset || len(after.Rows) != 0 {
		t.Fatal("missing file silently retained old rows", err)
	}
}

func BenchmarkPageSampleProjection(b *testing.B) {
	value := types.Sample{AccountID: 1, Gateway: "unified-142", Route: &types.RouteSample{Result: "productive"}}
	raw, _ := json.Marshal(value)
	b.ReportAllocs()
	for b.Loop() {
		var sample types.Sample
		if err := json.Unmarshal(raw, &sample); err != nil {
			b.Fatal(err)
		}
		_ = pageSample(sample)
	}
}
