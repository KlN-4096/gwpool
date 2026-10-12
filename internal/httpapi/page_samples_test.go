package httpapi

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gwpool/internal/store"
	"gwpool/internal/types"
)

type failingPageStore struct {
	*store.Store
	fail  bool
	reads int
}

func (s *failingPageStore) ReadPageSamples(cursor store.PageSampleCursor) (store.PageSampleDelta, error) {
	s.reads++
	if s.fail {
		return store.PageSampleDelta{}, errors.New("offline sample read failure")
	}
	return s.Store.ReadPageSamples(cursor)
}

func TestPageSampleCacheAppendImmutableReadersAndReadFailure(t *testing.T) {
	server, st := cacheTestServer(t)
	reader := &failingPageStore{Store: st}
	server.st = reader
	first := types.Sample{AccountID: 1, At: time.Now().UTC(), Gateway: "unified-142", Status: 200,
		Route: &types.RouteSample{Result: "productive"}}
	if err := st.AppendSample(first); err != nil {
		t.Fatal(err)
	}
	old, err := server.samples()
	if err != nil || len(old) != 1 || cap(old) != len(old) {
		t.Fatal("initial page cache", err)
	}
	original := old[0]
	second := first
	second.AccountID++
	if err := st.AppendSample(second); err != nil {
		t.Fatal(err)
	}
	if got, err := server.samples(); err != nil || len(got) != 1 || reader.reads != 1 {
		t.Fatal("existing five-second freshness contract changed")
	}
	server.smAt = time.Time{}
	reader.fail = true
	if got, err := server.samples(); err == nil || got != nil ||
		len(server.sm) != 1 || !reflect.DeepEqual(old[0], original) {
		t.Fatal("failed read published a partial delta or served stale success")
	}
	reader.fail = false
	got, err := server.samples()
	if err != nil || len(got) != 2 || got[1].AccountID != 2 ||
		len(old) != 1 || !reflect.DeepEqual(old[0], original) {
		t.Fatal("append mutated an already published reader", err)
	}
	server.smAt = time.Time{}
	if next, err := server.samples(); err != nil || &next[0] != &got[0] {
		t.Fatal("unchanged refresh cloned the sample array", err)
	}
}

func TestPageSampleCacheReplacesProvisionalTailWithoutMutatingReaders(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	path := filepath.Join(dir, "samples.jsonl")
	if err := os.WriteFile(path, []byte("{\"AccountID\":1}\n{\"AccountID\":2}"), 0600); err != nil {
		t.Fatal(err)
	}
	server := &Server{st: st}
	old, err := server.samples()
	if err != nil || len(old) != 2 || !server.smTail {
		t.Fatal("valid unterminated row not visible", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.WriteString("broken\n{\"AccountID\":3}\n")
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal(writeErr, closeErr)
	}
	server.smAt = time.Time{}
	next, err := server.samples()
	if err != nil || len(next) != 2 || next[1].AccountID != 3 ||
		old[1].AccountID != 2 || server.smTail {
		t.Fatal("changed invalid tail overwrote an existing reader", err)
	}
}
