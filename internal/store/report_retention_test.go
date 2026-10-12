package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gwpool/internal/contact"
	"gwpool/internal/cooldown"
	"gwpool/internal/types"
)

// Select using the previous forward-filter/reverse-quota algorithm as an
// independent oracle. Only the production mutation/delta generation is optimized.
func referenceReportRetention[T any](before []T, report T, keepNew bool,
	keep func(T) bool, key func(T) reportKey, limit, reporterLimit int,
) []T {
	next := make([]T, 0, len(before)+1)
	for _, row := range before {
		if keep(row) {
			next = append(next, row)
		}
	}
	if keepNew {
		next = append(next, report)
	}
	counts := map[int64]int{}
	retained := make([]T, 0, len(next))
	for i := len(next) - 1; i >= 0; i-- {
		row := next[i]
		id := key(row).Reporter
		if counts[id] < reporterLimit {
			retained = append(retained, row)
			counts[id]++
		}
	}
	for i, j := 0, len(retained)-1; i < j; i, j = i+1, j-1 {
		retained[i], retained[j] = retained[j], retained[i]
	}
	if len(retained) > limit {
		retained = retained[len(retained)-limit:]
	}
	return retained
}

func seededReportStore(t *testing.T, state State) *Store {
	t.Helper()
	dir := t.TempDir()
	state.OwnerKeysMigrated = true
	body, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateFile), body, filePerm); err != nil {
		t.Fatal(err)
	}
	return open(t, dir)
}

func TestReportRetentionPreservesOrderingAndReplay(t *testing.T) {
	now := time.Now().UTC()
	for _, size := range []int{0, 1200, contact.MaxReports + 7} {
		for _, staleNew := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows-%d-stale-%v", size, staleNew), func(t *testing.T) {
				var contacts []types.ContactReport
				var cooldowns []types.CooldownReport
				for i := 0; i < size; i++ {
					// Interleaved expired/late events, with one reporter at its
					// own cap before the entire table reaches the global cap.
					at := now.Add(-time.Duration(i%48) * time.Hour)
					if i%17 == 0 {
						at = now.Add(-8 * 24 * time.Hour)
					}
					reporter := int64(i % 5)
					if i < 1200 {
						reporter = 0
					}
					contacts = append(contacts, types.ContactReport{
						ReporterID: reporter, ReceivedAt: now,
						ContactObservation: types.ContactObservation{
							ID: fmt.Sprint(i), At: at, Outcome: "full",
						},
					})
					cooldowns = append(cooldowns, types.CooldownReport{
						ID: fmt.Sprint(i), ReporterID: reporter, Result: "full",
						ObservedAt: at, ReceivedAt: now,
					})
				}
				at := now
				if staleNew {
					at = now.Add(-8 * 24 * time.Hour)
				}
				c := types.ContactReport{ReporterID: 0, ReceivedAt: now,
					ContactObservation: types.ContactObservation{ID: "new", At: at, Outcome: "full"}}
				d := types.CooldownReport{ID: "new", ReporterID: 0,
					ObservedAt: at, ReceivedAt: now, Result: "full"}
				wantContacts := referenceReportRetention(contacts, c, !staleNew,
					func(r types.ContactReport) bool { return !r.At.Before(now.Add(-contact.Retention)) },
					contactKey, contact.MaxReports, contact.MaxReporterReports)
				wantCooldowns := referenceReportRetention(cooldowns, d, true,
					func(r types.CooldownReport) bool { return !r.EventAt().Before(now.Add(-cooldown.Retention)) },
					cooldownKey, cooldown.MaxReports, cooldown.MaxReporterReports)
				s := seededReportStore(t, State{ContactReports: contacts, CooldownReports: cooldowns})
				if added, err := s.AddContactReport(c); !added || err != nil {
					t.Fatal(added, err)
				}
				if added, err := s.AddCooldownReport(d); !added || err != nil {
					t.Fatal(added, err)
				}
				assertReports := func(st *Store) {
					t.Helper()
					gotContacts, gotCooldowns := st.ContactReports(), st.CooldownReports()
					if len(gotContacts) != len(wantContacts) ||
						(len(gotContacts) > 0 && !reflect.DeepEqual(gotContacts, wantContacts)) ||
						!reflect.DeepEqual(gotCooldowns, wantCooldowns) {
						t.Fatal("retention changed insertion order, per-reporter quota or expired-new semantics")
					}
				}
				assertReports(s)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				assertReports(open(t, s.dir))
			})
		}
	}
}

func TestExpiredReportRetriesDoNotPruneOrLoseMerge(t *testing.T) {
	now := time.Now().UTC()
	c := types.ContactReport{ReporterID: 1, ReceivedAt: now.Add(-8 * 24 * time.Hour),
		ContactObservation: types.ContactObservation{ID: "expired", At: now.Add(-8 * 24 * time.Hour), Outcome: "full"}}
	d := types.CooldownReport{ID: "expired", ReporterID: 1, Result: "full", ReceivedAt: c.ReceivedAt}
	s := seededReportStore(t, State{ContactReports: []types.ContactReport{c}, CooldownReports: []types.CooldownReport{d}})
	before := s.Revisions()
	if added, err := s.AddContactReport(c); added || err != nil {
		t.Fatal(added, err)
	}
	if added, err := s.AddCooldownReport(d); added || err != nil {
		t.Fatal(added, err)
	}
	if s.Revisions() != before || len(s.ContactReports()) != 1 || len(s.CooldownReports()) != 1 {
		t.Fatal("duplicate replay pruned expired evidence or advanced its revision")
	}
	changed := d
	changed.Result = "degraded"
	if _, err := s.AddCooldownReport(changed); !errors.Is(err, types.ErrCooldownReportConflict) {
		t.Fatal("expired same-ID conflict was not rejected", err)
	}
	c.WindowFinal, c.FullWindowMS = true, 42
	c.ReceivedAt = now
	if added, err := s.AddContactReport(c); added || err != nil {
		t.Fatal(added, err)
	}
	if got := s.ContactReports()[0]; got.FullWindowMS != 42 || got.ReceivedAt.Equal(now) {
		t.Fatal("window completion lost its original receipt time or was pruned")
	}
}

func TestReportPruningFailureRestoresSliceAndRevision(t *testing.T) {
	now := time.Now().UTC()
	oldTime := now.Add(-8 * 24 * time.Hour)
	c := types.ContactReport{ReporterID: 1, ReceivedAt: oldTime,
		ContactObservation: types.ContactObservation{ID: "old", At: oldTime, Outcome: "full"}}
	d := types.CooldownReport{ID: "old", ReporterID: 1, Result: "full", ReceivedAt: oldTime}
	s := seededReportStore(t, State{ContactReports: []types.ContactReport{c}, CooldownReports: []types.CooldownReport{d}})
	for _, kind := range []string{"contact", "cooldown"} {
		before := s.Revisions()
		s.journal.file = &faultyJournal{journalIO: s.journal.file, syncFail: true}
		var err error
		if kind == "contact" {
			next := c
			next.ID, next.At = "new", now
			_, err = s.AddContactReport(next)
		} else {
			next := d
			next.ID, next.ReceivedAt = "new", now
			_, err = s.AddCooldownReport(next)
		}
		if err == nil || s.Revisions() != before ||
			!reflect.DeepEqual(s.ContactReports(), []types.ContactReport{c}) ||
			!reflect.DeepEqual(s.CooldownReports(), []types.CooldownReport{d}) {
			t.Fatal("failed pruning transaction changed acknowledged state", kind)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, s.dir)
	if !reflect.DeepEqual(reopened.ContactReports(), []types.ContactReport{c}) ||
		!reflect.DeepEqual(reopened.CooldownReports(), []types.CooldownReport{d}) {
		t.Fatal("failed report pruning survived journal replay")
	}
}

func TestContactWindowMergeAvoidsWholeSliceCopyAndRestoresFailedRow(t *testing.T) {
	now := time.Now().UTC()
	reports := make([]types.ContactReport, contact.MaxReports)
	for i := range reports {
		reports[i] = types.ContactReport{ReporterID: int64(i/1000 + 1), ReceivedAt: now,
			ContactObservation: types.ContactObservation{ID: fmt.Sprint(i), At: now, Outcome: "full"}}
	}
	s := seededReportStore(t, State{ContactReports: reports})
	snapshot := s.ContactReports()
	beforeRevision := s.Revisions()
	address := &s.st.ContactReports[0]
	target := 99
	update := reports[target]
	update.WindowFinal, update.FullWindowMS = true, 1500
	originalJournal := s.journal.file
	s.journal.file = &faultyJournal{journalIO: originalJournal, syncFail: true}
	if _, err := s.AddContactReport(update); err == nil {
		t.Fatal("injected sync failure was not surfaced")
	}
	if s.st.ContactReports[target] != snapshot[target] || s.Revisions() != beforeRevision {
		t.Fatal("failed merge changed row or revision")
	}
	s.journal.file = originalJournal
	if added, err := s.AddContactReport(update); added || err != nil {
		t.Fatal(added, err)
	}
	if &s.st.ContactReports[0] != address {
		t.Fatal("single-row completion copied the entire report slice")
	}
	if snapshot[target].WindowFinal || !s.ContactReports()[target].WindowFinal {
		t.Fatal("prior reader snapshot mutated or update lost")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	got := open(t, s.dir).ContactReports()
	if len(got) != len(reports) || got[target] != update {
		t.Fatal("window merge changed order or failed WAL recovery")
	}
}
