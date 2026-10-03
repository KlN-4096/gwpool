package store

import (
	"errors"
	"gwpool/internal/types"
	"testing"
	"time"
)

func TestContactRoundPersistsAndWindowUpdateDoesNotDoubleCount(t *testing.T) {
	dir := t.TempDir()
	st := open(t, dir)
	r := types.ContactReport{ContactObservation: types.ContactObservation{ID: "one", At: time.Now().UTC(),
		Gateway: "unified-1", Outcome: "full"}, ReporterID: 1, ReceivedAt: time.Now().UTC()}
	if added, err := st.AddContactReport(r); !added || err != nil {
		t.Fatal(added, err)
	}
	final := r
	final.FullWindowMS = 100000
	final.WindowFinal = true
	if added, err := st.AddContactReport(final); added || err != nil {
		t.Fatal(added, err)
	}
	if added, err := st.AddContactReport(r); added || err != nil {
		t.Fatal("late initial overwrote final", added, err)
	}
	copy := open(t, dir).ContactReports()
	if len(copy) != 1 || copy[0].FullWindowMS != 100000 {
		t.Fatal(copy)
	}
	bad := r
	bad.Outcome = "refreshed"
	if _, err := st.AddContactReport(bad); !errors.Is(err, types.ErrContactReportConflict) {
		t.Fatal("rewrote outcome", err)
	}
	bad = final
	bad.FullWindowMS++
	if _, err := st.AddContactReport(bad); !errors.Is(err, types.ErrContactReportConflict) {
		t.Fatal("rewrote final duration", err)
	}
	r.ReporterID = 2
	if added, err := st.AddContactReport(r); !added || err != nil {
		t.Fatal("reporter identity collision", added, err)
	}
}
