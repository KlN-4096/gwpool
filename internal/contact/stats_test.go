package contact

import (
	"gwpool/internal/types"
	"testing"
	"time"
)

func TestContactStatisticsKeepDenominatorsAndStrataSeparate(t *testing.T) {
	now := time.Now().UTC()
	base := types.ContactReport{ContactObservation: types.ContactObservation{Gateway: "unified-1", Model: "model-a",
		Criterion: "echo-v1", Source: "foreground", First: "repeat", At: now, GapKnown: true, ElapsedSeconds: 3700}, ReporterID: 1}
	var reports []types.ContactReport
	for _, outcome := range []string{"full", "refreshed", "unknown"} {
		r := base
		r.Outcome = outcome
		if outcome == "full" {
			r.FullWindowMS = 80000
			r.WindowFinal = true
		}
		reports = append(reports, r)
	}
	for _, change := range []func(*types.ContactReport){
		func(r *types.ContactReport) { r.Model = "model-b" },
		func(r *types.ContactReport) { r.Criterion = "echo-v2" },
		func(r *types.ContactReport) { r.ElapsedSeconds = 8000 },
		func(r *types.ContactReport) { r.Source = "background" },
		func(r *types.ContactReport) { r.First = "unknown" },
		func(r *types.ContactReport) { r.AccountTag = "other" },
	} {
		r := base
		r.Outcome = "full"
		change(&r)
		reports = append(reports, r)
	}
	rows := Summarize(reports, now)
	if len(rows) != 7 {
		t.Fatalf("mixed strata: %+v", rows)
	}
	found := false
	for _, row := range rows {
		if row.Refreshed == 1 {
			found = true
			if row.Full != 1 || row.Unknown != 1 || row.WindowSamples != 1 || row.WindowMeanMS != 80000 {
				t.Fatal(row)
			}
		}
	}
	if !found {
		t.Fatal("lost conclusive samples")
	}
	old := base
	old.At = now.Add(-Retention - time.Second)
	if len(Summarize([]types.ContactReport{old}, now)) != 0 {
		t.Fatal("expired sample counted")
	}
}
