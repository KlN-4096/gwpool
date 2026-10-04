package contact

import (
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestPrioritiesExcludeUnknownAndOtherModelsAndSources(t *testing.T) {
	now := time.Now()
	base := types.ContactReport{ContactObservation: types.ContactObservation{
		Gateway: "unified-142", Model: "luna", Criterion: "state-echo-v1", Source: "foreground", At: now, Outcome: "full",
	}}
	reports := make([]types.ContactReport, PriorityMinSamples)
	for i := range reports {
		reports[i] = base
		reports[i].ReporterID = int64(i + 1)
	}
	reports[0].Outcome = "refreshed"
	for _, change := range []func(*types.ContactReport){
		func(r *types.ContactReport) { r.Outcome = "unknown" },
		func(r *types.ContactReport) { r.Model = "astra" },
		func(r *types.ContactReport) { r.Source = "business" },
		func(r *types.ContactReport) { r.Source = "background" },
		func(r *types.ContactReport) { r.Criterion = "different" },
		func(r *types.ContactReport) { r.At = now.Add(-Retention - time.Second) },
	} {
		r := base
		change(&r)
		reports = append(reports, r)
	}
	p := Priorities(reports, "luna", now)["unified-142"]
	if p.Full != 4 || p.Samples != 5 || p.Model != "luna" {
		t.Fatalf("wrong denominator: %+v", p)
	}
	if len(Priorities(reports[:4], "luna", now)) != 0 || len(Priorities(reports, "", now)) != 0 {
		t.Fatal("unknown/model-less priority must be absent")
	}
	reports[0].Outcome = "full"
	if Priorities(reports, "luna", now)["unified-142"].Full != 5 {
		t.Fatal("priority was not recomputed")
	}
}
