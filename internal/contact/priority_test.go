package contact

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestAllPrioritiesMatchPerModelReference(t *testing.T) {
	now := time.Now().UTC()
	var reports []types.ContactReport
	for i := range 500 {
		r := types.ContactReport{ContactObservation: types.ContactObservation{
			Gateway: fmt.Sprintf("unified-%d", i%3), Model: fmt.Sprintf("model-%d", i%2),
			Criterion: "state-echo-v1", Source: "foreground", Outcome: "full", At: now,
		}}
		switch i % 11 {
		case 0:
			r.At = now.Add(-Retention - time.Nanosecond)
		case 1:
			r.At = now.Add(-Retention)
		case 2:
			r.At = now.Add(5*time.Minute + time.Nanosecond)
		case 3:
			r.At = now.Add(5 * time.Minute)
		case 4:
			r.Outcome = "refreshed"
		case 5:
			r.Outcome = "unknown"
		case 6:
			r.Source = "business"
		case 7:
			r.Criterion = "other"
		case 8:
			r.Model = ""
		}
		reports = append(reports, r)
	}
	for _, model := range []string{"sparse", "only-invalid"} {
		row := reports[10]
		row.Model = model
		if model == "only-invalid" {
			row.Outcome = "unknown"
		}
		reports = append(reports, row)
	}
	before := append([]types.ContactReport(nil), reports...)
	got := AllPriorities(reports, now)
	for _, model := range []string{"model-0", "model-1", "sparse", "only-invalid"} {
		if !reflect.DeepEqual(got[model], Priorities(reports, model, now)) {
			t.Fatalf("model %q differs from per-model oracle", model)
		}
	}
	if _, exists := got[""]; exists || !reflect.DeepEqual(reports, before) {
		t.Fatal("empty model admitted or input mutated")
	}
}

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
