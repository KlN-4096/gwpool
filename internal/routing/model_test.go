package routing

import (
	"testing"
	"time"

	"gwpool/internal/types"
)

func fixtureRows() []Observation {
	var rows []Observation
	for _, donor := range []string{"a", "b"} {
		for _, c := range []struct{ country, gateway string }{{"US", "unified-1"}, {"JP", "unified-2"}} {
			for i := 0; i < 10; i++ {
				rows = append(rows, Observation{Donor: donor, Country: c.country, Gateway: c.gateway,
					Requested: types.Exit{Region: c.country}, At: time.Unix(int64(i+1), 0)})
			}
		}
	}
	return rows
}

func TestRoutingCountryForecastUsesFrozenPastEvidence(t *testing.T) {
	rows := fixtureRows()
	model := New(rows)
	rows[0].Gateway = "unified-999" // The model does not retain the mutable input.
	for _, c := range []struct{ country, gateway string }{{"US", "unified-1"}, {"JP", "unified-2"}} {
		p := model.ForExit("a", types.Exit{Region: c.country})
		if p.Gateway != c.gateway || p.Country != c.country || p.TrainingSamples != 40 ||
			p.DonorSamples != 20 || p.CountrySamples != 20 || p.ExitSamples != 20 ||
			p.GeoBasis != "historical-requested-exit" || p.Model != Version {
			t.Fatalf("unexpected forecast: %+v", p)
		}
	}
}

func TestRoutingForecastAbstainsWithoutExitOrDonorSupport(t *testing.T) {
	model := New(fixtureRows())
	for _, query := range []struct {
		donor string
		exit  types.Exit
	}{{"unknown", types.Exit{Region: "US"}}, {"a", types.Exit{Region: "CA"}},
		{"a", types.Exit{Region: "US", City: "Seattle"}}} {
		if p := model.ForExit(query.donor, query.exit); p.Gateway != "" {
			t.Fatalf("invented supported route for %+v: %+v", query, p)
		}
	}
	if p := New(fixtureRows()[:MinSupport-1]).ForExit("a", types.Exit{Region: "US"}); p.Gateway != "" {
		t.Fatalf("sparse evidence should abstain: %+v", p)
	}
	if p := (*Model)(nil).ForExit("a", types.Exit{Region: "US"}); p.Gateway != "" {
		t.Fatal("nil model did not abstain")
	}
}

func TestRoutingForecastDoesNotCallRequestedCountryObserved(t *testing.T) {
	rows := fixtureRows()
	for i := range rows {
		if rows[i].Country == "JP" {
			rows[i].Requested = types.Exit{Region: "CA"}
		}
	}
	p := New(rows).ForExit("a", types.Exit{Region: "CA"})
	if p.Country != "JP" || p.Gateway != "unified-2" {
		t.Fatalf("should infer past observed JP, not assume requested CA: %+v", p)
	}
	for i := range rows {
		rows[i].Requested = types.Exit{Region: "CA"}
	}
	if p := New(rows).ForExit("a", types.Exit{Region: "CA"}); p.Country != "" || p.Gateway != "" {
		t.Fatalf("mixed actual countries should abstain: %+v", p)
	}
	for i := range rows {
		rows[i].Country = "HK"
	}
	if p := New(rows).ForExit("a", types.Exit{Region: "CA"}); p.Gateway != "" {
		t.Fatalf("historical unsupported Geo should not promote a candidate: %+v", p)
	}
}

func TestRoutingForecastHasDeterministicTies(t *testing.T) {
	rows := fixtureRows()
	for i := range rows {
		rows[i].Country, rows[i].Requested = "US", types.Exit{Region: "US"}
	}
	model := New(rows)
	for i := 0; i < 30; i++ {
		if p := model.ForExit("a", types.Exit{Region: "US"}); p.Gateway != "unified-2" {
			t.Fatalf("tie differs from frozen research rule: %+v", p)
		}
	}
}

func TestRoutingBareSampleWhitelist(t *testing.T) {
	base := types.Sample{AccountID: 1, At: time.Now(), Status: 200, Gateway: "unified-1",
		Provider: "webshare", Purpose: types.PurposeInventory, Observed: types.Observed{Country: "US"}}
	if !BareSample(base) {
		t.Fatal("valid sample rejected")
	}
	for _, edit := range []func(*types.Sample){
		func(s *types.Sample) { s.Provider = "seed" },
		func(s *types.Sample) { s.Provider = "" },
		func(s *types.Sample) { s.Purpose = "" },
		func(s *types.Sample) { s.Purpose = types.PurposeRenew },
		func(s *types.Sample) { s.Purpose = types.PurposeDelivery },
		func(s *types.Sample) { s.Status = 429 },
		func(s *types.Sample) { s.Status = 0 },
		func(s *types.Sample) { s.Observed.Country = "" },
		func(s *types.Sample) { s.At = time.Time{} },
		func(s *types.Sample) { s.Gateway = "unified-1.bad" },
		func(s *types.Sample) { s.Gateway = "unified-0" },
	} {
		sample := base
		edit(&sample)
		if BareSample(sample) {
			t.Fatalf("ineligible sample admitted: %+v", sample)
		}
	}
	if DonorKey("") != "" || DonorKey("upstream-a") == DonorKey("upstream-b") ||
		DonorKey("upstream-a") == "upstream-a" {
		t.Fatal("invalid donor key")
	}
}
