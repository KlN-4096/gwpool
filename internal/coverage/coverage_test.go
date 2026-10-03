package coverage

import (
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

func TestCoverageDirectoryIsDNSCandidateListNotSequentialCeiling(t *testing.T) {
	view := Build(config.Defaults(), nil, nil, time.Now())
	if view.DNSCount != 201 || len(view.Unassigned) != 201 || view.Found != 0 {
		t.Fatalf("wrong candidate inventory: dns=%d unknown=%d found=%d", view.DNSCount, len(view.Unassigned), view.Found)
	}
	found215, found10 := false, false
	for _, row := range view.Candidates {
		found215 = found215 || row.Name == "unified-215"
		found10 = found10 || row.Name == "unified-10"
	}
	if !found215 || found10 {
		t.Fatal("DNS list was replaced with an invented contiguous range")
	}
	for _, group := range view.Groups {
		if group.Done {
			t.Fatal("unknown target scope cannot mean fully covered")
		}
	}
}

func TestCoverageMatchesObservedExitAndSeparatesHistoricalFromReady(t *testing.T) {
	cfg := config.Defaults()
	cfg.CoverageGroups = []config.CoverageGroup{
		{Key: "east", Name: "East", ExitKeys: []string{"east-asia"}, Targets: []string{"unified-1", "unified-2"}, Complete: true},
	}
	gateways := map[string]types.Gateway{
		"unified-1": {ReachableExits: []types.Exit{{Region: "JP"}}},
		"unified-2": {ReachableExits: []types.Exit{{Region: "US"}}},
	}
	pairs := map[string]types.Pair{"unified-1": {Cookie: "fake-cookie", ExpiresAt: time.Now().Add(-time.Minute)}}
	view := Build(cfg, gateways, pairs, time.Now())
	if group := view.Groups[0]; group.Found != 1 || group.Ready != 0 || group.Done || len(group.Missing) != 1 {
		t.Fatalf("another exit/expired pair distorted coverage: %+v", group)
	}
	gateways["unified-2"] = types.Gateway{ReachableExits: []types.Exit{{Region: "JP"}}}
	if !Build(cfg, gateways, pairs, time.Now()).Groups[0].Done {
		t.Fatal("complete historical target should stay complete without a live ticket")
	}
	priority := IncompleteRegions(cfg, gateways)
	if priority["east-asia"] || !priority["us-east"] {
		t.Fatal("completed group should yield exploration to an unknown group")
	}
}

func TestDatacenterDirectoryHasAutomaticDenominatorsIndependentOfExit(t *testing.T) {
	now := time.Now()
	gateways := map[string]types.Gateway{
		"unified-1":   {ReachableExits: []types.Exit{{Region: "JP"}}},
		"unified-999": {},
	}
	pairs := map[string]types.Pair{"unified-1": {Cookie: "fake", ExpiresAt: now.Add(time.Hour)}}
	view := Build(config.Defaults(), gateways, pairs, now)
	total, found, ready := 0, 0, 0
	countries := map[string]bool{}
	for _, group := range view.Datacenters {
		total, found, ready = total+group.Total, found+group.Found, ready+group.Ready
		countries[group.Country] = true
		if group.Country == "US" && group.City == "San Antonio" && group.Found != 1 {
			t.Fatal("physical directory location was confused with JP proxy exit")
		}
	}
	if len(view.Datacenters) != 39 || len(countries) != 22 || total != 201 || found != 1 ||
		ready != 1 || view.UnknownDatacenters != 1 {
		t.Fatalf("wrong historical catalogue totals: groups=%d countries=%d total=%d found=%d ready=%d unknown=%d",
			len(view.Datacenters), len(countries), total, found, ready, view.UnknownDatacenters)
	}
	for name := range datacenterDirectory {
		gateways[name] = types.Gateway{}
	}
	for _, group := range Build(config.Defaults(), gateways, nil, now).Datacenters {
		if !group.Complete || len(group.Missing) != 0 || group.Ready != 0 {
			t.Fatal("expired tickets must not remove historical directory completion")
		}
	}
}
