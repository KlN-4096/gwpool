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
