package httpapi

import (
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

func TestRegionCardsOmitEmptyGlobalCandidatesAndFailedSamples(t *testing.T) {
	cfg, err := config.GlobalCountryRegions(config.Defaults())
	if err != nil {
		t.Fatal(err)
	}
	grid := regionCells(cfg, 1, map[string]types.RegionMint{
		"1|europe": {AccountID: 1, RegionKey: "europe", Count: 1, LastAt: time.Now()},
	}, []types.Sample{
		{AccountID: 1, RegionKey: "europe", Gateway: "unified-141", Status: 200},
		{AccountID: 1, RegionKey: "us-west", Gateway: "unified-42", Status: 401},
		{AccountID: 1, RegionKey: "us-east", Gateway: "unified-43", Status: 0},
	}, nil, time.Hour)
	if len(grid.Cells) != 1 || grid.Cells[0].Key != "europe" {
		t.Fatalf("account card must contain only successful records, not all %d candidates: %d cells", len(cfg.Regions), len(grid.Cells))
	}
	if grid.Recorded != 1 || grid.Total != 250 || len(grid.Groups) != 1 {
		t.Fatalf("missing compact summary/groups: %+v", grid)
	}
	full := regionCells(cfg, 1, nil, nil, nil, time.Hour, true)
	if len(full.Cells) != 250 {
		t.Fatal("explicit account-detail request must still expose unvisited exits")
	}
}

func TestPoolDisplayGroupsAreSparseAndPreserveEmptySelection(t *testing.T) {
	cfg, err := config.GlobalCountryRegions(config.Defaults())
	if err != nil {
		t.Fatal(err)
	}
	cells := poolRegionCells(cfg, []gatewayRow{{Name: "unified-141"}}, map[string]types.Gateway{
		"unified-141": {ReachableExits: []types.Exit{{Region: "DE"}}},
	})
	markPoolRegionLinks(cells, "country-sg")
	groups := poolDisplayGroups(cfg, cells, false)
	count := 0
	for _, group := range groups {
		for _, country := range group.Countries {
			count += len(country.Cells)
		}
	}
	if count != 2 {
		t.Fatalf("want one populated exit plus selected empty exit, got %d", count)
	}
}

func TestPoolRegionDefaultSelectionDoesNotSelectUnknown(t *testing.T) {
	for _, query := range []string{"", "invalid", poolRegionQuery} {
		cells := []poolRegionCell{{All: true}, {Key: "europe"}, {Key: "", Label: "未归类"}}
		markPoolRegionLinks(cells, query)
		if cells[0].Active != (query != poolRegionQuery) || cells[2].Active != (query == poolRegionQuery) {
			t.Fatalf("query=%q active all=%v unknown=%v", query, cells[0].Active, cells[2].Active)
		}
	}
}

func TestRegionsCoveredCountsOnlyEnabledDefinitions(t *testing.T) {
	cfg := config.Defaults()
	cfg.Regions = config.DefaultRegions()
	cfg.Regions[0].Enabled = false
	got := regionsCovered(cfg, map[string]types.RegionMint{
		"a": {AccountID: 1, RegionKey: cfg.Regions[0].Key, Count: 1},
		"b": {AccountID: 1, RegionKey: cfg.Regions[1].Key, Count: 1},
	})
	if got[1] != 1 {
		t.Fatalf("disabled exit inflated active coverage: %v", got)
	}
}
