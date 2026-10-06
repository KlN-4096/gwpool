package httpapi

import (
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

func TestRegionCardsUseNineAreasAndExcludeFailedSamples(t *testing.T) {
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
	if len(grid.Cells) != 9 {
		t.Fatalf("account card must contain nine areas, not all %d exits: %d cells", len(cfg.Regions), len(grid.Cells))
	}
	if grid.Recorded != 1 || grid.Total != 9 {
		t.Fatalf("missing area summary: %+v", grid)
	}
	for _, cell := range grid.Cells {
		if cell.Key != "europe" && len(cell.Gateways) != 0 {
			t.Fatal("failed sample created an area landing")
		}
	}
}

func TestPoolAreaGroupsDeduplicateCountriesAndPreserveEmptySelection(t *testing.T) {
	cells := poolRegionCells([]gatewayRow{{Name: "unified-141"}})
	markPoolRegionLinks(cells, "southeast-asia")
	if len(cells) != 10 {
		t.Fatalf("want all + nine areas, got %d", len(cells))
	}
	for _, cell := range cells {
		if cell.Key == "europe" && cell.Total != 1 {
			t.Fatal("same gateway counted once per country")
		}
		if cell.Key == "southeast-asia" && (!cell.Active || cell.Total != 0) {
			t.Fatal("empty area cannot be selected")
		}
	}
}

func TestAccountAreaGroupsDeduplicateCountryAndCityAliases(t *testing.T) {
	cfg, err := config.GlobalCountryRegions(config.Defaults())
	if err != nil {
		t.Fatal(err)
	}
	grid := regionCells(cfg, 1, nil, []types.Sample{
		{AccountID: 1, RegionKey: "europe", Gateway: "unified-1", Status: 200},
		{AccountID: 1, RegionKey: "country-de", Gateway: "unified-1", Status: 200},
		{AccountID: 1, RegionKey: "us-east", Gateway: "unified-2", Status: 200},
		{AccountID: 1, RegionKey: "us-west", Gateway: "unified-2", Status: 200},
	}, nil, time.Hour)
	if len(grid.Cells) != 9 || grid.Recorded != 2 {
		t.Fatalf("area dedup failed: %+v", grid)
	}
	for _, cell := range grid.Cells {
		if (cell.Key == "europe" || cell.Key == "north-america") &&
			(len(cell.Gateways) != 1 || cell.Gateways[0].Dup) {
			t.Fatalf("country alias created a duplicate gateway: %+v", cell)
		}
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
