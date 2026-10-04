package config

import (
	"testing"

	"gwpool/internal/types"
)

func TestCandidateRegionsExcludeUnsupportedCountries(t *testing.T) {
	cfg := Defaults()
	cfg.Regions = append(cfg.RegionDefinitions(),
		ExplorationRegion{Key: "country-hk", Name: "HK", Country: "HK", Enabled: true},
		ExplorationRegion{Key: "country-aq", Name: "AQ", Country: "AQ", Enabled: true},
		ExplorationRegion{Key: "country-ng", Name: "NG", Country: "NG", Enabled: true},
	)
	found := map[string]bool{}
	for _, region := range cfg.CandidateRegions(types.Exit{}) {
		found[region.Exit.Region] = true
	}
	if found["HK"] || found["AQ"] || !found["JP"] || !found["NG"] {
		t.Fatalf("candidate countries must intersect upstream support: %v", found)
	}
	if _, exists := cfg.RegionByKey("country-hk"); !exists {
		t.Fatal("blocking new attempts must not erase historical exit definitions")
	}
}
