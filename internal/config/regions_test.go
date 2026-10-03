package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gwpool/internal/proxycatalog"
	"gwpool/internal/types"
)

func TestGlobalCountryDefinitionsFitWithoutIncreasingMintBudget(t *testing.T) {
	old := Defaults()
	next := old
	next.Regions = old.RegionDefinitions()
	seen := map[string]bool{}
	for _, r := range next.Regions {
		if r.State == "" && r.City == "" {
			seen[r.Country] = true
		}
	}
	for _, c := range proxycatalog.CliproxyCountries() {
		if !seen[c.Code] {
			next.Regions = append(next.Regions, ExplorationRegion{
				Key: "country-" + strings.ToLower(c.Code), Name: c.Code,
				Country: c.Code, Enabled: true,
			})
			seen[c.Code] = true
		}
	}
	if err := ValidateRegionTransition(old, next); err != nil {
		t.Fatal(err)
	}
	if len(seen) < 200 {
		t.Fatal("global candidates were silently truncated")
	}
}

func TestGlobalCountryExpansionIsIdempotentAndPreservesExistingSettings(t *testing.T) {
	old := Defaults()
	old.Regions = old.RegionDefinitions()
	old.Regions[0].Enabled = false
	next, err := GlobalCountryRegions(old)
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Regions) != 250 || len(next.CoverageGroups) != len(old.Groups()) {
		t.Fatalf("expected 249 countries plus Seattle, without empty per-country target groups: %d/%d",
			len(next.Regions), len(next.CoverageGroups))
	}
	if next.Regions[0].Enabled || next.UpstreamPerRegionHour != old.UpstreamPerRegionHour {
		t.Fatal("global expansion changed existing enable state or rate limit")
	}
	again, err := GlobalCountryRegions(next)
	if err != nil || !reflect.DeepEqual(next.Regions, again.Regions) ||
		!reflect.DeepEqual(next.CoverageGroups, again.CoverageGroups) {
		t.Fatalf("repeated expansion drifted: %v", err)
	}
}

func TestGlobalCountryExpansionPreservesExplicitEmptyGroups(t *testing.T) {
	old := Defaults()
	old.CoverageGroups = []CoverageGroup{}
	next, err := GlobalCountryRegions(old)
	if err != nil {
		t.Fatal(err)
	}
	again, err := GlobalCountryRegions(next)
	if err != nil || next.CoverageGroups == nil || len(next.Groups()) != 0 ||
		!reflect.DeepEqual(next.CoverageGroups, again.CoverageGroups) {
		t.Fatal("empty target groups became default country groups")
	}
}

func TestRegionsAddExitKeepsStableHistoryAndIndependentCandidates(t *testing.T) {
	c := Defaults()
	c.Regions = append(c.RegionDefinitions(), ExplorationRegion{Key: "africa-south", Name: "南非",
		Country: "ZA", City: "Cape Town", Enabled: true})
	if err := ValidateRegionTransition(Defaults(), c); err != nil {
		t.Fatal(err)
	}
	e := types.Exit{Region: "ZA", City: "Cape Town"}
	if c.RegionKeyOf(e) != "africa-south" || len(c.CandidateRegions(types.Exit{})) != 10 {
		t.Fatal("新增出口没有进入实际候选/反查")
	}
	c.Regions[9].Enabled = false
	if c.RegionKeyOf(e) != "africa-south" || len(c.CandidateRegions(e)) != 0 {
		t.Fatal("停用应停止候选但保留历史反查")
	}
	old := c
	c.Regions = append([]ExplorationRegion(nil), c.Regions...)
	c.Regions[9].City = "Johannesburg"
	if ValidateRegionTransition(old, c) == nil {
		t.Fatal("不可把旧ID换成不同出口")
	}
}

func TestRegionsRejectInjectionAliasesAndFalseComplete(t *testing.T) {
	for _, bad := range []ExplorationRegion{
		{Key: "x", Name: "X", Country: "ZA", City: "a-sid-evil", Enabled: true},
		{Key: "x", Name: "X", Country: "US", Enabled: true},
		{Key: "unknown", Name: "X", Country: "ZA", Enabled: true},
	} {
		c := Defaults()
		c.Regions = append(c.RegionDefinitions(), bad)
		if c.validateExploration() == nil {
			t.Fatalf("accepted invalid region: %s", bad.Key)
		}
	}
	c := Defaults()
	c.CoverageGroups = []CoverageGroup{{Key: "test", Name: "Test", ExitKeys: []string{"us-east"}, Complete: true}}
	if c.validateExploration() == nil {
		t.Fatal("empty directory cannot be complete")
	}
}

func TestRegionsPatchDoesNotWriteChangedExitOrExposeSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	orig := `{"admin_key":"test-secret",` + okProxies + `}`
	if err := os.WriteFile(path, []byte(orig), 0o600); err != nil {
		t.Fatal(err)
	}
	defs := DefaultRegions()
	defs[0].Country = "CA"
	if Patch(path, map[string]any{"regions": defs}) == nil {
		t.Fatal("rewrote a stable exit")
	}
	got, _ := os.ReadFile(path)
	if string(got) != orig {
		t.Fatal("rejected change must leave file untouched")
	}
}
