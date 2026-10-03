package config

import (
	"os"
	"path/filepath"
	"testing"

	"gwpool/internal/types"
)

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
