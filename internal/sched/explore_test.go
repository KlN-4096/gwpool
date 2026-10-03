package sched

import (
	"context"
	"errors"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/coverage"
	"gwpool/internal/types"
)

type explorationBindingFailure struct {
	*fakeMinter
	seen []string
}

func (m *explorationBindingFailure) NewSIDFor(exit types.Exit, _ bool) (string, error) {
	m.seen = append(m.seen, exitKey(exit))
	return "", types.ErrNoExit
}

func TestExploreRotatesGlobalCountriesEvenWhenEveryAttemptFails(t *testing.T) {
	for _, bindingFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "mint-failure", true: "binding-failure"}[bindingFailure], func(t *testing.T) {
			s, _, mn, clk := rig(t, types.ModePool)
			cfg, err := config.GlobalCountryRegions(config.Defaults())
			if err != nil {
				t.Fatal(err)
			}
			s.opt.Live = config.NewLive("", cfg)
			mn.mintErr = errors.New("offline country inventory unavailable")
			binding := &explorationBindingFailure{fakeMinter: mn}
			if bindingFailure {
				s.mint = binding
			}
			for range len(cfg.Regions) {
				s.exploreNewGateway(context.Background())
				// New budget window must not restart country choice from the front.
				clk.add(time.Hour)
			}
			seen := map[string]bool{}
			if bindingFailure {
				for _, region := range binding.seen {
					seen[region] = true
				}
			} else {
				for _, shot := range mn.mints {
					seen[shot.RegionKey] = true
				}
			}
			if len(seen) != len(cfg.Regions) {
				t.Fatalf("only attempted %d/%d country exits; failures starved later candidates", len(seen), len(cfg.Regions))
			}
		})
	}
}

func TestExploreWithFullInventoryPrefersIncompleteAndNeverExceedsOneShot(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	cfg := config.Defaults()
	cfg.SpareTarget = 0 // 明确让库存层零发，下面数的是专门探索。
	cfg.Regions = cfg.RegionDefinitions()
	for i := range cfg.Regions {
		cfg.Regions[i].Enabled = cfg.Regions[i].Key == "us-east" || cfg.Regions[i].Key == "us-west"
	}
	cfg.CoverageGroups = []config.CoverageGroup{
		{Key: "done", Name: "Done", ExitKeys: []string{"us-east"}, Targets: []string{"unified-1"}, Complete: true},
		{Key: "next", Name: "Next", ExitKeys: []string{"us-west"}, Targets: []string{"unified-215"}, Complete: true},
	}
	s.opt.Live = config.NewLive("", cfg)
	mn.land[exitKey(types.Exit{Region: "US", City: "Seattle"})] = "unified-215"
	s.markDemand()
	s.warmOnce(context.Background())
	if got := mn.mintedGateways(); len(got) != 1 || got[0] != "unified-215" {
		t.Fatalf("expected one discovery in the incomplete exit, got %v", got)
	}
	if st.pairs["unified-215"].RegionKey != "us-west" {
		t.Fatal("exploration bypassed the normal pair ledger")
	}
	s.warmOnce(context.Background())
	if len(mn.mintedGateways()) != 1 {
		t.Fatal("all explicitly complete targets should no longer consume exploration budget")
	}
}

func TestExploreStillHonorsIdleAndRegionQuota(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.opt.Live = liveCfg(func(c *config.Config) { c.SpareTarget = 0 })
	s.warmOnce(context.Background())
	if len(mn.mintedGateways()) != 0 {
		t.Fatal("idle pool started exploring")
	}
	mn.rateLimit = 1
	mn.rateUsed = map[string]int{}
	for _, r := range types.Regions {
		mn.rateUsed[r.Key] = 1
	}
	s.markDemand()
	s.warmOnce(context.Background())
	if len(mn.mintedGateways()) != 0 {
		t.Fatal("new discovery bypassed region quota")
	}
}

func TestExploreUsesActualExitEvenWhenRegionHasSeed(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	cfg := config.Defaults()
	cfg.SpareTarget = 0
	cfg.Regions = cfg.RegionDefinitions()
	for i := range cfg.Regions {
		cfg.Regions[i].Enabled = cfg.Regions[i].Key == "us-east"
	}
	cfg.CoverageGroups = []config.CoverageGroup{{
		Key: "east", Name: "East", ExitKeys: []string{"us-east"}, Targets: []string{"unified-215"}, Complete: true,
	}}
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		RegionKey: "us-east", ExpiresAt: clk.now().Add(types.PairLifetime), CflbExpiresAt: clk.now().Add(types.PairLifetime)}
	mn.land["US"] = "unified-215"
	s.opt.Live = config.NewLive("", cfg)
	s.markDemand()
	s.warmOnce(context.Background())
	if len(mn.seedShots()) != 0 {
		t.Fatal("专门探索不能借种子绕开实际区域出口")
	}
	view := coverage.Build(cfg, st.gws, st.pairs, clk.now())
	if !view.Groups[0].Done || view.Groups[0].Found != 1 {
		t.Fatal("actual exit discovery did not complete its target coverage")
	}
}
