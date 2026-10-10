package sched

import (
	"context"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/routes"
	"gwpool/internal/types"
)

func TestRenewDriftIsDeferredAndRechecked(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	duePair(st, clk, "unified-9", 1, "south-america")
	landBare(mn, "unified-2")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})
	renewOn(s)
	s.renewExpiring(context.Background())
	s.renewExpiring(context.Background())
	if got := len(mn.mintedGateways()); got != 1 {
		t.Fatalf("drift retried immediately: %d", got)
	}
	clk.add(5 * time.Minute)
	s.renewExpiring(context.Background())
	if got := len(mn.mintedGateways()); got != 2 {
		t.Fatalf("drift became a permanent blacklist: %d", got)
	}
}

func TestExploreDuplicateIsDeferredAndRechecked(t *testing.T) {
	s, _, mn, clk := rig(t, types.ModePool)
	cfg := config.Defaults()
	cfg.Regions = cfg.RegionDefinitions()
	for i := range cfg.Regions {
		cfg.Regions[i].Enabled = cfg.Regions[i].Key == "us-east"
	}
	s.opt.Live = config.NewLive("", cfg)
	landBare(mn, "unified-1")
	s.exploreNewGateway(context.Background())
	s.exploreNewGateway(context.Background())
	if got := len(mn.mintedGateways()); got != 1 {
		t.Fatalf("duplicate exploration retried immediately: %d", got)
	}
	clk.add(5 * time.Minute)
	s.exploreNewGateway(context.Background())
	if got := len(mn.mintedGateways()); got != 1 {
		t.Fatalf("combination backoff bypassed the dedicated 15-minute interval: %d", got)
	}
	clk.add(10 * time.Minute)
	s.exploreNewGateway(context.Background())
	if got := len(mn.mintedGateways()); got != 2 {
		t.Fatalf("duplicate exploration was not rechecked: %d", got)
	}
}

func TestWarmReservesFirstShotForExploration(t *testing.T) {
	s, st, _, _ := rig(t, types.ModePool)
	addAccount(st, 2, "acct-2")
	s.opt.Live = liveCfg(func(c *config.Config) { c.MinPairs = 4; c.SpareTarget = 0 })
	s.markDemand()
	s.warmOnce(context.Background())
	if len(st.samples) == 0 || st.samples[0].Purpose != types.PurposeExplore {
		t.Fatal("maintenance consumed the first opportunity before dedicated exploration")
	}
}

func TestLowYieldRetryIsBoundedAndSuccessResets(t *testing.T) {
	_, _, _, clk := rig(t, types.ModePool)
	descriptor := routes.Describe(types.Creds{AccountID: "one"}, types.Exit{Region: "US"},
		types.RouteSource{ID: routes.Digest("source")}, routes.Bare, "")
	var state types.RouteState
	for i := 0; i < 20; i++ {
		attempt := types.RouteAttempt{Route: descriptor, StartedAt: clk.now(), At: clk.now(),
			Status: 200, Gateway: "one"}
		state = routes.Advance(state, attempt, types.StockGain{})
		delay := state.NextCheck.Sub(clk.now())
		if delay < 5*time.Minute || delay > time.Hour {
			t.Fatal(delay)
		}
		clk.add(delay)
		if !routes.Allowed(state, clk.now(), false) {
			t.Fatal("retry did not reopen at its deadline")
		}
	}
	state = routes.Advance(state, types.RouteAttempt{Route: descriptor, StartedAt: clk.now(), At: clk.now(),
		Status: 200, Gateway: "one"}, types.StockGain{ExtendedMS: 1})
	if state.LowYieldStreak != 0 {
		t.Fatal("productive result without unresolved target miss did not reset streak")
	}
	if routes.Allowed(state, clk.now(), false) {
		t.Fatal("switching task kind bypassed route hold")
	}
}

func TestDisabledExplorationExitStillRenewsExistingSeedRoute(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	duePair(st, clk, "unified-9", 1, "south-america")
	landBare(mn, "unified-9")
	cfg := config.Defaults()
	cfg.RenewPairs = true
	cfg.Regions = cfg.RegionDefinitions()
	for i := range cfg.Regions {
		cfg.Regions[i].Enabled = false
	}
	s := New(st, mn, Options{Now: clk.now, RenewGap: -1, Live: config.NewLive("", cfg)})
	s.renewExpiring(context.Background())
	if len(mn.mintedGateways()) != 1 {
		t.Fatal("disabled exploration also disabled existing seed renewal")
	}
}

type regionCapabilityMinter struct{ *fakeMinter }

func (m *regionCapabilityMinter) SupportsDynamicExit(e types.Exit) bool { return e.Region != "US" }
func (m *regionCapabilityMinter) NewSIDFor(e types.Exit, sticky bool) (string, error) {
	if !m.SupportsDynamicExit(e) {
		return "", types.ErrNoExit
	}
	return m.fakeMinter.NewSIDFor(e, sticky)
}

func TestUnsupportedExitDoesNotStarveExplorationOrInventory(t *testing.T) {
	for _, explore := range []bool{true, false} {
		s, st, mn, _ := rig(t, types.ModePool)
		s.mint = &regionCapabilityMinter{mn}
		if explore {
			s.exploreNewGateway(context.Background())
		} else {
			recordKnownRecovery(t, st)
			s.premintBare(context.Background(), 1)
		}
		if len(mn.mintedGateways()) == 0 {
			t.Fatalf("unsupported first exit blocked other exits: explore=%v", explore)
		}
	}
}

func TestDirectedAndQueuedWorkRespectProviderCapabilities(t *testing.T) {
	s, _, mn, _ := rig(t, types.ModePool)
	s.mint = &regionCapabilityMinter{mn}
	q := NewMintAllQueue(s)
	status := q.Enqueue()
	if status.Skipped != 2 {
		t.Fatalf("unsupported US exits were queued: %+v", status)
	}
	result, retry := q.execute(context.Background(), mintAllJob{accountID: 1, region: types.Regions[0]})
	if retry || result.attempted || result.Reason != "供应商不支持此出口" {
		t.Fatal(result, retry)
	}
}
