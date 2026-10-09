package sched

import (
	"context"
	"testing"
	"time"

	"gwpool/internal/config"
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
	s, _, _, clk := rig(t, types.ModePool)
	for i := 0; i < 20; i++ {
		delay := s.noteWorkYield("explore|one", false)
		if delay < 5*time.Minute || delay > time.Hour {
			t.Fatal(delay)
		}
		clk.add(delay)
		if s.workDeferred("explore|one") {
			t.Fatal("retry did not reopen at its deadline")
		}
	}
	s.noteWorkYield("explore|one", true)
	if got := s.noteWorkYield("explore|one", false); got != 5*time.Minute {
		t.Fatal("productive result did not reset", got)
	}
	if s.workDeferred("renew|one") {
		t.Fatal("exploration hold leaked to renewal")
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
		s, _, mn, _ := rig(t, types.ModePool)
		s.mint = &regionCapabilityMinter{mn}
		if explore {
			s.exploreNewGateway(context.Background())
		} else {
			s.premintBare(context.Background(), 1)
		}
		if len(mn.mintedGateways()) == 0 {
			t.Fatalf("unsupported first exit blocked other exits: explore=%v", explore)
		}
	}
}

type timedEchoMinter struct {
	*fakeMinter
	clk *clock
}

func (m *timedEchoMinter) VerifyFull(ctx context.Context, c types.Creds, e types.Exit, sid, pair, ticket string) (*types.Verdict, error) {
	m.clk.add(1250 * time.Millisecond)
	return m.fakeMinter.VerifyFull(ctx, c, e, sid, pair, ticket)
}
func TestEchoSampleIncludesActualElapsedTime(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModeSingle)
	s.mint = &timedEchoMinter{mn, clk}
	if _, err := s.Deliver(context.Background(), 1, Request{}); err != nil {
		t.Fatal(err)
	}
	for _, sample := range st.samples {
		if sample.Purpose == types.PurposeVerify {
			if sample.DurationMS != 1250 {
				t.Fatal(sample.DurationMS)
			}
			return
		}
	}
	t.Fatal("missing verification sample")
}

func TestDirectedAndQueuedWorkRespectProviderCapabilities(t *testing.T) {
	s, _, mn, _ := rig(t, types.ModePool)
	s.mint = &regionCapabilityMinter{mn}
	unsupported := types.Gateway{ReachableExits: []types.Exit{{Region: "US"}}}
	if _, ok := s.pickExit(unsupported); ok {
		t.Fatal("directed work selected an unsupported exit")
	}
	unsupported.ReachableExits = append(unsupported.ReachableExits, types.Exit{Region: "BR"})
	for range 100 {
		if e, ok := s.pickExit(unsupported); !ok || e.Region != "BR" {
			t.Fatal("mixed gateway selected unsupported exit", e)
		}
	}
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
