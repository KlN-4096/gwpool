package sched

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/routing"
	"gwpool/internal/types"
)

type routingTestStore struct {
	*fakeStore
	rows  []routing.Observation
	err   error
	reads int
}

func (s *routingTestStore) RoutingObservations(time.Time) (routing.History, error) {
	s.reads++
	return routing.History{Observations: append([]routing.Observation(nil), s.rows...)}, s.err
}

func inventoryRoutingFixture(mode string) (*Scheduler, *routingTestStore, *fakeMinter, *clock) {
	clk := newClock()
	base, minter := newStore(clk), newMinter(clk)
	st := &routingTestStore{fakeStore: base}
	region := types.Regions[0]
	for id := int64(1); id <= 2; id++ {
		account := fmt.Sprintf("acct-%d", id)
		addAccount(base, id, account)
		actual := fmt.Sprintf("unified-%d", id)
		for i := 0; i < 10; i++ {
			st.rows = append(st.rows, routing.Observation{Donor: routing.DonorKey(account),
				Country: "US", Gateway: actual, Requested: region.Exit, At: clk.now().Add(-time.Minute)})
		}
	}
	for _, name := range []string{"unified-1", "unified-2", "unified-3"} {
		base.gws[name] = types.Gateway{Name: name, ReachableExits: []types.Exit{region.Exit}}
	}
	base.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(time.Hour), CflbExpiresAt: clk.now().Add(time.Hour)}
	_ = base.NoteRegionMint(1, region.Key, "unified-3", clk.now().Add(-time.Hour))
	_ = base.NoteRegionMint(2, region.Key, "unified-2", clk.now().Add(-time.Hour))
	minter.landByAcct = map[string]string{
		comboKey(1, exitKey(region.Exit)): "unified-1",
		comboKey(2, exitKey(region.Exit)): "unified-2",
	}
	s := New(st, minter, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) {
			c.InventoryRouting, c.MinPairs, c.SpareTarget = mode, 2, 0
		})})
	return s, st, minter, clk
}

func TestInventoryRoutingShadowPreservesSelectionAssistOnlyReorders(t *testing.T) {
	for _, mode := range []string{config.InventoryRoutingOff, config.InventoryRoutingShadow, config.InventoryRoutingAssist} {
		t.Run(mode, func(t *testing.T) {
			s, st, minter, clk := inventoryRoutingFixture(mode)
			s.runInventory(context.Background(), 1, false)
			want := "unified-1"
			if mode == config.InventoryRoutingAssist {
				want = "unified-2"
			}
			mints := minter.mintedGateways()
			if len(mints) != 1 || mints[0] != want || len(st.samples) != 1 {
				t.Fatalf("changed attempt budget or wrong selection: mints=%v samples=%d", mints, len(st.samples))
			}
			p := st.samples[0].InventoryPrediction
			if mode == config.InventoryRoutingOff {
				if p != nil || st.reads != 0 {
					t.Fatal("off mode read history or recorded a forecast")
				}
			} else if p == nil || p.Gateway != want || p.Mode != mode || p.TrainingSamples != 20 ||
				!p.DecidedAt.Equal(clk.now()) || !p.TrainedThrough.Before(p.DecidedAt) || st.reads != 1 {
				t.Fatalf("forecast not frozen before attempt: %+v reads=%d", p, st.reads)
			}
		})
	}
}

func TestInventoryRoutingNeverOverridesHourlyBalanceOrBudget(t *testing.T) {
	for _, used := range []int{1, 2} {
		s, st, minter, _ := inventoryRoutingFixture(config.InventoryRoutingAssist)
		minter.rateLimit = 2
		minter.ratePerAcct = map[string]map[string]int{"acct-2": {"US": used}}
		s.runInventory(context.Background(), 1, false)
		if got := minter.mintedGateways(); len(got) != 1 || got[0] != "unified-1" {
			t.Fatalf("prediction overrode hourly balance/quota with used=%d: %v", used, got)
		}
		if len(st.samples) != 1 {
			t.Fatal("prediction introduced extra attempts")
		}
	}
}

func TestInventoryRoutingDoesNotPromotePredictionsToRecoveryEvidence(t *testing.T) {
	s, st, _, _ := inventoryRoutingFixture(config.InventoryRoutingAssist)
	// Only account 1 has a historical recovery route. A prediction for account 2
	// cannot jump from the bootstrap cohort ahead of that available route.
	delete(st.mints, slotKey(2, types.Regions[0].Key))
	r := &premintRound{want: 1, tried: map[string]bool{}, aimed: map[string]bool{}, allowBootstrap: true,
		routing: s.newInventoryRoutingRound()}
	plan, ok := s.premintPick(r)
	if !ok {
		t.Fatal("no historical candidate")
	}
	plan.release()
	if plan.acct.ID != 1 || plan.expected != "unified-3" || r.unproven != 0 {
		t.Fatalf("prediction became recovery evidence: account=%d expected=%q unproven=%d",
			plan.acct.ID, plan.expected, r.unproven)
	}

	// With no provenance left, assistance may order bootstrap candidates but
	// still consumes the original unproven counter and never writes expected.
	st.mints = map[string]types.RegionMint{}
	r = &premintRound{want: 1, tried: map[string]bool{}, aimed: map[string]bool{}, allowBootstrap: true,
		routing: s.newInventoryRoutingRound()}
	plan, ok = s.premintPick(r)
	if !ok {
		t.Fatal("no bootstrap candidate")
	}
	plan.release()
	if plan.expected != "" || r.unproven != 1 {
		t.Fatalf("bootstrap budget bypassed: expected=%q unproven=%d", plan.expected, r.unproven)
	}
}

func TestInventoryRoutingFailureAndAbstentionKeepOriginalPath(t *testing.T) {
	for _, kind := range []string{"history-error", "sparse"} {
		t.Run(kind, func(t *testing.T) {
			s, st, minter, _ := inventoryRoutingFixture(config.InventoryRoutingAssist)
			if kind == "history-error" {
				st.err = errors.New("test history unavailable")
			} else {
				st.rows = st.rows[:2]
			}
			s.runInventory(context.Background(), 1, false)
			if got := minter.mintedGateways(); len(got) != 1 || got[0] != "unified-1" {
				t.Fatalf("missing model altered baseline: %v", got)
			}
		})
	}
}

func TestInventoryRoutingIncludesFailedBindingGeoAndHTTPAttempts(t *testing.T) {
	for _, failure := range []string{"binding", "geo", "http"} {
		t.Run(failure, func(t *testing.T) {
			s, st, minter, _ := inventoryRoutingFixture(config.InventoryRoutingShadow)
			switch failure {
			case "binding":
				minter.sidErr = types.ErrNoExit
			case "geo":
				minter.geoCountry = "HK"
			case "http":
				minter.mintStatus = 429
			}
			s.runInventory(context.Background(), 1, false)
			if len(st.samples) != 1 || st.samples[0].InventoryPrediction == nil {
				t.Fatalf("failed attempt missing from shadow denominator: %+v", st.samples)
			}
			wantStatus := 0
			if failure == "http" {
				wantStatus = 429
			}
			if st.samples[0].Status != wantStatus || st.samples[0].InventoryPrediction.Country != "US" {
				t.Fatal("failure overwritten or posterior Geo substituted into prediction")
			}
			if failure != "http" && len(minter.mints) != 0 {
				t.Fatal("failed preflight sent a model request")
			}
		})
	}
}

func TestInventoryRoutingPredictionScopeAndCopy(t *testing.T) {
	s, st, minter, _ := inventoryRoutingFixture(config.InventoryRoutingShadow)
	region := types.Regions[0]
	p := &types.InventoryPrediction{Model: routing.Version, Gateway: "unified-1", Country: "US"}
	ctx := withInventoryPrediction(context.Background(), 1, region.Exit, p)
	p.Gateway = "unified-999"
	if got := inventoryPredictionFromContext(ctx, 1, region.Exit); got.Gateway != "unified-1" {
		t.Fatal("context retained mutable forecast pointer")
	}
	if inventoryPredictionFromContext(ctx, 2, region.Exit) != nil ||
		inventoryPredictionFromContext(ctx, 1, types.Exit{Region: "JP"}) != nil {
		t.Fatal("forecast leaked to another account/exit")
	}
	landBare(minter, "unified-1")
	_, _ = s.mintOnceFor(ctx, st.accounts[0], region.Exit, "sid", types.Pair{}, "", "", types.PurposeDelivery)
	if len(st.samples) != 1 || st.samples[0].InventoryPrediction != nil {
		t.Fatal("inventory predictor affected a non-inventory attempt")
	}
}

func TestInventoryRoutingRegionRankOnlyBreaksFairnessTies(t *testing.T) {
	regions := []types.Region{{Key: "us", Exit: types.Exit{Region: "US"}},
		{Key: "jp", Exit: types.Exit{Region: "JP"}}}
	rank := func(r types.Region) int {
		if r.Key == "jp" {
			return 2
		}
		return 0
	}
	pick, _, _, ok := bestRegionRanked(regions, nil, 2, 50, nil, nil, nil, rank)
	if !ok || pick.Key != "jp" {
		t.Fatal("rank did not break an otherwise equal region tie")
	}
	pick, _, _, ok = bestRegionRanked(regions, nil, 2, 50, map[string]int64{"JP": 1}, nil, nil, rank)
	if !ok || pick.Key != "us" {
		t.Fatal("rank overrode country traversal fairness")
	}
	pick, _, _, ok = bestRegionRanked(regions, map[string]int{"JP": 2}, 2, 50, nil, nil, nil, rank)
	if !ok || pick.Key != "us" {
		t.Fatal("rank overrode country quota")
	}
}
