package sched

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/routes"
	"gwpool/internal/types"
)

func TestMintDriftKeepsLongerActualLanding(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	landBare(mn, "unified-2")
	mn.pairLife = 12 * time.Minute
	old := types.Pair{Gateway: "unified-2", Cookie: cookiePrefix + "unified-2",
		ExpiresAt: clk.now().Add(40 * time.Minute), CflbExpiresAt: clk.now().Add(40 * time.Minute)}
	st.pairs[old.Gateway] = old
	s := New(st, mn, Options{Now: clk.now, MintGap: -1, RenewGap: -1})

	_, err := s.mintOnce(context.Background(), st.accounts[0], types.Exit{Region: "BR"},
		"dynamic", types.Pair{}, "unified-9", "unified-9")
	if !errors.Is(err, errLandingDrift) {
		t.Fatalf("expected landing drift, got %v", err)
	}
	got, ok := st.Pair(old.Gateway)
	if !ok || got.ExpiresAt.Before(old.ExpiresAt) {
		t.Fatalf("drift replaced 40-minute stock with shorter stock: old=%s got=%s",
			old.ExpiresAt, got.ExpiresAt)
	}
}

func inventoryTestCookie(gateway, cflb string, expiry time.Time) string {
	body := fmt.Sprintf(`{"host":"x.y.%s","exp":%d}`, gateway, expiry.Unix())
	return "__cflb=" + cflb + "; __oailb=e30." + base64.RawURLEncoding.EncodeToString([]byte(body)) + ".test"
}

func TestEstablishedInventoryDoesNotChaseSpareBeyondKnownGateways(t *testing.T) {
	s, st, _, _ := rig(t, types.ModePool)
	s.opt.Live = liveCfg(func(c *config.Config) { c.MinPairs, c.SpareTarget = 120, 20 })
	if got, want := s.PairTarget(), len(st.gws); got != want {
		t.Fatalf("maintenance target includes unknown numerical deficit: got=%d known=%d", got, want)
	}
}

func TestRenewCandidatesUseEffectiveExpiry(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	duePair(st, clk, "unified-1", 1, "us-east")
	pair := st.pairs["unified-1"]
	pair.Cookie = inventoryTestCookie(pair.Gateway, "old", clk.now().Add(time.Hour))
	pair.ExpiresAt, pair.CflbExpiresAt = clk.now().Add(6*time.Minute), clk.now().Add(6*time.Minute)
	st.pairs[pair.Gateway] = pair
	st.donors = map[string]types.RegionDonor{"us-east": {
		RegionKey: "us-east", Cflb: "longer", ExpiresAt: clk.now().Add(40 * time.Minute)}}
	s := New(st, mn, Options{Now: clk.now})
	due, _ := s.pairsDueForRenew()
	if len(due) != 0 {
		t.Fatal("raw six-minute ticket scheduled for renewal despite forty-minute effective life")
	}
}

func TestRegionDonorShortageCanRefreshAnAlreadyLiveLanding(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	us := types.Regions[0]
	duePair(st, clk, "unified-1", 1, us.Key)
	pair := st.pairs["unified-1"]
	pair.Cookie = inventoryTestCookie(pair.Gateway, "old", clk.now().Add(time.Hour))
	pair.ExpiresAt, pair.CflbExpiresAt = clk.now().Add(7*time.Minute), clk.now().Add(7*time.Minute)
	st.pairs[pair.Gateway] = pair
	landBare(mn, pair.Gateway)
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.MinPairs, c.SpareTarget, c.RenewPairs = 0, 0, true })})
	s.maintainInventory(context.Background())
	if got := len(mn.mintedGateways()); got != 1 {
		t.Fatalf("live landing suppressed necessary region donor supply: calls=%d", got)
	}
	if s.bestCflb()[us.Key].Exp.Sub(clk.now()) <= seedCflbFloor {
		t.Fatal("region donor was not extended")
	}
	s.maintainInventory(context.Background())
	if got := len(mn.mintedGateways()); got != 1 {
		t.Fatalf("successful donor replenishment caused redundant renewal: %d", got)
	}
}

func TestInventoryDriftCannotRetryByChangingTarget(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	us := types.Regions[0]
	for _, name := range []string{"unified-1", "unified-2", "unified-3"} {
		st.gws[name] = types.Gateway{Name: name, ReachableExits: []types.Exit{us.Exit}}
		st.pairs[name] = types.Pair{Gateway: name, Cookie: cookiePrefix + name,
			MintedBy: 1, RegionKey: us.Key, ExpiresAt: clk.now().Add(-time.Minute)}
	}
	st.pairs["unified-2"] = types.Pair{Gateway: "unified-2", Cookie: cookiePrefix + "unified-2",
		MintedBy: 1, RegionKey: us.Key, ExpiresAt: clk.now().Add(time.Hour),
		CflbExpiresAt: clk.now().Add(time.Hour)}
	landBare(mn, "unified-2")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.MinPairs, c.SpareTarget = 0, 0 })})
	s.maintainInventory(context.Background())
	first := len(mn.mintedGateways())
	if first != 1 {
		t.Fatalf("expected one actual route observation, got %d", first)
	}
	s.maintainInventory(context.Background())
	if got := len(mn.mintedGateways()); got != first {
		t.Fatalf("same route was retried by switching missing target: before=%d after=%d", first, got)
	}
}

type routeBudgetMinter struct {
	*fakeMinter
	reservations int
}

func (m *routeBudgetMinter) BeginAttempt(ctx context.Context, cr types.Creds, exit types.Exit) (context.Context, error) {
	m.reservations++
	return m.fakeMinter.BeginAttempt(ctx, cr, exit)
}

func TestSharedRouteHoldSkipsBeforeBudgetAndAllowsUsefulLanding(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	us := types.Regions[0]
	landBare(mn, "b")
	st.gws["b"] = types.Gateway{Name: "b", ReachableExits: []types.Exit{us.Exit}}
	st.pairs["b"] = types.Pair{Gateway: "b", Cookie: cookiePrefix + "b",
		RegionKey: us.Key, MintedBy: 1, ExpiresAt: clk.now().Add(7 * time.Minute)}
	counted := &routeBudgetMinter{fakeMinter: mn}
	s := New(st, counted, Options{Now: clk.now, RenewGap: -1, MintGap: -1})
	descriptor := routes.Describe(st.accounts[0].Creds, us.Exit, mn.RouteSources(us.Exit, false)[0], routes.Bare, "")
	attempt := types.RouteAttempt{Route: descriptor, StartedAt: clk.now(), At: clk.now(),
		Status: 200, Gateway: "b", Expected: "a"}
	if _, err := st.CommitMint(types.MintCommit{Route: &attempt, At: clk.now()}); err != nil {
		t.Fatal(err)
	}
	for _, purpose := range []string{types.PurposeInventory, types.PurposeExplore, types.PurposeRenew} {
		_, _, _, release, err := s.bindRoute(context.Background(), st.accounts[0], us.Exit, types.Pair{}, "another-target", purpose, false)
		if release != nil {
			release()
		}
		if !errors.Is(err, errRouteDeferred) {
			t.Fatalf("purpose %s bypassed the shared route hold: %v", purpose, err)
		}
	}
	if counted.reservations != 0 || mn.sidSeq != 0 {
		t.Fatal("a deferred route consumed budget or bound a fresh SID")
	}
	ctx, account, sid, release, err := s.bindRoute(context.Background(), st.accounts[0], us.Exit,
		types.Pair{}, "b", types.PurposeInventory, false)
	if err != nil {
		t.Fatalf("useful work at the known actual landing blocked: %v", err)
	}
	got, err := s.mintOnceFor(ctx, account, us.Exit, sid, types.Pair{}, "", "", types.PurposeInventory)
	release()
	if err != nil || !got.gain.Productive() {
		t.Fatalf("needed B extension did not commit: %v", err)
	}
	state := st.RouteStates(clk.now())[routes.Key(descriptor)]
	if state.LastMissed != "a" || routes.Allowed(state, clk.now(), false) {
		t.Fatal("B's gain cleared the original route miss")
	}
	if len(st.samples) != 1 || st.samples[0].Route == nil || !st.samples[0].Route.Gain.Productive() ||
		st.samples[0].Route.Target != "b" || st.samples[0].Route.NextCheck.IsZero() ||
		st.samples[0].Route.BeforeExpiresAt != clk.now().Add(7*time.Minute) ||
		!st.samples[0].Route.AfterExpiresAt.After(st.samples[0].Route.BeforeExpiresAt) {
		t.Fatal("committed benefit and next check missing from observation")
	}
}

func TestRouteClaimIsSharedAcrossLocalAliasesAndReleased(t *testing.T) {
	s, st, mn, _ := rig(t, types.ModePool)
	exit := types.Regions[0].Exit
	source := mn.RouteSources(exit, false)[0]
	descriptor := routes.Describe(st.accounts[0].Creds, exit, source, routes.Bare, "")
	key := routes.Key(descriptor)
	release, ok := s.claimRoute(key)
	if !ok {
		t.Fatal("first claim failed")
	}
	if second, ok := s.claimRoute(key); ok {
		second()
		t.Fatal("same stable route admitted twice")
	}
	release()
	release, ok = s.claimRoute(key)
	if !ok {
		t.Fatal("route stayed in flight after release")
	}
	release()
}

func TestMaintenanceTaskBudgetAndKnownCoverageAcrossRounds(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	us := types.Regions[0]
	total := maintenanceTasksPerRound + 5
	mn.landByAcct = map[string]string{}
	for i := 1; i <= total; i++ {
		id, gateway := int64(i), fmt.Sprintf("unified-%d", i)
		addAccount(st, id, fmt.Sprintf("acct-%d", i))
		st.gws[gateway] = types.Gateway{Name: gateway, ReachableExits: []types.Exit{us.Exit}}
		if err := st.NoteRegionMint(id, us.Key, gateway, clk.now()); err != nil {
			t.Fatal(err)
		}
		mn.landByAcct[comboKey(id, exitKey(us.Exit))] = gateway
	}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.MintConcurrency, c.MinPairs, c.SpareTarget = 3, 190, 20 })})
	s.maintainInventory(context.Background())
	if got := len(mn.mintedGateways()); got != maintenanceTasksPerRound {
		t.Fatalf("shared task budget multiplied by workers: %d", got)
	}
	s.maintainInventory(context.Background())
	if got := len(mn.mintedGateways()); got != total || s.FreeGateways() != total || s.PairTarget() != total {
		t.Fatalf("remaining known work was lost or numerical reserve was chased: calls=%d live=%d target=%d",
			got, s.FreeGateways(), s.PairTarget())
	}
}

func TestMaintenanceProtectsScarceLiveStockBeforeRecoverableMissing(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	duePair(st, clk, "rare", 1, types.Regions[0].Key)
	second := types.Regions[1]
	st.gws["missing"] = types.Gateway{Name: "missing", ReachableExits: []types.Exit{second.Exit}}
	if err := st.NoteRegionMint(2, second.Key, "missing", clk.now()); err != nil {
		t.Fatal(err)
	}
	s := New(st, mn, Options{Now: clk.now})
	renewOn(s)
	plan, pending := s.maintenancePlan(false)
	if pending != 0 || len(plan) != 2 || plan[0].renewal == nil || plan[0].renewal.gateway != "rare" ||
		plan[1].selection.gateway != "missing" {
		t.Fatal("scarce expiring stock was placed behind ordinary recovery")
	}
}

type selectedSourceMinter struct {
	*fakeMinter
	sources  []types.RouteSource
	selected string
}

func (m *selectedSourceMinter) RouteSources(types.Exit, bool) []types.RouteSource {
	return append([]types.RouteSource(nil), m.sources...)
}

func (m *selectedSourceMinter) NewSIDForRoute(exit types.Exit, source string, seed bool) (string, error) {
	m.selected = source
	return m.fakeMinter.NewSIDForRoute(exit, source, seed)
}

func TestRecentRouteEvidenceRequiresCurrentIdentityLocationAndSource(t *testing.T) {
	for _, change := range []string{"unchanged", "expired", "future", "disabled-account", "deleted-account",
		"changed-identity", "changed-source", "removed-source", "changed-city", "disabled-region", "seed-only"} {
		t.Run(change, func(t *testing.T) {
			clk := newClock()
			st, mn := newStore(clk), newMinter(clk)
			addAccount(st, 1, "acct-1")
			region := types.Regions[0]
			source := mn.RouteSources(region.Exit, false)[0]
			minter := &selectedSourceMinter{fakeMinter: mn, sources: []types.RouteSource{source}}
			// No legacy ReachableExits/RegionMint/Pair provenance: only a
			// recent exact observation can justify this recovery candidate.
			st.gws["b"] = types.Gateway{Name: "b"}
			descriptor := routes.Describe(st.accounts[0].Creds, region.Exit, source, routes.Bare, "")
			if change == "seed-only" {
				descriptor.Mode, descriptor.Seed = routes.Seed, routes.Digest("seed")
			}
			if change == "changed-city" {
				descriptor.Exit.City = "another city"
			}
			attempt := types.RouteAttempt{Route: descriptor, StartedAt: clk.now(), At: clk.now(), Gateway: "b", Status: 200}
			if _, err := st.CommitMint(types.MintCommit{At: clk.now(), Route: &attempt}); err != nil {
				t.Fatal(err)
			}
			s := New(st, minter, Options{Now: clk.now, Live: liveCfg(nil)})
			switch change {
			case "expired":
				if len(s.inventoryEvidence()) != 1 {
					t.Fatal("initial evidence not available")
				}
				clk.add(routes.RecoveryRetention)
			case "future":
				clk.add(-time.Minute)
			case "disabled-account":
				st.accounts[0].Enabled = false
			case "deleted-account":
				st.accounts = nil
			case "changed-identity":
				st.accounts[0].Creds.AccountID = "other-account"
			case "changed-source":
				minter.sources[0].ID = routes.Digest("changed-config")
			case "removed-source":
				minter.sources = nil
			case "disabled-region":
				cfg := s.cfg()
				cfg.Regions = cfg.RegionDefinitions()
				for i := range cfg.Regions {
					if cfg.Regions[i].Key == region.Key {
						cfg.Regions[i].Enabled = false
					}
				}
				s.opt.Live = config.NewLive("", cfg)
			}
			evidence := s.inventoryEvidence()
			if change == "unchanged" {
				if len(evidence) != 1 || evidence[0].account != 1 || evidence[0].gateway != "b" ||
					evidence[0].region != region.Key {
					t.Fatalf("exact current route not usable: %+v", evidence)
				}
			} else if len(evidence) != 0 {
				t.Fatalf("%s observation became a current recovery route: %+v", change, evidence)
			}
		})
	}
}

func TestBindRoutePrefersOnlyRecentExactSourceMatch(t *testing.T) {
	for _, recent := range []bool{true, false} {
		t.Run(fmt.Sprint(recent), func(t *testing.T) {
			clk := newClock()
			st, mn := newStore(clk), newMinter(clk)
			addAccount(st, 1, "acct-1")
			exit := types.Regions[0].Exit
			first, matched := types.RouteSource{ID: routes.Digest("first")}, types.RouteSource{ID: routes.Digest("matched")}
			minter := &selectedSourceMinter{fakeMinter: mn, sources: []types.RouteSource{first, matched}}
			descriptor := routes.Describe(st.accounts[0].Creds, exit, matched, routes.Bare, "")
			attempt := types.RouteAttempt{Route: descriptor, StartedAt: clk.now(), At: clk.now(), Status: 200, Gateway: "b"}
			if _, err := st.CommitMint(types.MintCommit{At: clk.now(), Route: &attempt}); err != nil {
				t.Fatal(err)
			}
			// Permit rechecking both sources; this test isolates source
			// preference from temporary low-yield eligibility and the separate
			// historical-source tier (covered in recovery_sources_test.go).
			st.recovery = nil
			if recent {
				clk.add(routes.RetryBase)
			} else {
				clk.add(routes.RetryMax)
			}
			s := New(st, minter, Options{Now: clk.now})
			_, _, _, release, err := s.bindRoute(context.Background(), st.accounts[0], exit, types.Pair{}, "b", types.PurposeInventory, false)
			if err != nil {
				t.Fatal(err)
			}
			release()
			want := first.ID
			if recent {
				want = matched.ID
			}
			if minter.selected != want {
				t.Fatal("source preference ignored recency or exact source evidence")
			}
		})
	}
}

func TestUnproductiveDonorHoldCannotBeBypassedByAnotherTask(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	exit := types.Regions[0].Exit
	descriptor := routes.Describe(st.accounts[0].Creds, exit, mn.RouteSources(exit, false)[0], routes.Bare, "")
	attempt := types.RouteAttempt{Route: descriptor, StartedAt: clk.now(), At: clk.now(), Status: 200, Gateway: "b", Donor: true}
	if _, err := st.CommitMint(types.MintCommit{At: clk.now(), Route: &attempt}); err != nil {
		t.Fatal(err)
	}
	state := st.RouteStates(clk.now())[routes.Key(descriptor)]
	for _, target := range []string{"", "a", "c"} {
		if s.routeCanWork(state, target, true) {
			t.Fatalf("unchanged donor need bypassed low-yield hold via target %q", target)
		}
	}
	clk.add(routes.RetryBase)
	if !s.routeCanWork(state, "a", true) {
		t.Fatal("backoff became permanent")
	}
	if len(mn.mintedGateways()) != 0 {
		t.Fatal("eligibility or elapsed backoff itself emitted an upstream request")
	}
}

func TestMaintenanceFallsBackToIndependentSeedWhenBareRouteDeferred(t *testing.T) {
	for _, allowRenew := range []bool{true, false} {
		t.Run(fmt.Sprint(allowRenew), func(t *testing.T) {
			clk := newClock()
			st, mn := newStore(clk), newMinter(clk)
			addAccount(st, 1, "acct-1")
			addAccount(st, 2, "acct-2")
			region := types.Regions[0]
			duePair(st, clk, "a", 1, region.Key)
			mn.land[exitKey(region.Exit)] = "a"
			pair := st.pairs["a"]
			pair.ExpiresAt = clk.now().Add(-time.Minute)
			st.pairs["a"] = pair
			descriptor := routes.Describe(st.accounts[0].Creds, region.Exit, mn.RouteSources(region.Exit, false)[0], routes.Bare, "")
			attempt := types.RouteAttempt{Route: descriptor, StartedAt: clk.now(), At: clk.now(),
				Status: 200, Gateway: "b", Expected: "a"}
			if _, err := st.CommitMint(types.MintCommit{At: clk.now(), Route: &attempt}); err != nil {
				t.Fatal(err)
			}
			s := New(st, mn, Options{Now: clk.now, MintGap: -1, RenewGap: -1,
				Live: liveCfg(func(c *config.Config) { c.RenewPairs = allowRenew })})
			s.maintainInventory(context.Background())
			want := 0
			if allowRenew {
				want = 1
			}
			if len(mn.mintedGateways()) != want || len(mn.seedShots()) != want {
				t.Fatalf("independent seed route not used according to switch: calls=%d seed=%d want=%d",
					len(mn.mintedGateways()), len(mn.seedShots()), want)
			}
			if allowRenew {
				if _, ready := s.freshPair("a", types.DeliverFloor); !ready {
					t.Fatal("seed fallback did not restore the intended gateway")
				}
			}
			state := st.RouteStates(clk.now())[routes.Key(descriptor)]
			if state.Attempts != 1 || state.LastMissed != "a" || routes.Allowed(state, clk.now(), false) {
				t.Fatal("seed fallback changed independent bare-route miss/backoff")
			}
		})
	}
}

func TestOlderRouteClueStillPermitsBoundedRecoveryWithoutLegacyProvenance(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	region := types.Regions[0]
	st.gws["a"] = types.Gateway{Name: "a"} // No geographically verified legacy route.
	descriptor := routes.Describe(st.accounts[0].Creds, region.Exit, mn.RouteSources(region.Exit, false)[0], routes.Bare, "")
	attempt := types.RouteAttempt{Route: descriptor, StartedAt: clk.now(), At: clk.now(), Status: 200, Gateway: "a"}
	if _, err := st.CommitMint(types.MintCommit{At: clk.now(), Route: &attempt}); err != nil {
		t.Fatal(err)
	}
	clk.add(routes.RetryMax)
	landBare(mn, "a")
	s := New(st, mn, Options{Now: clk.now, MintGap: -1, RenewGap: -1, Live: liveCfg(nil)})
	s.maintainInventory(context.Background())
	if len(mn.mintedGateways()) != 1 {
		t.Fatal("older, unexpired source clue lost all targeted recovery eligibility")
	}
	if _, ready := s.freshPair("a", types.DeliverFloor); !ready {
		t.Fatal("known gateway was not restored by bounded recheck")
	}
	s.maintainInventory(context.Background())
	if len(mn.mintedGateways()) != 1 {
		t.Fatal("successful limited recheck forced another request without stock need")
	}
}

func TestMaintenanceLogsEffectiveStockTimelineWithoutSecrets(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	region := types.Regions[0]
	st.gws["a"] = types.Gateway{Name: "a"}
	st.pairs["a"] = types.Pair{Gateway: "a", RegionKey: region.Key,
		Cookie:        inventoryTestCookie("a", "private-donor-material", clk.now().Add(time.Hour)),
		ExpiresAt:     clk.now().Add(7 * time.Minute),
		CflbExpiresAt: clk.now().Add(7 * time.Minute)}
	var log bytes.Buffer
	s := New(st, mn, Options{Now: clk.now, Log: slog.New(slog.NewJSONHandler(&log, nil))})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Snapshot evidence does not depend on admitting an upstream task.
	s.runMaintenance(ctx, false)
	clk.add(3 * time.Minute)
	s.runMaintenance(ctx, false)
	st.donors = map[string]types.RegionDonor{region.Key: {
		RegionKey: region.Key, Cflb: "another-private-donor", ExpiresAt: clk.now().Add(40 * time.Minute)}}
	s.runMaintenance(ctx, false)
	type stockRow struct {
		Gateway          string    `json:"gateway"`
		Ready            bool      `json:"ready"`
		DeliverableUntil time.Time `json:"deliverable_until"`
	}
	type event struct {
		Message string     `json:"msg"`
		At      time.Time  `json:"observed_at"`
		Stock   []stockRow `json:"stock"`
	}
	var snapshots []event
	decoder := json.NewDecoder(strings.NewReader(log.String()))
	for decoder.More() {
		var next event
		if err := decoder.Decode(&next); err != nil {
			t.Fatal(err)
		}
		if next.Message == "库存可交付快照" {
			snapshots = append(snapshots, next)
		}
	}
	if len(snapshots) != 3 {
		t.Fatalf("no per-gateway availability timeline: got %d snapshots", len(snapshots))
	}
	for i, want := range []bool{true, false, true} {
		if len(snapshots[i].Stock) != 1 || snapshots[i].Stock[0].Gateway != "a" || snapshots[i].Stock[0].Ready != want {
			t.Fatalf("snapshot %d not projected against current committed donor", i)
		}
	}
	if snapshots[0].Stock[0].DeliverableUntil != snapshots[1].Stock[0].DeliverableUntil ||
		!snapshots[1].At.After(snapshots[1].Stock[0].DeliverableUntil) ||
		snapshots[2].Stock[0].DeliverableUntil != clk.now().Add(40*time.Minute-types.DeliverFloor) {
		t.Fatal("snapshot omitted actual delivery cutoff needed to bound missing intervals")
	}
	if strings.Contains(log.String(), "private-donor") || strings.Contains(log.String(), "__oailb") ||
		len(mn.mintedGateways()) != 0 {
		t.Fatal("inventory observation exposed ticket material or emitted an upstream request")
	}
}

func TestMissingTasksUseActualProjectedDeliveryDeadlines(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	region := types.Regions[0]
	for i, gateway := range []string{"a-newer", "z-older"} {
		duePair(st, clk, gateway, 1, region.Key)
		pair := st.pairs[gateway]
		pair.ExpiresAt = clk.now().Add(-time.Duration(i+1) * time.Minute)
		st.pairs[gateway] = pair
	}
	s := New(st, mn, Options{Now: clk.now})
	plan, _ := s.maintenancePlan(false)
	if len(plan) != 2 || plan[0].selection.gateway != "z-older" ||
		plan[0].deadline != st.pairs["z-older"].ExpiresAt.Add(-types.DeliverFloor) {
		t.Fatal("missing work lost actual delivery deadlines and sorted by name instead")
	}
}

func TestBindRouteRechecksAccountBeforeBudgetWithoutRefreshHook(t *testing.T) {
	for _, change := range []string{"disabled", "deleted", "credential-changed"} {
		t.Run(change, func(t *testing.T) {
			clk := newClock()
			st, mn := newStore(clk), newMinter(clk)
			addAccount(st, 1, "acct-1")
			stale := st.accounts[0]
			counted := &routeBudgetMinter{fakeMinter: mn}
			s := New(st, counted, Options{Now: clk.now})
			switch change {
			case "disabled":
				st.accounts[0].Enabled = false
			case "deleted":
				st.accounts = nil
			case "credential-changed":
				st.accounts[0].Creds.AccessToken = "replacement-unknown-member-credential"
			}
			ctx, selected, _, release, err := s.bindRoute(context.Background(), stale, types.Regions[0].Exit,
				types.Pair{}, "a", types.PurposeExplore, false)
			if release != nil {
				release()
			}
			if change == "credential-changed" {
				work, planned := ctx.Value(routeWorkKey{}).(routeWork)
				if err != nil || !planned || selected.Creds != st.accounts[0].Creds ||
					work.attempt.Route.Identity != routes.CredentialIdentity(st.accounts[0].Creds) {
					t.Fatal("selection used stale credentials after the account lock")
				}
			} else if !errors.Is(err, ErrAccountUnusable) || counted.reservations != 0 || mn.sidSeq != 0 {
				t.Fatal("disabled/deleted account was bound or consumed budget")
			}
		})
	}
}

func TestDiscoveredGatewayAutomaticallyJoinsKnownMaintenance(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	landBare(mn, "unified-999")
	s := New(st, mn, Options{Now: clk.now, MintGap: -1, RenewGap: -1, Live: liveCfg(nil)})
	s.exploreNewGateway(context.Background())
	if s.PairTarget() != 1 || len(mn.mintedGateways()) != 1 {
		t.Fatal("discovery did not become a concrete known gateway")
	}
	if err := st.DeletePair("unified-999"); err != nil {
		t.Fatal(err)
	}
	s.maintainInventory(context.Background())
	if _, ready := s.freshPair("unified-999", types.DeliverFloor); !ready || len(mn.mintedGateways()) != 2 ||
		st.samples[1].Purpose != types.PurposeInventory {
		t.Fatal("newly discovered missing gateway was not maintained without another exploration turn")
	}
}

func TestMaintenanceSeedFallbackNeverAddsAttemptAfterBareFailure(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	region := types.Regions[0]
	duePair(st, clk, "a", 1, region.Key)
	pair := st.pairs["a"]
	pair.ExpiresAt = clk.now().Add(-time.Minute)
	st.pairs["a"] = pair
	mn.mintErr = errors.New("synthetic transport failure")
	s := New(st, mn, Options{Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.RenewPairs = true })})
	s.maintainInventory(context.Background())
	if len(mn.mintedGateways()) != 1 || len(mn.seedShots()) != 0 {
		t.Fatal("failed bare request acquired an additional seed attempt from the same task")
	}
}
