package sched

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/mint"
	"gwpool/internal/routes"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

func TestRecoverySourcesRememberEarlierLandingAfterDrift(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	us := types.Regions[0]
	for _, name := range []string{"unified-1", "unified-2"} {
		st.gws[name] = types.Gateway{Name: name, ReachableExits: []types.Exit{us.Exit}}
	}
	descriptor := routes.Describe(st.accounts[0].Creds, us.Exit, mn.RouteSources(us.Exit, false)[0], routes.Bare, "")
	for i, gateway := range []string{"unified-1", "unified-2"} {
		at := clk.now().Add(time.Duration(i-2) * time.Minute)
		attempt := types.RouteAttempt{Route: descriptor, StartedAt: at, At: at, Status: 200, Gateway: gateway}
		if _, err := st.CommitMint(types.MintCommit{At: at, Route: &attempt}); err != nil {
			t.Fatal(err)
		}
		if err := st.NoteRegionMint(1, us.Key, gateway, at); err != nil {
			t.Fatal(err)
		}
	}
	s := New(st, mn, Options{Now: clk.now})
	for _, route := range s.inventoryEvidence() {
		if route.account == 1 && route.region == us.Key && route.gateway == "unified-1" {
			return
		}
	}
	t.Fatal("a later landing erased the earlier gateway's recovery source")
}

func TestRecoverySourceBindingUsesExactHistoricalProviderAfterDrift(t *testing.T) {
	for _, mode := range []string{"historical", "recent-over-historical", "historical-held", "expired"} {
		t.Run(mode, func(t *testing.T) {
			s, st, mn, clk := recoveryRig()
			p := types.RouteSource{ID: routes.Digest("provider-p")}
			q := types.RouteSource{ID: routes.Digest("provider-q")}
			minter := &selectedSourceMinter{fakeMinter: mn, sources: []types.RouteSource{p, q}}
			s.mint = minter
			for i, gateway := range []string{"a", "b"} {
				at := clk.now().Add(-time.Duration(2-i) * time.Hour)
				attempt := types.RouteAttempt{
					Route:     routes.Describe(st.accounts[0].Creds, types.Regions[0].Exit, q, routes.Bare, ""),
					StartedAt: at, At: at, Status: 200, Gateway: gateway}
				if _, err := st.CommitMint(types.MintCommit{At: at, Route: &attempt}); err != nil {
					t.Fatal(err)
				}
			}
			want := q.ID
			switch mode {
			case "recent-over-historical":
				at := clk.now().Add(-10 * time.Minute)
				attempt := types.RouteAttempt{Route: routes.Describe(st.accounts[0].Creds, types.Regions[0].Exit, p, routes.Bare, ""),
					StartedAt: at, At: at, Status: 200, Gateway: "a"}
				if _, err := st.CommitMint(types.MintCommit{At: at, Route: &attempt}); err != nil {
					t.Fatal(err)
				}
				want = p.ID
			case "historical-held":
				attempt := types.RouteAttempt{Route: routes.Describe(st.accounts[0].Creds, types.Regions[0].Exit, q, routes.Bare, ""),
					StartedAt: clk.now(), At: clk.now(), Failure: "transport"}
				if _, err := st.CommitMint(types.MintCommit{At: clk.now(), Route: &attempt}); err != nil {
					t.Fatal(err)
				}
				want = p.ID
			case "expired":
				clk.add(routes.RecoveryRetention)
				want = p.ID
			}
			_, _, _, release, err := s.bindRoute(context.Background(), st.accounts[0], types.Regions[0].Exit,
				types.Pair{}, "a", types.PurposeInventory, false)
			if release != nil {
				release()
			}
			if err != nil || minter.selected != want {
				t.Fatal("exact historical source preference lost recency, expiry or shared hold semantics")
			}
		})
	}
}

func TestRecoveryExplorationShortTargetHitIsNotRestored(t *testing.T) {
	s, st, mn, _ := recoveryRig()
	mn.pairLife, mn.cflbLife = time.Minute, time.Minute
	var log bytes.Buffer
	s.opt.Log = slog.New(slog.NewJSONHandler(&log, nil))
	s.maintainInventory(context.Background())
	if _, ready := s.freshPair("a", types.DeliverFloor); ready {
		t.Fatal("short ticket became deliverable")
	}
	var found bool
	decoder := json.NewDecoder(&log)
	for decoder.More() {
		var event struct {
			Message  string `json:"msg"`
			Hit      bool   `json:"target_hit"`
			Restored bool   `json:"target_restored"`
		}
		if err := decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
		if event.Message == "缺票新身份探索" {
			found = true
			if !event.Hit || event.Restored {
				t.Fatal("target hit was confused with deliverable recovery")
			}
		}
	}
	if !found || len(st.samples) != 1 || !st.samples[0].Route.TargetHit || st.samples[0].Route.Gain.CoverageAdded {
		t.Fatal("short-hit evidence or committed coverage distinction lost")
	}
}

func recoveryRig() (*Scheduler, *fakeStore, *fakeMinter, *clock) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	us := types.Regions[0]
	st.gws["a"] = types.Gateway{Name: "a", ReachableExits: []types.Exit{us.Exit}}
	landBare(mn, "a")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.MinPairs, c.SpareTarget = 0, 0 })})
	s.markDemand()
	return s, st, mn, clk
}

func TestRecoveryExplorationRunsAfterOldSourcesAndOnlyIfStillMissing(t *testing.T) {
	for _, drift := range []bool{false, true} {
		t.Run(fmt.Sprint(drift), func(t *testing.T) {
			s, st, mn, clk := recoveryRig()
			us := types.Regions[0]
			if err := st.NoteRegionMint(1, us.Key, "a", clk.now()); err != nil {
				t.Fatal(err)
			}
			if drift {
				mn.landByAcct = map[string]string{comboKey(1, exitKey(us.Exit)): "b"}
			}
			s.maintainInventory(context.Background())
			want := 1
			if drift {
				want = 2
			}
			if len(st.samples) != want || st.samples[0].Purpose != types.PurposeInventory ||
				st.samples[0].AccountID != 1 {
				t.Fatal("new identity ran before old sources or ran despite a restored target")
			}
			if drift && (st.samples[1].Purpose != types.PurposeRecovery ||
				st.samples[1].AccountID != 2 || st.samples[1].Route.Target != "a") {
				t.Fatal("failed historical recovery did not try a distinct identity for the same target")
			}
			if _, ready := s.freshPair("a", types.DeliverFloor); !ready {
				t.Fatal("target was not restored")
			}
		})
	}
}

func TestRecoveryExplorationOpportunityAndTargetFairness(t *testing.T) {
	s, st, mn, clk := recoveryRig()
	st.gws["a"] = types.Gateway{Name: "a"} // Unknown exit cannot be guessed.
	st.gws["b"] = types.Gateway{Name: "b", ReachableExits: []types.Exit{types.Regions[0].Exit}}
	landBare(mn, "b")
	s.maintainInventory(context.Background())
	if len(mn.mints) != 0 || s.recoveryExploration.next != clk.now().Add(recoveryExploreInterval) {
		t.Fatal("no-candidate target emitted a request or failed to consume its opportunity")
	}
	st.gws["a"] = types.Gateway{Name: "a", ReachableExits: []types.Exit{types.Regions[0].Exit}}
	s.maintainInventory(context.Background())
	if len(mn.mints) != 0 {
		t.Fatal("maintenance tick reacquired exploration opportunity")
	}
	clk.add(recoveryExploreInterval)
	s.markDemand()
	s.maintainInventory(context.Background())
	if len(st.samples) != 1 || st.samples[0].Route.Target != "b" {
		t.Fatal("unrecoverable first target starved later missing gateway")
	}
	if _, ready := s.freshPair("a", types.DeliverFloor); ready {
		t.Fatal("side landing counted as restoring the target")
	}
}

func TestRecoveryExplorationDeduplicatesAliasesAndRemembersFailures(t *testing.T) {
	s, st, mn, clk := recoveryRig()
	st.accounts[1].Creds = st.accounts[0].Creds // Two rows, one upstream identity.
	addAccount(st, 3, "acct-3")
	mn.mintErr = errors.New("synthetic transport error")
	for i := 0; i < 3; i++ {
		s.markDemand()
		s.maintainInventory(context.Background())
		clk.add(recoveryExploreInterval)
	}
	if len(st.samples) != 2 || st.samples[0].AccountID != 1 || st.samples[1].AccountID != 3 {
		t.Fatal("aliases or repeated ticks retried an already tried identity")
	}
	restarted := New(st, mn, s.opt)
	restarted.markDemand()
	restarted.maintainInventory(context.Background())
	if len(st.samples) != 2 {
		t.Fatal("restart ignored retained route attempt exclusions")
	}
	clk.add(recoveryRetryInterval)
	restarted.markDemand()
	restarted.maintainInventory(context.Background())
	if len(st.samples) != 3 {
		t.Fatal("bounded identity recheck became a permanent blacklist")
	}
}

func TestRecoveryExplorationAdmissionGuards(t *testing.T) {
	for _, mode := range []string{"no-demand", "single-account", "off", "wake", "cancel", "held", "budget", "shared-route-hold"} {
		t.Run(mode, func(t *testing.T) {
			s, st, mn, clk := recoveryRig()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "no-demand":
				s.lastDeliver = time.Time{}
			case "single-account":
				st.accounts = st.accounts[:1]
			case "off":
				s.opt.Live = liveCfg(func(c *config.Config) { c.SpareTarget = 191 })
			case "cancel":
				cancel()
			case "held":
				for _, a := range st.accounts {
					s.holdAccount(a, time.Hour, "synthetic")
				}
			case "budget":
				mn.rateLimit, mn.rateUsed = 1, map[string]int{"US": 1}
			case "shared-route-hold":
				for _, a := range st.accounts {
					attempt := types.RouteAttempt{
						Route:     routes.Describe(a.Creds, types.Regions[0].Exit, mn.RouteSources(types.Regions[0].Exit, false)[0], routes.Bare, ""),
						StartedAt: clk.now(), At: clk.now(), Expected: "another-target", Failure: "transport"}
					if _, err := st.CommitMint(types.MintCommit{At: clk.now(), Route: &attempt}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if mode == "wake" {
				for i := 0; i < 5; i++ {
					s.maintainOnDemand(ctx)
				}
			} else {
				s.maintainInventory(ctx)
			}
			if len(mn.mints) != 0 || mn.sidSeq != 0 {
				t.Fatal("guard was bypassed by recovery exploration")
			}
			if mode == "wake" && !s.recoveryExploration.next.IsZero() {
				t.Fatal("demand wake consumed a periodic exploration opportunity")
			}
		})
	}
}

func TestRecoveryExplorationLockRechecks(t *testing.T) {
	for _, change := range []string{"restored", "disabled", "deleted", "identity", "region", "provider", "cancel"} {
		t.Run(change, func(t *testing.T) {
			s, st, mn, clk := recoveryRig()
			counted := &routeBudgetMinter{fakeMinter: mn}
			s.mint = counted
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st.afterLock = func(id int64) {
				st.afterLock = nil
				switch change {
				case "restored":
					st.pairs["a"] = types.Pair{Gateway: "a", Cookie: cookiePrefix + "a",
						ExpiresAt: clk.now().Add(time.Hour), CflbExpiresAt: clk.now().Add(time.Hour)}
				case "disabled":
					st.accounts[0].Enabled = false
				case "deleted":
					st.accounts = nil
				case "identity":
					st.accounts[0].Creds = st.accounts[1].Creds
					s.noteRecoveryAttempt("a", st.accounts[1])
				case "region":
					s.opt.Live = liveCfg(func(c *config.Config) {
						c.Regions = []config.ExplorationRegion{{Key: "country-us", Country: "US", Enabled: false}}
					})
				case "provider":
					s.mint = &selectedSourceMinter{fakeMinter: mn}
				case "cancel":
					cancel()
				}
			}
			s.maintainInventory(ctx)
			if len(mn.mints) != 0 || mn.sidSeq != 0 || counted.reservations != 0 {
				t.Fatalf("%s queued stale plan spent a budget or sent a request", change)
			}
		})
	}
}

func TestRecoveryExplorationSharesTwelveTaskLimit(t *testing.T) {
	s, st, mn, clk := recoveryRig()
	st.accounts = nil
	us := types.Regions[0]
	mn.landByAcct = map[string]string{}
	for i := 1; i <= maintenanceTasksPerRound; i++ {
		id, gateway := int64(i), fmt.Sprintf("known-%02d", i)
		addAccount(st, id, fmt.Sprintf("acct-%d", i))
		st.gws[gateway] = types.Gateway{Name: gateway, ReachableExits: []types.Exit{us.Exit}}
		if err := st.NoteRegionMint(id, us.Key, gateway, clk.now()); err != nil {
			t.Fatal(err)
		}
		mn.landByAcct[comboKey(id, exitKey(us.Exit))] = gateway
	}
	s.maintainInventory(context.Background())
	if len(mn.mints) != maintenanceTasksPerRound || !s.recoveryExploration.next.IsZero() {
		t.Fatal("new identity task exceeded the existing round budget")
	}
	for _, sample := range st.samples {
		if sample.Purpose == types.PurposeRecovery {
			t.Fatal("probe displaced normal runnable recovery")
		}
	}
}

func TestRecoveryExplorationFailedBindingSpendsDurableBudget(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	for i := 1; i <= 2; i++ {
		if _, err := st.AddAccount(types.Account{Enabled: true, Creds: types.Creds{
			AccountID: fmt.Sprintf("acct-%d", i), AccessToken: "fake"}}); err != nil {
			t.Fatal(err)
		}
	}
	us := types.Regions[0]
	if _, err := st.UpsertGateway("a", 1, us.Exit); err != nil {
		t.Fatal(err)
	}
	clk := newClock()
	mn := newMinter(clk)
	mn.sidErr = types.ErrNoExit
	live := liveCfg(nil)
	m := &durableAttemptMinter{fakeMinter: mn, budget: &mint.Client{Live: live, Attempts: st}}
	s := New(st, m, Options{Mode: types.ModePool, Now: clk.now, Live: live, MintGap: -1, RenewGap: -1})
	s.markDemand()
	s.maintainInventory(context.Background())
	if st.CountryAttempts("acct-1")["US"].Count != 1 || len(mn.mints) != 0 {
		t.Fatal("failed binding refunded durable budget or sent upstream")
	}
	rows, err := st.Samples()
	if err != nil || len(rows) != 1 || rows[0].Purpose != types.PurposeRecovery || rows[0].Route.Target != "a" {
		t.Fatal("recovery failure was not recorded separately from normal inventory/discovery")
	}
	if len(st.RecoverySources(clk.now())) != 0 {
		t.Fatal("binding failure became a positive recovery source")
	}
}

func TestRecoverySourcesPendingGatewayCanTryNewIdentity(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	us := types.Regions[0]
	st.gws["unified-7"] = types.Gateway{Name: "unified-7", ReachableExits: []types.Exit{us.Exit}}
	landBare(mn, "unified-7")
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.MinPairs, c.SpareTarget = 0, 0 })})
	s.markDemand()
	s.maintainInventory(context.Background())
	if _, ready := s.freshPair("unified-7", types.DeliverFloor); !ready {
		t.Fatal("known missing gateway remained pending without a bounded new-identity attempt")
	}
	if len(mn.mintedGateways()) != 1 {
		t.Fatal("recovery exploration was not a single task")
	}
}
