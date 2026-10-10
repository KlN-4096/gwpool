package sched

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

func TestInventoryRestoresKnownRouteBeforeUntriedCountries(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	us := types.Regions[0]
	st.gws["unified-7"] = types.Gateway{Name: "unified-7", ReachableExits: []types.Exit{us.Exit}}
	seedGateway(st, mn, "unified-1", 2, 2)
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(time.Hour), CflbExpiresAt: clk.now().Add(time.Hour)}
	landBare(mn, "unified-1")
	mn.landByAcct = map[string]string{comboKey(1, exitKey(us.Exit)): "unified-7"}
	for i := 0; i < 8; i++ {
		if err := st.NoteRegionMint(1, us.Key, "unified-7", clk.now().Add(-2*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.SpareTarget = 2 })})
	s.maintainInventory(context.Background())
	got := mn.mintedGateways()
	if len(got) != 1 || got[0] != "unified-7" || s.FreeGateways() != 2 {
		t.Fatalf("缺已知网关却先尝试陌生国家：minted=%v, live=%d", got, s.FreeGateways())
	}
}

func TestInventoryTargetIncludesKnownRecoverableGateways(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	for i, r := range types.Regions[:3] {
		id := int64(i + 1)
		addAccount(st, id, "account")
		gateway := []string{"unified-1", "unified-2", "unified-3"}[i]
		st.gws[gateway] = types.Gateway{Name: gateway, ReachableExits: []types.Exit{r.Exit}}
		if err := st.NoteRegionMint(id, r.Key, gateway, clk.now()); err != nil {
			t.Fatal(err)
		}
	}
	st.gws["unified-4"] = types.Gateway{Name: "unified-4"} // 保留待恢复目标，不创造路线。
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now,
		Live: liveCfg(func(c *config.Config) { c.MinPairs, c.SpareTarget = 2, 0 })})
	if got := s.PairTarget(); got != 4 {
		t.Fatalf("known pending gateway disappeared from objective: got=%d", got)
	}
	_, pending := s.maintenancePlan(false)
	if pending != 1 {
		t.Fatalf("missing route not distinguished from runnable work: %d", pending)
	}
	st.accounts[2].Enabled = false
	_, pending = s.maintenancePlan(false)
	if got := s.PairTarget(); got != 4 || pending != 2 {
		t.Fatalf("disabled source should suspend work, not erase the objective: target=%d pending=%d", got, pending)
	}
}

func TestPairTargetIncludesExternalConsumerDemand(t *testing.T) {
	s, st, _, clk := rig(t, types.ModePool)
	st.accounts = append(st.accounts, types.Account{ID: 50, Enabled: true,
		Creds: types.Creds{AccountID: types.AnonPrefix + "external"}})
	for _, gw := range []string{"unified-1", "unified-2", "unified-3", "unified-4"} {
		st.slots[slotKey(50, gw)] = types.Slot{AccountID: 50, Gateway: gw, LastDeliver: clk.now()}
	}
	if got := s.PairTarget(); got != len(st.gws) || s.busiestDeliveries(clk.now().Add(-time.Hour)) != 0 {
		t.Fatalf("legacy external slots changed supply demand: %d", got)
	}
	for _, gw := range []string{"unified-1", "unified-2", "unified-3", "unified-4"} {
		s.external.delivered(types.ExternalConsumerID("external"), gw, clk.now())
	}
	if s.PairTarget() != len(st.gws) || s.busiestDeliveries(clk.now().Add(-time.Hour)) != 4 {
		t.Fatal("consumer demand should remain recorded without inflating known inventory")
	}
}

func TestAnonymousShortageWakesInventoryBeforePeriodicTick(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	us := types.Regions[0]
	seedGateway(st, mn, "unified-7", 0, 1)
	if err := st.NoteRegionMint(1, us.Key, "unified-7", clk.now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, WarmInterval: time.Hour,
		MintGap: -1, RenewGap: -1, Live: liveCfg(func(c *config.Config) { c.SpareTarget = 1 })})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	defer func() { cancel(); <-done }()
	if _, err := s.DeliverAnonymous(ctx, "external", Request{Gateway: "unified-7"}, true); err == nil {
		t.Fatal("初始缺库存应先返回缺票")
	}
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) && s.FreeGateways() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if s.FreeGateways() != 1 {
		t.Fatal("有效缺票请求没有在周期巡检前唤醒补货")
	}
}

func TestNamedRecoveryUsesHistoricalAccountAndExitTogether(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	for id := int64(1); id <= 3; id++ {
		addAccount(st, id, fmt.Sprintf("acct-%d", id))
	}
	us := types.Regions[0]
	seedGateway(st, mn, "unified-7", 0, 3)
	landBare(mn, "unified-1")
	mn.landByAcct = map[string]string{comboKey(3, exitKey(us.Exit)): "unified-7"}
	if err := st.NoteRegionMint(3, us.Key, "unified-7", clk.now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1})
	s.runInventory(context.Background(), 1, false)
	got, ok := st.Pair("unified-7")
	if !ok || got.MintedBy != 3 {
		t.Fatalf("后台恢复没有保持账号与出口组合: %+v", got)
	}
}

func TestInventoryDefersBadPredictionAndDoesNotCountShortPair(t *testing.T) {
	s, _, _, clk := rig(t, types.ModePool)
	plan := premintPlan{
		acct: types.Account{ID: 1}, region: types.Regions[0], expected: "unified-7",
	}
	round := &premintRound{want: 1}
	s.premintNote(round, plan, minted{pair: types.Pair{
		Gateway: "unified-7", ExpiresAt: clk.now().Add(types.DeliverFloor - time.Second),
	}}, nil, false)
	if round.got != 0 || round.missed != 1 {
		t.Fatal("短票被当作补货成功")
	}
}

func TestInventoryBudgetExhaustionDoesNotFallBackToUnprovenCountries(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	us := types.Regions[0]
	seedGateway(st, mn, "unified-7", 0, 1)
	if err := st.NoteRegionMint(1, us.Key, "unified-7", clk.now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	mn.rateLimit, mn.rateUsed = 2, map[string]int{"US": 2}
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.MinPairs, c.SpareTarget = 0, 0 })})
	s.maintainInventory(context.Background())
	if s.PairTarget() != 1 || len(mn.mintedGateways()) != 0 {
		t.Fatal("已知恢复路线暂时没额度，却改为乱扫陌生国家或降低维护目标")
	}
}

func TestInventoryDoesNotLetUnrecoverableLivePairMaskMissingKnownGateway(t *testing.T) {
	for _, demand := range []bool{false, true} {
		t.Run(fmt.Sprint("demand=", demand), func(t *testing.T) {
			clk := newClock()
			st, mn := newStore(clk), newMinter(clk)
			addAccount(st, 1, "acct-1")
			addAccount(st, 2, "acct-2")
			for i, gateway := range []string{"unified-1", "unified-2"} {
				region := types.Regions[i]
				seedGateway(st, mn, gateway, i, int64(i+1))
				if err := st.NoteRegionMint(int64(i+1), region.Key, gateway, clk.now().Add(-time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			st.gws["unified-3"] = types.Gateway{Name: "unified-3"} // 没有恢复路线的临时活票。
			for _, gateway := range []string{"unified-1", "unified-3"} {
				st.pairs[gateway] = types.Pair{Gateway: gateway, Cookie: cookiePrefix + gateway,
					ExpiresAt: clk.now().Add(time.Hour), CflbExpiresAt: clk.now().Add(time.Hour)}
			}
			s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
				Live: liveCfg(func(c *config.Config) { c.MinPairs, c.SpareTarget = 0, 0 })})
			s.markDemand()
			if demand {
				s.maintainOnDemand(context.Background())
			} else {
				s.maintainInventory(context.Background())
			}
			if _, ready := s.freshPair("unified-2", types.DeliverFloor); !ready {
				t.Fatal("总数够了，但明确要求维护的已知网关仍未补回")
			}
		})
	}
}

func TestDemandWakeCannotBorrowBootstrapExplorationBudget(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	seedGateway(st, mn, "unified-1", 0, 1)
	if err := st.NoteRegionMint(1, types.Regions[0].Key, "unified-1", clk.now()); err != nil {
		t.Fatal(err)
	}
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(time.Hour), CflbExpiresAt: clk.now().Add(time.Hour)}
	landEachRegionSomewhereNew(mn)
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.MinPairs = 10 })})
	s.markDemand()
	for i := 0; i < 3; i++ {
		s.maintainOnDemand(context.Background())
	}
	if len(mn.mintedGateways()) != 0 {
		t.Fatal("客户端连续唤醒借用了陌生国家的探索预算")
	}
}

func TestInventoryKeepsExpiredTicketProvenanceAfterOneRouteDrift(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	us := types.Regions[0]
	for _, gateway := range []string{"unified-1", "unified-7"} {
		st.gws[gateway] = types.Gateway{Name: gateway, ReachableExits: []types.Exit{us.Exit}}
	}
	st.pairs["unified-1"] = types.Pair{Gateway: "unified-1", Cookie: cookiePrefix + "unified-1",
		ExpiresAt: clk.now().Add(time.Hour), CflbExpiresAt: clk.now().Add(time.Hour)}
	st.pairs["unified-7"] = types.Pair{Gateway: "unified-7", Cookie: cookiePrefix + "unified-7",
		MintedBy: 1, RegionKey: us.Key, ExpiresAt: clk.now().Add(-time.Minute),
		CflbExpiresAt: clk.now().Add(-time.Minute)}
	if err := st.NoteRegionMint(1, us.Key, "unified-1", clk.now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	mn.land[exitKey(us.Exit)] = "unified-7"
	s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now, MintGap: -1, RenewGap: -1,
		Live: liveCfg(func(c *config.Config) { c.MinPairs, c.SpareTarget = 0, 0 })})
	if s.PairTarget() != 2 {
		t.Fatal("一次漂移覆盖最新台账后，丢掉了旧票唯一的来源线索")
	}
	s.maintainInventory(context.Background())
	if _, ready := s.freshPair("unified-7", types.DeliverFloor); !ready {
		t.Fatal("有原票来源却再也不尝试恢复稀有网关")
	}
	cfg := s.opt.Live.Get()
	cfg.SpareTarget = types.GatewayCeiling + 1
	s.opt.Live = config.NewLive("", cfg)
	if _, _, gap := s.inventoryGap(); gap != 0 {
		t.Fatal("显式关闭预铸后，历史恢复路线重新打开了补货")
	}
}
