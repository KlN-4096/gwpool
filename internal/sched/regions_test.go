package sched

import (
	"context"
	"testing"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

func TestRegionsCustomExitFlowsThroughMintLedgerAndQueue(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	addAccount(st, 2, "acct-2")
	cfg := config.Defaults()
	cfg.Regions = cfg.RegionDefinitions()
	for i := range cfg.Regions {
		cfg.Regions[i].Enabled = false
	}
	custom := config.ExplorationRegion{Key: "africa-south", Name: "South Africa", Country: "ZA", Enabled: true}
	cfg.Regions = append(cfg.Regions, custom)
	mn.land[exitKey(custom.Exit())] = "unified-215"
	s := New(st, mn, Options{Now: clk.now, MintGap: -1, RenewGap: -1, Live: config.NewLive("", cfg)})
	if _, r, ok := s.PickMinter(0); !ok || r.Key != custom.Key || r.Exit != custom.Exit() {
		t.Fatal("custom exit was not chosen")
	}
	q := NewMintAllQueue(s)
	if status := q.Enqueue(custom.Key); status.Total != 2 {
		t.Fatalf("should enqueue only two enabled-region combinations: %+v", status)
	}
	status := finishMintAll(t, q)
	if status.Succeeded != 2 {
		t.Fatalf("custom region did not reach actual mint path: %+v", status)
	}
	if pair := st.pairs["unified-215"]; pair.RegionKey != custom.Key {
		t.Fatalf("custom pair label lost: %q", pair.RegionKey)
	}
	for _, m := range st.mints {
		if m.RegionKey != custom.Key {
			t.Fatalf("ledger lost custom region: %s", m.RegionKey)
		}
	}
	if e, ok := s.renewalExit(custom.Key); !ok || e != custom.Exit() {
		t.Fatal("renewal cannot use the new stable region")
	}
	// 入队后停用，执行时也要重新核对，而非沿用队列快照。
	cfg.Regions = append([]config.ExplorationRegion(nil), cfg.Regions...)
	cfg.Regions[len(cfg.Regions)-1].Enabled = false
	s.opt.Live = config.NewLive("", cfg)
	result, retry := q.execute(context.Background(), mintAllJob{accountID: 1, region: types.Region{Key: custom.Key, Exit: custom.Exit()}})
	if retry || result.attempted || result.Reason != "区域已停用或定义已变更" {
		t.Fatalf("disabled queued exit was executed: %+v", result)
	}
}

func TestRegionsDisabledExitKeepsExistingDeliveryAndSeedRenewalButStopsDynamicWork(t *testing.T) {
	s, st, mn, clk := rig(t, types.ModePool)
	seedPairs(st, clk)
	cfg := config.Defaults()
	cfg.Regions = cfg.RegionDefinitions()
	cfg.Regions[0].Enabled = false
	s.opt.Live = config.NewLive("", cfg)
	if _, err := s.DeliverAnonymous(context.Background(), "member", Request{Gateway: "unified-1"}, true); err != nil {
		t.Fatalf("disabled exploration exit must not invalidate a live route cookie: %v", err)
	}
	if len(mn.mintedGateways()) != 0 || mn.echoes != 0 {
		t.Fatal("using an existing ticket unexpectedly touched upstream")
	}
	if _, ok := s.renewalExit("us-east"); !ok {
		t.Fatal("disabled dynamic exit blocked an existing seed route")
	}
	if _, _, err := s.premintSID("us-east"); err == nil {
		t.Fatal("disabled exit was used for background minting")
	}
}
