package sched

import (
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

type cacheMinter struct {
	*fakeMinter
	supported bool
}

func (m *cacheMinter) SupportsDynamicExit(types.Exit) bool { return m.supported }

func TestInventoryEvidenceCacheTracksStateAndDynamicSupport(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	account, err := st.AddAccount(types.Account{Enabled: true, Creds: types.Creds{AccessToken: "fake", AccountID: "one"}})
	if err != nil {
		t.Fatal(err)
	}
	region := types.Regions[0]
	if _, err := st.UpsertGateway("g", account.ID, region.Exit); err != nil {
		t.Fatal(err)
	}
	if err := st.NoteRegionMint(account.ID, region.Key, "g", time.Now()); err != nil {
		t.Fatal(err)
	}
	minter := &cacheMinter{fakeMinter: newMinter(newClock()), supported: true}
	s := New(st, minter, Options{})
	if len(s.inventoryEvidence()) != 1 {
		t.Fatal("cache did not retain recovery evidence")
	}
	if len(s.inventoryEvidence()) != 1 {
		t.Fatal("cache did not retain recovery evidence on repeated read")
	}
	if _, count := s.inventoryEvidenceSnapshot(); count != 1 {
		t.Fatal("recoverable count does not match cached evidence")
	}
	minter.supported = false
	if len(s.inventoryEvidence()) != 0 {
		t.Fatal("proxy support change served stale evidence")
	}
	if _, count := s.inventoryEvidenceSnapshot(); count != 0 {
		t.Fatal("recoverable count ignored proxy support change")
	}
	minter.supported = true
	if err := st.SetAccountEnabled(account.ID, false); err != nil {
		t.Fatal(err)
	}
	if len(s.inventoryEvidence()) != 0 {
		t.Fatal("disabled account served stale evidence")
	}
	if err := st.SetAccountEnabled(account.ID, true); err != nil {
		t.Fatal(err)
	}
	if len(s.inventoryEvidence()) != 1 {
		t.Fatal("restored account did not invalidate evidence")
	}
	s.opt.Live = liveCfg(func(c *config.Config) { c.Regions = []config.ExplorationRegion{} })
	if len(s.inventoryEvidence()) != 0 {
		t.Fatal("region configuration change served stale evidence")
	}
}
