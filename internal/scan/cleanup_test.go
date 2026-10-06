package scan

import (
	"context"
	"gwpool/internal/types"
	"testing"
)

func TestScanStaleSnapshotAfterCleanupDoesNotMint(t *testing.T) {
	f := newFake()
	sc, st := newScanner(t, f)
	a, err := st.AddAccount(types.Account{Enabled: true, Creds: types.Creds{AccessToken: "fake", AccountID: "deleted"}})
	if err != nil {
		t.Fatal(err)
	}
	stale := a
	if err = st.DisableAccount(a.ID, "铸票 HTTP 401：expired"); err != nil {
		t.Fatal(err)
	}
	a, _ = st.Account(a.ID)
	if n, err := st.DeleteUnauthorizedAccounts([]types.AccountCleanupCandidate{types.CleanupCandidate(a)}); err != nil || n != 1 {
		t.Fatal("fixture cleanup failed")
	}
	if _, err = sc.ScanRegion(context.Background(), stale, types.Regions[0]); err == nil {
		t.Error("stale snapshot minted after deletion")
	}
	if len(f.sids) != 0 || len(st.Slots()) != 0 || len(st.RegionMints()) != 0 {
		t.Fatal("deleted account produced upstream request or resurrected records")
	}
}
