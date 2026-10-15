package scan

import (
	"context"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestScanExitTimezoneOnlyComesFromPairedGeo(t *testing.T) {
	for _, mode := range []string{"sticky", "coarse-nonsticky", "missing-timezone"} {
		t.Run(mode, func(t *testing.T) {
			minter := newFake()
			minter.observed["JP"] = types.Observed{Country: "JP", IP: "192.0.2.1", Timezone: "Asia/Tokyo"}
			scanner, st := newScanner(t, minter)
			switch mode {
			case "coarse-nonsticky":
				scanner.Coarse, minter.nonSticky = true, true
			case "missing-timezone":
				minter.observed["JP"] = types.Observed{Country: "JP", IP: "192.0.2.1"}
			}
			start := time.Now()
			landing, err := scanner.ScanRegion(context.Background(), acct,
				types.Region{Key: "east-asia", Exit: types.Exit{Region: "JP"}})
			if err != nil {
				t.Fatal(err)
			}
			pair, ok := st.Pair(landing.Gateway)
			if !ok {
				t.Fatal("scan did not retain its new ticket")
			}
			if mode == "sticky" {
				samples, err := st.Samples()
				if err != nil {
					t.Fatal(err)
				}
				if pair.MintExit.Timezone != "Asia/Tokyo" || pair.MintExit.ObservedAt.Before(start) ||
					pair.MintExit.ObservedAt.After(samples[0].At) || len(minter.geoSIDs) != 1 ||
					len(minter.sids) != 1 || minter.geoSIDs[0] != minter.sids[0] {
					t.Fatal("scan timezone was not tied to its pre-mint Geo observation")
				}
			} else if pair.MintExit != (types.ExitTimezoneObservation{}) {
				t.Fatal("unpaired or absent Geo timezone acquired invented provenance")
			}
		})
	}
}
