package sched

import (
	"context"
	"errors"
	"testing"
	"time"

	"gwpool/internal/types"
)

type timezoneMinter struct {
	*fakeMinter
	zone     string
	geoCost  time.Duration
	geoCalls int
}

func (m *timezoneMinter) Geo(ctx context.Context, exit types.Exit, sid string) (types.Observed, error) {
	m.geoCalls++
	observed, err := m.fakeMinter.Geo(ctx, exit, sid)
	m.clk.add(m.geoCost)
	observed.Timezone = m.zone
	return observed, err
}

func TestMintExitTimezoneFollowsActualObservationThroughDelivery(t *testing.T) {
	clk := newClock()
	st, mn := newStore(clk), newMinter(clk)
	addAccount(st, 1, "acct-1")
	landBare(mn, "unified-142")
	mn.mintCost = 5 * time.Second
	minter := &timezoneMinter{fakeMinter: mn, zone: "Asia/Tokyo", geoCost: 2 * time.Second}
	pool := New(st, minter, Options{Now: clk.now, MintGap: -1, RenewGap: -1})
	want := types.ExitTimezoneObservation{Timezone: minter.zone, ObservedAt: clk.now().Add(minter.geoCost)}
	_, err := pool.mintOnce(context.Background(), st.accounts[0], types.Exit{Region: "US"},
		"dynamic", types.Pair{}, "unified-142", "unified-142")
	if err != nil {
		t.Fatal(err)
	}
	pair, ok := st.Pair("unified-142")
	if !ok || pair.MintExit != want {
		t.Fatal("mint used requested/datacenter region or post-mint commit time as exit provenance")
	}
	delivery, err := pool.DeliverAnonymous(context.Background(), "external-member", Request{Gateway: pair.Gateway}, true)
	if err != nil || delivery.MintExit != want || minter.geoCalls != 1 || len(mn.mintedGateways()) != 1 {
		t.Fatal("delivery lost the selected pair's metadata or performed extra Geo/mint work", err)
	}
}

func TestMintExitTimezoneIsNotOverwrittenByUnretainedObservation(t *testing.T) {
	for _, mode := range []string{"new-oailb", "no-new-cookie", "shorter-drift", "failure", "nonsticky-seed", "missing-timezone"} {
		t.Run(mode, func(t *testing.T) {
			clk := newClock()
			st, mn := newStore(clk), newMinter(clk)
			addAccount(st, 1, "acct-1")
			const gateway = "unified-142"
			landBare(mn, gateway)
			old := types.Pair{Gateway: gateway, Cookie: cookiePrefix + gateway,
				ExpiresAt: clk.now().Add(40 * time.Minute), CflbExpiresAt: clk.now().Add(40 * time.Minute),
				MintExit: types.ExitTimezoneObservation{Timezone: "Asia/Tokyo", ObservedAt: clk.now().Add(-time.Minute)}}
			st.pairs[gateway] = old
			minter := &timezoneMinter{fakeMinter: mn, zone: "Europe/Warsaw"}
			pool := New(st, minter, Options{Now: clk.now, MintGap: -1, RenewGap: -1})
			sid, target, current := "dynamic", gateway, old
			want := types.ExitTimezoneObservation{Timezone: minter.zone, ObservedAt: clk.now()}
			switch mode {
			case "no-new-cookie":
				mn.noNewPair, want = true, old.MintExit
			case "shorter-drift":
				mn.pairLife = 12 * time.Minute
				current, target, want = types.Pair{}, "unified-9", old.MintExit
			case "failure":
				mn.mintErr, want = errors.New("offline failure"), old.MintExit
			case "nonsticky-seed":
				sid, want = "seed1", types.ExitTimezoneObservation{}
			case "missing-timezone":
				minter.zone, want = "", types.ExitTimezoneObservation{}
			}
			_, err := pool.mintOnce(context.Background(), st.accounts[0], types.Exit{Region: "US"},
				sid, current, target, target)
			wantError := mode == "failure" || mode == "shorter-drift"
			if (err != nil) != wantError {
				t.Fatalf("unexpected mint outcome: %v", err)
			}
			retained, _ := st.Pair(gateway)
			if retained.MintExit != want {
				t.Fatal("retained oailb acquired metadata from the wrong observation")
			}
		})
	}
}
