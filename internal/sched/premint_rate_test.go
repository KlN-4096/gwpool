package sched

import (
	"errors"
	"fmt"
	"testing"

	"gwpool/internal/types"
)

func TestPremintLocalRateLimitDoesNotHaltOtherAccounts(t *testing.T) {
	for _, refusal := range []error{types.ErrRateLimited, fmt.Errorf("upstream 429: %w", types.ErrRateLimited)} {
		s, _, _, _ := rig(t, types.ModePool)
		round := &premintRound{want: 2}
		limited := premintPlan{acct: types.Account{ID: 1}, region: types.Regions[0]}
		if !s.premintNote(round, limited, minted{}, refusal, false) || !round.more() {
			t.Fatal("one account/region limit halted the shared round")
		}
		if round.halt {
			t.Fatal("a local rate limit must not set the global halt flag")
		}
		healthy := premintPlan{acct: types.Account{ID: 2}, region: types.Regions[1]}
		s.premintNote(round, healthy, minted{pair: types.Pair{Gateway: "unified-2",
			ExpiresAt: s.now().Add(types.PairLifetime)}, gain: types.StockGain{CoverageAdded: true}}, nil, false)
		got, missed, _ := round.result()
		if got != 1 || missed != 1 {
			t.Fatalf("healthy account should continue; upstream refusals still spend the failure budget: got=%d missed=%d", got, missed)
		}
		s.premintNote(round, limited, minted{}, refusal, false)
		if round.more() {
			t.Fatal("rate-limited outcomes bypassed the existing failure budget")
		}
	}
}

func TestPremintNoExitStillStopsSharedRound(t *testing.T) {
	s, _, _, _ := rig(t, types.ModePool)
	round := &premintRound{want: 2}
	plan := premintPlan{acct: types.Account{ID: 1}, region: types.Regions[0]}
	if s.premintNote(round, plan, minted{}, errors.Join(types.ErrNoExit, errors.New("offline")), false) || round.more() {
		t.Fatal("no available exit must still stop the round")
	}
}
