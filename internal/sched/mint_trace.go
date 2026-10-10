package sched

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gwpool/internal/routes"
	"gwpool/internal/types"
)

type mintTrace struct {
	pool        *Scheduler
	attempt     types.RouteAttempt
	tracked     bool
	committed   bool
	failure     string
	sample      types.Sample
	routeState  types.RouteState
	stockBefore time.Time
	stockAfter  time.Time
}

func (s *Scheduler) newMintTrace(ctx context.Context, a types.Account, exit types.Exit,
	sid string, seed types.Pair, expected, purpose string, prediction *types.InventoryPrediction,
) (*mintTrace, error) {
	attempt, tracked := s.attemptRoute(ctx, a, exit, sid, seed, expected)
	trace := &mintTrace{pool: s, attempt: attempt, tracked: tracked, sample: types.Sample{
		AccountID: a.ID, RegionKey: s.sampleRegion(exit), Requested: exit,
		Provider: s.mint.Provider(sid), Purpose: purpose, InventoryPrediction: prediction,
	}}
	if work, planned := ctx.Value(routeWorkKey{}).(routeWork); planned && !tracked {
		trace.attempt, trace.tracked, trace.failure = work.attempt, true, "binding"
		return trace, fmt.Errorf("%w: selected route changed before send", errRouteDeferred)
	}
	return trace, nil
}

func (t *mintTrace) commit(pair types.Pair) (types.MintCommitResult, error) {
	t.committed = true // Never retry a failed durable commit as a separate write.
	commit := types.MintCommit{Pair: pair, At: t.sample.At}
	if t.tracked {
		t.attempt.At, t.attempt.Status, t.attempt.Gateway = t.sample.At, t.sample.Status, t.sample.Gateway
		t.attempt.TargetMatch = t.sample.TargetMatch
		commit.Route = &t.attempt
	}
	if pair.Cookie == "" && commit.Route == nil {
		return types.MintCommitResult{}, nil
	}
	result, err := t.pool.st.CommitMint(commit)
	if err != nil {
		t.failure = "persistence"
		return types.MintCommitResult{}, err
	}
	t.routeState = result.Route
	t.stockBefore, t.stockAfter = result.BeforeExpiresAt, result.AfterExpiresAt
	return result, nil
}

func (t *mintTrace) finish(result *minted, finalErr *error) {
	if t.sample.At.IsZero() {
		t.sample.At = t.pool.now()
	}
	if !t.committed && t.tracked && !errors.Is(*finalErr, context.Canceled) {
		t.attempt.At, t.attempt.Status = t.sample.At, t.sample.Status
		t.attempt.Gateway, t.attempt.Failure = t.sample.Gateway, t.failure
		if t.attempt.Failure == "" {
			t.attempt.Failure = "invalid_response"
		}
		committed, err := t.pool.st.CommitMint(types.MintCommit{At: t.attempt.At, Route: &t.attempt})
		if err != nil {
			t.failure = "persistence"
			*finalErr = errors.Join(*finalErr, fmt.Errorf("%w: route evidence: %w", ErrMintFailed, err))
		} else {
			t.routeState = committed.Route
		}
	}
	if t.tracked {
		outcome := "no_gain"
		switch {
		case t.failure != "":
			outcome = t.failure + "_error"
		case errors.Is(*finalErr, context.Canceled):
			outcome = "canceled"
		case result.gain.Productive():
			outcome = "productive"
		}
		t.sample.Route = &types.RouteSample{ID: routes.Key(t.attempt.Route), Route: t.attempt.Route,
			StartedAt: t.attempt.StartedAt, Target: t.attempt.Expected,
			TargetHit: t.attempt.Expected != "" && t.attempt.Expected == t.sample.Gateway && t.failure == "",
			Result:    outcome, Gain: result.gain, NextCheck: t.routeState.NextCheck, Donor: t.attempt.Donor}
		t.sample.Route.BeforeExpiresAt, t.sample.Route.AfterExpiresAt = t.stockBefore, t.stockAfter
	}
	if t.sample.Err == "" && *finalErr != nil {
		t.sample.Err = types.SampleErr("", *finalErr)
	}
	t.pool.sample(t.sample)
}
