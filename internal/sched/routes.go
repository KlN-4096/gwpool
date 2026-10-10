package sched

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"gwpool/internal/mint"
	"gwpool/internal/routes"
	"gwpool/internal/types"
)

var errRouteDeferred = errors.New("route temporarily deferred")

type routeWorkKey struct{}
type routeWork struct {
	attempt types.RouteAttempt
}

func (s *Scheduler) routeCanWork(state types.RouteState, expected string, donorNeeded bool) bool {
	if state.LastAttempt.IsZero() || !s.now().Before(state.NextCheck) {
		return true
	}
	useful := donorNeeded && (!state.LastDonor || state.LastProductive)
	if expected != "" && state.Gateway == expected {
		pair, ready := s.freshPair(expected, types.DeliverFloor)
		needsTicket := !ready || pair.ExpiresAt.Sub(s.now()) <= renewPairLead
		useful = useful || (needsTicket && (state.LastTarget != expected || state.LastProductive))
	}
	return routes.Allowed(state, s.now(), useful)
}

func (s *Scheduler) describeRoute(a types.Account, exit types.Exit, source types.RouteSource, seed types.Pair) types.RouteDescriptor {
	mode, cflb := routes.Bare, ""
	if seed.Cookie != "" {
		mode, cflb = routes.Seed, mint.CflbOf(seed.Cookie)
	}
	return routes.Describe(a.Creds, exit, source, mode, cflb)
}

func (s *Scheduler) claimRoute(key string) (func(), bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.routesInFlight[key] {
		return nil, false
	}
	if s.routesInFlight == nil {
		s.routesInFlight = map[string]bool{}
	}
	s.routesInFlight[key] = true
	return func() {
		s.mu.Lock()
		delete(s.routesInFlight, key)
		s.mu.Unlock()
	}, true
}

// routeAvailable filters a candidate without reserving an attempt or rotating
// a proxy. bindRoute rechecks after the shared account lock is acquired.
func (s *Scheduler) routeAvailable(a types.Account, exit types.Exit, seed types.Pair, expected string, donorNeeded bool) bool {
	states := s.st.RouteStates(s.now())
	for _, source := range s.mint.RouteSources(exit, seed.Cookie != "") {
		descriptor := s.describeRoute(a, exit, source, seed)
		if s.routeCanWork(states[routes.Key(descriptor)], expected, donorNeeded) {
			return true
		}
	}
	return false
}

// Snapshot once per selection, not once per account×country candidate.
func (s *Scheduler) routeFilter(expected map[string]string, donorNeeds map[string]bool) func(types.Account, types.Region) bool {
	states := s.st.RouteStates(s.now())
	sources := map[types.Exit][]types.RouteSource{}
	identities := map[int64]string{}
	return func(account types.Account, region types.Region) bool {
		options, loaded := sources[region.Exit]
		if !loaded {
			options = s.mint.RouteSources(region.Exit, false)
			sources[region.Exit] = options
		}
		identity, loaded := identities[account.ID]
		if !loaded {
			identity = routes.CredentialIdentity(account.Creds)
			identities[account.ID] = identity
		}
		for _, source := range options {
			descriptor := routes.DescribeIdentity(identity, region.Exit, source, routes.Bare, "")
			if s.routeCanWork(states[routes.Key(descriptor)], expected[comboKey(account.ID, region.Key)], donorNeeds[region.Key]) {
				return true
			}
		}
		return false
	}
}

// bindRoute preselects and claims an exact source before the attempt is spent.
// A later local binding failure still consumes the original reserved attempt.
// Callers hold the Store account lock until the returned release is called.
func (s *Scheduler) bindRoute(ctx context.Context, a types.Account, exit types.Exit,
	seed types.Pair, expected, purpose string, donorNeeded bool,
) (context.Context, types.Account, string, func(), error) {
	ctx, current, err := s.prepareAccount(ctx, a)
	if err != nil {
		return ctx, a, "", nil, err
	}
	a = current
	sources := s.mint.RouteSources(exit, seed.Cookie != "")
	if len(sources) == 0 {
		return ctx, a, "", nil, types.ErrNoExit
	}
	// Prefer an actual recent match under the exact source/seed conditions.
	// Keep the provider's rotation order within equally supported options.
	states := s.st.RouteStates(s.now())
	sort.SliceStable(sources, func(i, j int) bool {
		matches := func(source types.RouteSource) bool {
			state := states[routes.Key(s.describeRoute(a, exit, source, seed))]
			return expected != "" && state.Gateway == expected && !state.ObservedAt.IsZero() &&
				!s.now().Before(state.ObservedAt) && s.now().Sub(state.ObservedAt) < routes.RetryMax
		}
		return matches(sources[i]) && !matches(sources[j])
	})
	for _, source := range sources {
		descriptor := s.describeRoute(a, exit, source, seed)
		key := routes.Key(descriptor)
		release, claimed := s.claimRoute(key)
		if !claimed {
			continue
		}
		state := s.st.RouteStates(s.now())[key]
		if !s.routeCanWork(state, expected, donorNeeded) {
			release()
			continue
		}
		ctx, err = s.beginAttempt(ctx, a, exit)
		if err != nil {
			release()
			return ctx, a, "", nil, err
		}
		start := s.now()
		work := routeWork{attempt: types.RouteAttempt{Route: descriptor, StartedAt: start, Expected: expected, Donor: donorNeeded}}
		ctx = context.WithValue(ctx, routeWorkKey{}, work)
		sid, err := s.mint.NewSIDForRoute(exit, source.ID, seed.Cookie != "")
		if err != nil {
			attempt := work.attempt
			attempt.At, attempt.Failure = s.now(), "binding"
			committed, saveErr := s.st.CommitMint(types.MintCommit{At: attempt.At, Route: &attempt})
			outcome := "binding_error"
			if saveErr != nil {
				outcome = "persistence_error"
			}
			s.sample(types.Sample{AccountID: a.ID, RegionKey: s.sampleRegion(exit), Requested: exit,
				At: attempt.At, Purpose: purpose, TargetMatch: "unknown", Err: types.SampleErr("", errors.Join(err, saveErr)),
				InventoryPrediction: inventoryPredictionFromContext(ctx, a.ID, exit),
				Route: &types.RouteSample{ID: key, Route: descriptor, StartedAt: start, Target: expected,
					Result: outcome, NextCheck: committed.Route.NextCheck, Donor: donorNeeded}})
			release()
			return ctx, a, "", nil, errors.Join(err, saveErr)
		}
		return ctx, a, sid, release, nil
	}
	return ctx, a, "", nil, fmt.Errorf("%w: recent landing or in-flight route", errRouteDeferred)
}

func (s *Scheduler) attemptRoute(ctx context.Context, a types.Account, exit types.Exit,
	sid string, seed types.Pair, expected string,
) (types.RouteAttempt, bool) {
	if !s.mint.SeedBound(sid) {
		seed = types.Pair{}
	}
	if work, ok := ctx.Value(routeWorkKey{}).(routeWork); ok {
		// The exact binding must agree with preselection; a random new SID
		// must never silently redirect a previously checked route.
		actual := s.describeRoute(a, exit, s.mint.RouteSource(sid), seed)
		if actual != work.attempt.Route {
			return types.RouteAttempt{}, false
		}
		return work.attempt, true
	}
	source := s.mint.RouteSource(sid)
	if source.ID == "" {
		return types.RouteAttempt{}, false
	}
	descriptor := s.describeRoute(a, exit, source, seed)
	if err := routes.Validate(descriptor); err != nil {
		return types.RouteAttempt{}, false
	}
	return types.RouteAttempt{Route: descriptor, StartedAt: s.now(), Expected: expected}, true
}
