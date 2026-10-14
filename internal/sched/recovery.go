package sched

import (
	"context"
	"sort"
	"sync"
	"time"

	"gwpool/internal/routes"
	"gwpool/internal/types"
)

const (
	recoveryExploreInterval = 15 * time.Minute
	recoveryRetryInterval   = 24 * time.Hour
	maxRecoveryAttempts     = 4096
)

type recoveryExplorationState struct {
	mu    sync.Mutex
	next  time.Time
	last  string
	tried map[string]time.Time
}

func recoveryAttemptKey(gateway, identity string) string {
	return routes.Digest("recovery-attempt-v1", gateway, identity)
}

// Opportunities/cursor are process-local like dedicated discovery. A restart
// reconstructs conservative identity exclusions from retained route attempts;
// overwritten route history is not a complete attempt ledger. Durable country
// and account budgets remain authoritative across every restart.
func (s *Scheduler) exploreRecovery(ctx context.Context) int {
	if ctx.Err() != nil || s.maintenanceOff() || s.idle() || len(s.usable()) < 2 {
		return 0
	}
	live := s.liveGateways()
	var missing []string
	for gateway := range s.st.Gateways() {
		if !live[gateway] {
			missing = append(missing, gateway)
		}
	}
	if len(missing) == 0 {
		return 0
	}
	sort.Strings(missing)
	state := &s.recoveryExploration
	state.mu.Lock()
	now := s.now()
	if now.Before(state.next) {
		state.mu.Unlock()
		return 0
	}
	state.next = now.Add(recoveryExploreInterval) // No candidates still consumes this opportunity.
	index := sort.Search(len(missing), func(i int) bool { return missing[i] > state.last })
	if index == len(missing) {
		index = 0
	}
	gateway := missing[index]
	state.last = gateway
	state.mu.Unlock()
	return s.runInventorySelection(ctx, 1, inventorySelection{gateway: gateway, recoveryExplore: true})
}

// Exit evidence is about the requested route, never the datacenter's location.
// Historical identities are excluded even if their old provider has gone away.
func (s *Scheduler) recoveryCohort(gateway string) (map[types.Exit]bool, map[string]bool) {
	exits, historical := map[types.Exit]bool{}, map[string]bool{}
	known, exists := s.st.Gateways()[gateway]
	if !exists {
		return exits, historical
	}
	normalize := func(exit types.Exit) types.Exit {
		return routes.DescribeIdentity("", exit, types.RouteSource{}, routes.Bare, "").Exit
	}
	for _, exit := range known.ReachableExits {
		exits[normalize(exit)] = true
	}
	if reader, ok := s.st.(interface {
		RecoverySources(time.Time) map[string]types.RecoverySource
	}); ok {
		for _, source := range reader.RecoverySources(s.now()) {
			if source.Gateway == gateway && routes.RecoveryFresh(source, s.now()) {
				exits[source.Route.Exit] = true
				historical[source.Route.Identity] = true
			}
		}
	}
	// Preserve existing legacy recovery semantics without manufacturing a
	// historical identity in persistence.
	identities := map[int64]string{}
	for _, account := range s.usable() {
		identities[account.ID] = routes.CredentialIdentity(account.Creds)
	}
	for _, evidence := range s.inventoryEvidence() {
		if evidence.gateway == gateway {
			historical[identities[evidence.account]] = true
		}
	}
	return exits, historical
}

func (s *Scheduler) recoveryTried(gateway string) map[string]bool {
	now := s.now()
	out := map[string]bool{}
	for _, state := range s.routeStates(now) {
		if state.Route.Mode == routes.Bare && state.LastTarget == gateway &&
			!now.Before(state.LastAttempt) && now.Sub(state.LastAttempt) < recoveryRetryInterval {
			out[state.Route.Identity] = true
		}
	}
	state := &s.recoveryExploration
	state.mu.Lock()
	defer state.mu.Unlock()
	for key, at := range state.tried {
		if !now.Before(at.Add(recoveryRetryInterval)) {
			delete(state.tried, key)
		}
	}
	for _, account := range s.usable() {
		identity := routes.CredentialIdentity(account.Creds)
		if _, exists := state.tried[recoveryAttemptKey(gateway, identity)]; exists {
			out[identity] = true
		}
	}
	return out
}

func (s *Scheduler) noteRecoveryAttempt(gateway string, account types.Account) {
	state := &s.recoveryExploration
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.tried == nil {
		state.tried = map[string]time.Time{}
	}
	state.tried[recoveryAttemptKey(gateway, routes.CredentialIdentity(account.Creds))] = s.now()
	for len(state.tried) > maxRecoveryAttempts {
		oldest := ""
		for key, at := range state.tried {
			if oldest == "" || at.Before(state.tried[oldest]) ||
				(at.Equal(state.tried[oldest]) && key < oldest) {
				oldest = key
			}
		}
		delete(state.tried, oldest)
	}
}

func (s *Scheduler) recoveryCandidateAllowed(account types.Account, exit types.Exit, gateway string) bool {
	if s.maintenanceOff() || s.idle() || len(s.usable()) < 2 {
		return false
	}
	if _, ready := s.freshPair(gateway, types.DeliverFloor); ready {
		return false
	}
	exits, historical := s.recoveryCohort(gateway)
	identity := routes.CredentialIdentity(account.Creds)
	if historical[identity] || s.recoveryTried(gateway)[identity] {
		return false
	}
	for _, region := range s.dynamicRegions(types.Exit{}) {
		if region.Exit == exit && exits[routes.DescribeIdentity("", exit, types.RouteSource{}, routes.Bare, "").Exit] {
			return true
		}
	}
	return false
}

func (s *Scheduler) pickRecoveryExploration(round *premintRound) (premintPlan, bool) {
	exits, historical := s.recoveryCohort(round.selection.gateway)
	tried := s.recoveryTried(round.selection.gateway)
	identities := map[int64]string{}
	for _, account := range s.usable() {
		identities[account.ID] = routes.CredentialIdentity(account.Creds)
	}
	allowed := map[string]bool{}
	for _, region := range s.dynamicRegions(types.Exit{}) {
		exit := routes.DescribeIdentity("", region.Exit, types.RouteSource{}, routes.Bare, "").Exit
		allowed[region.Key] = exits[exit]
	}
	round.mu.Lock()
	defer round.mu.Unlock()
	if round.halt || round.attempted > 0 || round.missed >= round.want || round.productive >= round.want {
		return premintPlan{}, false
	}
	// Shared route availability, budgets, country fairness, holds and throttle
	// are still applied by minterFor; this policy only narrows its candidates.
	policy := &premintPolicy{route: s.routeFilter(nil, nil), skip: func(id int64, key string) bool {
		identity := identities[id]
		return !allowed[key] || historical[identity] || tried[identity] || round.tried[identity]
	}}
	for i := 0; i < premintPickTries; i++ {
		account, region, ok := s.minterFor(types.Exit{}, 0, s.opt.RenewGap, policy)
		if !ok {
			round.halt = true
			return premintPlan{}, false
		}
		round.tried[identities[account.ID]] = true // aliases cannot retry this identity
		release, free := s.claimRegion(account.ID, region.Key)
		if !free {
			continue
		}
		if s.claimAccount(account.ID, s.opt.RenewGap) {
			return premintPlan{acct: account, region: region, expected: round.selection.gateway, release: release}, true
		}
		release()
	}
	return premintPlan{}, false
}
