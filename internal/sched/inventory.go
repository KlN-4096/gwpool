package sched

import (
	"context"
	"slices"
	"sync"
	"time"

	"gwpool/internal/store"
	"gwpool/internal/types"
)

type inventoryRoute struct {
	account int64
	region  string
	gateway string
}

type inventoryEvidenceCache struct {
	mu          sync.Mutex
	ready       bool
	revision    store.Revisions
	regions     []types.Region
	routes      []inventoryRoute
	recoverable int
}

// A source account/exit is a recovery prediction, not a guaranteed landing.
// Keep stored ticket provenance as well as the latest country landing: one
// drift must not erase the only historical way back to an expired rare ticket.
func (s *Scheduler) inventoryEvidence() []inventoryRoute {
	routes, _ := s.inventoryEvidenceSnapshot()
	return routes
}

func countRecoverable(routes []inventoryRoute) int {
	seen := make(map[string]bool)
	for _, route := range routes {
		seen[route.gateway] = true
	}
	return len(seen)
}

func (s *Scheduler) inventoryEvidenceSnapshot() ([]inventoryRoute, int) {
	regions := s.dynamicRegions(types.Exit{})
	versioned, ok := s.st.(interface{ Revisions() store.Revisions })
	if !ok {
		routes := s.inventoryEvidenceFor(regions)
		return routes, countRecoverable(routes)
	}
	cache := &s.evidenceCache
	cache.mu.Lock()
	defer cache.mu.Unlock()
	rev := versioned.Revisions()
	key := store.Revisions{Accounts: rev.Accounts, Gateways: rev.Gateways, Pairs: rev.Pairs, Mints: rev.Mints}
	if cache.ready && cache.revision == key && slices.Equal(cache.regions, regions) {
		return cache.routes, cache.recoverable // immutable; callers only iterate this evidence
	}
	cache.routes = s.inventoryEvidenceFor(regions)
	cache.recoverable = countRecoverable(cache.routes)
	cache.ready, cache.revision, cache.regions = true, key, slices.Clone(regions)
	return cache.routes, cache.recoverable
}

func (s *Scheduler) inventoryEvidenceFor(enabledRegions []types.Region) []inventoryRoute {
	accounts := make(map[int64]bool)
	for _, a := range s.usable() {
		accounts[a.ID] = true
	}
	regions := make(map[string]types.Exit)
	for _, r := range enabledRegions {
		regions[r.Key] = r.Exit
	}
	gateways := s.st.Gateways()
	seen := make(map[string]bool)
	var routes []inventoryRoute
	add := func(account int64, region, name string) {
		exit, enabled := regions[region]
		gateway, known := gateways[name]
		if !accounts[account] || !enabled || !known || name == "" {
			return
		}
		for _, observed := range gateway.ReachableExits {
			if observed == exit {
				key := comboKey(account, region) + "|" + name
				if !seen[key] {
					routes = append(routes, inventoryRoute{account: account, region: region, gateway: name})
					seen[key] = true
				}
				break
			}
		}
	}
	for _, m := range s.st.RegionMints() {
		add(m.AccountID, m.RegionKey, m.Gateway)
	}
	for name := range gateways {
		if pair, ok := s.st.Pair(name); ok {
			add(pair.MintedBy, pair.RegionKey, name)
		}
	}
	return routes
}

func (s *Scheduler) inventoryRoutes() (map[string]string, int) {
	live := s.liveGateways()
	routes := make(map[string]string)
	recoverable := make(map[string]bool)
	for _, route := range s.inventoryEvidence() {
		key := comboKey(route.account, route.region)
		recoverable[route.gateway] = true
		old := routes[key]
		prefer := old == "" || (live[old] && !live[route.gateway])
		if old != "" && live[old] == live[route.gateway] {
			oldDeferred := s.workDeferred("inventory|" + key + "|" + old)
			nextDeferred := s.workDeferred("inventory|" + key + "|" + route.gateway)
			prefer = (oldDeferred && !nextDeferred) ||
				(oldDeferred == nextDeferred && route.gateway < old)
		}
		if prefer {
			routes[key] = route.gateway
		}
	}
	return routes, len(recoverable)
}

func (s *Scheduler) inventoryGap() (live, target, gap int) {
	available := s.liveGateways()
	missing := make(map[string]bool)
	for _, route := range s.inventoryEvidence() {
		if !available[route.gateway] {
			missing[route.gateway] = true
		}
	}
	live, target = len(available), s.PairTarget()
	if target == 0 {
		return live, target, 0 // Explicit maintenance-off remains authoritative.
	}
	// Unrelated live tickets cannot substitute for a missing recoverable
	// gateway. Still respect the pre-existing total inventory ceiling.
	gap = max(0, min(max(target-live, len(missing)), types.GatewayCeiling-live))
	return live, target, gap
}

// Named recovery must pick the account and exit as one historical route.
// Choosing a gateway's exit and an unrelated idle account predicts a different
// landing. Prefer another source; using the consumer is the bounded fallback.
func (s *Scheduler) recoveryMinter(gateway string, consumerID int64) (types.Account, types.Region, bool) {
	routes := make(map[string]bool)
	for _, route := range s.inventoryEvidence() {
		if route.gateway == gateway {
			routes[comboKey(route.account, route.region)] = true
		}
	}
	policy := &premintPolicy{skip: func(id int64, key string) bool {
		combo := comboKey(id, key)
		return !routes[combo] || s.workDeferred("inventory|"+combo+"|"+gateway)
	}}
	for _, exclude := range []int64{consumerID, 0} {
		if account, region, ok := s.minterFor(types.Exit{}, exclude, 0, policy); ok {
			return account, region, true
		}
	}
	return types.Account{}, types.Region{}, false
}

// Only bootstrap a target that cannot be covered by known recovery routes.
// Normal maintenance of an established pool must not spend its entire gap
// trying unproven countries; dedicated exploration has its own one-shot turn.
const inventoryBootstrapBudget = 2

// Requests coalesce into one bounded background round. Repeated clients cannot
// turn their HTTP retry rate into a matching rate of upstream maintenance.
const demandMaintenanceGap = 5 * time.Second

func (s *Scheduler) maintainOnDemand(ctx context.Context) {
	s.backgroundMu.Lock()
	defer s.backgroundMu.Unlock()
	if ctx.Err() != nil || s.idle() || len(s.usable()) < 2 {
		return
	}
	live, target, gap := s.inventoryGap()
	if gap == 0 {
		return
	}
	s.opt.Log.Info("取票需求唤醒补货", "live", live, "target", target)
	s.runInventory(ctx, gap, false)
}
