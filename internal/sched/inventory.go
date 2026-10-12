package sched

import (
	"context"
	"slices"
	"sync"
	"time"

	"gwpool/internal/routes"
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
	legacy, _ := s.inventoryEvidenceSnapshot()
	out := slices.Clone(legacy)
	seen := map[string]bool{}
	for _, route := range out {
		seen[comboKey(route.account, route.region)+"|"+route.gateway] = true
	}
	accounts := map[string][]types.Account{}
	for _, account := range s.usable() {
		identity := routes.CredentialIdentity(account.Creds)
		accounts[identity] = append(accounts[identity], account)
	}
	type routeLocation struct {
		source string
		exit   types.Exit
	}
	available := map[routeLocation][]types.Region{}
	for _, region := range s.dynamicRegions(types.Exit{}) {
		for _, source := range s.mint.RouteSources(region.Exit, false) {
			descriptor := routes.DescribeIdentity("", region.Exit, source, routes.Bare, "")
			key := routeLocation{source: source.ID, exit: descriptor.Exit}
			available[key] = append(available[key], region)
		}
	}
	known, now := s.st.Gateways(), s.now()
	for _, state := range s.routeStates(now) {
		if state.Route.Mode != routes.Bare || state.Gateway == "" || state.ObservedAt.IsZero() ||
			now.Before(state.ObservedAt) || now.Sub(state.ObservedAt) >= routes.Retention {
			continue
		}
		// Older observations remain limited-recheck clues, not recent source
		// preferences. bindRoute uses RetryMax for positive preference and
		// rechecks the route's shared NextCheck before spending any attempt.
		if _, exists := known[state.Gateway]; !exists {
			continue
		}
		for _, region := range available[routeLocation{source: state.Route.Source, exit: state.Route.Exit}] {
			for _, account := range accounts[state.Route.Identity] {
				key := comboKey(account.ID, region.Key) + "|" + state.Gateway
				if !seen[key] {
					seen[key] = true
					out = append(out, inventoryRoute{account: account.ID, region: region.Key, gateway: state.Gateway})
				}
			}
		}
	}
	return out
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
			prefer = route.gateway < old
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
	for name := range s.st.Gateways() {
		if !available[name] {
			missing[name] = true
		}
	}
	live, target = len(available), s.PairTarget()
	if target == 0 {
		return live, target, 0 // Explicit maintenance-off remains authoritative.
	}
	// Unrelated live tickets cannot substitute for a missing known gateway.
	// Actual work is bounded separately, not by an assumed upstream total.
	gap = max(0, max(target-live, len(missing)))
	return live, target, gap
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
	s.runMaintenance(ctx, false)
}
