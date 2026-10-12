package sched

import (
	"sync"
	"time"

	"gwpool/internal/routes"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

type routeStateCache struct {
	mu          sync.Mutex
	ready       bool
	revision    uint64
	from, until time.Time
	states      map[string]types.RouteState
}

// Only the immutable evidence table is reused. NextCheck, positive-observation
// age, useful-stock checks and the claimed route are re-evaluated at execution.
func (s *Scheduler) routeStates(now time.Time) map[string]types.RouteState {
	versioned, ok := s.st.(interface{ Revisions() store.Revisions })
	if !ok {
		return s.st.RouteStates(now)
	}
	cache := &s.routeCache
	cache.mu.Lock()
	defer cache.mu.Unlock()
	revision := versioned.Revisions().Routes
	if cache.ready && cache.revision == revision && !now.Before(cache.from) &&
		(cache.until.IsZero() || now.Before(cache.until)) {
		return cache.states
	}
	states := s.st.RouteStates(now)
	var until time.Time
	for _, state := range states {
		expiry := state.LastAttempt.Add(routes.Retention)
		if until.IsZero() || expiry.Before(until) {
			until = expiry
		}
	}
	cache.ready, cache.revision, cache.from, cache.until = true, revision, now, until
	cache.states = states
	return states
}

func (s *Scheduler) currentRouteState(key string, now time.Time) types.RouteState {
	if narrow, ok := s.st.(interface {
		RouteState(string, time.Time) types.RouteState
	}); ok {
		return narrow.RouteState(key, now)
	}
	return s.st.RouteStates(now)[key]
}
