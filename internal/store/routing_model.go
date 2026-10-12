package store

import (
	"sync"
	"time"

	"gwpool/internal/routing"
)

type routingModelCache struct {
	mu                      sync.Mutex
	history                 *routingHistoryCache
	historyRev, accountsRev uint64
	from, until             time.Time
	model                   *routing.Model
}

// RoutingModel borrows an immutable model only while its exact observation set
// is unchanged. New samples, account identity changes and evidence time bounds
// invalidate it; a later task still learns from an earlier task's real result.
func (s *Store) RoutingModel(now time.Time) (routing.ModelSnapshot, error) {
	loaded, err := s.ensureRoutingHistory(now)
	if err != nil {
		return routing.ModelSnapshot{}, err
	}
	cache := &s.routingModel
	cache.mu.Lock()
	defer cache.mu.Unlock()
	s.mu.RLock()
	history := s.routingHistory
	out := routing.ModelSnapshot{Loaded: loaded, Clipped: history.clipped, SkippedLines: history.skippedLines}
	historyRev, accountsRev := history.revision, s.revisions.Accounts
	if cache.model != nil && cache.history == history &&
		cache.historyRev == historyRev && cache.accountsRev == accountsRev &&
		!now.Before(cache.from) && (cache.until.IsZero() || now.Before(cache.until)) {
		out.Model = cache.model
		s.mu.RUnlock()
		return out, nil
	}
	observations, from, until := s.routingObservationsLocked(now)
	s.mu.RUnlock()
	model := routing.New(observations)
	cache.history, cache.historyRev, cache.accountsRev = history, historyRev, accountsRev
	cache.from, cache.until, cache.model = from, until, model
	out.Model = model
	return out, nil
}
