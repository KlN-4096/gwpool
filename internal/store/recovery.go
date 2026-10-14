package store

import (
	"errors"
	"sort"
	"time"

	"gwpool/internal/routes"
	"gwpool/internal/types"
)

func (s *Store) RecoverySources(now time.Time) map[string]types.RecoverySource {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]types.RecoverySource)
	for key, source := range s.st.Recovery {
		if routes.RecoveryFresh(source, now) {
			out[key] = source
		}
	}
	return out
}

func rememberRecovery(sources map[string]types.RecoverySource, source types.RecoverySource) {
	key := routes.RecoveryKey(source)
	if source.LastSeen.After(sources[key].LastSeen) {
		sources[key] = source
	}
}

func recoveryDelta(old, next map[string]types.RecoverySource, change *stateChange) {
	for key, source := range next {
		if previous, ok := old[key]; !ok || previous != source {
			if change.Recovery == nil {
				change.Recovery = map[string]types.RecoverySource{}
			}
			change.Recovery[key] = source
		}
	}
	for key := range old {
		if _, ok := next[key]; !ok {
			change.DropRecovery = append(change.DropRecovery, key)
		}
	}
	sort.Strings(change.DropRecovery)
}

// Startup migration only uses observations with their original exact identity,
// source and time. Pair/RegionMint cannot supply those historical facts.
func (s *Store) prepareRecoverySources(now time.Time) error {
	for key, source := range s.st.Recovery {
		if !routes.ValidRecoverySource(key, source) {
			return errors.New("invalid persisted recovery source")
		}
	}
	next := cloneMap(s.st.Recovery)
	if next == nil {
		next = map[string]types.RecoverySource{}
	}
	for _, state := range s.st.Routes {
		source := types.RecoverySource{Gateway: state.Gateway, Route: state.Route, LastSeen: state.ObservedAt}
		if routes.ValidRecoverySource(routes.RecoveryKey(source), source) && routes.RecoveryFresh(source, now) {
			rememberRecovery(next, source)
		}
	}
	routes.PruneRecoverySources(next, now)
	change := stateChange{}
	recoveryDelta(s.st.Recovery, next, &change)
	if len(change.Recovery)+len(change.DropRecovery) == 0 {
		return nil
	}
	old := s.st.Recovery
	s.st.Recovery = next
	if err := s.save(change); err != nil {
		s.st.Recovery = old
		return err
	}
	return nil
}
