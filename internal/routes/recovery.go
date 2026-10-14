package routes

import (
	"sort"
	"time"

	"gwpool/internal/types"
)

const (
	RecoveryRetention    = 7 * 24 * time.Hour
	MaxRecoverySources   = 4096
	MaxSourcesPerGateway = 16
)

func RecoveryKey(source types.RecoverySource) string {
	return Digest("recovery-source-v1", source.Gateway, Key(source.Route))
}

// A different subsequent landing must not replace this gateway's observation.
// Seed observations cannot establish a reproducible bare route for new accounts.
func RecoveryObservation(attempt types.RouteAttempt) (types.RecoverySource, bool) {
	source := types.RecoverySource{Gateway: attempt.Gateway, Route: attempt.Route, LastSeen: attempt.At}
	ok := attempt.Status == 200 && attempt.Failure == "" && ValidRecoverySource(RecoveryKey(source), source)
	return source, ok
}

func ValidRecoverySource(key string, source types.RecoverySource) bool {
	return source.Gateway != "" && len(source.Gateway) <= 128 && !source.LastSeen.IsZero() &&
		source.Route.Mode == Bare && source.Route.Seed == "" && Validate(source.Route) == nil &&
		key == RecoveryKey(source)
}

func RecoveryFresh(source types.RecoverySource, now time.Time) bool {
	return !now.Before(source.LastSeen) && now.Sub(source.LastSeen) < RecoveryRetention
}

// Keep newest evidence deterministically, independently of the short route TTL.
func PruneRecoverySources(sources map[string]types.RecoverySource, now time.Time) {
	keys := make([]string, 0, len(sources))
	for key, source := range sources {
		if !now.Before(source.LastSeen.Add(RecoveryRetention)) {
			delete(sources, key)
			continue
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := sources[keys[i]].LastSeen, sources[keys[j]].LastSeen
		if !a.Equal(b) {
			return a.After(b)
		}
		return keys[i] < keys[j]
	})
	counts, kept := map[string]int{}, 0
	for _, key := range keys {
		gateway := sources[key].Gateway
		if counts[gateway] >= MaxSourcesPerGateway || kept >= MaxRecoverySources {
			delete(sources, key)
			continue
		}
		counts[gateway]++
		kept++
	}
}
