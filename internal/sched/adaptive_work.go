package sched

import (
	"gwpool/internal/types"
	"time"
)

func (s *Scheduler) dynamicRegions(want types.Exit) []types.Region {
	var regions []types.Region
	for _, r := range s.cfg().CandidateRegions(want) {
		if s.mint.SupportsDynamicExit(r.Exit) {
			regions = append(regions, r)
		}
	}
	return regions
}

const (
	lowYieldRetryBase = 5 * time.Minute
	lowYieldRetryMax  = time.Hour
)

type workRetry struct {
	delay time.Duration
	until time.Time
}

// These holds affect only maintenance/exploration work, never consumer eligibility.
// They are bounded retries, not a global gateway blacklist.
func (s *Scheduler) workDeferred(key string) bool {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	return now.Before(s.workRetries[key].until)
}

func (s *Scheduler) noteWorkYield(key string, productive bool) time.Duration {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if productive {
		delete(s.workRetries, key)
		return 0
	}
	if s.workRetries == nil {
		s.workRetries = map[string]workRetry{}
	}
	delay := s.workRetries[key].delay * 2
	if delay < lowYieldRetryBase {
		delay = lowYieldRetryBase
	}
	if delay > lowYieldRetryMax {
		delay = lowYieldRetryMax
	}
	s.workRetries[key] = workRetry{delay: delay, until: now.Add(delay)}
	return delay
}
