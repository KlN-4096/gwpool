package sched

import "gwpool/internal/types"

func (s *Scheduler) dynamicRegions(want types.Exit) []types.Region {
	var regions []types.Region
	for _, r := range s.cfg().CandidateRegions(want) {
		if s.mint.SupportsDynamicExit(r.Exit) {
			regions = append(regions, r)
		}
	}
	return regions
}
