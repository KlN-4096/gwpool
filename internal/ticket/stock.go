package ticket

import (
	"sort"
	"time"

	"gwpool/internal/types"
)

// One retained donor per logical region; old pair provenance remains readable.
const MaxRegionDonors = 512

type StockPlan struct {
	Result types.MintCommitResult
	Donors map[string]types.RegionDonor
}

// PlanStock does not mutate its input maps. Call under the store's commit lock:
// reading, comparison and publication must not race another mint's result.
func PlanStock(pairs map[string]types.Pair, donors map[string]types.RegionDonor, commit types.MintCommit) StockPlan {
	now, candidate := commit.At, commit.Pair
	old, present := pairs[candidate.Gateway]
	beforeBest := Best(pairs, donors, now)
	before := Rehome(old, beforeBest, now)
	next := make(map[string]types.RegionDonor, len(donors)+1)
	for region, donor := range donors {
		if donor.ExpiresAt.After(now) {
			next[region] = donor
		}
	}
	// Preserve donors belonging to an incumbent that a different logical
	// region may replace. Legacy pairs have no donor observation timestamp.
	for region, donor := range beforeBest {
		if donor.Exp.After(next[region].ExpiresAt) {
			next[region] = types.RegionDonor{RegionKey: region, Cflb: donor.Cflb, ExpiresAt: donor.Exp}
		}
	}
	cflb := CflbOf(candidate.Cookie)
	var donorGain int64
	if candidate.RegionKey != "" && cflb != "" && candidate.CflbExpiresAt.After(now) {
		region := candidate.RegionKey
		if candidate.CflbExpiresAt.After(next[region].ExpiresAt) {
			next[region] = types.RegionDonor{RegionKey: region, Cflb: cflb,
				ExpiresAt: candidate.CflbExpiresAt, ObservedAt: now}
		}
		donorGain = lifetimeGain(beforeBest[region].Exp, candidate.CflbExpiresAt, now)
	}
	pruneDonors(next)
	if next[candidate.RegionKey].Cflb != cflb {
		donorGain = 0
	}
	// The incoming cflb must be considered for BOTH tickets. Rejecting a short
	// oailb must not throw away a donor capable of rescuing other gateway tickets.
	best := Best(pairs, next, now)
	incumbent := Rehome(old, best, now)
	candidate = Rehome(candidate, best, now)
	result := types.MintCommitResult{Pair: incumbent}
	if !present || old.Cookie == "" || candidate.ExpiresAt.After(incumbent.ExpiresAt) {
		result.Pair, result.PairChanged = candidate, candidate != old
	}
	result.Gain = types.StockGain{
		CoverageAdded:   before.ExpiresAt.Sub(now) < types.DeliverFloor && result.Pair.ExpiresAt.Sub(now) >= types.DeliverFloor,
		ExtendedMS:      lifetimeGain(before.ExpiresAt, result.Pair.ExpiresAt, now),
		DonorExtendedMS: donorGain,
	}
	result.BeforeExpiresAt, result.AfterExpiresAt = before.ExpiresAt, result.Pair.ExpiresAt
	return StockPlan{Result: result, Donors: next}
}

func lifetimeGain(before, after, now time.Time) int64 {
	floor := now.Add(types.DeliverFloor)
	if before.Before(floor) {
		before = floor
	}
	if !after.After(before) {
		return 0
	}
	return after.Sub(before).Milliseconds()
}

func pruneDonors(donors map[string]types.RegionDonor) {
	if len(donors) <= MaxRegionDonors {
		return
	}
	keys := make([]string, 0, len(donors))
	for key := range donors {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left, right := donors[keys[i]].ExpiresAt, donors[keys[j]].ExpiresAt
		if left.Equal(right) {
			return keys[i] < keys[j]
		}
		return left.Before(right)
	})
	for _, key := range keys[:len(keys)-MaxRegionDonors] {
		delete(donors, key)
	}
}
