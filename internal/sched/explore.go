package sched

import (
	"context"
	"time"

	"gwpool/internal/coverage"
	"gwpool/internal/types"
)

const dedicatedExplorationInterval = 15 * time.Minute

// exploreNewGateway 每15分钟最多获得一次机会；库存已满也可低频发现。
// 必须由持有 backgroundMu 且有近期需求的 warmOnce 调用；所有锁、限额、落账沿用铸票主路径。
func (s *Scheduler) exploreNewGateway(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	now := s.now()
	if now.Before(s.nextExploration) {
		return
	}
	// Consume the opportunity even when candidates/egress fail. Retrying those
	// every maintenance tick would defeat the dedicated exploration interval.
	// This process-local gate never throttles inventory repair or renewal.
	s.nextExploration = now.Add(dedicatedExplorationInterval)
	before := s.st.Gateways()
	incomplete := coverage.IncompleteRegions(s.cfg(), before)
	landed := s.lastLandings()
	regions := s.dynamicRegions(types.Exit{})
	for try := 0; try < len(regions) && try < premintPickTries && ctx.Err() == nil; try++ {
		// Advance before binding/sending. Failed countries must not monopolize
		// the original nine-region-sized total budget on every reset.
		s.mu.Lock()
		target := regions[s.exploreRegionCursor%len(regions)]
		s.exploreRegionCursor = (s.exploreRegionCursor + 1) % len(regions)
		s.mu.Unlock()
		if !incomplete[target.Key] {
			continue
		}
		var account types.Account
		var region types.Region
		var ok bool
		// Within the next country's turn, prefer an unseen account combination.
		for _, onlyUnseen := range []bool{true, false} {
			policy := &premintPolicy{skip: func(id int64, key string) bool {
				combo := comboKey(id, key)
				return s.workDeferred("explore|"+combo) || (onlyUnseen && landed[combo] != "")
			}}
			account, region, ok = s.minterFor(target.Exit, 0, s.opt.RenewGap, policy)
			if ok {
				break
			}
		}
		if !ok {
			continue
		}
		release, free := s.claimRegion(account.ID, region.Key)
		if !free {
			continue
		}
		if !s.claimAccount(account.ID, s.opt.RenewGap) {
			release()
			continue
		}
		// Dedicated discovery never borrows a seed route.
		unlock := s.st.LockAccount(account.ID)
		attemptCtx, err := s.beginAttempt(ctx, account, region.Exit)
		var sid string
		var got minted
		if err == nil {
			sid, err = s.mint.NewSIDFor(region.Exit, true)
			if err != nil {
				s.sampleAttemptFailure(account, region.Exit, "", types.PurposeExplore, types.Observed{}, err)
			}
		}
		if err == nil {
			got, err = s.mintOnceFor(attemptCtx, account, region.Exit, sid, types.Pair{}, "", "", types.PurposeExplore)
		}
		unlock()
		release()
		isNew, gateway := false, ""
		if err == nil {
			gateway = got.pair.Gateway
			_, existed := before[gateway]
			isNew = !existed
		}
		s.noteWorkYield("explore|"+comboKey(account.ID, region.Key), isNew)
		s.opt.Log.Info("新网关探索一发", "account", account.ID, "region", region.Key,
			"gateway", gateway, "new_gateway", isNew, "succeeded", err == nil)
		return // At most one upstream attempt, on success or failure.
	}
}
