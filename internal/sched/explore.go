package sched

import (
	"context"

	"gwpool/internal/coverage"
	"gwpool/internal/types"
)

// exploreNewGateway 每轮最多派一发；每两轮至少先于维护获得机会，库存已满也可发现。
// 必须由持有 backgroundMu 且有近期需求的 warmOnce 调用；所有锁、限额、落账沿用铸票主路径。
func (s *Scheduler) exploreNewGateway(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	before := s.st.Gateways()
	incomplete := coverage.IncompleteRegions(s.cfg(), before)
	landed, tried := s.lastLandings(), map[string]bool{}
	// 先找从未成功走过的账号×出口；都不可用才重新尝试已走过的未满区。
	for _, onlyUnseen := range []bool{true, false} {
		policy := &premintPolicy{skip: func(id int64, key string) bool {
			combo := comboKey(id, key)
			return !incomplete[key] || tried[combo] || s.workDeferred("explore|"+combo) || (onlyUnseen && landed[combo] != "")
		}}
		for try := 0; try < premintPickTries && ctx.Err() == nil; try++ {
			account, region, ok := s.minterFor(types.Exit{}, 0, s.opt.RenewGap, policy)
			if !ok {
				break
			}
			tried[comboKey(account.ID, region.Key)] = true
			release, free := s.claimRegion(account.ID, region.Key)
			if !free {
				continue
			}
			if !s.claimAccount(account.ID, s.opt.RenewGap) {
				release()
				continue
			}
			// 专门探索必须经过配置的实际出口，不能借种子把路由钉回已知大区。
			// 库存补铸/续票仍按原有种子策略；这一发用住宅/支持定向的粘性出口。
			sid, err := s.mint.NewSIDFor(region.Exit, true)
			if err != nil {
				release()
				return
			}
			unlock := s.st.LockAccount(account.ID)
			got, err := s.mintOnceFor(ctx, account, region.Exit, sid, types.Pair{}, "", "", types.PurposeExplore)
			unlock()
			release()
			isNew, gateway := false, ""
			if err == nil {
				gateway = got.pair.Gateway
				_, existed := before[gateway]
				isNew = !existed
				s.noteWorkYield("explore|"+comboKey(account.ID, region.Key), isNew)
			}
			s.opt.Log.Info("新网关探索一发", "account", account.ID, "region", region.Key,
				"gateway", gateway, "new_gateway", isNew, "succeeded", err == nil)
			return // 成功、重复、失败都不在同轮追加第二发。
		}
	}
}
