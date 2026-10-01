package sched

import (
	"context"
	"errors"
	"sort"
	"time"

	"gwpool/internal/types"
)

// Run 跑后台保温，直到 ctx 取消。
//
// **单账号模式直接返回，一发都不打**：扫描者 = 消费者时，保温那一发恰好烧掉
// 你马上要用的那个窗口（SPEC 红线 5）。单账号模式按需现铸、用完即弃。
func (s *Scheduler) Run(ctx context.Context) {
	if s.opt.Mode != types.ModePool {
		s.opt.Log.Info("单账号模式：不保温", "mode", s.opt.Mode)
		return
	}
	t := time.NewTicker(s.opt.WarmInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.warmOnce(ctx)
		}
	}
}

// warmOnce 巡检一轮，把快过期的 pair 续上。
//
// 保温成本 = 每网关每 65 分钟一发 ping；烧掉的是**扫描账号**的窗口，
// 消费账号的首次接触不受影响，这正是多账号模式成立的前提。
func (s *Scheduler) warmOnce(ctx context.Context) {
	// 无需求不扫不续：静默够久就让 pair 自然过期，成本归零（SPEC 第 7 节）。
	// 更要紧的是速率纪律——没人要还一直续，比业务消耗得更快。
	if s.idle() {
		return
	}
	// 只有一个号的「池子模式」就是红线 5 本身：保温那一发必然烧它自己的窗口，
	// 它再来取就全是 degraded ⇒ 全 503。所以凑不够两个号就不保温，
	// 行为自动退化成单账号模式（按需现铸），等第二个号进来再恢复。
	if n := len(s.usable()); n < 2 {
		s.opt.Log.Warn("可用账号不足两个，暂不保温（保温会烧掉这个号自己要用的窗口）", "accounts", n)
		return
	}
	// 全员在全部已知网关上都还在冷却里 ⇒ 预铸出来的票交给谁都是降级的，白烧一发。
	if s.premintWasted() {
		s.opt.Log.Info("所有号在所有已知网关上都还在冷却期，本轮不预铸")
		return
	}
	// 目标活 pair 数 = 最忙那个号碰过的网关数 + SpareTarget。够了就一发不打：
	// 续期不是免费的，每一发都在烧铸票者的窗口、吃速率预算。
	live, target := s.livePairs(), s.PairTarget()
	if live >= target {
		return
	}
	s.opt.Log.Info("预铸：补到目标深度", "live", live, "target", target)

	for name, gw := range s.st.Gateways() {
		if ctx.Err() != nil {
			return
		}
		if live >= target {
			return
		}
		if p, ok := s.st.Pair(name); ok && p.ExpiresAt.Sub(s.now()) > types.RenewLead {
			continue
		}
		exit, ok := pickExit(gw)
		if !ok {
			continue
		}
		// 铸过 pair 最少的号优先，节流同一个号两次续期至少隔 RenewGap。
		// 都在节流里就跳过这一轮，pair 还有 RenewLead 的余量，等下一轮不迟。
		sc, ok := s.warmMinter(s.opt.RenewGap)
		if !ok {
			continue
		}
		sid, err := s.mint.NewSID(true)
		if err != nil {
			s.opt.Log.Warn("要不到粘性出口，本轮不保温", "err", err)
			return
		}
		cur, _ := s.st.Pair(name)
		// 带着旧 pair 打：pair 自我续期，上游会回一张**同网关**的新 65 分钟 pair。
		unlock := s.st.LockAccount(sc.ID)
		_, err = s.mintOnce(ctx, sc, exit, sid, cur, name)
		unlock()
		switch {
		case err == nil:
			live++
			s.clearRenewFails(name)
		case errors.Is(err, types.ErrRateLimited):
			// 速率闸不是这个网关的错，别给它记失败预算，也别接着打。
			s.opt.Log.Warn("预铸撞上速率闸，本轮停", "gateway", name, "reason", err)
			return
		default:
			s.opt.Log.Info("保温失败", "gateway", name, "scanner", sc.ID, "label", sc.Label, "reason", err)
			s.noteRenewFail(name, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.opt.MintGap):
		}
	}
}

func (s *Scheduler) idle() bool {
	s.mu.Lock()
	last := s.lastDeliver
	s.mu.Unlock()
	// 零值 = 从没人要过：冷启动不该先花钱保温。
	return last.IsZero() || s.now().Sub(last) > s.opt.IdleAfter
}

// livePairs 数有活 pair 的网关：剩余还够交付的那些。
func (s *Scheduler) livePairs() int {
	now := s.now()
	n := 0
	for name := range s.st.Gateways() {
		if p, ok := s.st.Pair(name); ok && p.ExpiresAt.Sub(now) >= types.DeliverFloor {
			n++
		}
	}
	return n
}

// PairTarget 是目标活 pair 数：最忙那个号**真的用过**几张 pair，再加 SpareTarget。
//
// 「用过」= 槽位上有 LastDeliver，也就是真的有人在这个网关上取走过一张票。
// **两条交付路径都算**：上传者自己取（验过满血的）和外部账号拿一把 key 替自己取
// （池子没它的凭据、答不了它的满血，但它确实用掉了一张票，一样是供给压力）。
// 早先按 LastVerdict 判，那会漏掉整条外部消费者的路——池子对外服务得越多，
// 目标深度反而不涨。
//
// **刻意不用 LastTouch**：铸票本身也是一次 touch，拿它当基数就成了自激——预铸一张
// 让目标 +1、于是再铸一张、再 +1，一路把所有网关铸满，+2 的闸等于不存在。
//
// 目标天然被已知网关数封顶，所以当最忙的号把所有网关都用过之后，目标会高于网关总数
// ——那个缺口**铸票补不上**（pair 是路由，烧掉的是槽位），只能靠发现新网关。
func (s *Scheduler) PairTarget() int {
	used := map[int64]int{}
	for _, sl := range s.st.Slots() {
		if !sl.LastDeliver.IsZero() {
			used[sl.AccountID]++
		}
	}
	busiest := 0
	for _, a := range s.usable() {
		if n := used[a.ID]; n > busiest {
			busiest = n
		}
	}
	return busiest + types.SpareTarget
}

// premintWasted 报告这一轮预铸没人能用：所有托管号在所有已知网关上都还在
// SlotCooldown 里，铸出来的票发给谁都是降级的。
//
// **只看 LastDeliver，不看 LastTouch**：冷却是「满血窗口被用掉了」才起算的，
// 而 LastTouch 连失败的续期都记——拿它判会让保温自己把自己关掉（一发失败的续期
// 把槽位标成刚碰过 ⇒ 下一轮不预铸 ⇒ 死 pair 永远攒不满失败预算）。
//
// **有外部消费者在取票就不算白烧**：pair 是纯路由，拿一把 key 替自己取票的外部账号
// 在这些网关上大概率还是首次接触（2026-10-01 实测一张 pair 能同时服务多个账号），
// 池子答不了它的满血，但不该替它判死。
func (s *Scheduler) premintWasted() bool {
	gws, hosted := s.st.Gateways(), s.usable()
	if len(gws) == 0 || len(hosted) == 0 {
		return false
	}
	ids := make(map[int64]bool, len(hosted))
	for _, a := range hosted {
		ids[a.ID] = true
	}
	now, cooling := s.now(), 0
	for _, sl := range s.st.Slots() {
		if sl.LastDeliver.IsZero() || now.Sub(sl.LastDeliver) >= types.SlotCooldown {
			continue
		}
		if _, ok := gws[sl.Gateway]; !ok {
			continue // 已经被遗忘的网关，不算在冷却里
		}
		if !ids[sl.AccountID] {
			return false
		}
		cooling++
	}
	// 槽位按 (账号 × 网关) 唯一，所以数够这个积就是全员都在冷却里。
	return cooling >= len(ids)*len(gws)
}

// warmMinter 挑铸票者：**铸过的 pair 最少的号**，并列时挑最久没铸的。
//
// 和交付路径上的 minterFor 刻意不同：那边要挑「在这个网关上已经废掉的号」好省下
// 别人的窗口；预铸没有特定消费者可以让，就只按负载轮，别让一个号替全池扛。
func (s *Scheduler) warmMinter(throttle time.Duration) (types.Account, bool) {
	now := s.now()
	s.mu.Lock()
	minted := make(map[int64]int, len(s.mintCount))
	for k, v := range s.mintCount {
		minted[k] = v
	}
	lastUsed := make(map[int64]time.Time, len(s.lastMintedAt))
	for k, v := range s.lastMintedAt {
		lastUsed[k] = v
	}
	s.mu.Unlock()

	var cs []types.Account
	for _, a := range s.usable() {
		if throttle > 0 && now.Sub(lastUsed[a.ID]) < throttle {
			continue
		}
		cs = append(cs, a)
	}
	if len(cs) == 0 {
		return types.Account{}, false
	}
	sort.Slice(cs, func(i, j int) bool {
		if mi, mj := minted[cs[i].ID], minted[cs[j].ID]; mi != mj {
			return mi < mj
		}
		if ui, uj := lastUsed[cs[i].ID], lastUsed[cs[j].ID]; !ui.Equal(uj) {
			return ui.Before(uj)
		}
		return cs[i].ID < cs[j].ID
	})
	return cs[0], true
}
