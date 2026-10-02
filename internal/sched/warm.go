package sched

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"gwpool/internal/mint"
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
	// 目标活 pair 数 = 最忙那个号用过的 pair 数 + SpareTarget。够了就一发不打：
	// 铸票不是免费的，每一发都在烧执行者的窗口、吃它那个区域的速率预算。
	live, target := s.livePairs(), s.PairTarget()
	if live >= target {
		return
	}
	s.opt.Log.Info("预铸：补到目标深度", "live", live, "target", target)
	if s.cfg().RenewPairs {
		s.renewStalePairs(ctx, live, target)
		return
	}
	s.premintBare(ctx, target-live)
}

// seedCflbFloor 是一张票还能当「大区种子」用的最低 `__cflb` 剩余寿命。
//
// 它只要活过这一次往返：路由在请求到达上游的那一刻就定下来了，之后这张 cflb 什么时候
// 死都不影响这一发的落点。给 60 秒是为了兜住代理慢的那几次（铸票超时本身是 90 秒级）。
const seedCflbFloor = 60 * time.Second

// seedPairFor 找一张能把路由钉到这个大区的票：该区域里 `__cflb` 活得最久的那张。
//
// **谁铸的无所谓**，所以别人的票一样好用：大区跟着 `__cflb` 走，而落到哪个网关由
// *执行这一发的账号*决定（实测数据见 mint.CflbOnly）。这正是它的价值——不必在那个
// 大区有住宅出口，也能摸到自己在那儿的网关。
func (s *Scheduler) seedPairFor(regionKey string) (types.Pair, bool) {
	if regionKey == "" {
		return types.Pair{}, false
	}
	floor := s.now().Add(seedCflbFloor)
	var best types.Pair
	for name := range s.st.Gateways() {
		p, ok := s.st.Pair(name)
		if !ok || p.RegionKey != regionKey || !p.CflbExpiresAt.After(floor) {
			continue
		}
		// 没有 __cflb 的票钉不住任何路由，当种子是零价值的（见 mint.PairInfo）。
		if mint.CflbOnly(p.Cookie) == "" {
			continue
		}
		if best.Cookie == "" || p.CflbExpiresAt.After(best.CflbExpiresAt) {
			best = p
		}
	}
	return best, best.Cookie != ""
}

// premintBare 裸铸补深度：挑一个 (账号 × 大区) 组合打一发，**落到哪个网关算哪个**。
//
// 为什么这是默认而续期是选项：续期能让一张票一直活着，但每续一发都要烧掉执行者在
// 那个网关上的窗口，换来的只是「同一个落点的新 cookie」——而池子缺的是落点不是票。
// 同样一发上游花在裸铸上，至少有机会摸到一个没见过的网关。
//
// 挑组合而不是「挑号、让它随便落」：(账号 × 大区) 基本是一对一的，所以台账上
// 「这个组合上次落在哪」就是「再派它会落到哪」的预测（types.RegionMint.Gateway）。
// 据此分成三类：
//
//	预测落点没有活票  → 挑它。这正好覆盖了「整批票同时到期」那种缺口
//	这个组合没铸过    → 挑它。这是唯一可能摸出新网关的地方
//	预测落点已有活票  → 跳过，连一发都不用打就知道是白打
//
// 旧版靠「连续两发没让活 pair 总数涨就整轮收手」止损。那个计数器是全轮全局的、
// 不分账号不分区域，而 2026-10-02 现网 105 个网关里 64% 本来就有活票 ⇒ 连着两发
// 撞上的概率约 41%，差不多一半的轮次在第二发就死了（24 小时触发 279 次、至少 578
// 发白打）。按组合筛掉了其中**可预测**的那部分：预测落点已有活票的组合一发都不用打。
//
// 但止损闸不能直接删掉，**台账是空的时候筛不动**：没铸过的组合预测不出落点，
// 于是「账号数 × 9」个组合会全部被当成值得一试，一轮就能打出上千发
// （113 个号 × 9 = 1017）。所以留一个预算，但口径换成「失手」：
// 一轮里最多容忍和缺口一样多的失手（missBudget = n）⇒ 总发数封顶 2n。
// 和旧版的差别在于它数的是**预测错**，不是「随机撞上」——台账热了之后基本不触发。
//
// 于是停止条件四条：补到目标、失手用完、没有可挑的组合（穷举，候选集有限）、撞上闸门。
//
// 种子票那一格是成本上的关键：某个大区手上有活 `__cflb` 时，这一发走我们自己的普通
// 机房出口（config.KindSeed），不花住宅代理 ⇒ 「去摸新落点」基本免费。
// 没有种子票的大区照旧走住宅。
func (s *Scheduler) premintBare(ctx context.Context, n int) {
	tried := map[string]bool{} // 本轮试过的 (账号 × 大区)
	aimed := map[string]bool{} // 本轮已经有人冲着去的网关，别两个组合撞同一个
	missed := 0
	for got := 0; got < n && missed < n; {
		if ctx.Err() != nil {
			return
		}
		live := s.liveGateways()
		landed := s.lastLandings()
		skip := func(id int64, key string) bool {
			combo := comboKey(id, key)
			if tried[combo] {
				return true
			}
			gw := landed[combo]
			if gw == "" {
				return false // 没铸过 ⇒ 去摸新落点
			}
			return live[gw] || aimed[gw]
		}
		// 本小时打得最少的号优先、它终身没走过的区域优先；
		// 节流同一个号两次铸票至少隔 RenewGap。
		a, r, ok := s.minterFor(types.Exit{}, 0, s.opt.RenewGap, skip)
		if !ok {
			s.opt.Log.Info("没有可挑的 (账号 × 大区) 组合了，本轮预铸收尾",
				"got", got, "want", n, "tried", len(tried))
			return
		}
		tried[comboKey(a.ID, r.Key)] = true
		if gw := landed[comboKey(a.ID, r.Key)]; gw != "" {
			aimed[gw] = true
		}
		// 同一个区域一次只许一发裸铸在打。占不住就换下一个组合——不排队，见 claimRegion。
		release, free := s.claimRegion(r.Key)
		if !free {
			continue
		}
		sid, seed, err := s.premintSID(r.Key)
		if err != nil {
			release()
			s.opt.Log.Warn("要不到出口，本轮不预铸", "region", r.Key, "err", err)
			return
		}
		unlock := s.st.LockAccount(a.ID)
		// want 留空：裸铸就是去看它能落到哪，谈不上「落点漂移」。带种子也一样——
		// 种子只钉大区，具体网关是这个号自己的，出手前并不知道是哪个。
		got2, err := s.mintOnce(ctx, a, r.Exit, sid, seed, "", "")
		unlock()
		release()
		switch {
		case err == nil:
			s.opt.Log.Info("裸铸拿到落点", "account", a.ID, "label", a.Label,
				"region", r.Key, "gateway", got2.pair.Gateway, "seeded", seed.Cookie != "")
			// 只有「落到一个刚才没有活票的网关」才算补上了一格。落回已有活票的网关
			// 说明预测错了（区域划分随 cohort 变，SPEC 第 4 节）——台账已经按实际
			// 落点更新过了，这个组合也标成试过了，所以不会原地打转，接着挑下一个。
			if !live[got2.pair.Gateway] {
				got++
				break
			}
			missed++
			s.opt.Log.Info("落回了已有活票的网关，算一次失手，换下一个组合",
				"account", a.ID, "region", r.Key, "gateway", got2.pair.Gateway,
				"missed", missed, "budget", n)
		case errors.Is(err, types.ErrRateLimited), errors.Is(err, types.ErrNoExit):
			// 闸门和出口故障都不是这个号的错，也不是重试能解决的，本轮直接停。
			s.opt.Log.Warn("预铸撞上闸门，本轮停", "account", a.ID, "region", r.Key, "reason", err)
			return
		default:
			s.opt.Log.Info("裸铸失败", "account", a.ID, "label", a.Label,
				"region", r.Key, "reason", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.opt.MintGap):
		}
	}
}

// premintSID 给这一发预铸挑出口：该大区有活种子票就走我们自己的普通机房出口，
// 否则回落到住宅。返回的 pair 是要送出去的种子（零值 = 裸打）。
//
// 种子出口要不到时**也回落**而不是报错：它只是省钱的那条路，少了它池子照样能铸票。
func (s *Scheduler) premintSID(regionKey string) (string, types.Pair, error) {
	if seed, ok := s.seedPairFor(regionKey); ok {
		sid, err := s.mint.NewSeedSID()
		if err == nil {
			return sid, seed, nil
		}
		s.opt.Log.Debug("没有种子出口，这一发回落住宅", "region", regionKey, "err", err)
	}
	sid, err := s.mint.NewSID(true)
	return sid, types.Pair{}, err
}

// lastLandings 是 (账号 × 大区) → 上次落点，key = comboKey。空串的组合不进来。
func (s *Scheduler) lastLandings() map[string]string {
	out := map[string]string{}
	for _, m := range s.st.RegionMints() {
		if m.Gateway != "" {
			out[comboKey(m.AccountID, m.RegionKey)] = m.Gateway
		}
	}
	return out
}

// comboKey 是 (账号 × 大区) 在本文件这几张临时表里的键。
//
// 刻意不用 store.MintKey：那个键的格式归 store 所有（见 Store.RegionMints 的注释），
// 这里只是进程内的一轮临时记账，不该跟着它走。
func comboKey(accountID int64, regionKey string) string {
	return strconv.FormatInt(accountID, 10) + "|" + regionKey
}

// renewStalePairs 是 renew_pairs 打开时的保温：带着旧 pair 去续同一个网关。
//
// 上游会回一张**同网关**的新 65 分钟 pair（pair 自我续期，SPEC 第 6 节）。
// 代价是烧掉执行者在那个网关上的窗口——想「稳住已知落点」而不是「多摸落点」时才开它。
func (s *Scheduler) renewStalePairs(ctx context.Context, live, target int) {
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
		// 本小时打得最少的号优先，节流同一个号两次续期至少隔 RenewGap。
		// 都在节流里就跳过这一轮，pair 还有 RenewLead 的余量，等下一轮不迟。
		// 出口由这个网关定死，所以只要人、不要它挑的区域。
		sc, _, ok := s.minterFor(exit, 0, s.opt.RenewGap, nil)
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
		//
		// 要拿网关锁：不拿的话保温和交付路径的现铸能同时为同一个网关各打一发，
		// 而一张 pair 本来就能同时服务多个消费者（types.Pair），第二发纯属白烧
		// 一个槽位加一发请求——正是这一轮要消掉的浪费。
		// 锁序恒为「先网关、再账号」，和交付那边一致 ⇒ 不存在 ABBA。
		relGW := s.lockGateway(name)
		unlock := s.st.LockAccount(sc.ID)
		_, err = s.mintOnce(ctx, sc, exit, sid, cur, name, name)
		unlock()
		relGW()
		switch {
		case err == nil:
			// 按实测重数，**不是** live++：上游接受了一张还有效的 pair 时不下发新
			// cookie（SPEC 第 2 节），那张票的 ExpiresAt 一个字没变 ⇒ 无条件自增会让
			// 计数器虚高、本轮提前收手，而深度其实没涨。
			live = s.livePairs()
			s.clearRenewFails(name)
		case errors.Is(err, types.ErrRateLimited), errors.Is(err, types.ErrNoExit):
			// 闸门和基础设施故障不是这个网关的错，别给它记失败预算，也别接着打。
			s.opt.Log.Warn("续期撞上闸门，本轮停", "gateway", name, "reason", err)
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
//
// 就是 FreeGateways 本身——保留这个名字是因为保温这边说的是「深度」、HTTP 那边说的是
// 「空闲」，但读数必须是**同一个**：两份实现会让「硬筛掉没票的网关」和「该不该预铸」
// 用两把尺子量同一件事。
func (s *Scheduler) livePairs() int { return s.FreeGateways() }

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
// 目标高于已知网关数时，缺口靠裸铸、探索扫描摸新网关来补；但上游网关总共就那么多，
// 所以封顶在 types.GatewayCeiling——不封的话最忙的号用遍全部网关之后目标永远够不着，
// 每一轮保温都会为它白打几发。
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
	// 富余张数是配置项：0 就是「全按需，只补被人用掉的」。见 config.Config.SpareTarget。
	spare := s.cfg().SpareTarget
	// 填得比网关总数还大 = 停止预铸，不报错（用户 10-03 定的）：那个数本来就铸不出来，
	// 照着它补只会每轮白打。目标归零，warmOnce 一发都不打；交付路径的现铸不受影响。
	if spare > types.GatewayCeiling {
		return 0
	}
	return min(busiest+spare, types.GatewayCeiling)
}

// premintWasted 报告这一轮预铸没人能用：所有托管号在所有已知网关上都还在
// 冷却里，铸出来的票发给谁都是降级的。
//
// **只看 LastDeliver，不看 LastTouch**：冷却是「满血窗口被用掉了」才起算的，
// 而 LastTouch 连失败的续期都记——拿它判会让保温自己把自己关掉（一发失败的续期
// 把槽位标成刚碰过 ⇒ 下一轮不预铸 ⇒ 死 pair 永远攒不满失败预算）。
//
// **有外部消费者在取票就不算白烧**：pair 是纯路由，拿一把 key 替自己取票的外部账号
// 在这些网关上大概率还是首次接触（2026-10-01 实测一张 pair 能同时服务多个账号），
// 池子答不了它的满血，但不该替它判死。
//
// 「是不是外部消费者」按 types.AnonPrefix 判，**不能**按「是不是当前可用账号」：
// usable() 要求 Enabled 且有凭据，而一个 400/401/403 就会自动停用一个托管号
// （mintOnce / scan）。按可用性判的话，只要有任一个号在近 cooldown 内交付过之后被停用，
// 这条成本闸就对整池永久失效。
func (s *Scheduler) premintWasted() bool {
	gws, hosted := s.st.Gateways(), s.usable()
	if len(gws) == 0 || len(hosted) == 0 {
		return false
	}
	ids := make(map[int64]bool, len(hosted))
	for _, a := range hosted {
		ids[a.ID] = true
	}
	external := map[int64]bool{}
	for _, a := range s.st.Accounts() {
		if strings.HasPrefix(a.Creds.AccountID, types.AnonPrefix) {
			external[a.ID] = true
		}
	}
	now, cooling := s.now(), 0
	cooldown := s.cfg().SlotCooldown()
	for _, sl := range s.st.Slots() {
		if sl.LastDeliver.IsZero() || now.Sub(sl.LastDeliver) >= cooldown {
			continue
		}
		if _, ok := gws[sl.Gateway]; !ok {
			continue // 已经被遗忘的网关，不算在冷却里
		}
		if external[sl.AccountID] {
			return false
		}
		if !ids[sl.AccountID] {
			continue // 已停用的托管号：它不会再来取票，不该让它的历史解开这条闸
		}
		cooling++
	}
	// 槽位按 (账号 × 网关) 唯一，所以数够这个积就是全员都在冷却里。
	return cooling >= len(ids)*len(gws)
}

// 「挑谁去铸」只有 minterFor 一份了。
//
// 早先保温这边另有一个 warmMinter（按「铸过几张」轮），理由是交付路径那边要专挑
// 「在这个网关上已经废掉的号」、两套取向相反。2026-10-01 那条规则删掉之后
// （见 minterFor 的注释），两边要的是同一件事：本小时打得最少、没走过的区域优先。
