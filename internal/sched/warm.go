package sched

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/mint"
	"gwpool/internal/routes"
	"gwpool/internal/types"
)

// Run 跑后台保温，直到 ctx 取消。
//
// 单账号模式不运行后台保温；消费者始终只取现票，不在 HTTP 请求内补铸。
func (s *Scheduler) Run(ctx context.Context) {
	if s.opt.Mode != types.ModePool {
		s.opt.Log.Info("单账号模式：不保温", "mode", s.opt.Mode)
		return
	}
	t := time.NewTicker(s.opt.WarmInterval)
	defer t.Stop()
	wake := time.NewTimer(demandMaintenanceGap)
	if !wake.Stop() {
		<-wake.C
	}
	defer wake.Stop()
	var lastWake time.Time
	var wakeAt <-chan time.Time
	runWake := func() {
		lastWake = time.Now()
		wakeAt = nil
		s.maintainOnDemand(ctx)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.warmOnce(ctx)
		case <-s.inventoryWake:
			if gap := time.Until(lastWake.Add(demandMaintenanceGap)); gap > 0 {
				if wakeAt == nil {
					wake.Reset(gap)
					wakeAt = wake.C
				}
			} else {
				runWake()
			}
		case <-wakeAt:
			runWake()
		}
	}
}

// warmOnce 安排一轮按真实供给需要构成的维护任务和独立低频探索。
func (s *Scheduler) warmOnce(ctx context.Context) {
	s.backgroundMu.Lock()
	defer s.backgroundMu.Unlock()
	// 无需求不扫不续：静默够久就让 pair 自然过期，成本归零（SPEC 第 7 节）。
	// 更要紧的是速率纪律——没人要还一直续，比业务消耗得更快。
	if s.idle() {
		return
	}
	if s.maintenanceOff() {
		return
	}
	// 只有一个号的「池子模式」就是红线 5 本身：保温那一发必然烧它自己的窗口，
	// 它再来取就全是 degraded ⇒ 全 503。所以凑不够两个号就不保温，
	// 继续交付现票，等第二个可用账号进来再恢复后台维护。
	if n := len(s.usable()); n < 2 {
		s.opt.Log.Warn("可用账号不足两个，暂不保温（保温会烧掉这个号自己要用的窗口）", "accounts", n)
		return
	}
	// 维护按有效库存和区域种子建立有界任务，不因虚高的数量目标盲打。
	// 交替先行，既防维护吃光探索机会，也避免少账号时探索长期占住续票账号。
	// 专门探索每15分钟最多一次机会，共用全部原配额；这不是新增独立速率额度。
	if s.maintenanceFirst {
		s.maintainInventory(ctx)
		s.exploreNewGateway(ctx)
	} else {
		s.exploreNewGateway(ctx)
		s.maintainInventory(ctx)
	}
	s.maintenanceFirst = !s.maintenanceFirst
}

// maintainInventory schedules concrete stock and regional-donor needs.
func (s *Scheduler) maintainInventory(ctx context.Context) {
	s.runMaintenance(ctx, true)
}

// renewPairLead 是「剩这么多就该续了」。
//
// 10 分钟：保温一分钟一轮，而续一张要走完抢号 → 拿两把锁 → 一发上游（几秒到几十秒），
// 还得留出「撞上 RenewGap 被跳过、下一轮再来」的余量。给太短票在排队时就死了，
// 给太长等于提前把还能用的寿命烧掉。
const renewPairLead = 10 * time.Minute

// renewBudgetPerRound 是一轮最多续几张。
//
// 它不是性能旋钮是**供给闸**：每张续票都是一发真实上游请求，吃的是那个号那个区域的
// 小时预算（和裸铸、state-echo 共用）。一轮封顶之后，剩下的下一轮接着续 ——
// 而排序把难到的放在最前面，所以被推到下一轮的永远是「好摸的那些」。
const renewBudgetPerRound = 12

// renewTarget 是一张等着续的票，连同续它要用的那个号。
type renewTarget struct {
	gateway string
	acct    types.Account
	exit    types.Exit
	pair    types.Pair
	source  types.Pair // 锁前原始记录；pair 已装上捐赠者，不能拿它比对库里的票。
	// reach 是台账中仍可用的不同凭据身份数，只是恢复来源稀缺程度的线索。
	reach int
}

// renewExpiring 沿已有票的来源账号及同区合法 cflb 尝试恢复，不把历史
// 来源当永久映射。路线退避、实际落点与原子保优均走共享路径。
// 独立入口保留有界续票预算；自动维护由 maintenancePlan 与缺票/种子任务排序。
func (s *Scheduler) renewExpiring(ctx context.Context) {
	if !s.cfg().RenewPairs || s.maintenanceOff() {
		return
	}
	due, noDonor := s.pairsDueForRenew()
	s.renewCandidates(ctx, due, noDonor)
}

func (s *Scheduler) renewCandidates(ctx context.Context, due []renewTarget, noDonor int) int {
	if len(due) == 0 {
		if noDonor > 0 {
			// 不打这一行的话「整池 30 张等着续但一张活 cflb 都借不到」和「没票到期」
			// 在日志里长得一模一样，而前者要人去看大区分布、后者什么都不用做。
			s.opt.Log.Info("续票：到期的票一张都借不到活 __cflb，本轮跳过", "no_donor", noDonor)
		}
		return 0
	}
	attempted, renewed, unchanged, failed, skipped, deferred, drifted, productive := 0, 0, 0, 0, 0, 0, 0, 0
	for _, t := range due {
		if ctx.Err() != nil || attempted >= renewBudgetPerRound {
			break
		}
		if !s.routeAvailable(t.acct, t.exit, t.pair, t.gateway, false) {
			deferred++
			continue
		}
		used, limit := s.mint.Rate(t.acct.Creds.AccountID)
		if s.accountHeldFor(t.acct.ID) > 0 || types.MintBudgetFull(used, limit, s.cfg().AccountAttemptLimit(), t.exit.Region) {
			skipped++
			continue
		}
		// 防封节奏：同一个号两次铸票至少隔 RenewGap。抢不到就留给下一轮，
		// 票还有 renewPairLead 的余量。
		if !s.claimAccount(t.acct.ID, s.opt.RenewGap) {
			skipped++
			continue
		}
		// 锁序恒为「先网关、再账号」，和交付那边一致 ⇒ 不存在 ABBA。
		relGW := s.lockGateway(t.gateway)
		unlock := s.st.LockAccount(t.acct.ID)
		current, present := s.st.Pair(t.gateway)
		acct, enabled := s.account(t.acct.ID)
		effective, _ := s.freshPair(t.gateway, types.DeliverFloor)
		if !present || current != t.source || !enabled || ctx.Err() != nil ||
			t.pair.CflbExpiresAt.Sub(s.now()) < seedCflbFloor || effective.ExpiresAt.Sub(s.now()) > renewPairLead ||
			!s.cfg().RenewPairs || s.maintenanceOff() {
			// 等锁时可能已经换票、停号或耗尽种子寿命；不能拿旧快照再盖回去。
			unlock()
			relGW()
			skipped++
			continue
		}
		attemptCtx, acct, sid, releaseRoute, err := s.bindRoute(ctx, acct, t.exit, t.pair, t.gateway, types.PurposeRenew, false)
		var got minted
		if _, reserved := attemptCtx.Value(routeWorkKey{}).(routeWork); reserved {
			attempted++
		}
		if err == nil {
			// want 和 bias 都填这个网关：这是一发**定向**铸票，落点漂了要被当成漂移处理
			// （mintOnce 自己判），别静悄悄把别的网关的票记成这一张续好了。
			got, err = s.mintOnceFor(attemptCtx, acct, t.exit, sid, t.pair, t.gateway, t.gateway, types.PurposeRenew)
		}
		if releaseRoute != nil {
			releaseRoute()
		}
		unlock()
		relGW()
		if got.gain.Productive() {
			productive++
		}
		switch {
		case err == nil && !got.gain.CoverageAdded && got.gain.ExtendedMS == 0:
			unchanged++
			s.opt.Log.Info("续票未延长", "gateway", t.gateway, "account", t.acct.ID,
				"region", t.pair.RegionKey, "remaining_s", int(got.pair.ExpiresAt.Sub(s.now()).Seconds()))
		case err == nil:
			renewed++
			s.opt.Log.Info("续票", "gateway", t.gateway, "account", t.acct.ID,
				"region", t.pair.RegionKey, "reach", t.reach, "landed", got.pair.Gateway,
				"remaining_s", int(got.pair.ExpiresAt.Sub(s.now()).Seconds()))
		case errors.Is(err, types.ErrRateLimited):
			// 本号本区限流不是全池故障；也覆盖预检后刚被其他请求用完配额的竞态。
			skipped++
			s.opt.Log.Info("续票账号限流，跳过", "gateway", t.gateway, "account", t.acct.ID)
		case errors.Is(err, errRouteDeferred):
			deferred++
		case errors.Is(err, errLandingDrift):
			drifted++
			s.opt.Log.Info("续票目标漂移", "gateway", t.gateway, "landed", got.pair.Gateway,
				"account", t.acct.ID, "region", t.pair.RegionKey, "stock_gain", got.gain)
		case errors.Is(err, types.ErrNoExit):
			s.opt.Log.Warn("续票无种子出口，本轮停", "gateway", t.gateway, "reason", err)
			return attempted
		default:
			failed++
			s.opt.Log.Info("续票失败", "gateway", t.gateway, "account", t.acct.ID,
				"region", t.pair.RegionKey, "reach", t.reach, "reason", err, "stock_gain", got.gain)
		}
	}
	s.opt.Log.Info("续票一轮结束", "due", len(due), "attempted", attempted, "renewed", renewed, "unchanged", unchanged, "failed", failed,
		"drifted", drifted, "productive", productive, "throttled", skipped, "deferred", deferred, "no_donor", noDonor)
	return attempted
}

// pairsDueForRenew 列出该续的票，难到的排前面。
//
// 采用同大区最长寿的活 `__cflb`：上游若只补发 __oailb，新票仍受这张 cflb 的死期约束，
// 借长寿 cflb 才能让池内新票的 TTL 一并延长（用户确认保留这条换盆语义）。
// “同区 + 来源账号”只预测落点，不保证命中原网关；临期不等于已经过期，也不能仅凭
// 历史漂移比例认定原因。每次仍核对响应落点，且只有实际 TTL 延长才计入 renewed。
// 第二个返回值 = 到期了但借不到活捐赠者的张数，只用来打日志（见调用方）。
func (s *Scheduler) pairsDueForRenew() ([]renewTarget, int) {
	now := s.now()
	// 捐赠者池子：按大区挑最长寿的 `__cflb`（换盆用的同一份）。
	best := s.bestCflb()
	// 台账里每个网关被几个**不同的号**落到过。只数「上次落点」是因为台账就只记这个
	// （types.RegionMint.Gateway），它是现有数据里离「这个网关好不好摸」最近的量。
	identities := map[int64]string{}
	for _, account := range s.usable() {
		identities[account.ID] = routes.CredentialIdentity(account.Creds)
	}
	reach := map[string]map[string]bool{}
	for _, m := range s.st.RegionMints() {
		identity := identities[m.AccountID]
		if m.Gateway == "" || identity == "" || s.accountHeldFor(m.AccountID) > 0 {
			continue
		}
		if reach[m.Gateway] == nil {
			reach[m.Gateway] = map[string]bool{}
		}
		reach[m.Gateway][identity] = true
	}
	var out []renewTarget
	noDonor := 0
	for name := range s.st.Gateways() {
		p, ok := s.st.Pair(name)
		if !ok {
			continue
		}
		source := p
		p = mint.Rehome(p, best, now)
		if p.ExpiresAt.Sub(now) > renewPairLead {
			continue
		}
		// 种子出口靠 cookie 路由；e 仅用于速率桶、样本和新票的区域标签。
		// 不要求存在住宅历史出口，更不能从 ReachableExits 随机挑另一区。
		exit, ok := s.renewalExit(p.RegionKey)
		if !ok {
			continue
		}
		// 来源号被停用/删了就续不了这一张：换个号去打落的是别的网关（见函数头）。
		acct, ok := s.account(p.MintedBy)
		if !ok {
			continue
		}
		// 借一张同大区还活着的 `__cflb`（见函数头）。大区为空的老记录借不到 ——
		// 那是 b6c1987 之前发现铸票留下的，续不了，等它自然过期。
		// 门槛沿用 seedCflbFloor：剩不到 10 分钟的捐赠者铸出来的票生下来就发不出去。
		donor, ok := best[p.RegionKey]
		if !ok || donor.Exp.Sub(now) < seedCflbFloor {
			noDonor++
			continue
		}
		swapped := mint.SwapCflb(p.Cookie, donor.Cflb)
		if swapped == p.Cookie && mint.CflbOf(p.Cookie) != donor.Cflb {
			noDonor++ // 这张票连 __oailb 都没有，SwapCflb 换不动
			continue
		}
		p.Cookie, p.CflbExpiresAt = swapped, donor.Exp
		out = append(out, renewTarget{gateway: name, acct: acct, exit: exit,
			pair: p, source: source, reach: len(reach[name])})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].reach != out[j].reach {
			return out[i].reach < out[j].reach // 难到的先续
		}
		if !out[i].pair.ExpiresAt.Equal(out[j].pair.ExpiresAt) {
			return out[i].pair.ExpiresAt.Before(out[j].pair.ExpiresAt) // 一样稀有就先救快死的
		}
		return out[i].gateway < out[j].gateway // 稳定可复现
	})
	return out, noDonor
}

func (s *Scheduler) renewalExit(regionKey string) (types.Exit, bool) {
	if r, ok := s.cfg().RegionByKey(regionKey); ok {
		return r.Exit(), true
	}
	return types.Exit{}, false
}

// seedCflbFloor 是一张票还能当「大区种子」用的最低 `__cflb` 剩余寿命。
//
// 定这个数的**不是**这一次往返：路由在请求到达上游的那一刻就定下来了，之后那张 cflb
// 什么时候死都不影响这一发的落点（所以一度写成 60 秒）。真正定它的是**铸出来那张票
// 的寿命**：只送 `__cflb` 时上游一般不补发新的 `__cflb`（10-03 实测 3/3 不补），
// 于是新票原样留着种子那张，寿命跟着种子一起到点（见 types.MintResult.RouteDeadline
// 第二级）。种子剩 60 秒 ⇒ 铸出来的票也只剩 60 秒 ⇒ 低于 DeliverFloor、生下来就发不出去，
// 而且它还会被记成「补上了一格」（liveGateways 按 DeliverFloor 过滤，那一格其实是空的）。
//
// 所以门槛要让铸出来那张**确实能发**：DeliverFloor 是 300 秒，这里留到 10 分钟，
// 使用侧还有几分钟余量。低于它就回落住宅——住宅裸铸拿的是一张满寿命的新 cflb。
const seedCflbFloor = 10 * time.Minute

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

// premintBare restores known routes through dynamic exits. Unproven bootstrap
// is allowed only for an empty pool and remains separately bounded.
func (s *Scheduler) premintBare(ctx context.Context, n int) {
	s.runInventory(ctx, n, true)
}

func (s *Scheduler) runInventory(ctx context.Context, n int, allowBootstrap bool) int {
	return s.runInventorySelection(ctx, n, inventorySelection{bootstrap: allowBootstrap && len(s.st.Gateways()) == 0})
}

type inventorySelection struct {
	gateway   string
	donor     string
	bootstrap bool
}

func (s *Scheduler) runInventorySelection(ctx context.Context, n int, selection inventorySelection) int {
	workers := s.premintWorkers(n)
	r := &premintRound{want: n, tried: map[string]bool{}, aimed: map[string]bool{},
		allowBootstrap: selection.bootstrap, selection: selection}
	r.routing = s.newInventoryRoutingRound()
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.premintLoop(ctx, r)
		}()
	}
	wg.Wait()
	got, missed, combos := r.result()
	s.opt.Log.Info("预铸一轮结束", "want", n, "got", got, "missed", missed,
		"productive", r.productive, "attempted", r.attempted, "extended_ms", r.extendedMS,
		"donor_extended_ms", r.donorMS, "combos_tried", combos, "workers", workers)
	return r.attempted
}

// premintWorkers 是这一轮派几个 worker 去铸。三道封顶：
//
//   - 配置的 mint_concurrency（config.Load 已经把它钳在区域数以内）
//   - 缺口 n：worker 比缺口还多，多出来的那几个一定空跑
//   - 至少 1（0 个 worker = 保温彻底不铸票，那不是任何人想要的配置）
//
// **刻意不再按「有种子票的大区数」收一次**：claimRegion 保证一个号同时只有一发裸铸
// 走同一个大区，占不住的 worker 换组合、换不到就收尾。它**不**按大区整体拦（那一版
// 把并发焊死在 9，见 claimRegion），所以几个 worker 同时走同一个大区是正常的，
// 只要它们是不同的号。
func (s *Scheduler) premintWorkers(n int) int {
	return max(1, min(s.cfg().MintWorkers(), n))
}

// premintPickTries 是一次挑选最多抢几回账号。抢不到只会是「别的 worker 刚登记了
// 同一个号」，而登记之后 minterFor 自己就跳过它 ⇒ 最多碰并发数那么多次。
const premintPickTries = 4

// premintRound 是一轮预铸的共享进度，几个 worker 共用它。
//
// 每个字段都只在 mu 里读写，**循环条件本身也在里面**：got 和 missed 一起封顶这一轮
// 的总发数（最多 2n 发），各个 worker 分头各算一份就等于把那个封顶乘上并发数。
type premintRound struct {
	routing        *inventoryRoutingRound // immutable model; prediction cache protected by mu
	mu             sync.Mutex
	want           int // 缺口，同时也是失手预算
	got            int // 补上的格子数
	missed         int // 没有库存收益的结果，含失败。
	productive     int
	attempted      int
	extendedMS     int64
	donorMS        int64
	selection      inventorySelection
	halt           bool            // 整池同病：组合挑完了、要不到出口、撞上闸
	tried          map[string]bool // 本轮试过的 (账号 × 大区)
	aimed          map[string]bool // 本轮已经有人冲着去的网关，别两个组合撞同一个
	unproven       int             // 无恢复预测的引导尝试，不能用完正常补货的全部缺口预算。
	allowBootstrap bool            // 仅周期维护可引导冷池；客户端重试不能不断领取探索额度。
}

func (r *premintRound) more() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.halt && r.productive < r.want && r.missed < r.want
}

func (r *premintRound) stop() {
	r.mu.Lock()
	r.halt = true
	r.mu.Unlock()
}

func (r *premintRound) result() (got, missed, combos int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.got, r.missed, len(r.tried)
}

// premintPlan selects an attempt. Actual coverage/lifetime gain is measured
// atomically at commit, never inferred from a pre-request inventory count.
type premintPlan struct {
	acct       types.Account
	region     types.Region
	expected   string // 历史恢复预测；空值是受限的冷池引导，不是已证实恢复路线。
	prediction *types.InventoryPrediction
	donor      bool
	// release 放掉这个大区的裸铸标记，铸完**必须**调（见 claimRegion）。
	release func()
}

func (s *Scheduler) premintLoop(ctx context.Context, r *premintRound) {
	for ctx.Err() == nil && r.more() {
		plan, ok := s.premintPick(r)
		if !ok {
			// 两种情形都该让这个 worker 退休，而不是接着挑：组合挑完了（pick 里已经
			// 置了 halt），或者连试几个号都在 RenewGap 的节流里。后者**不能原地重试**
			// ——节流是 RenewGap 量级（几十秒），重试只会空转；别的 worker 还在跑，
			// 全退休了 wg.Wait 自然收尾，一分钟后的下一轮巡检再来。
			return
		}
		unlock := s.st.LockAccount(plan.acct.ID)
		// A queued plan is not permission to use disabled accounts/exits or to
		// remint a gateway another worker has already restored while we waited.
		acct, enabled := s.account(plan.acct.ID)
		if !enabled || !s.RegionEnabled(plan.region) || ctx.Err() != nil || s.maintenanceOff() {
			unlock()
			plan.release()
			continue
		}
		if acct.Creds.AccountID != plan.acct.Creds.AccountID {
			plan.prediction = nil
		}
		plan.acct = acct
		if plan.expected != "" {
			if _, ready := s.freshPair(plan.expected, types.DeliverFloor); ready {
				unlock()
				plan.release()
				continue
			}
		}
		if plan.donor {
			if _, needed := s.donorNeeds()[plan.region.Key]; !needed {
				unlock()
				plan.release()
				continue
			}
		}
		var seed types.Pair
		predictionCtx := withInventoryPrediction(ctx, plan.acct.ID, plan.region.Exit, plan.prediction)
		attemptCtx, selected, sid, releaseRoute, err := s.bindRoute(predictionCtx, plan.acct, plan.region.Exit, seed,
			plan.expected, types.PurposeInventory, plan.donor)
		plan.acct = selected
		if _, reserved := attemptCtx.Value(routeWorkKey{}).(routeWork); reserved {
			r.mu.Lock()
			r.attempted++
			r.mu.Unlock()
		}
		var got minted
		// want 留空避免把有收益的副落点当普通错误；真正的目标留在
		// routeWork.Expected 中，由共享路线证据单独记录是否命中。
		if err == nil {
			got, err = s.mintOnceFor(attemptCtx, plan.acct, plan.region.Exit, sid, seed, "", "", types.PurposeInventory)
		}
		if releaseRoute != nil {
			releaseRoute()
		}
		unlock()
		plan.release()
		if !s.premintNote(r, plan, got, err, seed.Cookie != "") {
			return
		}
		if !waitGap(ctx, s.opt.MintGap) {
			return
		}
	}
}

// premintPick 挑下一个 (账号 × 大区) 组合并就地占住它。
//
// 整段在 r.mu 里：挑选要读 tried/aimed，占住要写它们，分成两步就会让两个 worker
// 挑中同一个组合——而「一个组合一轮最多试一次」正是这个循环能靠穷举收尾的前提。
//
// 锁序恒为 r.mu → s.mu，反向不存在：r.mu 只有本文件这几个方法碰，而 minterFor 自己
// 那把 s.mu 在候选循环**之前**就放掉了（skip 不在它里面跑）。
func (s *Scheduler) premintPick(r *premintRound) (premintPlan, bool) {
	live, landed, seedless := s.liveGateways(), s.lastLandings(), s.seedlessRegions()
	recovery, recoverable := s.inventoryRoutes()
	if r.selection.gateway != "" {
		recovery = map[string]string{}
		for _, route := range s.inventoryEvidence() {
			if route.gateway == r.selection.gateway {
				recovery[comboKey(route.account, route.region)] = route.gateway
			}
		}
	}
	allowBootstrap := r.allowBootstrap && (recoverable < s.PairTarget() || r.selection.bootstrap)
	donorNeeds := map[string]bool{}
	if r.selection.donor != "" {
		donorNeeds[r.selection.donor] = true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.halt || r.productive >= r.want || r.missed >= r.want {
		return premintPlan{}, false
	}
	baseSkip := func(id int64, key string) bool {
		combo := comboKey(id, key)
		if r.tried[combo] {
			return true
		}
		if r.selection.donor != "" {
			return key != r.selection.donor
		}
		gw := recovery[combo]
		if gw == "" {
			gw = landed[combo]
		}
		if live[gw] || r.aimed[gw] {
			return true
		}
		return false
	}
	pol := &premintPolicy{
		route: s.routeFilter(recovery, donorNeeds),
		skip: func(id int64, key string) bool {
			return baseSkip(id, key) || (r.selection.donor == "" && recovery[comboKey(id, key)] == "")
		},
		// 丢了种子票的大区优先补：它一掉，那个大区的所有账号都回落到住宅出口，
		// 而**一张**票就能把整个大区的便宜路径恢复（种子不绑账号，见 seedPairFor）。
		// 所以它的收益高于普通的「去摸一个新落点」—— 后者只多一个槽位。
		prefer: func(key string) bool { return seedless[key] },
	}
	mode := s.cfg().InventoryRoutingMode()
	if r.routing != nil && mode == config.InventoryRoutingAssist {
		known := s.st.Gateways()
		pol.rank = func(account types.Account, region types.Region) int {
			return inventoryPredictionRank(r.routing.forecast(account, region), known, live, r.aimed)
		}
	}
	for try := 0; try < premintPickTries; try++ {
		// First restrict selection to a missing, historically reachable gateway.
		// Lifetime country fairness only ranks within that recovery cohort.
		pol.skip = func(id int64, key string) bool {
			return baseSkip(id, key) || (r.selection.donor == "" && recovery[comboKey(id, key)] == "")
		}
		a, reg, ok := s.minterFor(types.Exit{}, 0, s.opt.RenewGap, pol)
		if !ok && allowBootstrap && r.unproven < inventoryBootstrapBudget {
			pol.skip = baseSkip
			a, reg, ok = s.minterFor(types.Exit{}, 0, s.opt.RenewGap, pol)
		}
		if !ok {
			s.opt.Log.Info("没有可挑的 (账号 × 大区) 组合了，本轮预铸收尾",
				"got", r.got, "want", r.want, "tried", len(r.tried))
			r.halt = true
			return premintPlan{}, false
		}
		combo := comboKey(a.ID, reg.Key)
		r.tried[combo] = true
		gw := recovery[combo]
		if gw == "" {
			gw = landed[combo]
		}
		if gw != "" {
			r.aimed[gw] = true
		}
		// **先占大区、再占账号**，顺序不能反：占不住时账号那一占就白花了一个
		// RenewGap（几十秒），方向是少铸不是多铸。
		//
		// claimRegion 的粒度是 (账号 × 大区)，所以这里占不住只意味着**同一个号**在
		// 同一个大区已经有一发在飞 —— 和 r.tried 比起来是条更紧的线（tried 只管本轮，
		// 交付路径的现铸也会占）。按大区拦的那一版在这儿是个真坑：所有号的首选大区都是
		// 台账里铸得最少的那一个，几个 worker 开局必然撞在一起，实测只铸出一发就收尾。
		release, free := s.claimRegion(a.ID, reg.Key)
		if !free {
			continue // 这个号正在这个大区打；组合已标试过，挑下一个
		}
		if s.claimAccount(a.ID, s.opt.RenewGap) {
			expected := recovery[combo]
			if r.selection.donor != "" {
				expected = ""
			}
			if expected == "" && r.selection.donor == "" {
				r.unproven++
			}
			plan := premintPlan{acct: a, region: reg, expected: expected, release: release, donor: r.selection.donor != ""}
			if r.routing != nil && mode != config.InventoryRoutingOff {
				prediction := r.routing.forecast(a, reg)
				prediction.Mode, prediction.DecidedAt = mode, s.now()
				plan.prediction = &prediction
			}
			return plan, true
		}
		release()
	}
	return premintPlan{}, false
}

// premintNote 把一发的结果记进本轮进度，返回「这个 worker 还该接着挑吗」。
func (s *Scheduler) premintNote(r *premintRound, plan premintPlan,
	got minted, err error, seeded bool,
) bool {
	switch {
	case err == nil:
		s.opt.Log.Info("裸铸拿到落点", "account", plan.acct.ID, "label", plan.acct.Label,
			"region", plan.region.Key, "gateway", got.pair.Gateway, "seeded", seeded)
		// Use the atomic commit result, not a stale pre-request snapshot.
		// Extending B is productive even when the intended A remains missing.
		r.mu.Lock()
		if got.gain.Productive() {
			r.productive++
			if got.gain.CoverageAdded {
				r.got++
			}
			r.extendedMS += got.gain.ExtendedMS
			r.donorMS += got.gain.DonorExtendedMS
			r.mu.Unlock()
			return true
		}
		r.missed++
		missed, budget := r.missed, r.want
		r.mu.Unlock()
		s.opt.Log.Info("本次铸票未增加可交付库存或寿命",
			"account", plan.acct.ID, "region", plan.region.Key, "gateway", got.pair.Gateway,
			"missed", missed, "budget", budget)
		return true
	case errors.Is(err, types.ErrNoExit):
		// 无可用出口是全池故障。账号/区域限流不是，不能连带停止其它组合。
		s.opt.Log.Warn("预铸无可用出口，本轮停",
			"account", plan.acct.ID, "region", plan.region.Key, "reason", err)
		r.stop()
		return false
	case errors.Is(err, errRouteDeferred):
		return true
	default:
		// ErrRateLimited 也到这里：可能已打到上游并收到429，因此保留失手预算，
		// 但不设置全局 halt。tried/账号退避确保下一次选择别的可用组合。
		// 失败也吃失手预算。不吃的话「本轮总发数封顶 2n」就是空话：失败既不加 got
		// 也不加 missed ⇒ 循环只被候选集（账号数 × 9）兜住，一次系统性故障
		// （比如模型名下架、全员 400）就是一轮上百发真实上游请求。
		// 判据按结果不按原因：这一发没补上格子，就是一次失手。
		r.mu.Lock()
		r.missed++
		missed, budget := r.missed, r.want
		r.mu.Unlock()
		s.opt.Log.Info("裸铸失败", "account", plan.acct.ID, "label", plan.acct.Label,
			"region", plan.region.Key, "reason", err, "missed", missed, "budget", budget)
		return true
	}
}

// waitGap 等一个铸票间隔，ctx 死了回 false。
func waitGap(ctx context.Context, gap time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(gap):
		return true
	}
}

// premintSID 的新铸一律走实际动态出口。静态出口只续已有票，不占用新铸/探索任务。
func (s *Scheduler) premintSID(regionKey string) (string, types.Pair, error) {
	region, ok := s.cfg().RegionByKey(regionKey)
	if !ok || !region.Enabled {
		return "", types.Pair{}, types.ErrNoExit
	}
	sid, err := s.mint.NewSIDFor(region.Exit(), true)
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

// seedlessRegions 列出手上没有活种子票的大区。**一轮挑选只算一次**：bestRegion 会对
// 每个 (账号 × 大区) 都问一次 prefer，而 seedPairFor 一次要扫全部网关的 pair ⇒ 写成
// 闭包里现算就是「账号数 × 9 遍全表扫描」（113 个号时十万次 store.Pair），
// 而且整段在 r.mu 里。提出来是 9 遍。
func (s *Scheduler) seedlessRegions() map[string]bool {
	regions := s.cfg().CandidateRegions(types.Exit{})
	out := make(map[string]bool, len(regions))
	for _, r := range regions {
		if _, ok := s.seedPairFor(r.Key); !ok {
			out[r.Key] = true
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

// PairTarget reports known coverage, including gateways awaiting a new route.
// It is not a count of runnable attempts. Discovery and bootstrap have their
// own bounds, and idle protection is independent of this display value.
func (s *Scheduler) PairTarget() int {
	if s.maintenanceOff() {
		return 0
	}
	var known int
	if narrow, ok := s.st.(interface{ GatewayCount() int }); ok {
		known = narrow.GatewayCount()
	} else {
		known = len(s.st.Gateways())
	}
	if known > 0 {
		return known
	}
	// Numerical reserve settings only bound the initial empty-pool bootstrap.
	// Once a landing exists, ordinary maintenance addresses known stock only.
	cfg := s.cfg()
	busiest := s.busiestDeliveries(s.now().Add(-types.PairLifetime))
	return min(max(busiest+cfg.SpareTarget, cfg.MinPairs), inventoryBootstrapBudget)
}

func (s *Scheduler) maintenanceOff() bool { return s.cfg().SpareTarget > types.GatewayCeiling }

// 「挑谁去铸」只有 minterFor 一份了。
//
// 早先保温这边另有一个 warmMinter（按「铸过几张」轮），理由是交付路径那边要专挑
// 「在这个网关上已经废掉的号」、两套取向相反。2026-10-01 那条规则删掉之后
// （见 minterFor 的注释），两边要的是同一件事：本小时打得最少、没走过的区域优先。
