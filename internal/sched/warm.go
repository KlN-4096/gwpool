package sched

import (
	"context"
	"errors"
	"strconv"
	"sync"
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
	// 这里原先有一道 premintWasted：「全员在全部已知网关上都还在消费冷却里 ⇒ 预铸出来的票
	// 交给谁都是降级的」。消费冷却那条闸 2026-10-03 删了（见 sched.burning），这个状态
	// 不再存在 —— 剩下的那条闸是 183 秒量级、自己会过去，拿它判「本轮别铸」等于让池子
	// 在最该补货的时候停工。那道闸 24 小时内触发 279 次，是铸票慢的三个原因之一。
	//
	// 刹车没有少：目标深度（PairTarget）仍然封顶，组合挑完 premintPick 就收尾。
	//
	// 目标活 pair 数 = 最忙那个号用过的 pair 数 + SpareTarget。够了就一发不打：
	// 铸票不是免费的，每一发都在烧执行者的窗口、吃它那个区域的速率预算。
	live, target := s.livePairs(), s.PairTarget()
	if live >= target {
		return
	}
	s.opt.Log.Info("预铸：补到目标深度", "live", live, "target", target)
	s.premintBare(ctx, target-live)
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
	workers := s.premintWorkers(n)
	r := &premintRound{want: n, tried: map[string]bool{}, aimed: map[string]bool{}}
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
		"combos_tried", combos, "workers", workers)
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
	mu     sync.Mutex
	want   int             // 缺口，同时也是失手预算
	got    int             // 补上的格子数
	missed int             // 落回了已有活票的网关的发数
	halt   bool            // 整池同病：组合挑完了、要不到出口、撞上闸
	tried  map[string]bool // 本轮试过的 (账号 × 大区)
	aimed  map[string]bool // 本轮已经有人冲着去的网关，别两个组合撞同一个
}

func (r *premintRound) more() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.halt && r.got < r.want && r.missed < r.want
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

// premintPlan 是挑出来的一发：谁、经哪个大区、以及**出手前**哪些网关有活票。
//
// live 必须是出手前的快照：铸完再数的话自己刚落下的那张票也在里面，
// 「这一发有没有补上一格」就永远答「没有」。并发之后更要紧——别的 worker 的新落点
// 会把读数一起抬高，原先那个「全局活票数比铸票前多了没有」的判据会直接失效。
type premintPlan struct {
	acct   types.Account
	region types.Region
	live   map[string]bool
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
		sid, seed, err := s.premintSID(plan.region.Key)
		if err != nil {
			plan.release()
			// 出口要不到是整池同病，别的 worker 接着挑只是把同一句话重复 N 遍。
			s.opt.Log.Warn("要不到出口，本轮不预铸", "region", plan.region.Key, "err", err)
			r.stop()
			return
		}
		unlock := s.st.LockAccount(plan.acct.ID)
		// want 留空：裸铸就是去看它能落到哪，谈不上「落点漂移」。带种子也一样——
		// 种子只钉大区，具体网关是这个号自己的，出手前并不知道是哪个。
		got, err := s.mintOnce(ctx, plan.acct, plan.region.Exit, sid, seed, "", "")
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.halt {
		return premintPlan{}, false
	}
	pol := &premintPolicy{
		skip: func(id int64, key string) bool {
			combo := comboKey(id, key)
			if r.tried[combo] {
				return true
			}
			gw := landed[combo]
			if gw == "" {
				return false // 没铸过 ⇒ 去摸新落点
			}
			return live[gw] || r.aimed[gw]
		},
		// 丢了种子票的大区优先补：它一掉，那个大区的所有账号都回落到住宅出口，
		// 而**一张**票就能把整个大区的便宜路径恢复（种子不绑账号，见 seedPairFor）。
		// 所以它的收益高于普通的「去摸一个新落点」—— 后者只多一个槽位。
		prefer: func(key string) bool { return seedless[key] },
	}
	for try := 0; try < premintPickTries; try++ {
		// 本小时打得最少的号优先、丢了种子的大区优先、它终身没走过的区域优先；
		// 节流同一个号两次铸票至少隔 RenewGap。
		a, reg, ok := s.minterFor(types.Exit{}, 0, s.opt.RenewGap, pol)
		if !ok {
			s.opt.Log.Info("没有可挑的 (账号 × 大区) 组合了，本轮预铸收尾",
				"got", r.got, "want", r.want, "tried", len(r.tried))
			r.halt = true
			return premintPlan{}, false
		}
		combo := comboKey(a.ID, reg.Key)
		r.tried[combo] = true
		if gw := landed[combo]; gw != "" {
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
			return premintPlan{acct: a, region: reg, live: live, release: release}, true
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
		// 只有「落到一个出手前没有活票的网关」才算补上了一格。落回已有活票的网关
		// 说明预测错了（区域划分随 cohort 变，SPEC 第 4 节）——台账已经按实际落点
		// 更新过了，这个组合也标成试过了，所以不会原地打转，接着挑下一个。
		r.mu.Lock()
		if !plan.live[got.pair.Gateway] {
			r.got++
			r.mu.Unlock()
			return true
		}
		r.missed++
		missed, budget := r.missed, r.want
		r.mu.Unlock()
		s.opt.Log.Info("落回了已有活票的网关，算一次失手，换下一个组合",
			"account", plan.acct.ID, "region", plan.region.Key, "gateway", got.pair.Gateway,
			"missed", missed, "budget", budget)
		return true
	case errors.Is(err, types.ErrRateLimited), errors.Is(err, types.ErrNoExit):
		// 闸门和出口故障都不是这个号的错，也不是重试能解决的，而且**整池同病**：
		// 别的 worker 接着挑只是把同一句话重复 N 遍，本轮直接停。
		s.opt.Log.Warn("预铸撞上闸门，本轮停",
			"account", plan.acct.ID, "region", plan.region.Key, "reason", err)
		r.stop()
		return false
	default:
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

// seedlessRegions 列出手上没有活种子票的大区。**一轮挑选只算一次**：bestRegion 会对
// 每个 (账号 × 大区) 都问一次 prefer，而 seedPairFor 一次要扫全部网关的 pair ⇒ 写成
// 闭包里现算就是「账号数 × 9 遍全表扫描」（113 个号时十万次 store.Pair），
// 而且整段在 r.mu 里。提出来是 9 遍。
func (s *Scheduler) seedlessRegions() map[string]bool {
	out := make(map[string]bool, len(types.Regions))
	for _, r := range types.Regions {
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
	// 下界：不管有没有人在用，手上至少备这么多张（见 config.Config.MinPairs）。
	// 在 spare 那条「停止预铸」的早返回**之后**：那是运营方显式关掉预铸，下界不该把它打开。
	return min(max(busiest+spare, s.cfg().MinPairs), types.GatewayCeiling)
}

// 「挑谁去铸」只有 minterFor 一份了。
//
// 早先保温这边另有一个 warmMinter（按「铸过几张」轮），理由是交付路径那边要专挑
// 「在这个网关上已经废掉的号」、两套取向相反。2026-10-01 那条规则删掉之后
// （见 minterFor 的注释），两边要的是同一件事：本小时打得最少、没走过的区域优先。
