// Package sched 决定「下一张 pair 交给谁、从哪个网关拿」。
//
// 为什么调度只看「歇得最久」：降智的作用单位是 (账号 × 网关)，一个账号在某个网关上
// **首次接触**才有约 183 s 的满血窗口，之后静默失效（仍回 200 但答错）。
// 所以可交付的永远是「这个消费账号最久没碰过的那个网关」，和网关本身健不健康无关。
// 详见 SPEC.md 第 6、8 节。
package sched

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"
	"time"

	"gwpool/internal/types"
)

// Store 是 sched 需要的存储能力。按接口编程是为了能用 fake 跑测试：
// 测试绝不打真实上游（SPEC 红线 8）。
type Store interface {
	// Gateways 池子里的网关，key = 名字。必须带 ReachableExits：定向铸票全靠它。
	Gateways() map[string]types.Gateway

	// LongestRestedSlot 在 candidates 里挑这个账号歇得最久、且不在退避期的槽位。
	// 没有记录的 (账号 × 网关) 优先级最高——从没碰过才有满血窗口。
	LongestRestedSlot(accountID int64, candidates []string) (types.Slot, bool)

	// Slots 是整张槽位表，key = store.SlotKey。预铸算「最忙的号碰过几个网关」要它。
	Slots() map[string]types.Slot

	// Slot 取 (账号 × 网关) 的账本记录。选谁去续期要直接看它，不靠启发式代理。
	Slot(accountID int64, gateway string) (types.Slot, bool)

	// UpdateGateway 在存储的写锁内读-改-写一个网关（续期失败预算、停摆时间）。
	UpdateGateway(name string, f func(*types.Gateway)) error

	// UpdateSlot 在存储的写锁内读-改-写一个槽位。
	// 必须是原子的：交付（写 LastTouch）、判定（写 LastVerdict）、续期循环会并发改
	// 同一个槽位，各读各写会丢更新，丢掉 LastTouch 就等于把烧过的槽位当新鲜的发出去。
	UpdateSlot(accountID int64, gateway string, f func(*types.Slot)) error

	// DeletePair 丢掉某网关的 pair（连续续不上的死票）。网关不存在时不算错。
	DeletePair(gateway string) error

	// Pair 取某网关当前的 pair；新鲜度由调用方按 ExpiresAt 自己判断。
	Pair(gateway string) (types.Pair, bool)
	PutPair(p types.Pair) error

	// LockAccount 是**跨包唯一**的单账号互斥，返回解锁函数。
	// 必须由存储层持有：首扫（scan）和交付（sched）都要打同一个账号，
	// 两个包各自维护一张锁表就等于没锁——2026-09-29 实测 5~6 并发 20~30 分钟废掉一个通道。
	LockAccount(accountID int64) func()

	Accounts() []types.Account
	// AddAccount 用来给匿名调用方补占位账号（见 anonAccount）。
	AddAccount(a types.Account) (types.Account, error)

	// DisableAccount 因上游否决而自动停用，reason 是上游那段话。
	// 保温/续期是循环，不停用被否决的号就会一直按这个节奏白打。
	DisableAccount(id int64, reason string) error
	AppendSample(sm types.Sample) error
}

// Minter 是 sched 要的铸票能力：types.Minter 再加一条「要一个粘性 sid」。
//
// sid 必须**向铸票方要**，不能自己随便造一个：它内部把 sid 绑死到一条出口上，
// 铸票和随后的 state-echo 复用同一个 sid 才会走同一个出口 IP。自己造的字符串
// 要到第一次调用时才被动绑定，粘不粘得住就看运气了。
type Minter interface {
	types.Minter
	NewSID(sticky bool) (string, error)
}

// ErrNoFullSlot：所有候选槽位都没验过满血。宁可 503 也不放降智出去（SPEC 第 8 节）。
var ErrNoFullSlot = errors.New("没有验过满血的槽位可交付")

// errDegraded 标记「state-echo 判降级」。它和硬失败要分开记退避，见 degradedHold。
var errDegraded = errors.New("state-echo 判降级")

const (
	// 硬失败（非 200、网络错、落点漂移）指数退避：快重试会让供给更快耗尽
	// （2026-10-01 实测：两小时上千次铸票把能碰到的组合全烧空），SPEC 红线 7。
	backoffBase = time.Minute
	backoffMax  = 20 * time.Minute

	// 判降级只歇一个满血窗口，**不进指数**：state-echo 有假阴（满血号偶尔也下发新
	// state），翻倍会让一次误判把好槽位停到 20 分钟，而供给本来就只有几个槽位。
	degradedHold = types.FullWindow

	// minDeliverWindow 是交付的最低剩余窗口。窗口从**首次接触**起算，铸票 + echo
	// 两发往返要 20~90 s，剩不下多少就别发了——发一张马上过期的等于发降智。
	minDeliverWindow = 30 * time.Second

	// defaultMaxTriesPerDeliver 是一次交付最多试几个网关。
	//
	// 3 是在「别空手而归」和「别把池子摸干净」之间取的：每试一个都要真打一发上游
	// 验满血，试过的那个槽位不管结果都烧掉了。不设上限时一个消费者会把整池供给
	// 一轮摸空（实测连试 5 个、耗 90 s，最后还是 503），后面的人就没票了。
	defaultMaxTriesPerDeliver = 3
)

// Options 是调度器的可调参数。零值字段由 New 填默认。
type Options struct {
	Mode types.Mode

	// WarmInterval 保温巡检间隔（仅 ModePool）。
	WarmInterval time.Duration
	// IdleAfter：距上次交付超过它就停止保温，让 pair 自然过期、成本归零（SPEC 第 7 节）。
	IdleAfter time.Duration
	// MintGap 是两发铸票之间的间隔，速率纪律，别把供给打空。负数 = 不等（测试用）。
	MintGap time.Duration
	// RenewGap 是**同一个账号**两次续期之间的最小间隔。
	// 供给不是瓶颈（十几个网关一小时才十几发），打得密才是封号特征。
	RenewGap time.Duration

	// MaxTriesPerDeliver 是一次交付最多试几个网关，0 时按 defaultMaxTriesPerDeliver。
	// 每试一个都要真打一发上游验满血，而且不管验没验过那个槽位都烧掉了。
	MaxTriesPerDeliver int

	// PublicWhenSurplus：票充足时自动对没上传凭据的人开放，供给回落就自动关上。
	// MinFreeGateways 是开放门槛，见 FreeGateways 和 types.GatewaysForContinuous。
	PublicWhenSurplus bool
	MinFreeGateways   int

	Now func() time.Time
	Log *slog.Logger
}

// Scheduler 是交付与保温的唯一入口。
type Scheduler struct {
	st   Store
	mint Minter
	opt  Options

	mu      sync.Mutex
	backoff map[string]time.Duration // 槽位 → 当前指数退避时长；成功归零
	// lastMintedAt 记每个账号最近一次被用作铸票者，用来把续期摊到各个号上。
	// mintCount 记它总共铸过几张，预铸按这个挑最闲的那个号。
	// 都不落盘：重启后重新摊平没有坏处，而落盘就多一份要维护的状态。
	lastMintedAt map[int64]time.Time
	mintCount    map[int64]int
	lastDeliver  time.Time
}

func New(st Store, m Minter, opt Options) *Scheduler {
	if opt.Mode == "" {
		opt.Mode = types.ModeSingle
	}
	if opt.WarmInterval <= 0 {
		opt.WarmInterval = time.Minute
	}
	if opt.IdleAfter <= 0 {
		opt.IdleAfter = 15 * time.Minute
	}
	if opt.MintGap < 0 {
		opt.MintGap = 0
	} else if opt.MintGap == 0 {
		opt.MintGap = 3 * time.Second
	}
	if opt.MaxTriesPerDeliver <= 0 {
		opt.MaxTriesPerDeliver = defaultMaxTriesPerDeliver
	}
	if opt.RenewGap < 0 {
		opt.RenewGap = 0
	} else if opt.RenewGap == 0 {
		opt.RenewGap = 30 * time.Second
	}
	if opt.Now == nil {
		opt.Now = func() time.Time { return time.Now().UTC() }
	}
	if opt.Log == nil {
		opt.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Scheduler{st: st, mint: m, opt: opt,
		backoff: map[string]time.Duration{}, lastMintedAt: map[int64]time.Time{},
		mintCount: map[int64]int{}}
}

func (s *Scheduler) Mode() types.Mode { return s.opt.Mode }

func (s *Scheduler) now() time.Time { return s.opt.Now() }

// Deliver 为 accountID 这个账号交付一张验过满血的 pair。gatewayHint 非空时只考虑那个网关。
//
// **账号必须是参数，不能由调度器自己挑**：槽位是 (账号 × 网关) 二元组，
// 拿 A 算出来的槽位发给 B，「取歇得最久的」就整个失效了——B 可能刚在那儿烧过。
//
// 流程照 SPEC 第 8 节：选歇得最久的槽位 → 确保有剩余 ≥ DeliverFloor 的 pair →
// state-echo 验一次 → 满血则交付、降级则退避换下一个 → 全降级返回 ErrNoFullSlot。
//
// **不做任何跨消费者的互斥**：同一张 pair 同时发给几个账号是上游允许的，
// 2026-10-01 实测见 types.Pair 的注释。
//
// force = 「他手上那张不行了，换一个」。换不换由客户端决定：池子猜客户端什么时候
// 该换本来就是在猜，只有客户端知道自己那一发对不对劲。force 时**必须**换一个网关，
// 没得换就 ErrNoFullSlot——把原来那张再发一遍等于没换，客户端会死循环。
func (s *Scheduler) Deliver(ctx context.Context, accountID int64, gatewayHint string, force bool) (types.Delivery, error) {
	consumer, ok := s.account(accountID)
	if !ok {
		return types.Delivery{}, fmt.Errorf("账号 %d 用不了（不存在、已停用、或没有凭据）", accountID)
	}

	// 先记需求：即使这次 503 了，保温循环也该知道「有人在要」。
	s.markDemand()

	gws := s.st.Gateways()
	cands := make([]string, 0, len(gws))
	for name := range gws {
		if gatewayHint != "" && name != gatewayHint {
			continue
		}
		cands = append(cands, name)
	}
	if len(cands) == 0 {
		return types.Delivery{}, fmt.Errorf("池子里没有可用网关（hint=%q）", gatewayHint)
	}

	if force {
		kept := make([]string, 0, len(cands))
		for _, name := range cands {
			if s.holding(consumer.ID, name) {
				continue
			}
			kept = append(kept, name)
		}
		if len(kept) == 0 {
			return types.Delivery{}, ErrNoFullSlot
		}
		cands = kept
	}

	// 一次请求最多试这么多个网关。**每试一个都要真打一发上游去验满血**，试到的
	// 那个槽位不管验没验过都烧掉了——不设上限的话，一个倒霉的消费者会把整池的供给
	// 一轮摸干净，后面的人就无票可用了（2026-10-01 实测：一次请求连试 5 个、耗 90 秒，
	// 然后告诉调用方 503）。按需取票：试不出来就早点认 503，让供给留给下一个人。
	tries := s.opt.MaxTriesPerDeliver
	for len(cands) > 0 && tries > 0 {
		sl, ok := s.st.LongestRestedSlot(consumer.ID, cands)
		if !ok {
			break // 剩下的全在退避期
		}
		tries--
		d, err := s.tryGateway(ctx, consumer, gws[sl.Gateway])
		if err == nil {
			s.clearBackoff(consumer.ID, sl.Gateway)
			s.noteDeliver(consumer.ID, sl.Gateway)
			return d, nil
		}
		// 客户端自己断了（超时、取消）不算槽位的错：否则反复重试的客户端
		// 会把自己所有槽位逐个打进 20 分钟退避。
		if ctx.Err() != nil {
			s.opt.Log.Info("调用方中断", "account", consumer.ID, "gateway", sl.Gateway)
			break
		}
		s.opt.Log.Info("槽位不可交付", "account", consumer.ID, "gateway", sl.Gateway, "reason", err)
		// 速率闸不是槽位的错也不是账号的错：给它记退避等于拿速率纪律惩罚供给。
		// 直接认 503，下一个人来的时候配额可能已经腾出来了。
		if errors.Is(err, types.ErrRateLimited) {
			break
		}
		if errors.Is(err, errDegraded) {
			s.holdSlot(consumer.ID, sl.Gateway, degradedHold)
		} else {
			s.bumpBackoff(consumer.ID, sl.Gateway)
		}
		cands = drop(cands, sl.Gateway)
	}
	if tries == 0 && len(cands) > 0 {
		s.opt.Log.Info("试满上限仍没拿到满血票，剩下的留给别人",
			"account", consumer.ID, "tried", s.opt.MaxTriesPerDeliver, "left", len(cands))
	}
	return types.Delivery{}, ErrNoFullSlot
}

// FreeGateways 现算空闲网关数：有活 pair（剩余 ≥ DeliverFloor）的网关数。
//
// 「空闲」不再扣掉正在被人用的那些：一张 pair 可以同时给几个号用（types.Pair 注释里
// 的 2026-10-01 实测），所以「有人在用」不占名额。
//
// 每次请求现算、不缓存：供给是秒级变化的，缓存一个「充足」状态就等于拿着过期的
// 结论放匿名请求进来，而他们正是最该被挡在门外的那一拨。
func (s *Scheduler) FreeGateways() int {
	now := s.now()
	n := 0
	for name := range s.st.Gateways() {
		if p, ok := s.st.Pair(name); ok && p.ExpiresAt.Sub(now) >= types.DeliverFloor {
			n++
		}
	}
	return n
}

// PublicState 是「此刻对外开不开放」的全部读数。公开页要显示它，这是对外承诺。
type PublicState struct {
	Enabled   bool // 开关本身开没开
	Open      bool // 此刻放不放匿名进来
	Free      int
	Threshold int
}

// Public 现算一次开放状态。
func (s *Scheduler) Public() PublicState {
	st := PublicState{Enabled: s.opt.PublicWhenSurplus, Free: s.FreeGateways(), Threshold: s.opt.MinFreeGateways}
	st.Open = st.Enabled && st.Free >= st.Threshold
	return st
}

// ErrPublicClosed：此刻不对匿名开放（开关没开，或空闲数不够门槛）。
var ErrPublicClosed = errors.New("池子此刻不对外开放")

// DeliverAnonymous 给没上传凭据的人交付。
//
// 调度单位仍是 (账号 × 网关)：匿名的人没有池内账号，但他有自己的上游 Codex 账号，
// upstreamID 就是那个标识，槽位按 (占位账号 × 网关) 照常记。
// 他可以乱填，但乱填只会害他自己（拿到他其实烧过的网关），不影响别人，所以不防。
//
// 和上传者那条路的三点不同，都是「没有他的凭据」直接推出来的：
//  1. **不做 state-echo**——判据要用他自己的凭据打一发，池子没有 ⇒ VerifiedFull=false。
//     满血窗口也从他自己第一次用起算：池子一次都没替他碰过这个网关。
//  2. **不现铸**——铸票要烧某个上传者的窗口，不为匿名花这个钱，只发现成的。
//  3. **严格低优先级**——空闲数跌破门槛就拒，哪怕还剩一张。门槛是给上传者留的余量，
//     也是匿名这条路唯一的让路机制（pair 本身不互斥，见 types.Pair 的注释）。
func (s *Scheduler) DeliverAnonymous(ctx context.Context, upstreamID, gatewayHint string, force bool) (types.Delivery, error) {
	ps := s.Public()
	if !ps.Open {
		return types.Delivery{}, fmt.Errorf("%w（空闲 %d，门槛 %d）", ErrPublicClosed, ps.Free, ps.Threshold)
	}
	a, err := s.anonAccount(upstreamID)
	if err != nil {
		return types.Delivery{}, err
	}
	s.markDemand()

	now := s.now()
	cands := make([]string, 0, ps.Free)
	for name := range s.st.Gateways() {
		if gatewayHint != "" && name != gatewayHint {
			continue
		}
		p, ok := s.st.Pair(name)
		if !ok || p.ExpiresAt.Sub(now) < types.DeliverFloor {
			continue
		}
		// force：他手上那张不行了，排掉他正在用的。
		if force && s.holding(a.ID, name) {
			continue
		}
		cands = append(cands, name)
	}
	for len(cands) > 0 {
		sl, ok := s.st.LongestRestedSlot(a.ID, cands)
		if !ok {
			break
		}
		p, ok := s.st.Pair(sl.Gateway)
		// 现算一遍：从选候选到这儿之间，那张 pair 可能已经到点了。
		if !ok || p.ExpiresAt.Sub(s.now()) < types.DeliverFloor {
			cands = drop(cands, sl.Gateway)
			continue
		}
		at := s.now()
		s.touch(sl.Gateway, a.ID, at)
		// 外部消费者的用量也要进账本，否则「池子该备多深」这个目标永远看不见他们：
		// 池子对外服务得越多，目标深度反而不涨。
		s.noteDeliver(a.ID, sl.Gateway)
		// 运营方靠这行判断门槛设得对不对。
		s.opt.Log.Info("匿名交付", "upstream", upstreamID, "account", a.ID,
			"gateway", sl.Gateway, "free_gateways", ps.Free, "threshold", ps.Threshold)
		return types.Delivery{
			Gateway:      sl.Gateway,
			Cookie:       p.Cookie,
			ExpiresAt:    at.Add(types.DeliverTTL),
			ValidForS:    int(types.DeliverTTL / time.Second),
			VerifiedFull: false,
		}, nil
	}
	return types.Delivery{}, ErrNoFullSlot
}

// anonAccount 给匿名调用方找/建占位账号。
//
// 标识**加前缀后**才去匹配：直接拿他报的上游 ID 去找，他只要填一个上传者的 ID，
// 就能把那个上传者的槽位标成「刚烧过」，白白糟蹋别人的调度。前缀一隔，
// 匿名最多只能糟蹋自己那条账本。占位账号没有 access_token，永远不会被选去铸票。
func (s *Scheduler) anonAccount(upstreamID string) (types.Account, error) {
	id := types.AnonPrefix + upstreamID
	for _, a := range s.st.Accounts() {
		if a.Creds.AccountID == id {
			return a, nil
		}
	}
	return s.st.AddAccount(types.Account{
		Label:     "匿名 " + upstreamID,
		Creds:     types.Creds{AccountID: id},
		Enabled:   true,
		CreatedAt: s.now(),
	})
}

// minted 是一次铸票的产物。
//
// firstAt 是**请求发出去的时刻**，不是响应回来的时刻：满血窗口从首次接触起算，
// 交付有效期必须按它算，否则铸票 + echo 的 20~90 s 往返会被白白算进窗口里。
// sid 要带出来：state-echo 必须复用铸票时的 sid，否则可能换出口 IP、换网关。
// obs 也带出来，好让随后那一发 echo 的样本记上同一个出口地理（同 sid ⇒ 同 IP）。
type minted struct {
	pair    types.Pair
	ticket  string
	sid     string
	obs     types.Observed
	firstAt time.Time
}

// tryGateway 在一个网关上走完「确保 pair → 取消费者自己的活票 → state-echo」。
//
// **绝不同时持有两把账号锁**：补 pair 可能要锁别人（铸票者），echo 要锁消费者，
// 而 minterFor 会交叉选人（A 的铸票者是 B、B 的是 A 是常态）⇒ 嵌套就是 ABBA 死锁，
// 而且没有 WriteTimeout 兜底，两个账号会永久挂死。所以分两段，各锁各的、用完就放。
func (s *Scheduler) tryGateway(ctx context.Context, consumer types.Account, gw types.Gateway) (types.Delivery, error) {
	exit, ok := pickExit(gw)
	if !ok {
		return types.Delivery{}, fmt.Errorf("网关 %s 没有已知可达出口，没法定向铸票", gw.Name)
	}

	// 第一段：确保池里有张够新鲜的 pair。
	pair, fresh := s.freshPair(gw.Name)
	var m minted
	if !fresh {
		minter := consumer
		if s.opt.Mode == types.ModePool {
			// 保温/续期那一发交给别人打：它会烧掉铸票者在这个网关上的窗口，
			// 烧消费者自己的就等于白跑一趟（这正是单账号模式不保温的原因）。
			// 交付路径不节流：有人在等着拿 pair。
			if sc, ok := s.minterFor(gw, consumer.ID, 0); ok {
				minter = sc
			}
		}
		sid, err := s.mint.NewSID(true)
		if err != nil {
			return types.Delivery{}, fmt.Errorf("要不到粘性出口: %w", err)
		}
		unlock := s.st.LockAccount(minter.ID)
		got, err := s.mintOnce(ctx, minter, exit, sid, pair, gw.Name)
		unlock()
		if err != nil {
			return types.Delivery{}, err
		}
		pair = got.pair
		if minter.ID == consumer.ID {
			// 单账号模式：建 pair 那一发就是消费者的首次接触，票直接拿来用。
			m = got
		}
	}

	// 续完必须**重新判一次剩余**：上游接受了一张还有效的 pair 时不会下发新 cookie
	// （SPEC 第 2 节「好 pair 不会触发重新下发」），这时「续期」其实没发生，
	// 手里还是那张快到点的。发出去等于让消费者中途被重新路由到随机网关。
	if left := pair.ExpiresAt.Sub(s.now()); left < types.DeliverFloor {
		return types.Delivery{}, fmt.Errorf("网关 %s 的 pair 只剩 %v，没续上，不发", gw.Name, left.Truncate(time.Second))
	}

	// 第二段：消费者自己打一发拿属于自己的活票，再 echo。只持消费者这一把锁。
	// state-echo 的票必须是**消费账号自己的**：判据问的是「这个账号在这个网关上
	// 还满不满血」，扫描账号的票答的是另一个问题。
	unlock := s.st.LockAccount(consumer.ID)
	defer unlock()
	if m.ticket == "" {
		sid, err := s.mint.NewSID(true)
		if err != nil {
			return types.Delivery{}, fmt.Errorf("要不到粘性出口: %w", err)
		}
		got, err := s.mintOnce(ctx, consumer, exit, sid, pair, gw.Name)
		if err != nil {
			return types.Delivery{}, err
		}
		m, pair = got, got.pair
	}
	if m.ticket == "" {
		return types.Delivery{}, fmt.Errorf("网关 %s 没回 turn-state，无从 echo", gw.Name)
	}

	// sid 必须和铸那张票时是同一个，否则可能换出口 IP、换网关，判据就配不上对。
	// pair 送当前这张：上游若刚续发过，新旧钉的是同一个网关，路由一样。
	//
	// 只在交付前验一次。state-echo 有假阴（满血号偶尔也下发新 state），
	// 当持续闸门会误杀好槽位（SPEC 红线 4）。
	v, verr := s.mint.VerifyFull(ctx, consumer.Creds, exit, m.sid, pair.Cookie, m.ticket)
	s.recordEcho(consumer.ID, exit, gw.Name, m.obs, v, s.now())
	if verr != nil {
		return types.Delivery{}, fmt.Errorf("state-echo 失败: %w", verr)
	}
	if v == nil {
		return types.Delivery{}, errors.New("state-echo 不下结论（HTTP 非 200）")
	}
	if !v.Full {
		s.setVerdict(consumer.ID, gw.Name, "degraded")
		return types.Delivery{}, fmt.Errorf("%w: %s", errDegraded, v.Note)
	}
	s.setVerdict(consumer.ID, gw.Name, "full")

	// 有效期从**首次接触**起算，不是从 echo 回来起算（窗口在第一发就开始烧了）。
	// valid_for_s 是**满血窗口**的剩余量，不是 cookie 的有效期（那是 65 分钟）。
	// 消费端在这段时间内可以反复复用同一个 pair。
	exp := m.firstAt.Add(types.DeliverTTL)
	left := exp.Sub(s.now())
	if left < minDeliverWindow {
		return types.Delivery{}, fmt.Errorf("网关 %s 的满血窗口只剩 %v，不发", gw.Name, left.Truncate(time.Second))
	}
	return types.Delivery{
		Gateway:      gw.Name,
		Cookie:       pair.Cookie,
		ExpiresAt:    exp,
		ValidForS:    int(left / time.Second),
		VerifiedFull: true,
	}, nil
}

// mintOnce 打一发真实请求，并把它的后果如实落账。
//
// cur 是送进去的 pair（可能为零值 = 裸打）。want 是期望落点，漂移就算这次失败：
// 上游把我们扔到别的网关上了，那张票属于别的槽位。
func (s *Scheduler) mintOnce(ctx context.Context, a types.Account, e types.Exit, sid string, cur types.Pair, want string) (minted, error) {
	// 地理只用来给上传者看账本，查不到不影响铸票（同 sid ⇒ 同出口 IP）。
	obs, gerr := s.mint.Geo(ctx, e, sid)
	if gerr != nil {
		s.opt.Log.Debug("查出口地理失败", "account", a.ID, "err", gerr)
	}

	start := s.now()
	s.mu.Lock()
	s.lastMintedAt[a.ID] = start
	s.mintCount[a.ID]++
	s.mu.Unlock()
	res, err := s.mint.Mint(ctx, a.Creds, e, sid, cur.Cookie)
	at := s.now()
	// 每一发都要落样本：上传者视图靠它证明「我们只拿你的凭据打了 ping」。
	s.sample(types.Sample{
		AccountID: a.ID, RegionKey: exitKey(e), Requested: e, Observed: obs,
		Gateway: res.Gateway, Status: res.Status, At: at,
	})

	// 触碰按**保守方向**记：超时和网络错在这里分不清「请求出去了但响应丢了」
	// 和「根本没出站」，而前者窗口已经烧掉了。漏记一次的代价是发出一张降智 pair，
	// 多记一次只是让这个槽位多歇会儿——所以读不出落点就按想去的地方记。
	landed := res.Gateway
	if landed == "" {
		landed = want
	}
	if landed != "" {
		s.touch(landed, a.ID, at)
	}

	if err != nil {
		return minted{}, fmt.Errorf("铸票失败: %w", err)
	}
	if res.Status != 200 {
		msg := fmt.Sprintf("铸票 HTTP %d", res.Status)
		if res.ErrBody != "" {
			msg += "：" + res.ErrBody
		}
		// 上游否决这个账号就当场停用：保温/续期是个循环，不停用它会一直按这个
		// 节奏白打下去（SPEC 第 7 节）。429/5xx 不走这条，那是退避管的事。
		if types.RejectedByUpstream(res.Status) {
			if derr := s.st.DisableAccount(a.ID, msg); derr != nil {
				s.opt.Log.Error("自动停用失败", "account", a.ID, "err", derr)
			}
			s.opt.Log.Warn("账号被上游否决，已自动停用", "account", a.ID, "reason", msg)
		}
		return minted{}, errors.New(msg)
	}
	if res.Gateway == "" {
		return minted{}, errors.New("铸票没拿到落点")
	}

	p := cur
	if res.NewPair != "" {
		// 上游下发了新 cookie，顶掉旧的。没下发则说明它接受了送进去的 pair，
		// 网关没变，旧记录照旧有效（SPEC 第 2 节，别当失败）。
		p = types.Pair{Gateway: res.Gateway, Cookie: res.NewPair, ExpiresAt: res.PairExpires, MintedBy: a.ID}
		if err := s.st.PutPair(p); err != nil {
			s.opt.Log.Warn("写 pair 失败", "gateway", p.Gateway, "err", err)
		}
	}
	if res.Gateway != want && want != "" {
		return minted{}, fmt.Errorf("落点漂移：想要 %s，实到 %s", want, res.Gateway)
	}
	return minted{pair: p, ticket: res.TurnState, sid: sid, obs: obs, firstAt: start}, nil
}

// account 取一个能用来打上游的账号。
// 没有 access_token 的占位行（只回报触碰的外部账号）不行：state-echo 要用它自己的凭据打一发。
func (s *Scheduler) account(id int64) (types.Account, bool) {
	for _, a := range s.st.Accounts() {
		if a.ID == id && a.Enabled && a.Creds.AccessToken != "" {
			return a, true
		}
	}
	return types.Account{}, false
}

// usable 列出所有能打上游的账号。
func (s *Scheduler) usable() []types.Account {
	out := make([]types.Account, 0, 4)
	for _, a := range s.st.Accounts() {
		if a.Enabled && a.Creds.AccessToken != "" {
			out = append(out, a)
		}
	}
	return out
}

// minterFor 挑谁去铸/续这个网关的 pair。
//
// 续期是带着 pair 打的，路由由 cookie 决定 ⇒ 谁来续都行。但它**是一次真实接触**，
// 会烧掉执行者在这个网关上的窗口。所以挑人不是随意，而是**专挑已经烧了的**：
// 烧一个已经烧了的 (账号 × 网关) 代价为零，而挑一个还处在「好」态的去续，
// 等于白白销毁一个满血窗口。
//
// 直接看账本，不用「上次铸票者 / 首次发现者」这种代理指标：
//
//  1. LastVerdict == "degraded" ——烧它免费
//  2. 没有记录                  ——未知，次选
//  3. LastVerdict == "full"     ——最后才动，烧它要赔一个满血窗口
//
// 同档内挑**最久没被用作铸票者**的那个：单账号并发硬性为 1，全压一个号上会排队；
// 更要紧的是封号面——集中打一个号正是 2026-09-29 把通道打废的那个模式。
//
// throttle > 0 时跳过「刚铸过票不到这么久」的账号（保温路径用；交付路径传 0，
// 那边有人在等，节流会直接变成服务不可用）。
func (s *Scheduler) minterFor(gw types.Gateway, exclude int64, throttle time.Duration) (types.Account, bool) {
	now := s.now()
	s.mu.Lock()
	lastUsed := make(map[int64]time.Time, len(s.lastMintedAt))
	for k, v := range s.lastMintedAt {
		lastUsed[k] = v
	}
	s.mu.Unlock()

	type cand struct {
		a    types.Account
		tier int
		used time.Time
	}
	var cs []cand
	for _, a := range s.usable() {
		if a.ID == exclude {
			continue
		}
		if throttle > 0 && now.Sub(lastUsed[a.ID]) < throttle {
			continue
		}
		tier := 1 // 没有记录
		if sl, ok := s.st.Slot(a.ID, gw.Name); ok {
			switch sl.LastVerdict {
			case "degraded":
				tier = 0
			case "full":
				tier = 2
			}
		}
		cs = append(cs, cand{a: a, tier: tier, used: lastUsed[a.ID]})
	}
	if len(cs) == 0 {
		return types.Account{}, false
	}
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].tier != cs[j].tier {
			return cs[i].tier < cs[j].tier
		}
		if !cs[i].used.Equal(cs[j].used) {
			return cs[i].used.Before(cs[j].used)
		}
		return cs[i].a.ID < cs[j].a.ID
	})
	return cs[0].a, true
}

// freshPair 判断池里那张 pair 还够不够交付：剩余不足 DeliverFloor 就别发，
// 否则 65 分钟硬过期会在消费者手里到点（SPEC 第 6 节）。
func (s *Scheduler) freshPair(gateway string) (types.Pair, bool) {
	p, ok := s.st.Pair(gateway)
	if !ok {
		return types.Pair{Gateway: gateway}, false
	}
	return p, p.ExpiresAt.Sub(s.now()) >= types.DeliverFloor
}

// holding 报告这个账号此刻是不是正拿着这个网关的票。
//
// 只看槽位的 LastTouch，**不记额外状态**：交付路径上写 LastTouch 的正是用这个账号自己
// 的凭据碰这个网关的那一发，而一次交付只活 DeliverTTL 那么久，所以「DeliverTTL 内碰过」
// 就是「他手上那张」。force 要排掉的就是它——原来这件事靠 pair 的租约字段做，
// 而那套互斥 2026-10-01 被实测推翻删掉了（见 types.Pair 的注释），
// 但 force 必须换一个网关的承诺还在，所以改用这个现成的读数。
func (s *Scheduler) holding(accountID int64, gateway string) bool {
	sl, ok := s.st.Slot(accountID, gateway)
	if !ok || sl.LastTouch.IsZero() {
		return false
	}
	return s.now().Sub(sl.LastTouch) < types.DeliverTTL
}

// ---------- 槽位账本 ----------

func (s *Scheduler) updateSlot(accountID int64, gateway string, f func(*types.Slot)) error {
	err := s.st.UpdateSlot(accountID, gateway, func(sl *types.Slot) {
		sl.AccountID, sl.Gateway = accountID, gateway
		if sl.LastVerdict == "" {
			sl.LastVerdict = types.VerdictUnknown
		}
		f(sl)
	})
	if err != nil {
		s.opt.Log.Warn("写槽位失败", "account", accountID, "gateway", gateway, "err", err)
	}
	return err
}

func (s *Scheduler) touch(gateway string, accountID int64, at time.Time) {
	_ = s.updateSlot(accountID, gateway, func(sl *types.Slot) { sl.LastTouch = at })
}

func (s *Scheduler) setVerdict(accountID int64, gateway, verdict string) {
	_ = s.updateSlot(accountID, gateway, func(sl *types.Slot) { sl.LastVerdict = verdict })
}

func (s *Scheduler) sample(sm types.Sample) {
	if err := s.st.AppendSample(sm); err != nil {
		s.opt.Log.Warn("写样本失败", "account", sm.AccountID, "err", err)
	}
}

// recordEcho 把 state-echo 那一发也记进样本：它同样是一次用上传者凭据发出的真实请求，
// 账本漏了它上传者视图就不诚实了。
func (s *Scheduler) recordEcho(accountID int64, e types.Exit, gateway string, obs types.Observed, v *types.Verdict, at time.Time) {
	status := 0
	if v != nil {
		status = v.Status
	}
	s.sample(types.Sample{
		AccountID: accountID, RegionKey: exitKey(e), Requested: e, Observed: obs,
		Gateway: gateway, Status: status, At: at,
	})
}

// ---------- 退避 ----------

// backoffKey 只是进程内那张退避表的 map key，和存储层的槽位键没关系，
// 所以不复用 store 的 SlotKey——sched 刻意不认识 store。
func backoffKey(accountID int64, gateway string) string {
	return strconv.FormatInt(accountID, 10) + "|" + gateway
}

// bumpBackoff 翻倍退避，上限 20 分钟。只给硬失败用（非 200、网络错、落点漂移）。
// 退避时长只记在内存里：进程重启本来就该重新探一遍，落盘反而会把误判带过重启。
func (s *Scheduler) bumpBackoff(accountID int64, gateway string) {
	k := backoffKey(accountID, gateway)
	s.mu.Lock()
	d := s.backoff[k] * 2
	if d < backoffBase {
		d = backoffBase
	}
	if d > backoffMax {
		d = backoffMax
	}
	s.backoff[k] = d
	s.mu.Unlock()
	s.holdSlot(accountID, gateway, d)
}

// holdSlot 让一个槽位歇 d，不碰指数计数器。判降级走这条：那个判据有假阴，
// 歇一个满血窗口就够，不该让误判越积越长。
func (s *Scheduler) holdSlot(accountID int64, gateway string, d time.Duration) {
	until := s.now().Add(d)
	_ = s.updateSlot(accountID, gateway, func(sl *types.Slot) { sl.BackoffUntil = until })
}

func (s *Scheduler) clearBackoff(accountID int64, gateway string) {
	k := backoffKey(accountID, gateway)
	s.mu.Lock()
	delete(s.backoff, k)
	s.mu.Unlock()
	_ = s.updateSlot(accountID, gateway, func(sl *types.Slot) { sl.BackoffUntil = time.Time{} })
}

func (s *Scheduler) markDemand() {
	s.mu.Lock()
	s.lastDeliver = s.now()
	s.mu.Unlock()
}

// ---------- 小工具 ----------

// pickExit 随机取一条已知可达出口。随机而非固定第一条：城市库存不足时上游会
// 回退到别的国家（实测 city-Dallas 给过尼日利亚 IP），死磕一条出口会卡死在那次回退上。
func pickExit(g types.Gateway) (types.Exit, bool) {
	if len(g.ReachableExits) == 0 {
		return types.Exit{}, false
	}
	return g.ReachableExits[rand.IntN(len(g.ReachableExits))], true
}

func exitKey(e types.Exit) string {
	if e.City == "" {
		return e.Region
	}
	return e.Region + ":" + e.City
}

func drop(ss []string, v string) []string {
	out := ss[:0]
	for _, s := range ss {
		if s != v {
			out = append(out, s)
		}
	}
	return out
}

// noteRenewFail 给这张 pair 记一次续期失败。连败到预算就**丢掉这张票**，
// 下一轮改成裸铸。
//
// 刻意**不拉黑网关**：续不上的是这张具体的票，网关本身照旧——别的号随时可能在它
// 上面铸出一张新的，拉黑会把那条路也一起堵了。
func (s *Scheduler) noteRenewFail(name string, cause error) {
	drop := false
	if err := s.st.UpdateGateway(name, func(g *types.Gateway) {
		g.RenewFails++
		if g.RenewFails >= types.RenewFailBudget {
			g.RenewFails = 0
			drop = true
		}
	}); err != nil {
		s.opt.Log.Error("记续期失败没落盘", "gateway", name, "err", err)
		return
	}
	if !drop {
		return
	}
	s.opt.Log.Warn("这张 pair 连续续不上，丢掉改裸铸", "gateway", name,
		"budget", types.RenewFailBudget, "last", cause)
	if err := s.st.DeletePair(name); err != nil {
		s.opt.Log.Error("丢弃死票失败", "gateway", name, "err", err)
	}
}

// clearRenewFails 续上了就把预算还回去。
func (s *Scheduler) clearRenewFails(name string) {
	if err := s.st.UpdateGateway(name, func(g *types.Gateway) { g.RenewFails = 0 }); err != nil {
		s.opt.Log.Error("清续期失败计数没落盘", "gateway", name, "err", err)
	}
}

// noteDeliver 记一次「这个账号在这个网关上真的拿到票了」。
//
// 两条交付路径都要记：上传者自己取（验过满血）和外部账号取（池子答不了它的满血，
// 但它确实用掉了一张票）。目标深度靠这个读数算，漏记就等于对外服务不算供给压力。
func (s *Scheduler) noteDeliver(accountID int64, gateway string) {
	if err := s.st.UpdateSlot(accountID, gateway, func(sl *types.Slot) {
		sl.LastDeliver = s.now()
	}); err != nil {
		s.opt.Log.Warn("记交付失败", "account", accountID, "gateway", gateway, "err", err)
	}
}
