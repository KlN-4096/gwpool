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

	"gwpool/internal/config"
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

	// UpsertGateway 记一个落点，返回它对池子是不是全新的。
	//
	// 裸铸（预铸、发现）**必须**有这一步：落点不入网关表的话，那张票写进 Pairs 也没人
	// 看得见——Gateways() 是候选清单的唯一来源，不在表上的网关永远不会被挑中。
	// requested 是**请求过的**出口，不是观测到的地理（见 store.UpsertGateway）。
	UpsertGateway(name string, seenBy int64, requested types.Exit) (bool, error)

	// RegionMints 是 (账号 × 区域) 的终身铸票台账，key 的格式归 store 所有
	// （这里只认 RegionMint 自带的 AccountID / RegionKey 两个字段）。
	RegionMints() map[string]types.RegionMint

	// NoteRegionMint 记一次成功的铸票：挑下一个铸票者就看这本账。
	NoteRegionMint(accountID int64, regionKey string, at time.Time) error

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

	// Rate 报这个上游账号本小时**各区域**各打了几发，以及每区的上限。
	// 挑铸票者要据此跳过「九个区域全打满」的号——不先问一声的话，
	// 那一发会一路走到速率闸才被拒，而那时候要票的人已经等了一个往返。
	// limit <= 0 当「不设限」（没接配置的裸 mint.Client 才会这样，生产不会）。
	Rate(acct string) (used map[string]int, limit int)
}

// ErrNoFullSlot：一个可交付的槽位都没挑出来。宁可 503 也不放降智出去（SPEC 第 8 节）。
//
// 名字里的「Full」说的是**预期满血**（出了冷却、不在自己的满血窗口里），不是
// 「验过」：verify_full_at_deliver 默认关，默认路径上一发验证都不打。
// 具体是哪一种失败由 CoolingError / ErrNoLivePair / ErrMintFailed 这几个一起说，
// 它们都包在它里面 ⇒ 调用方「是不是该回 503、该不该去找新落点」的分流不用改。
var ErrNoFullSlot = errors.New("没有可交付的槽位")

// ErrDegraded 标记「state-echo 判降级」。它和硬失败要分开记退避，见 degradedHold。
//
// 导出是因为 HTTP 那头要把它翻成对外的错误码（degraded），而这件事只有判据这边知道。
var ErrDegraded = errors.New("state-echo 判降级")

// ErrUpstreamRejected / ErrMintFailed 把铸票失败分成「上游否决这个账号」和「别的」。
//
// 分开不是洁癖：否决（400/401/403）重试多少次都一样、账号已经被当场停用，
// 而别的（429、5xx、网络错、落点漂移）是「现在不行」。调用方据此告诉客户端
// 该不该再来（见 types.RejectedByUpstream）。
var (
	ErrUpstreamRejected = errors.New("上游否决了这个账号")
	ErrMintFailed       = errors.New("铸票失败")
)

// ErrNoLivePair：候选网关里一张活票都没有，而现铸也没成。
//
// 和 ErrMintFailed 的分工就是契约里那两句话的分工：no_live_pair 是「没有活票，
// 且现铸没成」，mint_failed 是「**其他**铸票失败」（例如交付前续期那一发）。
// 分开是因为**只有前者等得起**：等一会儿别人可能铸出一张票来，而那一次重试
// 一发上游都不用打；后者等多久都是同一句话。
var ErrNoLivePair = errors.New("候选网关里没有活票，现铸也没成")

// Request 是一次取票里**调用方说得上话**的那几件事。
//
// 收成一个结构体而不是继续加参数：这四个旋钮全是外部输入，长度和范围的校验在 HTTP
// 边界做完（见 httpapi），进到这里就当可信数据用。
type Request struct {
	// Gateway 非空 = 点名一个网关。他点名是因为**他**知道自己没烧过那个落点。
	Gateway string
	// Force = 「我手上那张不行了，必须换一个网关」。
	Force bool
	// Exclude 是调用方要跳过的网关。它自己那本账（按凭证域身份算的）比池子的准，
	// 所以这是池子**挑之前**就该排掉的，不是挑完再过滤。
	Exclude map[string]bool
	// ExcludeVersions 是调用方要跳过的**具体的票**（types.CookieVersion）。
	// 和 Exclude 的区别：同一个网关换了张新 pair 之后还能用，排的是票不是落点。
	ExcludeVersions map[string]bool
	// MinRemaining 是调用方能接受的 pair 最低剩余寿命。0 = 按服务端的 DeliverFloor。
	MinRemaining time.Duration
}

// Floor 是这一次请求的交付门槛。
//
// 只影响这一次的候选筛选：/gateways、FreeGateways 那几个读数是**池子的**健康度，
// 不该被某一个调用方的偏好改写。
func (r Request) Floor() time.Duration {
	if r.MinRemaining > 0 {
		return r.MinRemaining
	}
	return types.DeliverFloor
}

// Skips 报告这个网关 / 这张票是不是被调用方排除了。
//
// 两个名单一处判：筛候选和「等票」那条路都要用同一把尺子，分两份迟早对不上。
func (r Request) Skips(gateway, cookie string) bool {
	if r.Exclude[gateway] {
		return true
	}
	return len(r.ExcludeVersions) > 0 && r.ExcludeVersions[types.CookieVersion(cookie)]
}

// CoolingError 是「这个账号在所有候选网关上都还在冷却里」。
//
// 带 Wait 是因为 HTTP 那头要把它变成 Retry-After：冷却是**小时**量级，
// 让客户端按默认节奏重试等于白打几百发。
// Unwrap 回 ErrNoFullSlot ⇒ 调用方按它回 503、挂发现扫描，分流一个字都不用改。
type CoolingError struct {
	Gateways int           // 候选网关数
	Cooldown time.Duration // 当前生效的冷却时长
	Wait     time.Duration // 最早一个槽位出冷却还要多久；0 = 算不出来
}

func (e *CoolingError) Error() string {
	return fmt.Sprintf("这个账号在 %d 个候选网关上都还在 %v 的冷却里（最早 %v 后出来）",
		e.Gateways, e.Cooldown, e.Wait.Truncate(time.Second))
}

func (e *CoolingError) Unwrap() error { return ErrNoFullSlot }

const (
	// 硬失败（非 200、网络错、落点漂移）指数退避：快重试会让供给更快耗尽
	// （2026-10-01 实测：两小时上千次铸票把能碰到的组合全烧空），SPEC 红线 7。
	//
	// 10s 起、封顶 1 分钟（2026-10-01 从 1min/20min 压下来）：原来那套的问题是
	// 一次**外部**故障（出口熔断、调用方取消）会把一个账号的所有槽位挨个打进
	// 20 分钟退避，故障恢复了它还要再瞎等十几分钟——当天两次全池 502 就是这样
	// 被拖长的。退避是「别对着刚坏的东西连打」，不是罚站。
	backoffBase = 10 * time.Second
	backoffMax  = time.Minute

	// 判降级只歇一个满血窗口，**不进指数**：state-echo 有假阴（满血号偶尔也下发新
	// state），翻倍会让一次误判把好槽位停到 20 分钟，而供给本来就只有几个槽位。
	degradedHold = types.FullWindow

	// minDeliverWindow 是交付的最低剩余窗口。窗口从**首次接触**起算，铸票 + echo
	// 两发往返要 20~90 s，剩不下多少就别发了——发一张马上过期的等于发降智。
	minDeliverWindow = 30 * time.Second
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

	// MaxTriesPerDeliver 覆盖配置里的同名旋钮，0 = 按配置（生产留 0，好让它热生效）。
	// 每试一个网关都可能真打一发上游，而且不管成不成那个槽位都烧掉了。
	MaxTriesPerDeliver int

	// 「票充足时对匿名开放」的开关和门槛**刻意不在这儿**：它们归 config.PublicWhenSurplus，
	// Public() 每次现读 ⇒ 管理页改完立即生效。在这里留一份快照的话，
	// 调紧门槛这个紧急动作会静默失效（日志说改了、页面说生效了，实际没变）。

	// Live 是配置的活引用：交付前验不验满血、保温要不要续期、槽位冷却多久，
	// 三个旋钮每次**现读**它，于是管理页改完立即生效、不用重启
	// （重启会丢掉进程里的速率窗口和需求时间戳，而调度正依据那些）。
	// 为 nil 时 New 会装一份 config.Defaults——测试想换旋钮就塞一个自己的
	// config.NewLive("", cfg)，不必搭配置文件。
	Live *config.Live

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
	// lastMintedAt 记每个账号最近一次被用作铸票者，用来把铸票摊到各个号上。
	// 不落盘：重启后重新摊平没有坏处，而落盘就多一份要维护的状态。
	// 「总共铸过几发」刻意**不**自己记：那个读数在 mint 的速率窗口里，
	// 而那份是连 scan 那条路的发数一起算的，自己再记一份只会和它分叉。
	lastMintedAt map[int64]time.Time
	lastDeliver  time.Time
	// gwLocks 是「同一个网关一次只许有一发在铸」，见 lockGateway。
	gwLocks map[string]*sync.Mutex
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
	if opt.Live == nil {
		// 默认值只有一份，在 config.Defaults 里。
		opt.Live = config.NewLive("", config.Defaults())
	}
	return &Scheduler{st: st, mint: m, opt: opt,
		backoff: map[string]time.Duration{}, lastMintedAt: map[int64]time.Time{}}
}

func (s *Scheduler) Mode() types.Mode { return s.opt.Mode }

func (s *Scheduler) now() time.Time { return s.opt.Now() }

// cfg 取当前配置快照。一个判断只取一次：两次 Get() 之间配置可能变了。
func (s *Scheduler) cfg() config.Config { return s.opt.Live.Get() }

// Deliver 为 accountID 这个账号交付一张 pair。req.Gateway 非空时只考虑那个网关。
//
// **「验过满血」不是这条路的承诺**：verify_full_at_deliver 默认**关**
// （验证要用消费者自己的凭据打两发，恰好烧掉它马上要用的那个窗口），所以默认
// 交出去的票 verified_full=false。保证来自调度本身：候选都出了冷却、都不在
// 自己的满血窗口里。任何「反正交付前会验一次」的兜底推理都不成立。
//
// **账号必须是参数，不能由调度器自己挑**：槽位是 (账号 × 网关) 二元组，
// 拿 A 算出来的槽位发给 B，「取歇得最久的」就整个失效了——B 可能刚在那儿烧过。
//
// 流程照 SPEC 第 8 节：排掉调用方排除的 → 排掉不能给满血的 → 只在有活 pair 的网关里
// 挑（剩余 ≥ req.Floor()，一张都没有才现铸）→ 选歇得最久的 →（开了验证的话）
// state-echo 验一次 → 交付；降级则退避换下一个，挑不出来返回 ErrNoFullSlot
// （具体哪一种由 CoolingError / ErrNoLivePair 一起说，它们都包在里面）。
//
// **不做任何跨消费者的互斥**：同一张 pair 同时发给几个账号是上游允许的，
// 2026-10-01 实测见 types.Pair 的注释。交付有效期是**槽位的**窗口
// （消费账号 × 网关），不是那张 pair 的锁。
//
// force = 「他手上那张不行了，换一个」。换不换由客户端决定：池子猜客户端什么时候
// 该换本来就是在猜，只有客户端知道自己那一发对不对劲。force 时**必须**换一个网关，
// 没得换就 ErrNoFullSlot——把原来那张再发一遍等于没换，客户端会死循环。
func (s *Scheduler) Deliver(ctx context.Context, accountID int64, req Request) (types.Delivery, error) {
	consumer, ok := s.account(accountID)
	if !ok {
		return types.Delivery{}, fmt.Errorf("账号 %d 用不了（不存在、已停用、或没有凭据）", accountID)
	}
	gatewayHint, force := req.Gateway, req.Force

	// 先记需求：即使这次 503 了，保温循环也该知道「有人在要」。
	s.markDemand()

	// 窗口还开着的重复请求：原样再发一次，一发上游都不打（见 redeliver）。
	// force 跳过这条——他就是因为手上那张不行了才来的。
	if !force {
		if d, ok := s.redeliver(consumer.ID, req); ok {
			return d, nil
		}
	}

	gws := s.st.Gateways()
	cands := make([]string, 0, len(gws))
	for name := range gws {
		if gatewayHint != "" && name != gatewayHint {
			continue
		}
		// 排除名单在**有活 pair 的硬筛之前**生效：调用方那本账比池子的准
		// （池子只认它自己发过的票），挑完再过滤等于把一次筛选白跑一遍。
		//
		// 点名的网关同时又在排除名单里（客户端自相矛盾）时**按排除办**：
		// exclude 编码的是「我烧过这个」，照它办永远不会发出一张坏票；
		// 反过来让 gateway 压过 exclude，就可能把客户端自己说烧过的落点发回去。
		if p, _ := s.st.Pair(name); req.Skips(name, p.Cookie) {
			continue
		}
		cands = append(cands, name)
	}
	if len(cands) == 0 {
		// 包成 ErrNoFullSlot：对调用方来说「池子是空的」和「你的槽位烧完了」
		// 要做的事一样——回 503，然后去找新落点（见 httpapi.deliverFailed）。
		// 不包的话冷启动的池子永远不会触发发现，只能干等保温。
		return types.Delivery{}, fmt.Errorf("%w（池子里没有可用网关，hint=%q）", ErrNoFullSlot, gatewayHint)
	}

	cooldown := s.cfg().SlotCooldown()
	if force {
		kept := make([]string, 0, len(cands))
		for _, name := range cands {
			if s.holding(consumer.ID, name) {
				continue
			}
			kept = append(kept, name)
		}
		if len(kept) == 0 {
			// 他手上那张就是唯一的候选 ⇒ 和「全在冷却里」是同一件事（刚交付过的
			// 槽位本来就在冷却里），而且**等不起**：冷却是小时量级。
			return types.Delivery{}, &CoolingError{Gateways: len(cands), Cooldown: cooldown,
				Wait: s.coolingWait(consumer.ID, cands, cooldown)}
		}
		cands = kept
	}

	// 冷却过滤：窗口用完的 (账号 × 网关) 要歇满 SlotCooldown 才重新给满血，
	// 这期间它不参与挑选。**不给它开后门**——歇不够就发等于发降智的东西，
	// 而池子宁可 503（SPEC 第 8 节）。全烧完就让发现路径去找新落点。
	rested := s.notCooling(consumer.ID, cands, cooldown)
	if len(rested) == 0 {
		return types.Delivery{}, &CoolingError{Gateways: len(cands), Cooldown: cooldown,
			Wait: s.coolingWait(consumer.ID, cands, cooldown)}
	}
	cands = rested

	// 一次请求最多试这么多个网关。**每试一个都可能要真打一发上游**（没票就得现铸，
	// 开了验证还要再加两发），试到的那个槽位不管成不成都烧掉了——不设上限的话，
	// 一个倒霉的消费者会把整池的供给一轮摸干净，后面的人就无票可用了
	// （2026-10-01 实测：一次请求连试 5 个、耗 90 秒，然后告诉调用方 503）。
	// **硬筛：只在此刻有活 pair 的网关里挑。** 加上冷却过滤之后，候选里的槽位全都出了
	// 冷却、预期都是满血，这时候「歇得更久」再也买不到额外质量；而「已经有票」能省掉
	// 一发上游请求外加**一个铸票者的槽位**（铸票烧两个槽位：铸票者的 + 消费者的，
	// 直接发现成的只烧一个）。而槽位才是这个池子最紧的资源——每账号每小时 54 发请求，
	// 但槽位只有「已知网关数 ÷ 冷却」那么多（十几个网关、4 小时冷却 ≈ 3.5 个/小时）。
	//
	// 一张活 pair 都没有时**仍然现铸一发**（见 deliverByFreshMint）：不然冷池子永远 503，
	// 只能干等预铸。规则是「有票就绝不铸」，不是「永不铸」。
	withPair, _ := s.splitByPair(cands, req.Floor())
	if len(withPair) == 0 {
		// 点名了网关就定向铸（落点漂了算失败）：他点名是因为**他**知道自己没烧过那个
		// 落点，给他换一个等于没听他说话。没点名才是抽签。
		d, gateway, err := s.deliverByFreshMint(ctx, consumer, cands, cooldown, req)
		if err == nil {
			s.clearBackoff(consumer.ID, gateway)
			return d, nil
		}
		s.noteTryFailure(ctx, consumer, gateway, err)
		// 三个 %w：候选非空、只是一张活票都没有而现铸又没成 ⇒ 这条**等得起**
		// （别人铸出一张票来之后的那次重试一发上游都不用打）。
		// 这正是换票（force + exclude_versions）最常落到的那一格，归成
		// 「全在冷却里」会让 wait 在每一次轮换上都失效。
		return types.Delivery{}, fmt.Errorf("%w（%w，最后一个原因：%w）",
			ErrNoFullSlot, ErrNoLivePair, err)
	}
	cands = withPair

	// 有票的网关可以一个个试（试一个不花请求），但仍然封顶：每试一个槽位不管成不成
	// 都烧掉了，不设上限的话一个倒霉的消费者会把整池供给一轮摸干净
	// （2026-10-01 实测：一次请求连试 5 个、耗 90 秒，然后告诉调用方 503）。
	tries := s.maxTries()
	// 留住最后一个真实原因：ErrNoFullSlot 这一句话把「你的槽位都烧完了」和
	// 「出口全熔断了、谁都铸不出票」说成同一件事，而这两者要做的事完全相反。
	var last error
	for len(cands) > 0 && tries > 0 {
		sl, ok := s.st.LongestRestedSlot(consumer.ID, cands)
		if !ok {
			break // 剩下的全在退避期
		}
		tries--
		d, err := s.tryGateway(ctx, consumer, gws[sl.Gateway], req)
		if err == nil {
			s.clearBackoff(consumer.ID, sl.Gateway)
			return d, nil
		}
		last = err
		if !s.noteTryFailure(ctx, consumer, sl.Gateway, err) {
			break
		}
		cands = drop(cands, sl.Gateway)
	}
	if tries == 0 && len(cands) > 0 {
		s.opt.Log.Info("试满上限仍没拿到满血票，剩下的留给别人", "account", consumer.ID,
			"label", consumer.Label, "tried", s.maxTries(), "left", len(cands))
	}
	if last != nil {
		// 两个 %w：调用方既能 errors.Is 出 ErrNoFullSlot（该回 503），也能认出底下
		// 那个真正的故障（出口没了、速率闸、上游拒收）。页面上的「原始错误」就是这一句。
		return types.Delivery{}, fmt.Errorf("%w（最后一个原因：%w）", ErrNoFullSlot, last)
	}
	return types.Delivery{}, ErrNoFullSlot
}

// noteTryFailure 给一次失败的尝试善后，返回「还值得试下一个吗」。
//
// 两条交付路（挑有票的网关、回落现铸）共用它：善后规则只该有一份，分叉过的那阵子
// 回落路径既不记退避也不包 ErrNoFullSlot，调用方照着错误类型分流全错了。
func (s *Scheduler) noteTryFailure(ctx context.Context, consumer types.Account, gateway string, err error) bool {
	// 客户端自己断了（超时、取消）不算槽位的错：否则反复重试的客户端
	// 会把自己所有槽位逐个打进退避。
	if ctx.Err() != nil {
		s.opt.Log.Debug("调用方中断", "account", consumer.ID, "label", consumer.Label, "gateway", gateway)
		return false
	}
	s.opt.Log.Info("槽位不可交付", "account", consumer.ID, "label", consumer.Label,
		"gateway", gateway, "reason", err)
	// 速率闸和「出口全没了」都不是这个槽位的错，也不是这个账号的错：给它们记退避
	// 等于拿池子自己的故障惩罚供给——而且退避是按槽位记的，一次外部故障会把这个
	// 账号的槽位挨个打进退避，故障恢复后它还要再瞎等一轮（2026-10-01 现网实测）。
	// 这两种还**整池同病**：换个网关再试只是把同一句话重复 N 遍，白烧 N 发请求。
	// 直接认 503，下一个人来的时候配额/出口可能已经回来了。
	if errors.Is(err, types.ErrRateLimited) || errors.Is(err, types.ErrNoExit) {
		return false
	}
	if gateway == "" {
		return true // 连落点都没读出来，没有槽位可记
	}
	if errors.Is(err, ErrDegraded) {
		s.holdSlot(consumer.ID, gateway, degradedHold)
	} else {
		s.bumpBackoff(consumer.ID, gateway)
	}
	return true
}

// FreeGateways 现算空闲网关数：有活 pair（剩余 ≥ DeliverFloor）的网关数。
//
// 「空闲」不再扣掉正在被人用的那些：一张 pair 可以同时给几个号用（types.Pair 注释里
// 的 2026-10-01 实测），所以「有人在用」不占名额。
//
// 每次请求现算、不缓存：供给是秒级变化的，缓存一个「充足」状态就等于拿着过期的
// 结论放匿名请求进来，而他们正是最该被挡在门外的那一拨。
//
// 和保温的「深度目标」是同一个读数（livePairs 就是它的别名）：两份实现会让
// 「硬筛」和「该不该预铸」用两把尺子量同一件事。
// 门槛**不跟 min_remaining 走**：这是池子自己的健康度，不是某一个调用方的偏好。
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
//
// 开关和门槛**现读配置**，不读 New() 时的快照：管理页上那个按钮写着「保存并立即生效」，
// 而把门槛调紧是运营方最可能的紧急动作——匿名这条路唯一的让路机制就是它
// （pair 本身不互斥，见 types.Pair）。读快照的话日志说改了、页面说生效了，实际没变。
func (s *Scheduler) Public() PublicState {
	c := s.cfg().PublicWhenSurplus
	st := PublicState{Enabled: c.Enabled, Free: s.FreeGateways(), Threshold: c.MinFreeGateways}
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
func (s *Scheduler) DeliverAnonymous(ctx context.Context, upstreamID string, req Request) (types.Delivery, error) {
	ps := s.Public()
	if !ps.Open {
		return types.Delivery{}, fmt.Errorf("%w（空闲 %d，门槛 %d）", ErrPublicClosed, ps.Free, ps.Threshold)
	}
	a, err := s.anonAccount(upstreamID)
	if err != nil {
		return types.Delivery{}, err
	}
	gatewayHint, force, floor := req.Gateway, req.Force, req.Floor()
	s.markDemand()

	// 窗口内重复请求走同一条路：他也是消费者，重连重试不该去烧下一个落点。
	if !force {
		if d, ok := s.redeliver(a.ID, req); ok {
			return d, nil
		}
	}

	now := s.now()
	cooldown := s.cfg().SlotCooldown()
	cands := make([]string, 0, ps.Free)
	for name := range s.st.Gateways() {
		if gatewayHint != "" && name != gatewayHint {
			continue
		}
		p, ok := s.st.Pair(name)
		if !ok || p.ExpiresAt.Sub(now) < floor {
			continue
		}
		if req.Skips(name, p.Cookie) {
			continue
		}
		cands = append(cands, name)
	}
	// force 和冷却分两步筛、各自报错：和上传者那条路同一条规则——
	// 筛完为空就是「你的槽位全在冷却里」（等几小时，不可等待），
	// 而「一张活票都没有」是另一码事（上面那个循环就把它筛成空了，等得起）。
	if force {
		kept := make([]string, 0, len(cands))
		for _, name := range cands {
			if s.holding(a.ID, name) {
				continue // 他手上那张不行了，排掉他正在用的
			}
			kept = append(kept, name)
		}
		if len(kept) == 0 && len(cands) > 0 {
			return types.Delivery{}, &CoolingError{Gateways: len(cands), Cooldown: cooldown,
				Wait: s.coolingWait(a.ID, cands, cooldown)}
		}
		cands = kept
	}
	// 冷却对占位行一视同仁：「这个上游账号在这个网关上烧过了」和他有没有上传凭据无关。
	rested := s.notCooling(a.ID, cands, cooldown)
	if len(rested) == 0 && len(cands) > 0 {
		// 告诉他最早什么时候回来，别让他按默认节奏重试。
		return types.Delivery{}, &CoolingError{Gateways: len(cands), Cooldown: cooldown,
			Wait: s.coolingWait(a.ID, cands, cooldown)}
	}
	cands = rested
	for len(cands) > 0 {
		sl, ok := s.st.LongestRestedSlot(a.ID, cands)
		if !ok {
			break
		}
		p, ok := s.st.Pair(sl.Gateway)
		// 现算一遍：从选候选到这儿之间，那张 pair 可能已经到点了。
		if !ok || p.ExpiresAt.Sub(s.now()) < floor {
			cands = drop(cands, sl.Gateway)
			continue
		}
		at := s.now()
		// 一次读-改-写记下触碰、交付时刻和还原点。外部消费者的用量也要进账本，
		// 否则「池子该备多深」这个目标永远看不见他们：池子对外服务得越多，
		// 目标深度反而不涨。
		s.noteDeliver(a.ID, sl.Gateway, at, true)
		// 运营方靠这行判断门槛设得对不对。
		s.opt.Log.Info("匿名交付", "upstream", upstreamID, "account", a.ID,
			"gateway", sl.Gateway, "free_gateways", ps.Free, "threshold", ps.Threshold)
		return types.Delivery{
			Gateway:      sl.Gateway,
			Cookie:       p.Cookie,
			ExpiresAt:    at.Add(types.DeliverTTL),
			ValidForS:    validFor(types.DeliverTTL, p.ExpiresAt.Sub(at)),
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

// tryGateway 在一个**点名的**网关上交付：确保它有张够新鲜的 pair，然后走 finish。
//
// 走到这儿的网关通常已经有票（Deliver 硬筛过），所以这条路一般一发上游都不打。
func (s *Scheduler) tryGateway(ctx context.Context, consumer types.Account, gw types.Gateway, req Request) (types.Delivery, error) {
	exit, ok := pickExit(gw)
	if !ok {
		return types.Delivery{}, fmt.Errorf("网关 %s 没有已知可达出口，没法定向铸票", gw.Name)
	}
	floor := req.Floor()

	pair, fresh := s.freshPair(gw.Name, floor)
	var m minted
	if !fresh {
		// 同一个网关一次只许有一发在铸。**锁顺序恒为「先网关、再账号」**，
		// 而 finish 只拿账号锁、绝不回头要网关锁 ⇒ 不存在 ABBA。
		//
		// 为什么账号锁拦不住这件事：两个消费者在同一个网关上都没票时，minterFor
		// 大概率给他们挑**不同**的铸票者，于是两发都放出去了——而一张 pair 本来就能
		// 同时服务多个消费者（types.Pair 的 2026-10-01 实测），第二发纯属白烧
		// 一个槽位加一发请求。
		release := s.lockGateway(gw.Name)
		// 拿到锁之后重查一遍：排在前面那个人可能刚把票铸好了。
		if p, ok := s.freshPair(gw.Name, floor); ok {
			pair = p
			release()
			s.opt.Log.Debug("等到了别人刚铸的票，省下一发", "gateway", gw.Name, "account", consumer.ID)
		} else {
			got, err := s.mintPair(ctx, consumer, gw.Name, exit, pair, gw.Name, gw.Name)
			release()
			if err != nil {
				return types.Delivery{}, err
			}
			pair = got.pair
			if got.mintedBySelf {
				// 单账号模式：建 pair 那一发就是消费者的首次接触，票直接拿来用。
				m = got.minted
			}
		}
	}
	return s.finish(ctx, consumer, gw.Name, exit, pair, m, floor)
}

// deliverByFreshMint 是「一张活 pair 都没有」时的回落：**抽一次签**。
//
// 出口只能把落点「倾向」到某个网关，定不住。既然是抽签，「先试网关 X 再试网关 Y」
// 就是抽两次签、花两发请求烧两个铸票者槽位，而两次的分布一模一样。所以只铸一发，
// 然后看抽到的那个网关上消费者出不出冷却：出了就交付，没出就老实 503，
// 让发现路径（httpapi 在 ErrNoFullSlot 上挂的那一发）去找新落点。
//
// 这也是为什么 MaxTriesPerDeliver 只管「有票的网关一个个试」那条路：
// 让它乘在抽签上纯属多花请求。
//
// strict = 调用方点名了网关：那就定向铸、落点漂了算失败。他点名是因为**他**知道自己
// 没烧过那个落点，给他换一个等于没听他说话。
//
// 第二个返回值是这一发**实际落在**哪个网关上（失败时尽力给出意图落点），
// 调用方拿它记退避和交付——善后规则在 Deliver 那边，只有一份。
func (s *Scheduler) deliverByFreshMint(ctx context.Context, consumer types.Account,
	cands []string, cooldown time.Duration, req Request) (types.Delivery, string, error) {
	sl, ok := s.st.LongestRestedSlot(consumer.ID, cands)
	if !ok {
		return types.Delivery{}, "", fmt.Errorf("%w（候选网关全在退避期）", ErrNoFullSlot)
	}
	gws := s.st.Gateways()
	exit, ok := pickExit(gws[sl.Gateway])
	if !ok {
		return types.Delivery{}, sl.Gateway,
			fmt.Errorf("网关 %s 没有已知可达出口，没法铸票", sl.Gateway)
	}
	floor := req.Floor()

	// 冷却状态要在**铸票之前**定格：铸票那一发自己会 touch 消费者的槽位
	// （单账号模式下铸票者就是他本人），拿铸完之后的账本去判「他出冷却了吗」，
	// 答案恒为否——自己把自己判死。
	coolingBefore := s.coolingSet(consumer.ID, cooldown)

	release := s.lockGateway(sl.Gateway)
	// 别人可能刚为这个网关铸好了票——那就直接用，省下这一发。
	if p, fresh := s.freshPair(sl.Gateway, floor); fresh {
		release()
		d, err := s.finish(ctx, consumer, sl.Gateway, exit, p, minted{}, floor)
		return d, sl.Gateway, err
	}
	want := ""
	if req.Gateway != "" {
		// 调用方点名了网关：定向铸、落点漂了算失败。他点名是因为**他**知道自己
		// 没烧过那个落点，给他换一个等于没听他说话。
		want = sl.Gateway
	}
	got, err := s.mintPair(ctx, consumer, sl.Gateway, exit, s.pairFor(sl.Gateway, floor), want, sl.Gateway)
	if err != nil {
		release()
		return types.Delivery{}, sl.Gateway, err
	}
	release()
	landed := got.pair.Gateway
	if landed == "" {
		return types.Delivery{}, sl.Gateway, errors.New("现铸没读出落点")
	}
	// 闸 1（真的交付过票）要在铸完之后**重查一次**：铸票那几十秒里同一个账号的另一路
	// 交付可能刚在 landed 上取走过一张票，而那件事快照看不见 ⇒ 这里会 fail-open，
	// 把一个刚烧掉的槽位当首次接触发出去。
	// 闸 2（LastTouch 在一个满血窗口内）继续用快照：铸票那一发自己就是一次 touch，
	// 拿铸后的账本问它等于自己把自己判死。
	if coolingBefore[landed] || s.coolingByDeliver(consumer.ID, landed, cooldown) {
		return types.Delivery{}, landed,
			fmt.Errorf("现铸抽到 %s，而这个账号在它上面还在 %v 的冷却里", landed, cooldown)
	}
	var m minted
	if got.mintedBySelf {
		m = got.minted
	}
	s.opt.Log.Info("一张活票都没有，现铸一发", "account", consumer.ID, "label", consumer.Label,
		"asked", sl.Gateway, "landed", landed, "strict", want != "")
	d, err := s.finish(ctx, consumer, landed, exit, got.pair, m, floor)
	return d, landed, err
}

// pairFor 取这个网关手上那张**还够交付**的票，不够就是零值（= 裸铸）。
//
// 门槛就是 freshPair 那把尺子，不是「没硬过期」：带进去的那张 pair 还有效时上游会
// **接受它、不下发新 cookie**（SPEC 第 2 节），于是手里还是那张快到点的，finish 拒发
// ⇒ 消费者 503，而请求和铸票者的一个槽位已经花掉了。
// 判据是**送进去的那张还好不好**：烧过/过期的会被上游无视并换掉（SPEC 第 6 节说的
// 「经某网关铸票回一张新 pair」是这种情形），还有效的被接受且不换（第 2 节）。
// 而走到回落这条路的前提恰好是「所有票都低于门槛」= 还有效但太短 ⇒ 必然换不回新票。
func (s *Scheduler) pairFor(gateway string, floor time.Duration) types.Pair {
	if p, fresh := s.freshPair(gateway, floor); fresh {
		return p
	}
	return types.Pair{}
}

// finish 是两条交付路共用的尾段：验剩余 →（开了验证的话）取消费者自己的活票再 echo
// → 算有效期。
//
// **绝不同时持有两把账号锁**：补 pair 可能要锁别人（铸票者），echo 要锁消费者，
// 而 minterFor 会交叉选人（A 的铸票者是 B、B 的是 A 是常态）⇒ 嵌套就是 ABBA 死锁，
// 而且没有 WriteTimeout 兜底，两个账号会永久挂死。所以分两段，各锁各的、用完就放。
//
// m 是「补 pair 那一发正好是消费者自己打的」时带过来的（单账号模式）：它的 firstAt
// 才是真正的首次接触时刻，有效期必须按它算。
func (s *Scheduler) finish(ctx context.Context, consumer types.Account, gateway string,
	exit types.Exit, pair types.Pair, m minted, floor time.Duration) (types.Delivery, error) {
	// 续完必须**重新判一次剩余**：上游接受了一张还有效的 pair 时不会下发新 cookie
	// （SPEC 第 2 节「好 pair 不会触发重新下发」），这时「续期」其实没发生，
	// 手里还是那张快到点的。发出去等于让消费者中途被重新路由到随机网关。
	if left := pair.ExpiresAt.Sub(s.now()); left < floor {
		return types.Delivery{}, fmt.Errorf("网关 %s 的 pair 只剩 %v，没续上，不发", gateway, left.Truncate(time.Second))
	}

	// 没开验证就到这儿为止：**一发上游都不再打**。
	//
	// 这不是偷懒，是算过账的：消费账号在一个它没碰过的网关上本来就是满血，而验证
	// 要用它自己的凭据打一发（先铸一张属于它的活票、再 echo）——那两发恰好烧掉
	// 它马上要用的那个窗口，验完交出去的已经是个二手落点。加上 state-echo 有假阴
	// 无假阳（SPEC 第 3 节），关掉只少一个读数，不会把降智的东西放出去。
	// 所以判定如实写 unknown（**不是** degraded：我们没验，不是验坏了，见 markUnverified），
	// 交付照样记一次触碰——票交出去了，窗口就算烧了，少记一次会让它被当成新鲜的再发一遍。
	if !s.cfg().VerifyFullAtDeliver {
		at := s.now()
		selfMinted := !m.firstAt.IsZero()
		if selfMinted {
			// 单账号模式：建 pair 那一发就是消费者自己的首次接触，窗口从那时起算。
			at = m.firstAt
		}
		left := at.Add(types.DeliverTTL).Sub(s.now())
		if left < minDeliverWindow {
			return types.Delivery{}, fmt.Errorf("网关 %s 的满血窗口只剩 %v，不发", gateway, left.Truncate(time.Second))
		}
		// 触碰、交付时刻、还原点一次写完。selfMinted 时触碰已经由那一发真实请求记过了
		// （mintOnce 自己 touch 的），别再覆盖一遍——那会把还原点挪到这次交付之后。
		s.noteDeliver(consumer.ID, gateway, at, !selfMinted)
		s.markUnverified(consumer.ID, gateway)
		return types.Delivery{
			Gateway:      gateway,
			Cookie:       pair.Cookie,
			ExpiresAt:    at.Add(types.DeliverTTL),
			ValidForS:    validFor(left, pair.ExpiresAt.Sub(s.now())),
			VerifiedFull: false,
		}, nil
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
		got, err := s.mintOnce(ctx, consumer, exit, sid, pair, gateway, gateway)
		if err != nil {
			return types.Delivery{}, err
		}
		m, pair = got, got.pair
	}
	if m.ticket == "" {
		return types.Delivery{}, fmt.Errorf("网关 %s 没回 turn-state，无从 echo", gateway)
	}

	// sid 必须和铸那张票时是同一个，否则可能换出口 IP、换网关，判据就配不上对。
	// pair 送当前这张：上游若刚续发过，新旧钉的是同一个网关，路由一样。
	//
	// 只在交付前验一次。state-echo 有假阴（满血号偶尔也下发新 state），
	// 当持续闸门会误杀好槽位（SPEC 红线 4）。
	v, verr := s.mint.VerifyFull(ctx, consumer.Creds, exit, m.sid, pair.Cookie, m.ticket)
	s.recordEcho(consumer.ID, exit, gateway, m.obs, v, verr, s.now())
	if verr != nil {
		return types.Delivery{}, fmt.Errorf("state-echo 失败: %w", verr)
	}
	if v == nil {
		return types.Delivery{}, errors.New("state-echo 不下结论（HTTP 非 200）")
	}
	if !v.Full {
		s.setVerdict(consumer.ID, gateway, types.VerdictDegraded)
		return types.Delivery{}, fmt.Errorf("%w: %s", ErrDegraded, v.Note)
	}
	s.setVerdict(consumer.ID, gateway, types.VerdictFull)

	// 有效期从**首次接触**起算，不是从 echo 回来起算（窗口在第一发就开始烧了）。
	// valid_for_s 是**满血窗口**的剩余量，不是 cookie 的有效期（那是 65 分钟）。
	// 消费端在这段时间内可以反复复用同一个 pair。
	exp := m.firstAt.Add(types.DeliverTTL)
	left := exp.Sub(s.now())
	if left < minDeliverWindow {
		return types.Delivery{}, fmt.Errorf("网关 %s 的满血窗口只剩 %v，不发", gateway, left.Truncate(time.Second))
	}
	// 触碰不用再记：铸那张活票的那一发（mintOnce）已经记过，而它就是首次接触。
	s.noteDeliver(consumer.ID, gateway, m.firstAt, false)
	return types.Delivery{
		Gateway:      gateway,
		Cookie:       pair.Cookie,
		ExpiresAt:    exp,
		ValidForS:    validFor(left, pair.ExpiresAt.Sub(s.now())),
		VerifiedFull: true,
	}, nil
}

// validFor 是交付的建议有效期（秒）：满血窗口还剩多少、这张 pair 还能活多久，取小的。
//
// 必须取小：min_remaining 能把交付门槛压到 30 s，而窗口是 150 s —— 不取小就会把一张
// 只剩 40 秒的票标成 valid_for_s=150 发出去，消费端用到一半被重新路由到随机网关。
// 今天 DeliverFloor(300s) > DeliverTTL(150s) 所以撞不上，是 min_remaining 把它打开的。
func validFor(windowLeft, pairLeft time.Duration) int {
	if pairLeft < windowLeft {
		windowLeft = pairLeft
	}
	if windowLeft < 0 {
		return 0
	}
	return int(windowLeft / time.Second)
}

// pairMint 是「为这个网关补一张 pair」的结果。mintedBySelf 为真时那一发是消费者
// 自己打的（单账号模式），于是它同时也是消费者的首次接触，票可以直接拿来 echo。
type pairMint struct {
	minted
	mintedBySelf bool
}

// mintPair 为这个网关补一张 pair。调用方必须已持该网关的铸票锁。
//
// want 是期望落点，留空表示「落到哪算哪」（回落抽签那条路）。
// bias 是这一发**想**去哪：只用来在读不出落点时保守地记一次触碰。
func (s *Scheduler) mintPair(ctx context.Context, consumer types.Account, gateway string,
	exit types.Exit, cur types.Pair, want, bias string) (pairMint, error) {
	minter := consumer
	if s.opt.Mode == types.ModePool {
		// 补 pair 那一发交给别人打：它会烧掉铸票者在这个网关上的窗口，
		// 烧消费者自己的就等于白跑一趟（这正是单账号模式不保温的原因）。
		// 出口已经由网关定死，所以只要人、不要它挑的区域。
		// 交付路径不节流：有人在等着拿 pair。
		if sc, _, ok := s.minterFor(exit, consumer.ID, 0); ok {
			minter = sc
		}
	}
	sid, err := s.mint.NewSID(true)
	if err != nil {
		return pairMint{}, fmt.Errorf("要不到粘性出口: %w", err)
	}
	unlock := s.st.LockAccount(minter.ID)
	got, err := s.mintOnce(ctx, minter, exit, sid, cur, want, bias)
	unlock()
	if err != nil {
		return pairMint{}, err
	}
	return pairMint{minted: got, mintedBySelf: minter.ID == consumer.ID}, nil
}

// lockGateway 取这个网关的铸票锁，返回解锁函数。
//
// 只罩「为这个网关铸一张 pair」那一段。不落在 store 上：它拦的是**本进程**里
// 几路交付同时为一个网关铸票，和跨包的单账号并发（store.LockAccount，SPEC 红线 2）
// 是两件事。
func (s *Scheduler) lockGateway(name string) func() {
	s.mu.Lock()
	if s.gwLocks == nil {
		s.gwLocks = map[string]*sync.Mutex{}
	}
	lk := s.gwLocks[name]
	if lk == nil {
		lk = &sync.Mutex{}
		s.gwLocks[name] = lk
	}
	s.mu.Unlock()
	lk.Lock()
	return lk.Unlock
}

// mintOnce 打一发真实请求，并把它的后果如实落账。
//
// cur 是送进去的 pair（可能为零值 = 裸打）。
//
// want 是期望落点，漂移就算这次失败：上游把我们扔到别的网关上了，那张票属于别的槽位。
// 留空 = 不判漂移（抽签、预铸：出口只能把落点**倾向**到某个网关，定不住）。
//
// bias 和 want 刻意分开：读不出落点时（超时、网络错）要保守地按「想去的地方」记一次
// 触碰——那一发可能已经出站了，窗口已经烧了。不判漂移的路径照样有「想去哪」，
// 把这件事挂在 want 上会让抽签那条路漏记触碰，下次就把烧过的槽位当新鲜的发出去。
func (s *Scheduler) mintOnce(ctx context.Context, a types.Account, e types.Exit, sid string,
	cur types.Pair, want, bias string) (minted, error) {
	// 地理只用来给上传者看账本，查不到不影响铸票（同 sid ⇒ 同出口 IP）。
	obs, gerr := s.mint.Geo(ctx, e, sid)
	if gerr != nil {
		s.opt.Log.Debug("查出口地理失败", "account", a.ID, "err", gerr)
	}

	start := s.now()
	s.mu.Lock()
	s.lastMintedAt[a.ID] = start
	s.mu.Unlock()
	res, err := s.mint.Mint(ctx, a.Creds, e, sid, cur.Cookie)
	at := s.now()
	// 每一发都要落样本：上传者视图靠它证明「我们只拿你的凭据打了 ping」。
	s.sample(types.Sample{
		AccountID: a.ID, RegionKey: sampleRegion(e), Requested: e, Observed: obs,
		Gateway: res.Gateway, Status: res.Status, At: at,
		Err: types.SampleErr(res.ErrBody, err),
	})

	// 触碰按**保守方向**记：超时和网络错在这里分不清「请求出去了但响应丢了」
	// 和「根本没出站」，而前者窗口已经烧掉了。漏记一次的代价是发出一张降智 pair，
	// 多记一次只是让这个槽位多歇会儿——所以读不出落点就按想去的地方记。
	landed := res.Gateway
	if landed == "" {
		landed = bias
	}
	if landed != "" {
		s.touch(landed, a.ID, at)
	}

	if err != nil {
		// 包一层 ErrMintFailed 但不改这句话：底下可能还裹着速率闸 / 出口熔断，
		// 调用方按**更靠内**的那几个先分流，剩下的才算「铸票没成」。
		return minted{}, fmt.Errorf("%w: %w", ErrMintFailed, err)
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
			s.opt.Log.Warn("账号被上游否决，已自动停用", "account", a.ID, "label", a.Label, "reason", msg)
			return minted{}, fmt.Errorf("%w（%s）", ErrUpstreamRejected, msg)
		}
		return minted{}, fmt.Errorf("%w（%s）", ErrMintFailed, msg)
	}
	if res.Gateway == "" {
		return minted{}, fmt.Errorf("%w：没拿到落点", ErrMintFailed)
	}

	// 落点入网关表。裸铸（预铸、发现）**必须**有这一步：Gateways() 是候选清单的
	// 唯一来源，不在表上的网关哪怕有票也永远不会被挑中。定向铸票走到这儿是幂等的，
	// 顺带把这个请求过的出口补进 ReachableExits（「下次想回来该怎么请求」）。
	if _, err := s.st.UpsertGateway(res.Gateway, a.ID, e); err != nil {
		s.opt.Log.Warn("记网关失败", "gateway", res.Gateway, "err", err)
	}
	// 记进 (账号 × 区域) 台账：挑下一个铸票者时「这个号哪个区域还没走过」全靠它。
	if err := s.st.NoteRegionMint(a.ID, types.RegionKeyOf(e), at); err != nil {
		s.opt.Log.Warn("记区域台账失败", "account", a.ID, "err", err)
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
		return minted{}, fmt.Errorf("%w：落点漂移，想要 %s，实到 %s", ErrMintFailed, want, res.Gateway)
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

// PickMinter 挑「派谁去哪个区域打一发裸铸」，给发现路径用（见 httpapi.Scanner）。
//
// exclude 是要票的那个账号：铸票那一发会烧掉执行者在新落点上的窗口，让要票的人
// 自己去就等于白跑一趟——它找到的每个新网关都被那一发当场烧掉。
func (s *Scheduler) PickMinter(exclude int64) (types.Account, types.Region, bool) {
	return s.minterFor(types.Exit{}, exclude, 0)
}

// minterFor 挑「谁去铸这一发、经哪个区域」。
//
// 旧规则是「专挑在这个网关上已经降智的号」（烧一个已经烧了的槽位代价为零）。
// 2026-10-01 删掉了：它把铸票永久绑在最废的那几个号上，而池子真正缺的不是票是
// **没见过的落点**——同一个号在同一个区域反复打只会一直落回同一个网关。
// 新规则按 (账号 × 区域) 台账挑：
//
//  1. 候选 = 九个区域里至少还有一个区域没打满**本小时**配额的号；全满的跳过
//     （不先问一声的话那一发会一路走到速率闸才被拒，而要票的人已经等了一个往返）
//  2. 本小时总发数最少的优先——集中打一个号正是 2026-09-29 把通道打废的那个模式
//  3. 并列时优先它**终身没铸过**的区域：那是唯一可能出新落点的地方
//  4. 再并列挑最久没被用作铸票者的号
//
// want 非零时是**定向铸票**（续期、点名网关）：出口已经被网关的 ReachableExits 定死，
// 只剩「它在那个区域还有配额吗」这个问题，返回的区域就是那个出口自己。
//
// throttle > 0 时跳过「刚铸过票不到这么久」的账号（保温路径用；交付路径传 0，
// 那边有人在等，节流会直接变成服务不可用）。
func (s *Scheduler) minterFor(want types.Exit, exclude int64, throttle time.Duration) (types.Account, types.Region, bool) {
	now := s.now()
	s.mu.Lock()
	lastUsed := make(map[int64]time.Time, len(s.lastMintedAt))
	for k, v := range s.lastMintedAt {
		lastUsed[k] = v
	}
	s.mu.Unlock()

	// 台账按值重建索引：那张表的键格式归 store 所有（sched 刻意不认识 store 的键）。
	lifetime := map[int64]map[string]int{}
	for _, m := range s.st.RegionMints() {
		if lifetime[m.AccountID] == nil {
			lifetime[m.AccountID] = map[string]int{}
		}
		lifetime[m.AccountID][m.RegionKey] = m.Count
	}

	type cand struct {
		a      types.Account
		r      types.Region
		sent   int  // 本小时总发数
		unseen bool // 挑中的那个区域终身还没铸过
		used   time.Time
	}
	var cs []cand
	for _, a := range s.usable() {
		if a.ID == exclude {
			continue
		}
		if throttle > 0 && now.Sub(lastUsed[a.ID]) < throttle {
			continue
		}
		perRegion, limit := s.mint.Rate(a.Creds.AccountID)
		total := 0
		for _, n := range perRegion {
			total += n
		}
		pick, picked, ok := bestRegion(want, perRegion, limit, lifetime[a.ID])
		if !ok {
			continue // 九个区域（或点名的那一个）本小时都打满了
		}
		cs = append(cs, cand{a: a, r: pick, sent: total, unseen: picked == 0, used: lastUsed[a.ID]})
	}
	if len(cs) == 0 {
		return types.Account{}, types.Region{}, false
	}
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].sent != cs[j].sent {
			return cs[i].sent < cs[j].sent
		}
		if cs[i].unseen != cs[j].unseen {
			return cs[i].unseen
		}
		if !cs[i].used.Equal(cs[j].used) {
			return cs[i].used.Before(cs[j].used)
		}
		return cs[i].a.ID < cs[j].a.ID
	})
	return cs[0].a, cs[0].r, true
}

// bestRegion 给一个号挑它最该去的区域：还有本小时配额的里面，终身铸得最少的那个。
// 返回挑中的区域、它的终身计数、以及「还有没有配额」。
//
// limit <= 0 当「不设限」：只有没接配置的裸 mint.Client 会这样，生产的
// mint.perRegionHour 永远回正数。
func bestRegion(want types.Exit, perRegion map[string]int, limit int, lifetime map[string]int) (types.Region, int, bool) {
	var best types.Region
	bestCount, found := 0, false
	for _, r := range candidateRegions(want) {
		if limit > 0 && perRegion[r.Key] >= limit {
			continue
		}
		n := lifetime[r.Key]
		if !found || n < bestCount {
			best, bestCount, found = r, n, true
		}
	}
	return best, bestCount, found
}

// candidateRegions 是这一发可以走哪些区域。
//
// 定向铸票时只有那个出口本身，而它的区域 key 是反查出来的——反查不到就是空串，
// 和速率闸的分桶算法一致（见 mint.rateKey），于是配额问的和真正要花的是同一格。
func candidateRegions(want types.Exit) []types.Region {
	if want == (types.Exit{}) {
		return types.Regions
	}
	return []types.Region{{Key: types.RegionKeyOf(want), Exit: want}}
}

// maxTries 是一次交付最多试几个网关。
//
// Options 里给了就按它（测试用），否则现读配置 ⇒ 改完立即生效。
// 默认值只有一份，在 config.Defaults 里。
func (s *Scheduler) maxTries() int {
	if s.opt.MaxTriesPerDeliver > 0 {
		return s.opt.MaxTriesPerDeliver
	}
	return s.cfg().MaxTriesPerDeliver
}

// splitByPair 把候选网关分成「此刻有活 pair 的」和「要现铸的」两拨。
//
// 组内的排序仍然交给 LongestRestedSlot，所以「从没碰过的网关优先」照旧成立——
// 注意它给不存在的槽位行合成一个零值 Slot（LastTouch 是零时刻），于是没碰过的
// 永远排在碰过的前面。硬筛加在它前面之后这个行为没有坏处（组内全是出了冷却的槽位），
// 但别以为零值会被跳过。
func (s *Scheduler) splitByPair(cands []string, floor time.Duration) (withPair, without []string) {
	withPair = make([]string, 0, len(cands))
	without = make([]string, 0, len(cands))
	for _, name := range cands {
		if _, fresh := s.freshPair(name, floor); fresh {
			withPair = append(withPair, name)
		} else {
			without = append(without, name)
		}
	}
	return withPair, without
}

// freshPair 判断池里那张 pair 还够不够交付：剩余不足 floor 就别发，
// 否则 65 分钟硬过期会在消费者手里到点（SPEC 第 6 节）。
//
// floor 由调用方给（Request.Floor）：默认是 types.DeliverFloor，而调用方可以用
// min_remaining 把它压低到 30 s —— 「我这一发很短，残票也要」是只有它自己知道的事。
func (s *Scheduler) freshPair(gateway string, floor time.Duration) (types.Pair, bool) {
	p, ok := s.st.Pair(gateway)
	if !ok {
		return types.Pair{Gateway: gateway}, false
	}
	return p, p.ExpiresAt.Sub(s.now()) >= floor
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

// redeliver 把窗口内的重复请求原样答一遍：同一张 pair、剩余窗口当有效期，
// **一发上游都不打、不重置冷却、不烧新槽位**。
//
// 为什么必须有这条：DeliverTTL 这个窗口属于 (消费账号 × 网关)，不是那张 pair 的租约。
// 窗口里的客户端重连、重试、并发几路，问的都是同一个槽位，而重走一遍交付流程
// 会去烧**下一个**落点——供给只有个位数，这么漏一轮就没了。
//
// gatewayHint 为空时取他窗口里最近那一次交付：那就是他手上正在用的那张。
//
// 锚点就是槽位上的 LastDeliver，而那个字段存的是**窗口起算时刻**（首次接触那一发发出去
// 的时间，见 noteDeliver），不是「交付返回的时刻」。两者差一个 20~90 s 的往返：按后者算
// 的话，客户端在 start+140s 重连会被告知还剩 70 秒，而真实窗口只剩 10 秒，
// 池子顺带把一张窗口已关的 pair 原样发了出去。
func (s *Scheduler) redeliver(accountID int64, req Request) (types.Delivery, bool) {
	now := s.now()
	var best types.Slot
	for _, sl := range s.st.Slots() {
		if sl.AccountID != accountID || sl.LastDeliver.IsZero() {
			continue
		}
		if req.Gateway != "" && sl.Gateway != req.Gateway {
			continue
		}
		if now.Sub(sl.LastDeliver) >= types.DeliverTTL {
			continue
		}
		if best.LastDeliver.Before(sl.LastDeliver) {
			best = sl
		}
	}
	if best.Gateway == "" {
		return types.Delivery{}, false
	}
	// 那张票可能已经被续掉或硬过期了。续掉没关系（新旧钉的是同一个网关、路由一样），
	// 死掉就只能走正常流程重新来——发一张已经过期的 cookie 等于把他送去随机网关。
	p, ok := s.st.Pair(best.Gateway)
	if !ok || !p.ExpiresAt.After(now) {
		return types.Delivery{}, false
	}
	// 排除名单也要在这条路上生效：他带着 exclude 来，就是**不想要**手上那张了。
	if req.Skips(best.Gateway, p.Cookie) {
		return types.Delivery{}, false
	}
	exp := best.LastDeliver.Add(types.DeliverTTL)
	left := exp.Sub(now)
	// 两头都要够一秒：窗口剩不到一秒没意义，而那张 pair 剩不到一秒会让
	// valid_for_s 取小之后变成 0 —— 消费端（sub2api）把非正数当成坏响应直接丢弃。
	if left < time.Second || p.ExpiresAt.Sub(now) < time.Second {
		return types.Delivery{}, false
	}
	s.opt.Log.Info("窗口内重复请求，原样再发一次", "account", accountID,
		"gateway", best.Gateway, "left", left.Truncate(time.Second))
	return types.Delivery{
		Gateway:   best.Gateway,
		Cookie:    p.Cookie,
		ExpiresAt: exp,
		ValidForS: validFor(left, p.ExpiresAt.Sub(now)),
		// 照第一次的判定如实报：这一次什么都没验，不该凭空升级成 true。
		VerifiedFull: best.LastVerdict == types.VerdictFull,
	}, true
}

// notCooling 滤掉还在冷却里的候选网关。
//
// 冷却只看 LastDeliver（真的有人在这个网关上取走过一张票），**不看 LastTouch**：
// 后者连失败的续期、保温、扫描都记，拿它当冷却起点会把一个从没交付过的好槽位
// 按住几个小时。
//
// 匿名和跨账号的占位行一视同仁：它们的交付也写 LastDeliver，而「这个上游账号在
// 这个网关上烧过了」这件事和他有没有上传凭据毫无关系。
func (s *Scheduler) notCooling(accountID int64, cands []string, cooldown time.Duration) []string {
	cooling := s.coolingSet(accountID, cooldown)
	out := make([]string, 0, len(cands))
	for _, name := range cands {
		if cooling[name] {
			continue
		}
		out = append(out, name)
	}
	return out
}

// coolingSet 是这个账号此刻**不能**给满血的那些网关。
//
// 两条闸，各管一件事：
//
//  1. LastDeliver 在 cooldown 内——票真发出去过，窗口用完了，要歇满冷却
//  2. LastTouch 在一个满血窗口内、**且此刻不在退避中**——预铸、扫描、别人的
//     续期都会 touch，而那一发就在眼前（183 s 内），交出去等于明知故犯
//
// 第 2 条的豁免是「**此刻仍在**退避中」，不是「有过退避记录」：触碰也可能来自一次
// 失败的尝试（铸票超时按保守方向记了触碰），那种情形归指数退避管，两层闸叠起来会让
// 退避永远到不了第二级。但 BackoffUntil 只被写、只在**该槽位成功交付时**才清零
// ⇒ 按「字段非零」豁免等于让这条闸对任何失败过一次的槽位永久沉默：
// 一次铸票超时同时写 LastTouch=T 和 BackoffUntil=T+10s，T+10s 之后退避到点、
// 闸 1 没有 LastDeliver 不管、闸 2 被豁免 ⇒ 这个槽位在它自己正在烧的满血窗口里
// （剩下 123 秒）被当成首次接触发出去。backoffMax 只有 1 分钟，管不到窗口尾部。
// 正在退避的槽位 LongestRestedSlot 本来就跳过，豁免它不损失任何东西。
//
// 而且**不拿 LastTouch 当冷却起点**：它连失败的续期都记，那会把好槽位按住几个小时
// （同一个坑在 premintWasted 那边也写着）。
func (s *Scheduler) coolingSet(accountID int64, cooldown time.Duration) map[string]bool {
	now := s.now()
	out := map[string]bool{}
	for _, sl := range s.st.Slots() {
		if sl.AccountID != accountID {
			continue
		}
		switch {
		case !sl.LastDeliver.IsZero() && now.Sub(sl.LastDeliver) < cooldown:
			out[sl.Gateway] = true
		case !now.Before(sl.BackoffUntil) && !sl.LastTouch.IsZero() && now.Sub(sl.LastTouch) < types.FullWindow:
			out[sl.Gateway] = true
		}
	}
	return out
}

// coolingByDeliver 是闸 1 单独一问：这个槽位在 cooldown 内真的交付过票吗。
//
// 给「铸完之后重查一次」用（见 deliverByFreshMint）：那条路必须用铸票前的快照判闸 2，
// 但闸 1 在铸票那几十秒里可能被同账号另一路交付写上，而快照看不见。
func (s *Scheduler) coolingByDeliver(accountID int64, gateway string, cooldown time.Duration) bool {
	sl, ok := s.st.Slot(accountID, gateway)
	return ok && !sl.LastDeliver.IsZero() && s.now().Sub(sl.LastDeliver) < cooldown
}

// coolingWait 是这些候选里最早一个出冷却还要多久。一个都算不出来就回 0。
//
// 只看闸 1（LastDeliver）：闸 2 那 183 秒和「等多久能再取票」不是一回事——
// 它过去之后槽位仍然可能在闸 1 里躺着几个小时，报小了等于骗客户端马上再来一发。
func (s *Scheduler) coolingWait(accountID int64, cands []string, cooldown time.Duration) time.Duration {
	want := make(map[string]bool, len(cands))
	for _, name := range cands {
		want[name] = true
	}
	now := s.now()
	best := time.Duration(0)
	for _, sl := range s.st.Slots() {
		if sl.AccountID != accountID || !want[sl.Gateway] || sl.LastDeliver.IsZero() {
			continue
		}
		left := sl.LastDeliver.Add(cooldown).Sub(now)
		if left <= 0 {
			continue
		}
		if best == 0 || left < best {
			best = left
		}
	}
	return best
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

// markUnverified 把判定写成 unknown —— **除非已经是 degraded**。
//
// degraded 是花一发 state-echo 换来的唯一读数，而走到这儿的那一发什么都没验
// （verify_full_at_deliver 关着）。无条件写 unknown 会把它洗掉，
// 于是页面上再也看不出哪个槽位真的验坏过。
func (s *Scheduler) markUnverified(accountID int64, gateway string) {
	_ = s.updateSlot(accountID, gateway, func(sl *types.Slot) {
		if sl.LastVerdict != types.VerdictDegraded {
			sl.LastVerdict = types.VerdictUnknown
		}
	})
}

func (s *Scheduler) sample(sm types.Sample) {
	if err := s.st.AppendSample(sm); err != nil {
		s.opt.Log.Warn("写样本失败", "account", sm.AccountID, "err", err)
	}
}

// recordEcho 把 state-echo 那一发也记进样本：它同样是一次用上传者凭据发出的真实请求，
// 账本漏了它上传者视图就不诚实了。
func (s *Scheduler) recordEcho(accountID int64, e types.Exit, gateway string, obs types.Observed, v *types.Verdict, err error, at time.Time) {
	status := 0
	note := ""
	if v != nil {
		status, note = v.Status, v.Note
	}
	sm := types.Sample{
		AccountID: accountID, RegionKey: exitKey(e), Requested: e, Observed: obs,
		Gateway: gateway, Status: status, At: at,
	}
	// 只在失败那几行填原因：Note 在满血那一发也有值，填上去就把「成功时为空」这个
	// 约定破了，账本里再也分不清哪行是真出过问题。
	if err != nil || status != 200 {
		sm.Err = types.SampleErr(note, err)
	}
	s.sample(sm)
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

// sampleRegion 是样本里那一列区域 key。
//
// 反查不到就退回「国家[:城市]」：账本和页面是给人看的，留个能认的字比空着好
// （而速率闸那边刻意不这么兜底——配额必须按真区域分桶，见 mint.rateKey）。
func sampleRegion(e types.Exit) string {
	if k := types.RegionKeyOf(e); k != "" {
		return k
	}
	return exitKey(e)
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
//
// at 是**满血窗口的起算时刻**（首次接触那一发发出去的时间），不是「现在」：
// 交付要先铸票、也许还要 echo，一个往返 20~90 s。存 now() 的话窗口内的重复请求
// （redeliver）会按一个往后挪了一个往返的锚点算有效期——真实窗口只剩 10 秒时池子
// 会承诺 70 秒，还把一张窗口已关的 pair 原样发出去；冷却也会跟着晚算一个往返。
//
// touchSelf = 这次交付自己记一次触碰（池子一发上游都没替这个消费者打，票是别人铸的，
// 纯保守记账）。false = 触碰已经由真实请求记过了，别覆盖它。
//
// 三个字段必须在**同一次**读-改-写里落下：Prev* 是「这次交付之前」的两个时刻，
// 分两次写就会把这次交付自己写进去的值当成旧值存下来，/release 还原完槽位看起来
// 还是刚烧过的。
func (s *Scheduler) noteDeliver(accountID int64, gateway string, at time.Time, touchSelf bool) {
	// 走 updateSlot 这个包装（而不是直接 st.UpdateSlot）：它负责补上 AccountID /
	// Gateway / 初始判定。这条路现在可能是一条新槽位行的**唯一**写入者
	// （池子一发上游都没替消费者打的时候），字段补不全的话那行就没有网关名，
	// 下一次挑选会拿它当候选、而它谁也不是。
	_ = s.updateSlot(accountID, gateway, func(sl *types.Slot) {
		sl.PrevDeliver, sl.PrevTouch = sl.LastDeliver, sl.LastTouch
		if touchSelf {
			sl.LastTouch = at
		}
		sl.LastDeliver = at
	})
}

// ErrNoRelease 是「这张票没得还」：过了受理窗口、找不到、或者已经还过了。
var ErrNoRelease = errors.New("没有可归还的交付")

// Release 还原一次「票取了但一发请求都没发出去」的交付，返回还原了几个槽位。
//
// 为什么值得有：交付一张票要烧掉一个 (账号 × 网关)，而那是整个池子最紧的资源
// （十几个网关、每个烧完歇几小时 ⇒ 每账号每小时只有 3.5 个左右）。消费端临时改主意、
// 上游先一步 503、请求在它自己那头被取消——这些情形里那个槽位其实一点没动，
// 不还回来就是白扔一个小时的供给。
//
// 只在交付后 DeliverTTL 内受理：过了这个窗口他很可能已经用过了，而「他说没用过」
// 池子无从验证。**失败即安全**：没人调它时行为和以前完全一样。
//
// 乱报只害他自己（拿回一个其实已经烧掉的槽位，下一发就是降智的），
// 所以和匿名那个 account 参数一样不设防（见 anonAccount）。
func (s *Scheduler) Release(accountID int64, cookieVersion string) (int, error) {
	now := s.now()
	n := 0
	for _, sl := range s.st.Slots() {
		if sl.AccountID != accountID || sl.LastDeliver.IsZero() {
			continue
		}
		if now.Sub(sl.LastDeliver) >= types.DeliverTTL {
			continue // 过了受理窗口
		}
		// 按票本身认，不按网关：同一个网关换过一张新 pair 之后，他手上那张已经不是
		// 这一次交付的票了，还原就成了凭空多给一个槽位。
		p, ok := s.st.Pair(sl.Gateway)
		if !ok || types.CookieVersion(p.Cookie) != cookieVersion {
			continue
		}
		prevDeliver, prevTouch := sl.PrevDeliver, sl.PrevTouch
		if err := s.st.UpdateSlot(accountID, sl.Gateway, func(x *types.Slot) {
			x.LastDeliver, x.LastTouch = prevDeliver, prevTouch
			// 还原点用完就清掉：第二次 release 要回 409，而不是把槽位还到更早。
			x.PrevDeliver, x.PrevTouch = time.Time{}, time.Time{}
		}); err != nil {
			return n, err
		}
		s.opt.Log.Info("归还槽位", "account", accountID, "gateway", sl.Gateway)
		n++
	}
	if n == 0 {
		return 0, ErrNoRelease
	}
	return n, nil
}
