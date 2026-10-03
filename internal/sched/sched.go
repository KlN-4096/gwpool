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
	"gwpool/internal/mint"
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
	NoteRegionMint(accountID int64, regionKey, gateway string, at time.Time) error

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

	// NewSeedSID 要一个绑在普通机房出口上的 sid，给「带种子 `__cflb` 铸票」用。
	//
	// 它**不粘性**（同一个 sid 不保证同一个出口 IP），所以只能用在「路由由 cookie
	// 决定、出口无所谓」的场合，不能用来做需要配对的 state-echo。
	// 没配这条出口就回错，调用方据此回落到住宅出口。
	NewSeedSID() (string, error)

	// SeedBound 报这个 sid 是不是绑在种子出口上。
	//
	// mintOnce 要它来分辨「出口国家不符」是真漂移还是种子铸票的常态：种子按构造就是
	// 从便宜机房出去、靠 `__cflb` 把路由钉到目标大区 ⇒ 观测到的国家必然不是目标大区。
	// **问绑定表而不是看调用方传了什么**：`cur` 这个参数在定向铸票那条路上也非空
	// （带着现有 pair 回去），拿它当种子判据会把定向铸票误判成种子。
	SeedBound(sid string) bool

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

// ErrAccountUnusable：点名的这个池内账号用不了（不存在、已停用、或凭据已死）。
//
// 必须和「池子暂时没货」分开：运营方逐个启用账号来验、或者管理员拿
// ?account=<已停用的号> 取票时，回「没有可交付的活票，稍后再试」会让他等下去
// （还挂满 wait、还去排发现扫描），而真实原因是那个号本身——等多久都不会变。
var ErrAccountUnusable = errors.New("这个账号用不了（不存在、已停用、或没有凭据）")

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

// CoolingError 是「这个账号在所有候选网关上的满血窗口此刻都在烧」。
//
// 2026-10-03 之前它说的是「都还在 SlotCooldown（4 小时）的消费冷却里」，而那条闸已经
// 删了（见 burning）—— 现在只剩「池子此刻正在拿这个号的凭据打上游」，**183 秒量级**。
// Cooldown 那个字段跟着删了：留着就会在错误串里打一个不再管事的 4h。
//
// 带 Wait 是因为 HTTP 那头要把它变成 Retry-After，而现在这个数小得多，
// 客户端等一下真的能拿到票（原先报 7200 秒）。
// Unwrap 回 ErrNoFullSlot ⇒ 调用方按它回 503、挂发现扫描，分流一个字都不用改。
type CoolingError struct {
	Gateways int           // 候选网关数
	Wait     time.Duration // 最早一个槽位烧完窗口还要多久；0 = 算不出来
}

func (e *CoolingError) Error() string {
	return fmt.Sprintf("池子此刻正在这个账号的 %d 个候选网关上烧满血窗口（最早 %v 后出来）",
		e.Gateways, e.Wait.Truncate(time.Second))
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
	// RenewGap 是**同一个账号**两次铸票之间的最小间隔（名字是续期时代留下的）。
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

	backgroundMu sync.Mutex // 自动保温与管理员全量队列共享后台并发预算。
	mu           sync.Mutex
	backoff      map[string]time.Duration // 槽位 → 当前指数退避时长；成功归零
	// lastMintedAt 记每个账号最近一次被用作铸票者，用来把铸票摊到各个号上。
	// 不落盘：重启后重新摊平没有坏处，而落盘就多一份要维护的状态。
	// 「总共铸过几发」刻意**不**自己记：那个读数在 mint 的速率窗口里，
	// 而那份是连 scan 那条路的发数一起算的，自己再记一份只会和它分叉。
	lastMintedAt map[int64]time.Time
	lastDeliver  time.Time
	// gwLocks 是「同一个网关一次只许有一发在铸」，见 lockGateway。
	gwLocks map[string]*sync.Mutex
	// mintingRegions 是「同一个区域一次只许有一发裸铸在打」，见 claimRegion。
	// 是个**标记**不是锁：裸铸出手前不知道落哪，拦不住只能按区域拦，
	// 而按区域上真锁会让两路交付在走到账号锁之前就互等（见 claimRegion 的注释）。
	mintingRegions map[string]bool
	// acctHold 是「上游 429 把这个账号按住了」的到点，见 holdAccount。
	//
	// 为什么要**账号级**而不是继续用槽位级的 bumpBackoff：槽位退避封顶只有 60 秒
	// （backoffMax），而额度窗口是小时级的 ⇒ 池子会对着一个彻底打不动的号按
	// 「每槽位每分钟一发」的节奏白打，上限只被速率闸挡着（6 发/区/时 × 9 区 = 54 发/时）。
	// 那 54 发不只是浪费，它们**吃掉的正是这个号本来能干正经铸票的配额**。
	//
	// 不落盘：进程重启本来就该重新探一遍（同 backoff 的理由），而落盘会把一次
	// 误判带过重启。
	acctHold map[int64]time.Time
}

// 上游 429 的账号级退避时长。
const (
	// acctHoldDefault 是上游没给 Retry-After 时的默认时长。按一小时：速率闸的窗口
	// 就是一小时，而 free 的额度窗口也是这个量级。
	acctHoldDefault = time.Hour
	// acctHoldMax 钳住上游给的 Retry-After。不钳的话上游报一个 72 小时就把这个号
	// **永久饿死**（同 sub2api 的 gwpool.MaxRetryAfter 一个道理）。
	acctHoldMax = 2 * time.Hour
)

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
		backoff: map[string]time.Duration{}, lastMintedAt: map[int64]time.Time{},
		acctHold: map[int64]time.Time{}}
}

func (s *Scheduler) Mode() types.Mode { return s.opt.Mode }

func (s *Scheduler) now() time.Time { return s.opt.Now() }

// cfg 取当前配置快照。一个判断只取一次：两次 Get() 之间配置可能变了。
func (s *Scheduler) cfg() config.Config { return s.opt.Live.Get() }

// Deliver 为 accountID 这个账号交付一张 pair。req.Gateway 非空时只考虑那个网关。
//
// **「验过满血」不是这条路的承诺**：verify_full_at_deliver 默认**关**
// （验证要用消费者自己的凭据多打两发上游，是成本取舍，见 config.Config.VerifyFullAtDeliver），
// 所以默认交出去的票 verified_full=false。保证来自调度本身：候选都出了冷却、都不在
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
		// 带上停用理由：页面上「手动暂停」和「凭据已死」长得一样，而后者要换票。
		return types.Delivery{}, fmt.Errorf("%w：账号 %d%s",
			ErrAccountUnusable, accountID, s.disabledNote(accountID))
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

	if force {
		kept := make([]string, 0, len(cands))
		for _, name := range cands {
			if s.holding(consumer.ID, name) {
				continue
			}
			kept = append(kept, name)
		}
		if len(kept) == 0 {
			// 他手上那张就是唯一的候选 ⇒ 没得换，和「全在烧」归同一个码。
			return types.Delivery{}, &CoolingError{Gateways: len(cands),
				Wait: s.burnWait(consumer.ID, cands)}
		}
		cands = kept
	}

	// 只滤掉池子此刻正在烧窗口的那些（183 秒量级，见 burning）。消费冷却不在这儿判了：
	// 「这个号在这个网关上取过票了」归客户端账本管。
	rested := s.notBurning(consumer.ID, cands)
	if len(rested) == 0 {
		return types.Delivery{}, &CoolingError{Gateways: len(cands),
			Wait: s.burnWait(consumer.ID, cands)}
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
	withPair := s.gatewaysWithPair(cands, req.Floor())
	// 「有票的网关里挑不出槽位」和「一张票都没有」要走同一条回落：LongestRestedSlot
	// 跳过正在退避的槽位 ⇒ 有票的那几个全在退避里时，这里不回落就会 503 而且一发不打，
	// 可现铸本来能成（退避窗口只有 10~60 秒，而另有一个没票但能铸的候选）。
	if len(withPair) > 0 {
		if _, ok := s.st.LongestRestedSlot(consumer.ID, withPair); !ok {
			withPair = nil
		}
	}
	if len(withPair) == 0 {
		// 点名了网关就定向铸（落点漂了算失败）：他点名是因为**他**知道自己没烧过那个
		// 落点，给他换一个等于没听他说话。没点名才是抽签。
		d, gateway, err := s.deliverByFreshMint(ctx, consumer, cands, req)
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

// HasCandidate 报告「此刻有没有一张这个账号拿得到、而且不用铸的票」。
//
// 这是 HTTP 那边 wait 轮询的判据，**必须和 Deliver 的前半段是同一把尺子**，所以
// 它就住在这儿、照抄那个顺序（排除 → force 排掉手上那张 → 冷却 → 只看有活票的）。
// 判据分成两份的后果是实打实的浪费：少了 accountID 就应用不了那两条冷却闸，而
// 「刚交付过的网关在冷却里、它那张 pair 还活着」是这个池子最常见的稳态
// （冷却 4 小时 ≫ pair 寿命 65 分钟）⇒ 每一次等待都会看见一张自己拿不到的票、
// 立刻重跑、白铸一发（1 发请求 + 2 个槽位）。
//
// 门槛就是「有现成票」：候选为空时 Deliver 会去现铸，而那正是等待想省掉的那一发——
// 所以这里只在**不用铸**的时候才说有。
func (s *Scheduler) HasCandidate(accountID int64, req Request) bool {
	// 窗口内重连那条路一发上游都不打，所以它成立就是「有候选」。
	// redeliver 是纯读（一个字段都不写），这样问没有副作用。
	if !req.Force {
		if _, ok := s.redeliver(accountID, req); ok {
			return true
		}
	}
	cands := make([]string, 0, 8)
	for name := range s.st.Gateways() {
		if req.Gateway != "" && name != req.Gateway {
			continue
		}
		if p, _ := s.st.Pair(name); req.Skips(name, p.Cookie) {
			continue
		}
		if req.Force && s.holding(accountID, name) {
			continue
		}
		cands = append(cands, name)
	}
	if len(cands) == 0 {
		return false
	}
	withPair := s.gatewaysWithPair(s.notBurning(accountID, cands), req.Floor())
	if len(withPair) == 0 {
		return false
	}
	// 还要问一次「这里面挑得出槽位吗」：LongestRestedSlot 会跳过**正在退避**的槽位，
	// 而那条闸恰好对正在退避的槽位豁免（见 burning）⇒ 少了这一问，刚失败过的槽位
	// 会被当成候选，等到的那一下重跑仍然是 503。
	_, ok := s.st.LongestRestedSlot(accountID, withPair)
	return ok
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
	if nothingWentOut(err) {
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
func (s *Scheduler) FreeGateways() int { return len(s.liveGateways()) }

// liveGateways 是此刻有一张「还够交付」的 pair 的网关集合。
//
// 预铸要的是集合不只是个数：「我预测要落到的那个网关现在有没有活票」才是
// 「这一发会不会白打」的判据，而旧版那个「铸完看总数涨没涨」的比对会把
// 「别的票刚好在这几秒里过期」算成自己白打了。
func (s *Scheduler) liveGateways() map[string]bool {
	now, best := s.now(), s.bestCflb()
	out := map[string]bool{}
	for name := range s.st.Gateways() {
		if p, ok := s.st.Pair(name); ok {
			if s.rehome(p, best).ExpiresAt.Sub(now) >= types.DeliverFloor {
				out[name] = true
			}
		}
	}
	return out
}

// bestCflb 按大区挑出最长寿的那张 `__cflb`（实现在 mint，页面那边共用同一份）。
func (s *Scheduler) bestCflb() map[string]mint.CflbLease {
	pairs := map[string]types.Pair{}
	for name := range s.st.Gateways() {
		if p, ok := s.st.Pair(name); ok {
			pairs[name] = p
		}
	}
	return mint.BestCflb(pairs, s.now())
}

func (s *Scheduler) rehome(p types.Pair, best map[string]mint.CflbLease) types.Pair {
	return mint.Rehome(p, best, s.now())
}

// PublicState 是「此刻对外开不开放」的全部读数。公开页要显示它，这是对外承诺。
type PublicState struct {
	Enabled bool // 开关本身开没开
	Open    bool // 此刻放不放匿名进来
	// Accounts 是此刻可用的托管账号数，Threshold 是够格所需的个数。
	// **刻意不是活 pair 数**：见 types.AccountsForContinuous——按需铸票的池子不囤票，
	// 按货架存量判等于把对外开放永远焊在关闭上。
	Accounts  int
	Threshold int
}

// Public 现算一次开放状态。
//
// 开关和门槛**现读配置**，不读 New() 时的快照：管理页上那个按钮写着「保存并立即生效」，
// 而把门槛调紧是运营方最可能的紧急动作——匿名这条路唯一的让路机制就是它
// （pair 本身不互斥，见 types.Pair）。读快照的话日志说改了、页面说生效了，实际没变。
//
// 账号数用 provenAccounts()**不是 len(usable())**：/upload 不验签，贴几份自制 JWT 就能
// 让 usable() 过线、把 /cookie 对全互联网免 key 打开（审查 S3）。真能贡献落点的账号
// 必须至少成功铸过一次票，那是伪造不出来的。代价：冷池或没人用过的池子里，这个数要等
// 账号被真正用起来才涨——但那正合理，一个还没铸出任何落点的池子本来就服务不了匿名。
func (s *Scheduler) Public() PublicState {
	c := s.cfg().PublicWhenSurplus
	st := PublicState{Enabled: c.Enabled, Accounts: s.provenAccounts(), Threshold: c.MinAccounts}
	st.Open = st.Enabled && st.Accounts >= st.Threshold
	return st
}

// provenAccounts 数「可用且至少成功铸过一次票」的托管账号。
//
// RegionMints 只记成功的铸票（见 types.RegionMint），所以有记录 = 真的打通过上游一次，
// 伪造的 access_token 打 /responses 会吃 400、永远攒不出记录。停用的、没凭据的、以及
// 只上传没铸过的都不算——它们此刻贡献不了可交付的落点。
func (s *Scheduler) provenAccounts() int {
	proven := map[int64]bool{}
	for _, m := range s.st.RegionMints() {
		if m.Count > 0 {
			proven[m.AccountID] = true
		}
	}
	n := 0
	for _, a := range s.usable() {
		if proven[a.ID] {
			n++
		}
	}
	return n
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
//  3. **严格低优先级**——托管账号数跌破门槛就拒，哪怕货架上还剩一张票。门槛是给
//     上传者留的余量，也是匿名这条路唯一的让路机制（pair 本身不互斥，见 types.Pair）。
//
// authed = 调用方**鉴权过了**，只是替别的上游账号取票（一把上传者 key 替他自己手上
// 好几个号用）。那种调用方走这个函数只因为前两条（池子没有那个号的凭据，验不了也
// 不替它铸），**第三条对他不成立**：门槛是给上传者留的余量，不该把上传者本人挡在
// 门外。不跳过的话默认配置下他每一发都是 503 public_closed，而那个码按契约说的是
// 「匿名此刻不开放」，对一个鉴权过的调用方是错的。
func (s *Scheduler) DeliverAnonymous(ctx context.Context, upstreamID string, req Request, authed bool) (types.Delivery, error) {
	if !authed {
		if ps := s.Public(); !ps.Open {
			return types.Delivery{}, fmt.Errorf("%w（托管账号 %d，门槛 %d）",
				ErrPublicClosed, ps.Accounts, ps.Threshold)
		}
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
	cands := make([]string, 0, len(s.st.Gateways()))
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
	// force 和「正在烧」分两步筛、各自报错：和上传者那条路同一条规则——
	// 筛完为空就是「池子此刻正在为你烧这些窗口」，而「一张活票都没有」是另一码事
	// （上面那个循环就把它筛成空了）。两个都等得起，只是该等多久不一样。
	if force {
		kept := make([]string, 0, len(cands))
		for _, name := range cands {
			if s.holding(a.ID, name) {
				continue // 他手上那张不行了，排掉他正在用的
			}
			kept = append(kept, name)
		}
		if len(kept) == 0 && len(cands) > 0 {
			return types.Delivery{}, &CoolingError{Gateways: len(cands),
				Wait: s.burnWait(a.ID, cands)}
		}
		cands = kept
	}
	// 对占位行一视同仁：「池子刚拿这个上游账号的凭据打过一发」和他有没有上传凭据无关。
	rested := s.notBurning(a.ID, cands)
	if len(rested) == 0 && len(cands) > 0 {
		// 告诉他最早什么时候回来，别让他按默认节奏重试。
		return types.Delivery{}, &CoolingError{Gateways: len(cands),
			Wait: s.burnWait(a.ID, cands)}
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
		s.noteDeliver(a.ID, sl.Gateway, at, true, p.Cookie)
		// 运营方靠这行判断门槛设得对不对，以及这些票到底是匿名拿走的还是
		// 鉴权过的跨账号调用方拿走的（后者不受门槛约束）。
		s.opt.Log.Info("无凭据交付", "upstream", upstreamID, "account", a.ID,
			"gateway", sl.Gateway, "authed", authed, "free_gateways", s.FreeGateways())
		exp, validFor := s.deliverWindow(at.Add(types.DeliverTTL), p.ExpiresAt)
		return types.Delivery{
			Gateway:      sl.Gateway,
			Region:       p.RegionKey,
			Cookie:       p.Cookie,
			ExpiresAt:    exp,
			ValidForS:    validFor,
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

// tryGateway 在一个选中的网关上交付那张现成的票。
//
// **这条路一发上游都不打**（开了交付前验证的话由 finish 自己去打）：走到这儿的网关
// 是 Deliver 从「有活票」那一拨里挑出来的，铸票只归 deliverByFreshMint。
// exit 只是传给 finish 备用（验证那一发要用它）。
func (s *Scheduler) tryGateway(ctx context.Context, consumer types.Account, gw types.Gateway, req Request) (types.Delivery, error) {
	exit, ok := pickExit(gw)
	if !ok {
		return types.Delivery{}, fmt.Errorf("网关 %s 没有已知可达出口", gw.Name)
	}
	floor := req.Floor()

	pair, fresh := s.freshPair(gw.Name, floor)
	if !fresh {
		// 走到这儿的网关是从「有活票」那一拨里挑出来的，唯一的到法是选中到这儿之间
		// 那张票正好跨过门槛。**不在这条路上铸**：要带进去的就是这张「还有效、只是太短」
		// 的 pair，而上游会接受它、不下发新 cookie（SPEC 第 2 节）⇒ finish 照样拒发，
		// 请求和铸票者的槽位白花（见 pairFor 的注释）。回落铸票归 deliverByFreshMint。
		return types.Delivery{}, fmt.Errorf("网关 %s 的 pair 刚跌破门槛（剩 %v），这一轮不发",
			gw.Name, pair.ExpiresAt.Sub(s.now()).Truncate(time.Second))
	}
	return s.finish(ctx, consumer, gw.Name, exit, pair, minted{}, floor)
}

// deliverByFreshMint 是「一张活 pair 都没有」时的回落：**只铸一发**。
//
// 分两条路，差别在「出口怎么来」：
//
//   - 调用方点名了网关 ⇒ 定向铸（mintToward）：出口只能从那个网关的已知可达出口里挑，
//     落点漂了算失败。他点名是因为**他**知道自己没烧过那个落点，给他换一个等于没听他说话。
//   - 没点名（常态）⇒ 按区域铸（mintByRegion）：先定派谁去，再从那个号没走过的区域出发。
//
// 两条都只铸一发：出口定不住落点，「先试网关 X 再试网关 Y」是抽两次签、花两发请求烧两个
// 铸票者槽位。铸完看落点上消费者出不出冷却，出了就交付，没出就老实 503，让发现路径
// （httpapi 在 ErrNoFullSlot 上挂的那一发）去找新落点。这也是为什么 MaxTriesPerDeliver
// 只管「有票的网关一个个试」那条路。
//
// 第二个返回值是这一发**实际落在**哪个网关上（落点还不知道时是空串，noteTryFailure 认它
// = 没有槽位可记），调用方拿它记退避和交付——善后规则在 Deliver 那边，只有一份。
func (s *Scheduler) deliverByFreshMint(ctx context.Context, consumer types.Account,
	cands []string, req Request) (types.Delivery, string, error) {
	sl, ok := s.st.LongestRestedSlot(consumer.ID, cands)
	if !ok {
		return types.Delivery{}, "", fmt.Errorf("%w（候选网关全在退避期）", ErrNoFullSlot)
	}
	floor := req.Floor()

	// 「正在烧」要在**铸票之前**定格：铸票那一发自己会 touch 消费者的槽位
	// （铸票者就是他本人时），拿铸完之后的账本去判「他的窗口还在烧吗」，
	// 答案恒为是——自己把自己判死。
	burningBefore := s.burning(consumer.ID)

	if req.Gateway != "" {
		return s.mintToward(ctx, consumer, sl.Gateway, burningBefore, req, floor)
	}
	return s.mintByRegion(ctx, consumer, burningBefore, req, floor)
}

// mintToward 是定向铸：出口从目标网关的 ReachableExits 里挑，落点漂了算失败。
func (s *Scheduler) mintToward(ctx context.Context, consumer types.Account, gateway string,
	burningBefore map[string]bool, req Request,
	floor time.Duration) (types.Delivery, string, error) {
	exit, ok := pickExit(s.st.Gateways()[gateway])
	if !ok {
		return types.Delivery{}, gateway,
			fmt.Errorf("网关 %s 没有已知可达出口，没法铸票", gateway)
	}
	release := s.lockGateway(gateway)
	// 别人可能刚为这个网关铸好了票——那就直接用，省下这一发。
	if p, fresh := s.freshPair(gateway, floor); fresh {
		release()
		// 闸 2 在这一支要**现读**，不能用 coolingBefore：等这把锁花掉的 20~90 秒里
		// 并没有「消费者自己铸票」这回事（那一发是别人打的），所以现读不会自己把自己
		// 判死；而池子模式下 minterFor 常常就挑中另一个消费者，于是等到的那张票恰好
		// 是**他自己**刚铸的 ⇒ 他对这个网关的满血窗口已经烧了几十秒，再按 now() 起算
		// 窗口会多报最多 57 秒（和 B-2 同类错，从另一条路进来）。
		if s.burning(consumer.ID)[gateway] {
			return types.Delivery{}, gateway, fmt.Errorf(
				"等锁期间这个账号刚碰过 %s，窗口已经在烧", gateway)
		}
		d, err := s.finish(ctx, consumer, gateway, exit, p, minted{}, floor)
		return d, gateway, err
	}
	minter := consumer
	if s.opt.Mode == types.ModePool {
		// 补 pair 那一发交给别人打：它会烧掉铸票者在这个网关上的窗口，
		// 烧消费者自己的就等于白跑一趟。出口已经由网关定死，所以只要人、不要它挑的区域。
		// 交付路径不节流：有人在等着拿 pair。
		if sc, _, ok := s.minterFor(exit, consumer.ID, 0, nil); ok {
			minter = sc
		}
	}
	got, err := s.mintPair(ctx, minter, consumer, exit, s.pairFor(gateway, floor), gateway, gateway)
	release()
	if err != nil {
		return types.Delivery{}, gateway, err
	}
	return s.afterMint(ctx, consumer, got, "网关 "+gateway, burningBefore, req, floor)
}

// mintByRegion 是按区域铸：**先定派谁去，再从那个号终身没走过的区域出发**，落哪算哪。
//
// 为什么不像定向铸那样先挑一个网关再倒推出口：落点由出口所在国家决定，而「这个号没走过
// 的区域」就约等于「它没烧过的网关」——那正是现铸要找的东西。倒推出口只会在这个号已经
// 走熟的那几个区域里反复抽签（出口取自网关的 ReachableExits，而那张表本身就是走过的区域
// 堆出来的），落点自然反复撞已经烧掉的网关，再被调用方的排除名单一一挡回来。
// 2026-10-02 现网：连撞 8 次，13 发铸票只走了 3 个区域，而手上 19 个网关的出口横跨 8 个国家。
//
// 顺序也只能是先人后区域：区域要从**铸票者自己**的台账上挑，而定向铸那条路出口先定死，
// 于是 minterFor 收到非零 want、candidateRegions 只剩那一格，台账彻底没机会参与决定。
//
// 不取网关铸票锁：那把锁拦的是「几路交付同时为**同一个网关**铸票」，而这条路一开始就不
// 知道会落到哪，没有网关名可锁。退化成按区域单飞（claimRegion，占不住就走不排队），
// 单账号并发 1 仍由 mintPair 里的 LockAccount 保证（SPEC 红线 2）。
// 也不带 pair 进去（裸铸）：带一张活票会让上游接受它、把落点钉回那张票的网关，
// 正好抵消掉按区域铸的全部意义。
func (s *Scheduler) mintByRegion(ctx context.Context, consumer types.Account,
	burningBefore map[string]bool, req Request,
	floor time.Duration) (types.Delivery, string, error) {
	minter, region, ok := s.mintPlan(consumer)
	if !ok {
		// 空串而不是某个网关名：这一发根本没打出去，记在谁头上都是冤枉。
		return types.Delivery{}, "",
			fmt.Errorf("%w：能派的账号九个区域本小时都打满了", ErrMintFailed)
	}
	// 这个号在这个大区已经有一发在打 ⇒ 不排队、直接认「暂时没票」。同号同出口的两发
	// 落点逐字相同，第二发白烧一个铸票者槽位，而槽位是池子最紧的资源。用 ErrNoLivePair 回：
	// Deliver 把它归成「等得起」那一格，调用方重试那一次前一路的票大概率已经在池里了。
	done, ok := s.claimRegion(minter.ID, region.Key)
	if !ok {
		return types.Delivery{}, "",
			fmt.Errorf("%w（账号 %d 在区域 %s 已经有一发裸铸在打，等它的结果）",
				ErrNoLivePair, minter.ID, region.Key)
	}
	defer done()

	got, err := s.mintPair(ctx, minter, consumer, region.Exit, types.Pair{}, "", "")
	if err != nil {
		// 落点未知 ⇒ 没有单个网关可记，而 noteTryFailure 遇空名什么都不记 ⇒ 失败的裸铸
		// 零代价，下一个人立刻从同一个出口再踩一遍。按出口所在国家**已知能到的全部网关**
		// 各记一次，和 mintOnce 读不出落点时记触碰用的是同一把尺子（多记只是多歇会儿，
		// 漏记会把烧过的当新鲜的发出去）。noteTryFailure 自己管调用方中断、速率闸、
		// 出口全没了这几种豁免，所以这里不重复判。
		for _, g := range s.gatewaysFrom(region.Exit) {
			s.noteTryFailure(ctx, consumer, g, err)
		}
		return types.Delivery{}, "", err
	}
	return s.afterMint(ctx, consumer, got, "区域 "+region.Key, burningBefore, req, floor)
}

// afterMint 是两条现铸路共用的尾段：落点出来之后的三道闸，然后交付。
//
// aim 只进日志，用来事后分辨这一发是定向铸还是按区域铸。
func (s *Scheduler) afterMint(ctx context.Context, consumer types.Account, got pairMint,
	aim string, burningBefore map[string]bool, req Request,
	floor time.Duration) (types.Delivery, string, error) {
	landed := got.pair.Gateway
	if landed == "" {
		return types.Delivery{}, "", errors.New("现铸没读出落点")
	}
	// **落点出来之后要再过一遍调用方的排除名单。** 候选集筛过了，但按区域铸和不判漂移的
	// 定向铸都可能落到调用方**明确排除**的网关上。而 exclude 编码的是「我自己烧过这个」
	// ——池子账本上没有记录，两条冷却闸都看不见。换票（force + exclude_versions 把有票的
	// 网关全排掉）正好最常落到这一格，于是客户端拿回一张自己刚烧过的落点，
	// 而验证默认关着 ⇒ 无从察觉。
	//
	// 判据用 got.pair.Cookie 而不是回头读账本：res.NewPair 为空（上游接受了送进去那张、
	// 不换 cookie）时两者会分叉。pair 照旧留在池里——白来的供给别扔，只是不发给他。
	if req.Skips(landed, got.pair.Cookie) {
		return types.Delivery{}, landed,
			fmt.Errorf("%w（现铸落到 %s，而调用方排除了它）", ErrNoLivePair, landed)
	}
	// 「池子刚烧过 (他 × landed)」用哪一份账本，取决于**这一发是不是他自己打的**：
	// 是他自己就必须用快照，否则等于拿自己刚写的 touch 把自己判死；
	// 不是他自己（别人代铸）那 20~90 秒里他一发都没打，现读才对——
	// 并发的发现扫描（scan 直接写 LastTouch，而且裸铸不受网关锁约束）或者
	// 「他正好被挑去给别人铸票」都会在这段时间里烧掉 (他 × landed)，而快照看不见。
	//
	// 原先这儿还有一问「铸完之后重查一次 LastDeliver」：铸票那几十秒里同一个账号的另一路
	// 交付可能刚在 landed 上取走过一张票。那条闸 2026-10-03 整个删了（见 burning），
	// 所以这里也不再问 —— 同一个号并发取两张落在同一网关的票现在是允许的，归客户端账本管。
	burned := burningBefore[landed]
	if !got.mintedBySelf {
		burned = s.burning(consumer.ID)[landed]
	}
	if burned {
		return types.Delivery{}, landed,
			fmt.Errorf("现铸落到 %s，而池子刚在一个满血窗口内拿这个账号的凭据打过一发", landed)
	}
	var m minted
	if got.mintedBySelf {
		m = got.minted
	}
	// 出口按**落到的那个**网关重取：手上那个是「想去 A / 从区域 R 出发」的，拿它去验 B
	// 大概率再漂一次、把一张好票判成失败。UpsertGateway 已经把这一发请求过的出口补进
	// landed 的 ReachableExits 了（就在 mintOnce 里），所以这里必然查得到。
	exit, ok := pickExit(s.st.Gateways()[landed])
	if !ok {
		return types.Delivery{}, landed, fmt.Errorf("网关 %s 没有已知可达出口", landed)
	}
	s.opt.Log.Info("一张活票都没有，现铸一发", "account", consumer.ID, "label", consumer.Label,
		"aim", aim, "landed", landed, "by_self", got.mintedBySelf)
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
	// 这不是偷懒，是算过账的：消费账号在一个它没碰过的网关上本来就是满血，而验证要用它
	// 自己的凭据打两发（先铸一张属于它的活票、再 echo）—— 那是 2 发上游请求，花在
	// 「每区每小时 6 发」那个共用预算上。加上 state-echo 有假阴无假阳（SPEC 第 3 节），
	// 关掉只少一个读数，不会把降智的东西放出去。
	//
	// **2026-10-02 修掉的一条过时理由**：这里原先还写着「那两发恰好烧掉它马上要用的那个
	// 窗口，验完交出去的已经是个二手落点」。那是错的 —— 当天实测满血窗口内连打多发都是满血
	// （窗口 ≥200s、窗口内 7/7），所以那两发不烧窗口，只是把窗口的计时提前启动。
	// 要不要开纯粹是成本取舍（见 config.Config.VerifyFullAtDeliver）。
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
		s.noteDeliver(consumer.ID, gateway, at, !selfMinted, pair.Cookie)
		s.markUnverified(consumer.ID, gateway)
		exp, validFor := s.deliverWindow(at.Add(types.DeliverTTL), pair.ExpiresAt)
		return types.Delivery{
			Gateway:      gateway,
			Region:       pair.RegionKey,
			Cookie:       pair.Cookie,
			ExpiresAt:    exp,
			ValidForS:    validFor,
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
	windowEnd := m.firstAt.Add(types.DeliverTTL)
	if left := windowEnd.Sub(s.now()); left < minDeliverWindow {
		return types.Delivery{}, fmt.Errorf("网关 %s 的满血窗口只剩 %v，不发", gateway, left.Truncate(time.Second))
	}
	// 触碰不用再记：铸那张活票的那一发（mintOnce）已经记过，而它就是首次接触。
	s.noteDeliver(consumer.ID, gateway, m.firstAt, false, pair.Cookie)
	exp, validFor := s.deliverWindow(windowEnd, pair.ExpiresAt)
	return types.Delivery{
		Gateway:      gateway,
		Region:       pair.RegionKey,
		Cookie:       pair.Cookie,
		ExpiresAt:    exp,
		ValidForS:    validFor,
		VerifiedFull: true,
	}, nil
}

// deliverWindow 算这一次交付的可用期末尾和建议有效期（秒）：满血窗口什么时候关、
// 这张 pair 什么时候硬过期，**取早的那个**。
//
// 必须取小：min_remaining 能把交付门槛压到 30 s，而窗口是 150 s —— 不取小就会把一张
// 只剩 40 秒的票标成 valid_for_s=150 发出去，消费端用到一半被重新路由到随机网关。
// 今天 DeliverFloor(300s) > DeliverTTL(150s) 所以撞不上，是 min_remaining 把它打开的。
//
// 两个返回值必须出自**同一次**计算：expires_at 说时刻、valid_for_s 说时长，
// 而它们讲的是同一件事。各算一次就会出现「两个字段描述同一件事却给不同的数」，
// 那是个陷阱——按哪个算都可能超出真实可用期。
func (s *Scheduler) deliverWindow(windowEnd, pairEnd time.Time) (time.Time, int) {
	end := windowEnd
	if pairEnd.Before(end) {
		end = pairEnd
	}
	left := end.Sub(s.now())
	if left < 0 {
		left = 0
	}
	return end, int(left / time.Second)
}

// pairMint 是「为这个网关补一张 pair」的结果。mintedBySelf 为真时那一发是消费者
// 自己打的（单账号模式），于是它同时也是消费者的首次接触，票可以直接拿来 echo。
type pairMint struct {
	minted
	mintedBySelf bool
}

// mintPair 用指定的铸票者打一发。定向铸时调用方必须已持该网关的铸票锁。
//
// **人由调用方定**：定向铸要「能到那个网关的出口」，先定出口再挑人；按区域铸要
// 「这个号没走过的区域」，先挑人再定出口。两者顺序相反，在这里自己挑人就只能服务
// 前者——那正是按区域铸挑不出新落点的老毛病（见 mintByRegion）。
//
// want 是期望落点，留空表示「落到哪算哪」。
// bias 是这一发**想**去哪：只用来在读不出落点时保守地记一次触碰。按区域铸没有「想去哪」
// ⇒ 传空串，由 mintOnce 按出口所在国家把能到的网关各记一次。
func (s *Scheduler) mintPair(ctx context.Context, minter, consumer types.Account,
	exit types.Exit, cur types.Pair, want, bias string) (pairMint, error) {
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

// claimRegion 占住「**这个号**此刻有一发裸铸走这个大区」，占不住返回 false。
//
// 粒度是 (账号 × 大区)，不是整个大区。裸铸出手前不知道会落到哪个网关，所以 SPEC B 条
// 「同一个网关一次只许有一发在铸」在这条路上只能近似，而最接近的近似是按 (账号 × 大区)：
// 同一个号同一个出口连铸几发，落点逐字相同（见 types.RegionMint.Gateway）⇒ 第二发白烧
// 一个槽位，该拦。
//
// **两个不同号同时走同一个大区不拦。** 原来拦了，理由是「两发裸铸去发现可能同一个落点」，
// 但实测不支持那个「可能」：us-east 一个大区 253 发摸到 36 个不同落点，105 个网关里
// 100 个被多个账号打到过 ⇒ 两个号走同一个大区大概率落到两个不同网关，拦住是白拦。
// 而代价是它成了并发的**物理上限 9**，开局还更糟：所有号的首选大区都是台账里铸得最少
// 的那一个，几个 worker 必然撞在一起（2026-10-02 实测，三个 worker 只铸出一发就收尾）。
// 现在上限由 mint_concurrency 自己管。
//
// **刻意是占不住就走，不是排队等。** 不能拿真锁去拦：两路交付各自的铸票者不同，排队
// 等于让后一路在走到**账号锁**之前就停住，而前一路正等着账号锁 ——
// TestNoDeadlockBetweenAccounts 那个屏障要的就是两路都能走到账号锁，任何一把挡在前面的
// 共享锁都会让它真死锁（2026-10-02 实测，全局锁版本 3 秒超时）。
//
// 占不住的那一路直接认「暂时没票」回去。调用方本来就会重试，而重试那一次
// 前一路的票大概率已经在池子里了 —— 一发上游都不用打。
func (s *Scheduler) claimRegion(accountID int64, regionKey string) (func(), bool) {
	key := comboKey(accountID, regionKey)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mintingRegions == nil {
		s.mintingRegions = map[string]bool{}
	}
	if s.mintingRegions[key] {
		return nil, false
	}
	s.mintingRegions[key] = true
	return func() {
		s.mu.Lock()
		delete(s.mintingRegions, key)
		s.mu.Unlock()
	}, true
}

// claimAccount 抢下「这个号现在去铸一发」，throttle 内已经有人登记过就回 false。
//
// 为什么挑人之后还要单独抢一次：登记（lastMintedAt）原先只在 mintOnce 里落笔，
// 那是一发上游**之后**的事，而 minterFor 读的就是它。串行时没问题；预铸并发起来
// （mint_concurrency > 1）之后，几个 worker 会在第一发落笔之前**全部选中同一个号**。
// 账号锁（store.LockAccount）拦得住并发，但拦不住「同一个号两次铸票至少隔 RenewGap」
// 那条防封节奏——而那正是 RenewGap 存在的全部理由（见 Options.RenewGap）。
//
// 抢到了但后面没铸成（要不到出口、撞上闸）会让这个号白歇一个 RenewGap。方向是
// **少铸不是多铸**，而池子里有上百个号，可以接受。
//
// throttle <= 0 时只登记、不拒：交付路径那边有人在等票，节流会直接变成服务不可用。
func (s *Scheduler) claimAccount(id int64, throttle time.Duration) bool {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if throttle > 0 && now.Sub(s.lastMintedAt[id]) < throttle {
		return false
	}
	s.lastMintedAt[id] = now
	return true
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
	// 被上游 429 按住的号一发都不许打。守卫放在这里而不是各个挑人的地方：mintOnce 是
	// 所有真实铸票的唯一出口（minterFor 挑的、mintPlan 回落自己上的、scan 和保温的
	// 都从这儿走），放在挑人处就会漏掉「找不到别人就自己上」那条。
	//
	// 包 ErrRateLimited 而不另造一个错：六个 errors.Is 调用点对「这个号铸不动、而且
	// 一发都没出站」的处理已经全对了（不记触碰、不罚槽位、回 rate_limited），
	// 新造一个 sentinel 要把那六处挨个补一遍。
	if left := s.accountHeldFor(a.ID); left > 0 {
		return minted{}, fmt.Errorf("%w（上游 429 按住了这个账号，还要等 %s）",
			types.ErrRateLimited, left.Round(time.Second))
	}

	// 地理只用来给上传者看账本，查不到不影响铸票（同 sid ⇒ 同出口 IP）。
	obs, gerr := s.mint.Geo(ctx, e, sid)
	if gerr != nil {
		s.opt.Log.Debug("查出口地理失败", "account", a.ID, "err", gerr)
	}

	start := s.now()
	s.mu.Lock()
	s.lastMintedAt[a.ID] = start
	s.mu.Unlock()
	res, err := s.mint.Mint(ctx, a.Creds, e, sid, cur.Cookie, cur.CflbExpiresAt)
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
	} else if !nothingWentOut(err) {
		// 读不出落点、而且这一发本来就没有「想去哪」（按区域铸）：按这个出口所在国家
		// **已知能到的全部网关**各记一次触碰。那一发可能已经出站、烧掉了其中某一个，
		// 而我们分不清是哪个。多记只是让那几个槽位多歇会儿，漏记会把烧过的当新鲜的发出去。
		//
		// 但「保守」只对**真分不清**的失败成立（超时、连接断）。要不到出口、撞上速率闸
		// 这两种是一发都没出站，照样铺开记就等于一次池子自己的故障把这个出口能到的网关
		// 全锁 4 小时 —— 而旧实现只按 bias 记一个，所以这是铺开记引入的新伤。
		for _, g := range s.gatewaysFrom(e) {
			s.touch(g, a.ID, at)
		}
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
		// 上游 429 = 这个号现在打不动（额度耗尽 / 打太猛），但它会自己好 ⇒ 不停用，
		// 按住整个账号等一轮。原来只给**槽位**记一次退避（封顶 60 秒），而额度窗口
		// 是小时级的，于是池子对着一个打不动的号每分钟重试一遍它的每个槽位，把速率闸
		// 的配额白白吃光——那个配额本来是留给正经铸票的。
		if res.Status == 429 {
			s.holdAccount(a, res.RetryAfter, msg)
			return minted{}, fmt.Errorf("%w（%s）", types.ErrRateLimited, msg)
		}
		// 上游否决这个账号就当场停用：保温/续期是个循环，不停用它会一直按这个
		// 节奏白打下去（SPEC 第 7 节）。5xx 不走这条，那是退避管的事。
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

	// 出口国家不符 ⇒ 这一发对「哪个大区能到哪个网关」什么都没证明，两本按大区分格的账
	// 都不能记。住宅代理库存不足时会回退到别的国家（1650 发里 23 次，要美国拿到了
	// BR/AR/CL/CD/JM/MQ/MX），而落点是上游按**真实出口 IP** 分的：
	//
	//   - 记进 ReachableExits 等于留一条假线索，pickExit 下次照它定向铸、必然漂移
	//   - 记进 (账号 × 大区) 台账等于把一个这个号还没走通的大区标成走过了，
	//     而 bestRegion 优先挑「终身没铸过」的 ⇒ 从此不再去试它
	//
	// 网关本体照记（它确实存在、确实要进候选清单），只是不记「从哪儿能到它」——
	// UpsertGateway 对零值出口会跳过追加那一步。
	//
	// obs 查不到地理时 Country 是空串 ⇒ 不判，照旧按请求的出口记：那是没有观测，
	// 不是观测到不符。
	// 种子铸票的「出口国家不符」是**常态不是漂移**：它故意从便宜机房出去，靠 `__cflb`
	// 把路由钉到目标大区（见 warm.premintSID）⇒ 观测到的国家必然是机房那个，九个大区里
	// 八个都对不上。两本账要拆开看：
	//
	//   - ReachableExits 问的是「从哪个**出口 IP** 能摸到这个网关」—— 种子什么都没证明
	//     （路由是 cookie 定的，不是 IP 定的），照旧不记。
	//   - (账号 × 大区) 台账问的是「这个号在这个大区落到过哪」—— 种子正好证明了这件事：
	//     `__cflb` 钉大区、网关 =(大区 × 账号)，拿到落点就说明这个号在那个大区落在这儿。
	//
	// 不拆开的后果是实测出来的（2026-10-03 一小时：360 发裸铸里 264 发被这道闸吞掉台账，
	// 于是 premintPick 的落点预测学不到东西、盲挑，293 发落回已有活票的网关 = 81% 白打）。
	seeded := s.mint.SeedBound(sid)
	reached := e
	if obs.Country != "" && obs.Country != e.Region {
		reached = types.Exit{}
		if !seeded {
			// 种子那条不打这行：它每轮都成立，打出来是 264 行/小时的噪声，
			// 而且「不符」这个词对它是误导。
			s.opt.Log.Info("出口国家不符，这一发不记可达出口、不记大区台账",
				"account", a.ID, "want", e.Region, "got", obs.Country, "gateway", res.Gateway)
		}
	}
	// 落点入网关表。裸铸（预铸、发现）**必须**有这一步：Gateways() 是候选清单的
	// 唯一来源，不在表上的网关哪怕有票也永远不会被挑中。定向铸票走到这儿是幂等的，
	// 顺带把这个请求过的出口补进 ReachableExits（「下次想回来该怎么请求」）。
	if _, err := s.st.UpsertGateway(res.Gateway, a.ID, reached); err != nil {
		s.opt.Log.Warn("记网关失败", "gateway", res.Gateway, "err", err)
	}
	// 记进 (账号 × 区域) 台账：挑下一个铸票者时「这个号哪个区域还没走过」、
	// 以及「再派这个组合会落到哪」全靠它。
	//
	// 区域 key 一定非空：pickExit 只给九个大区的代表出口，裸铸和扫描直接用 types.Regions。
	// 种子铸票照记（见上面那段）：它的出口国家对不上是常态，但大区是 `__cflb` 钉死的。
	if seeded || reached != (types.Exit{}) {
		if err := s.st.NoteRegionMint(a.ID, types.RegionKeyOf(e), res.Gateway, at); err != nil {
			s.opt.Log.Warn("记区域台账失败", "account", a.ID, "err", err)
		}
	}

	p := cur
	if res.NewPair != "" {
		// 上游下发了新 cookie，顶掉旧的。没下发则说明它接受了送进去的 pair，
		// 网关没变，旧记录照旧有效（SPEC 第 2 节，别当失败）。
		//
		// 寿命按**路由**算，不按 __oailb 算（见 types.MintResult.RouteDeadline）：
		// 把 cur 的 __cflb 死期带进去是关键的一条——上游常只补发 __oailb，
		// 那时旧 __cflb 原样留在 cookie 里，新 __oailb 的 65 分钟盖不住它。
		route, cflb := res.RouteDeadline(cur.CflbExpiresAt)
		p = types.Pair{Gateway: res.Gateway, Cookie: res.NewPair,
			ExpiresAt: route, CflbExpiresAt: cflb, MintedBy: a.ID,
			// 区域取**这一发实际用的出口**，和上面那条区域台账同一个口径。
			// 走 sampleRegion 的兜底：续期型铸票的出口取自 ReachableExits，不保证
			// 正好等于 Regions 里那九个代表出口，RegionKeyOf 会回空串 ⇒ 消费端整片
			// 画成「未归类」。退回「国家[:城市]」比空着能认。
			RegionKey: sampleRegion(e)}
		// **换盆之后再写：新票替换旧票，TTL 按能拿到的最长 `__cflb` 重算。**
		//
		// 不换盆就直接写会丢寿命：这一发很可能是带种子打的，而上游不补发 `__cflb` 时
		// （常态）新票原样留着**种子那张**，种子的门槛只有 seedCflbFloor（10 分钟）
		// ⇒ 一张还剩 50 分钟的旧票被一张只剩 10 分钟的新票顶掉，池子凭空少 40 分钟。
		//
		// 换盆比「短命就不覆盖」对：新票的 `__oailb` 是新鲜的 65 分钟，拖后腿的只有借来的
		// `__cflb`，而 `__cflb` 是可以换的（mint.SwapCflb）⇒ 换完的新票 ≥ 两张里任何一张。
		// best 取**整个大区**最长寿的那张，不只是这个网关的旧票，所以比两两相比还好。
		//
		// best 必须在 PutPair **之前**算：旧的 161 那张自己就是候选捐赠者，先写就把它挤掉了。
		p = s.rehome(p, s.bestCflb())
		if err := s.st.PutPair(p); err != nil {
			s.opt.Log.Warn("写 pair 失败", "gateway", p.Gateway, "err", err)
			return minted{}, fmt.Errorf("%w: 写 pair: %w", ErrMintFailed, err)
		}
	}
	if res.Gateway != want && want != "" {
		return minted{}, fmt.Errorf("%w：落点漂移，想要 %s，实到 %s", ErrMintFailed, want, res.Gateway)
	}
	return minted{pair: p, ticket: res.TurnState, sid: sid, obs: obs, firstAt: start}, nil
}

// disabledNote 把自动停用的理由补成一句话（没有就空串）。
//
// 只给错误串用：那句话最后落在失败卡片的 Detail 里，而「token_revoked」和
// 「运营手动暂停」要做的事完全不同。
func (s *Scheduler) disabledNote(id int64) string {
	for _, a := range s.st.Accounts() {
		if a.ID == id && a.DisabledReason != "" {
			return "（停用理由：" + a.DisabledReason + "）"
		}
	}
	return ""
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
	return s.minterFor(types.Exit{}, exclude, 0, nil)
}

// mintPlan 给按区域铸定「派谁去、从哪个区域出发」（见 mintByRegion）。
//
// 先找别人：烧别人的窗口，要票的人拿到票时对那个落点仍是首次接触。找不到别人就自己上
// ——单账号池里这是唯一能走的路，而且就算自己铸，挑一个**没走过的区域**也比在走熟的
// 那几个区域里反复抽签强：前者有机会落到没烧过的网关，后者几乎必然落回烧过的。
func (s *Scheduler) mintPlan(consumer types.Account) (types.Account, types.Region, bool) {
	if s.opt.Mode == types.ModePool {
		if a, r, ok := s.minterFor(types.Exit{}, consumer.ID, 0, nil); ok {
			return a, r, true
		}
	}
	r, ok := s.regionFor(consumer)
	return consumer, r, ok
}

// regionFor 给指定账号挑它最该去的区域：九个区域里本小时还有配额、且**终身铸得最少**的
// 那个。和 minterFor 里的那次 bestRegion 同一把尺子，差别只是这里人已经定了。
func (s *Scheduler) regionFor(a types.Account) (types.Region, bool) {
	perRegion, limit := s.mint.Rate(a.Creds.AccountID)
	lifetime := map[string]int{}
	for _, m := range s.st.RegionMints() {
		if m.AccountID == a.ID {
			lifetime[m.RegionKey] = m.Count
		}
	}
	r, _, _, ok := bestRegion(types.Exit{}, perRegion, limit, lifetime, nil, nil)
	return r, ok
}

// premintPolicy 是预铸给挑选器的两条偏好。交付路径传 nil（见 minterFor）。
//
// 合成一个结构而不是两个函数参数：这两条是同一件事的两半（哪些组合不要、哪些区域先要），
// 而 minterFor 的参数表已经够长了。
type premintPolicy struct {
	// skip 说这个 (账号, 区域) 组合这一轮不要。
	skip func(accountID int64, regionKey string) bool
	// prefer 说这个区域要优先（同一档防封规矩之内）。
	prefer func(regionKey string) bool
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
//  2. 本小时总发数最少的优先——集中打一个号正是 2026-09-29 把通道打废的那个模式。
//     这条是防封规矩，所以它永远排在后面那几条优化前面
//  3. 并列时优先**丢了种子票**的区域（见 premintPolicy.prefer）
//  4. 再并列优先它**终身没铸过**的区域：那是唯一可能出新落点的地方
//  5. 再并列挑最久没被用作铸票者的号
//
// want 非零时是**定向铸票**（点名网关）：出口已经被网关的 ReachableExits 定死，
// 只剩「它在那个区域还有配额吗」这个问题，返回的区域就是那个出口自己。
//
// throttle > 0 时跳过「刚铸过票不到这么久」的账号（保温路径用；交付路径传 0，
// 那边有人在等，节流会直接变成服务不可用）。
//
// pol 非 nil 时按预铸的两条偏好再筛/再排一道。交付路径传 nil：那边有人在等票，
// 少一个候选就可能直接变成 503。
func (s *Scheduler) minterFor(want types.Exit, exclude int64, throttle time.Duration,
	pol *premintPolicy) (types.Account, types.Region, bool) {
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
		reseed bool // 挑中的那个区域此刻没有种子票
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
		// 被上游 429 按住的号挑了也白挑：mintOnce 的守卫会把它原地撞回来，
		// 而要票的人已经等了一个往返。池子里还有别人时要挑别人。
		if s.accountHeldFor(a.ID) > 0 {
			continue
		}
		perRegion, limit := s.mint.Rate(a.Creds.AccountID)
		total := 0
		for _, n := range perRegion {
			total += n
		}
		var drop, prefer func(string) bool
		if pol != nil {
			if pol.skip != nil {
				drop = func(key string) bool { return pol.skip(a.ID, key) }
			}
			prefer = pol.prefer
		}
		pick, picked, reseed, ok := bestRegion(want, perRegion, limit, lifetime[a.ID], drop, prefer)
		if !ok {
			continue // 九个区域（或点名的那一个）本小时都打满了、或者都被 skip 排掉了
		}
		cs = append(cs, cand{a: a, r: pick, sent: total, reseed: reseed,
			unseen: picked == 0, used: lastUsed[a.ID]})
	}
	if len(cs) == 0 {
		return types.Account{}, types.Region{}, false
	}
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].sent != cs[j].sent {
			return cs[i].sent < cs[j].sent
		}
		if cs[i].reseed != cs[j].reseed {
			return cs[i].reseed
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

// bestRegion 给一个号挑它最该去的区域：还有本小时配额的里面，**先看 prefer**，
// 同档内挑终身铸得最少的那个。返回挑中的区域、它的终身计数、它是不是 prefer 那一档、
// 以及「还有没有配额」。
//
// limit <= 0 当「不设限」：只有没接配置的裸 mint.Client 会这样，生产的
// mint.perRegionHour 永远回正数。
//
// drop / prefer 非 nil 时分别排掉和优先某些区域（预铸用，见 premintPolicy）。
func bestRegion(want types.Exit, perRegion map[string]int, limit int, lifetime map[string]int,
	drop, prefer func(regionKey string) bool) (types.Region, int, bool, bool) {
	var best types.Region
	bestCount, bestPrefer, found := 0, false, false
	for _, r := range candidateRegions(want) {
		if limit > 0 && perRegion[r.Key] >= limit {
			continue
		}
		if drop != nil && drop(r.Key) {
			continue
		}
		p := prefer != nil && prefer(r.Key)
		n := lifetime[r.Key]
		// prefer 那一档整体优先；同档内才比终身计数。
		switch {
		case !found, p && !bestPrefer:
		case p == bestPrefer && n < bestCount:
		default:
			continue
		}
		best, bestCount, bestPrefer, found = r, n, p, true
	}
	return best, bestCount, bestPrefer, found
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

// gatewaysWithPair 留下此刻有活 pair 的候选网关。一张都没有（返回空）就该去现铸。
//
// 组内的排序仍然交给 LongestRestedSlot，所以「从没碰过的网关优先」照旧成立——
// 注意它给不存在的槽位行合成一个零值 Slot（LastTouch 是零时刻），于是没碰过的
// 永远排在碰过的前面。硬筛加在它前面之后这个行为没有坏处（组内全是出了冷却的槽位），
// 但别以为零值会被跳过。
func (s *Scheduler) gatewaysWithPair(cands []string, floor time.Duration) []string {
	out := make([]string, 0, len(cands))
	for _, name := range cands {
		if _, fresh := s.freshPair(name, floor); fresh {
			out = append(out, name)
		}
	}
	return out
}

// freshPair 判断池里那张 pair 还够不够交付：剩余不足 floor 就别发，
// 否则 65 分钟硬过期会在消费者手里到点（SPEC 第 6 节）。
//
// floor 由调用方给（Request.Floor）：默认是 types.DeliverFloor，而调用方可以用
// min_remaining 把它压低到 30 s —— 「我这一发很短，残票也要」是只有它自己知道的事。
// freshPair 是**唯一**的取票口（交付路径全从这儿拿），所以换 cflb 也只在这儿做：
// 返回的那张就是真正发出去的那一份，票号跟着它走（见 rehome 的三条红线）。
func (s *Scheduler) freshPair(gateway string, floor time.Duration) (types.Pair, bool) {
	p, ok := s.st.Pair(gateway)
	if !ok {
		return types.Pair{Gateway: gateway}, false
	}
	p = s.rehome(p, s.bestCflb())
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
	// 只在调用方**显式**给了门槛时才按它筛：他说「至少 600 秒」，窗口内重连却拿回
	// 一张只剩 40 秒的票就是骗他。
	// 默认门槛**不能**加在这儿：那会让窗口内的重连掉进正常流程去烧下一个落点，
	// 而省掉那个槽位正是 redeliver 存在的理由。
	if req.MinRemaining > 0 && p.ExpiresAt.Sub(now) < req.MinRemaining {
		return types.Delivery{}, false
	}
	exp, validFor := s.deliverWindow(best.LastDeliver.Add(types.DeliverTTL), p.ExpiresAt)
	// 够不够一秒按**取小之后**的那个数判：pair 剩不到一秒时 valid_for_s 会是 0，
	// 而消费端（sub2api）把非正数当成坏响应直接丢弃。
	if validFor < 1 {
		return types.Delivery{}, false
	}
	s.opt.Log.Info("窗口内重复请求，原样再发一次", "account", accountID,
		"gateway", best.Gateway, "left", exp.Sub(now).Truncate(time.Second))
	return types.Delivery{
		Gateway:   best.Gateway,
		Region:    p.RegionKey,
		Cookie:    p.Cookie,
		ExpiresAt: exp,
		ValidForS: validFor,
		// 照第一次的判定如实报：这一次什么都没验，不该凭空升级成 true。
		VerifiedFull: best.LastVerdict == types.VerdictFull,
	}, true
}

// notBurning 滤掉此刻正在烧窗口的候选网关。
//
// **只拦池子自己刚干的事。** 「这个消费号在这个网关上取过票了」那条闸 2026-10-03 删了
// ——用户定的：消费和账本归客户端记，池子不替它判「哪个网关用过」，理由是给上游减轻负担
// （见 burning）。
func (s *Scheduler) notBurning(accountID int64, cands []string) []string {
	burning := s.burning(accountID)
	out := make([]string, 0, len(cands))
	for _, name := range cands {
		if burning[name] {
			continue
		}
		out = append(out, name)
	}
	return out
}

// burning 是这个账号此刻**不能**给满血的那些网关：池子在一个满血窗口内拿这个号的凭据
// 真打过一发上游，而且此刻不在退避中。
//
// **只剩这一条闸了。** 原先还有一条「LastDeliver 在 SlotCooldown（4 小时）内」——
// 票真发出去过就按住 4 小时。2026-10-03 按用户的决定删掉：那条闸记的是**消费**，
// 而消费该由客户端自己的账本管，池子代劳只是替上游多扛一份判断。匿名消费者（拿别人
// key 替自己取票、没有客户端账本的那些）因此不再有池子侧的烧灼保护——用户原话
// 「躲是客户端的事情，它们没写池子不代劳」。
//
// 留下这一条的理由和那条正相反：**这一发是池子自己打的**，只有池子知道它发生过。
// 删了它，池子会拿 A 的凭据铸一张落在 G 的票、然后立刻把 G 发给 A，而 (A × G) 的窗口
// 正是刚才那一发烧掉的。那不是「不替客户端记账」，那是池子不知道自己干了什么。
// 顺带它也是 LongestRestedSlot 轮转的依据（按 LastTouch 排），没有它同一个落点会被
// 反复发给同一个人。
//
// 豁免是「**此刻仍在**退避中」，不是「有过退避记录」：触碰也可能来自一次失败的尝试
// （铸票超时按保守方向记了触碰），那种情形归指数退避管，两层闸叠起来会让退避永远到不了
// 第二级。但 BackoffUntil 只被写、只在**该槽位成功交付时**才清零 ⇒ 按「字段非零」豁免
// 等于让这条闸对任何失败过一次的槽位永久沉默：一次铸票超时同时写 LastTouch=T 和
// BackoffUntil=T+10s，T+10s 之后退避到点、这条闸被豁免 ⇒ 这个槽位在它自己正在烧的满血
// 窗口里（剩下 123 秒）被当成首次接触发出去。backoffMax 只有 1 分钟，管不到窗口尾部。
// 正在退避的槽位 LongestRestedSlot 本来就跳过，豁免它不损失任何东西。
func (s *Scheduler) burning(accountID int64) map[string]bool {
	now := s.now()
	out := map[string]bool{}
	for _, sl := range s.st.Slots() {
		if sl.AccountID != accountID {
			continue
		}
		if !now.Before(sl.BackoffUntil) && !sl.LastTouch.IsZero() &&
			now.Sub(sl.LastTouch) < types.FullWindow {
			out[sl.Gateway] = true
		}
	}
	return out
}

// burnWait 是这些候选里最早一个烧完窗口还要多久。一个都算不出来就回 0。
//
// 上限就是一个满血窗口（183 秒），不再是小时量级 —— 消费冷却那条闸删掉之后
// 「全员都不能给」只可能是「池子此刻正在为他铸票」，那是等得起的（见 burning）。
func (s *Scheduler) burnWait(accountID int64, cands []string) time.Duration {
	want := make(map[string]bool, len(cands))
	for _, name := range cands {
		want[name] = true
	}
	now := s.now()
	best := time.Duration(0)
	for _, sl := range s.st.Slots() {
		if sl.AccountID != accountID || !want[sl.Gateway] || sl.LastTouch.IsZero() {
			continue
		}
		// 正在退避的槽位 burning 本来就豁免 ⇒ 它不是「等窗口」等得出来的，别报进去。
		if now.Before(sl.BackoffUntil) {
			continue
		}
		left := sl.LastTouch.Add(types.FullWindow).Sub(now)
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

// bumpBackoff 翻倍退避，从 backoffBase 起、封顶 backoffMax。只给硬失败用
// （非 200、网络错、落点漂移）。**上游 429 不走这条**，它按住整个账号（holdAccount）：
// 槽位退避封顶才 1 分钟，而额度窗口是小时级的。
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

// holdAccount 按住整个账号：上游 429 说的是「这个号现在打不动」，不是某个槽位的事。
// 听上游的 Retry-After，没给就按 acctHoldDefault，一律钳进 (0, acctHoldMax]。
func (s *Scheduler) holdAccount(a types.Account, retryAfter time.Duration, reason string) {
	d := retryAfter
	if d <= 0 {
		d = acctHoldDefault
	}
	if d > acctHoldMax {
		d = acctHoldMax
	}
	until := s.now().Add(d)
	s.mu.Lock()
	// 取更晚的那个：同一轮里几个槽位并发撞 429 时别把已经记下的长退避改短。
	if cur, ok := s.acctHold[a.ID]; !ok || until.After(cur) {
		s.acctHold[a.ID] = until
	}
	s.mu.Unlock()
	s.opt.Log.Warn("上游按住了这个账号，本轮不再派它铸票", "account", a.ID, "label", a.Label,
		"hold", d.Round(time.Second), "retry_after", retryAfter.Round(time.Second), "reason", reason)
}

// AccountHolds 是此刻被上游按住的账号 → 还要等多久。只给管理页的读数用。
//
// 为什么非要上页面：这个退避一开，那个号在表里看起来**健康且启用**，却一发都不铸。
// 没有这一格的话运维对着它只能看出「不干活」，查不出是上游按住了（槽位退避有
// 「退避 X」那一格，账号级的原来只在 WARN 日志里）。
func (s *Scheduler) AccountHolds() map[int64]time.Duration {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int64]time.Duration, len(s.acctHold))
	for id, until := range s.acctHold {
		if d := until.Sub(now); d > 0 {
			out[id] = d
		}
	}
	return out
}

// accountHeldFor 报告这个账号还要被按住多久（0 = 可以用）。
func (s *Scheduler) accountHeldFor(id int64) time.Duration {
	s.mu.Lock()
	until, ok := s.acctHold[id]
	s.mu.Unlock()
	if !ok {
		return 0
	}
	if d := until.Sub(s.now()); d > 0 {
		return d
	}
	return 0
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

// gatewaysFrom 列出已知能从**这个出口**到达的网关。
//
// 比整个出口（国家 + 城市），不是只比国家。这条路的用途是「读不出落点、保守记触碰」，
// 方向本来就偏多记——但只比国家会多到离谱：`{US,""}` 会把只从 `{US,"Seattle"}` 摸到过
// 的网关也算进来，而那两个出口的落点集实测是 36 / 29、交集 27（2026-10-03 现网
// ReachableExits），于是一发失败的 us-east 裸铸会按住 64 个槽位里多出来的那 9 个
// ——它们从没在这个出口上被摸到过，按住纯属白按。ReachableExits 记的恰好就是
// 「请求过的那个出口」（见 store.UpsertGateway），逐字比它就是现成的正确答案。
func (s *Scheduler) gatewaysFrom(e types.Exit) []string {
	var out []string
	for name, g := range s.st.Gateways() {
		for _, re := range g.ReachableExits {
			if re == e {
				out = append(out, name)
				break
			}
		}
	}
	return out
}

// nothingWentOut 判这个失败是不是「一发都没出站」⇒ 既没烧掉任何槽位，也不是槽位的错。
//
// 速率闸是池子自己拦下来的，出口要不到是供应商的事，两样都和某个 (账号 × 网关) 无关，
// 而且**整池同病**：换个网关再试只是把同一句话重复 N 遍。记退避或记触碰都等于拿池子
// 自己的故障惩罚供给，故障恢复后还要再瞎等一轮（2026-10-01 现网实测）。
func nothingWentOut(err error) bool {
	return errors.Is(err, types.ErrRateLimited) || errors.Is(err, types.ErrNoExit)
}

// pickExit 随机取一条已知可达出口，**只认九个大区的代表出口**。
//
// 为什么挑剔：出口的大区 key 是反查出来的（types.RegionKeyOf 精确比 Region+City），
// 反查不到就是空串，而这个空串会被一路带进两本按大区分格的账：
//
//   - 速率闸的分桶键（mint.rateKey = acct|""）⇒ 九个大区的定向铸票挤进**同一个**
//     4 发/小时的桶，互相饿死，而九个大区的安全阀对这条路整个失效
//   - (账号 × 大区) 台账（NoteRegionMint）⇒ 多出一行键为空串的记录，挑下一个铸票者
//     就看这本账
//
// 在这儿拦掉，上面两处就永远拿到真大区，各自不必再兜底一次（那种兜底会让每个野出口
// 凭空多出一整份配额）。pickExit 是唯一会产出非九个之一的出口的地方：裸铸和扫描都
// 直接用 types.Regions 里的代表出口，而 ReachableExits 存的就是「请求过的出口」
// ⇒ 筛掉之后这条回路就闭上了，野出口再也写不回来。
//
// 现网 105 个网关的 ReachableExits 全是九个之一（2026-10-03 核过 state.json，
// no_standard_exit = 0），所以这道筛子今天一个候选都没排掉。
//
// 随机而非固定第一条：城市库存不足时上游会回退到别的国家（实测 city-Dallas 给过
// 尼日利亚 IP），死磕一条出口会卡死在那次回退上。
func pickExit(g types.Gateway) (types.Exit, bool) {
	std := make([]types.Exit, 0, len(g.ReachableExits))
	for _, e := range g.ReachableExits {
		if types.RegionKeyOf(e) != "" {
			std = append(std, e)
		}
	}
	if len(std) == 0 {
		return types.Exit{}, false
	}
	return std[rand.IntN(len(std))], true
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
// cookie 是这一次**真正发出去的那张票**的内容，它的 cookie_version 要一起记下来：
// /pair/renew 的归属闸拿它当判据（见 gotThisTicket——拿「池子现持那张」当判据是
// 假判据，谁都查得到）。
//
// Prev* 三个字段必须在**同一次**读-改-写里落下：它们是「这次交付之前」的那一组值，
// 分两次写就会把这次交付自己写进去的值当成旧值存下来，/release 还原完槽位看起来
// 还是刚烧过的。票号也在这一组里——漏掉它，还原完票号仍指向一张已经不在的票。
func (s *Scheduler) noteDeliver(accountID int64, gateway string, at time.Time, touchSelf bool, cookie string) {
	version := types.CookieVersion(cookie)
	// 走 updateSlot 这个包装（而不是直接 st.UpdateSlot）：它负责补上 AccountID /
	// Gateway / 初始判定。这条路现在可能是一条新槽位行的**唯一**写入者
	// （池子一发上游都没替消费者打的时候），字段补不全的话那行就没有网关名，
	// 下一次挑选会拿它当候选、而它谁也不是。
	_ = s.updateSlot(accountID, gateway, func(sl *types.Slot) {
		sl.PrevDeliver, sl.PrevTouch = sl.LastDeliver, sl.LastTouch
		sl.PrevDeliverVersion = sl.LastDeliverVersion
		if touchSelf {
			sl.LastTouch = at
		}
		sl.LastDeliver = at
		sl.LastDeliverVersion = version
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
	// 开着交付前验证时一律不受理：那条路池子**真用消费者的凭据打了两发**
	// （先铸一张属于它的活票，再 echo），槽位是实烧的——还了就是对池子说谎，
	// 下一个人会拿到一个已经烧掉的落点，而冷却会从 4 小时缩成一个满血窗口。
	// 失败闭合、不加状态：开关在交付和归还之间被改掉这种情形不管，
	// 窗口只有 150 秒，而且方向偏保守。
	if s.cfg().VerifyFullAtDeliver {
		return 0, fmt.Errorf("%w（交付前验证开着：这条路上的槽位是实烧的）", ErrNoRelease)
	}
	now := s.now()
	n := 0
	for _, sl := range s.st.Slots() {
		if sl.AccountID != accountID {
			continue
		}
		// 受理窗口 DeliverTTL：过了他很可能已经用过了，而「他说没用过」池子无从验证。
		if !s.gotThisTicket(sl, cookieVersion, now, types.DeliverTTL) {
			continue
		}
		prevDeliver, prevTouch, prevVersion := sl.PrevDeliver, sl.PrevTouch, sl.PrevDeliverVersion
		if err := s.st.UpdateSlot(accountID, sl.Gateway, func(x *types.Slot) {
			x.LastDeliver, x.LastTouch = prevDeliver, prevTouch
			// 票号跟着一起还原：留着这次交付的号，还原完它就指向一张已经不属于
			// 任何人的票，而 /pair/renew 的归属闸正是按它算。
			x.LastDeliverVersion = prevVersion
			// 还原点用完就清掉：第二次 release 要回 409，而不是把槽位还到更早。
			x.PrevDeliver, x.PrevTouch = time.Time{}, time.Time{}
			x.PrevDeliverVersion = ""
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

// gotThisTicket 报告这个槽位最近那次交付发出去的**就是** cookieVersion 这张票。
//
// 判据是槽位上记着的 LastDeliverVersion，**不是**「池子现持那张的号」。后者是
// 2026-10-02 复审抓到的假判据：现持那张的号谁都查得到（自己取一张同网关的票就知道），
// 于是一次交付能换到那张 pair 在整个窗口里被无限次改写（实测 5/5）。按交付记录算，
// 一次交付就只换到一次改写：改写之后池子现持的已经不是它了，/pair/renew 的票号闸
// 就把第二次挡住。
//
// 两个调用方共用这一份，窗口各自给：/release 用 DeliverTTL（见上），
// /pair/renew 用 PairLifetime（见 DeliveredTo）。判据写成两份迟早走散，而走散的
// 后果是静默的——一边认一张票是你的、另一边不认。
//
// 对 /release 这是**收紧**：原先比现持那张，所以「你被交付的是 V、池子换成了 W」
// 时它既会把报 W 的人放进来（W 的号是公开可得的），也会把报 V 的真主人拒掉。
func (s *Scheduler) gotThisTicket(sl types.Slot, cookieVersion string, now time.Time, window time.Duration) bool {
	if sl.LastDeliver.IsZero() || now.Sub(sl.LastDeliver) >= window {
		return false
	}
	// 零值 = 老 state.json 的记录（那时候不记票号）⇒ 一律不认，等下一次交付写上。
	return sl.LastDeliverVersion != "" && sl.LastDeliverVersion == cookieVersion
}

// DeliveredTo 报告这张票（cookieVersion）**确实被交付给过**这一行。
//
// /pair/renew 的归属闸就是它，而它是那条路上**唯一真正的**安全边界：
// 交回来的 cookie 本体全是调用方自己写的字符串——`__oailb` 是 JWT 但池子**验不了签**
// （上游的公钥不在我们手上，见 mint.parseOailb），`__cflb` 是密文、只查得出非空。
// 所以「落点对不对」这类判据是拿外部输入校验外部输入，挡 bug 可以，挡不住人。
// 能挡住人的只有**池子自己的交付记录**：你只能续你确实拿到过的那张票。
//
// 窗口是 PairLifetime 而不是 DeliverTTL（150 s）：消费端的续期发生在业务响应**之后**，
// 一条长流轻松超过 150 秒，按那个窗口算会把正常续期全拒掉。而一张票本来也活不过
// PairLifetime，拿它当窗口既够宽又不多给。
func (s *Scheduler) DeliveredTo(accountID int64, cookieVersion string) bool {
	now := s.now()
	for _, sl := range s.st.Slots() {
		if sl.AccountID == accountID && s.gotThisTicket(sl, cookieVersion, now, types.PairLifetime) {
			return true
		}
	}
	return false
}
