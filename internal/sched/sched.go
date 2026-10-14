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
	"sort"
	"sync"
	"time"

	"gwpool/internal/authrefresh"
	"gwpool/internal/config"
	"gwpool/internal/geography"
	"gwpool/internal/mint"
	"gwpool/internal/oauth"
	"gwpool/internal/ticket"
	"gwpool/internal/types"
)

// Store 是 sched 需要的存储能力。按接口编程是为了能用 fake 跑测试：
// 测试绝不打真实上游（SPEC 红线 8）。
type Store interface {
	// Gateways 池子里的网关，key = 名字。必须带 ReachableExits：定向铸票全靠它。
	Gateways() map[string]types.Gateway

	// Slots 是整张槽位表，key = store.SlotKey。预铸算「最忙的号碰过几个网关」要它。
	Slots() map[string]types.Slot

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
	CountryAttempts(account string) map[string]types.CountryAttempt

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
	TicketInventory() (map[string]types.Pair, map[string]types.RegionDonor)
	CommitMint(types.MintCommit) (types.MintCommitResult, error)
	RouteStates(time.Time) map[string]types.RouteState

	// LockAccount 是**跨包唯一**的单账号互斥，返回解锁函数。
	// 必须由存储层持有：首扫（scan）和交付（sched）都要打同一个账号，
	// 两个包各自维护一张锁表就等于没锁——2026-09-29 实测 5~6 并发 20~30 分钟废掉一个通道。
	LockAccount(accountID int64) func()

	Accounts() []types.Account

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
	NewSIDFor(exit types.Exit, sticky bool) (string, error)

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
	RouteSources(exit types.Exit, seed bool) []types.RouteSource
	NewSIDForRoute(exit types.Exit, sourceID string, seed bool) (string, error)
	RouteSource(sid string) types.RouteSource
}

// ErrNoFullSlot indicates that the requested gateway has no acceptable route
// ticket. It says nothing about consumer quality, cooldown or another gateway.
var ErrNoFullSlot = errors.New("没有可交付的槽位")

// ErrUpstreamRejected / ErrMintFailed 把铸票失败分成「上游否决这个账号」和「别的」。
//
// 分开不是洁癖：否决（400/401/403）重试多少次都一样、账号已经被当场停用，
// 而别的（429、5xx、网络错、落点漂移）是「现在不行」。调用方据此告诉客户端
// 该不该再来（见 types.RejectedByUpstream）。
var (
	ErrUpstreamRejected = errors.New("上游否决了这个账号")
	ErrMintFailed       = errors.New("铸票失败")
	errLandingDrift     = errors.New("落点漂移")
)

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

// Options 是调度器的可调参数。零值字段由 New 填默认。
type Options struct {
	Mode types.Mode
	// RefreshExpired runs under the shared account lock; only local expiry or
	// an exact upstream token_expired may invoke it.
	RefreshExpired authrefresh.RefreshFunc

	// WarmInterval 保温巡检间隔（仅 ModePool）。
	WarmInterval time.Duration
	// IdleAfter：距上次交付超过它就停止保温，让 pair 自然过期、成本归零（SPEC 第 7 节）。
	IdleAfter time.Duration
	// MintGap 是两发铸票之间的间隔，速率纪律，别把供给打空。负数 = 不等（测试用）。
	MintGap time.Duration
	// RenewGap 是**同一个账号**两次铸票之间的最小间隔（名字是续期时代留下的）。
	// 供给不是瓶颈（十几个网关一小时才十几发），打得密才是封号特征。
	RenewGap time.Duration

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

	backgroundMu        sync.Mutex // 自动保温与管理员全量队列共享后台并发预算。
	maintenanceFirst    bool       // backgroundMu 保护；交替优先，探索与续票互不长期挤掉机会。
	nextExploration     time.Time  // backgroundMu 保护；只限制专门探索机会，不限制维护。
	recoveryExploration recoveryExplorationState
	exploreRegionCursor int // mu保护；按尝试轮转，不因失败或小时配额重置而回到首国。
	mu                  sync.Mutex
	routesInFlight      map[string]bool
	// lastMintedAt 记每个账号最近一次被用作铸票者，用来把铸票摊到各个号上。
	// 不落盘：重启后重新摊平没有坏处，而落盘就多一份要维护的状态。
	// 「总共铸过几发」刻意**不**自己记：那个读数在 mint 的速率窗口里，
	// 而那份是连 scan 那条路的发数一起算的，自己再记一份只会和它分叉。
	lastMintedAt  map[int64]time.Time
	lastDeliver   time.Time
	inventoryWake chan struct{} // 合并真实取票需求；仅唤醒维护，不增加专门探索轮次。
	evidenceCache inventoryEvidenceCache
	routeCache    routeStateCache
	external      externalConsumers
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
		lastMintedAt: map[int64]time.Time{},
		acctHold:     map[int64]time.Time{}, inventoryWake: make(chan struct{}, 1)}
}

func (s *Scheduler) Mode() types.Mode { return s.opt.Mode }

func (s *Scheduler) now() time.Time { return s.opt.Now() }

// cfg 取当前配置快照。一个判断只取一次：两次 Get() 之间配置可能变了。
func (s *Scheduler) cfg() config.Config { return s.opt.Live.Get() }

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
	pairs, donors := s.st.TicketInventory()
	return ticket.Best(pairs, donors, s.now())
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
	return s.deliverExternal(ctx, upstreamID, req, authed)
}

// minted 是一次铸票的产物。
//
// firstAt 是**请求发出去的时刻**，不是响应回来的时刻：满血窗口从首次接触起算，
// 交付有效期必须按它算，否则铸票 + echo 的 20~90 s 往返会被白白算进窗口里。
// sid 要带出来：state-echo 必须复用铸票时的 sid，否则可能换出口 IP、换网关。
// obs 也带出来，好让随后那一发 echo 的样本记上同一个出口地理（同 sid ⇒ 同 IP）。
type minted struct {
	pair types.Pair
	gain types.StockGain
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
	return s.mintOnceFor(ctx, a, e, sid, cur, want, bias, types.PurposeDelivery)
}

func (s *Scheduler) mintAttempt(ctx context.Context, a types.Account, e types.Exit, sid string,
	cur types.Pair, want, bias, purpose string) (out minted, finalErr error) {
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

	sampleStarted := s.now()
	var prediction *types.InventoryPrediction
	if purpose == types.PurposeInventory && cur.Cookie == "" && !s.mint.SeedBound(sid) {
		prediction = inventoryPredictionFromContext(ctx, a.ID, e)
	}
	trace, err := s.newMintTrace(ctx, a, e, sid, cur, want, purpose, prediction)
	defer trace.finish(&out, &finalErr)
	if err != nil {
		return minted{}, err
	}
	obs, gerr := s.mint.Geo(ctx, e, sid)
	trace.sample.Observed = obs
	trace.sample.TargetMatch = e.MatchEvidence(obs, s.mint.SeedBound(sid))
	// Cookie routing does not prove the physical exit country is supported.
	// Seed requests retain their logical region for accounting only.
	if gerr == nil && !geography.Supported(obs.Country) {
		gerr = types.ErrUnsupportedCountry
	}
	if gerr != nil {
		trace.failure = "geo"
		return minted{}, fmt.Errorf("出口地理检查: %w", gerr)
	}

	start := s.now()
	s.mu.Lock()
	s.lastMintedAt[a.ID] = start
	s.mu.Unlock()
	res, err := s.mint.Mint(ctx, a.Creds, e, sid, cur.Cookie, cur.CflbExpiresAt)
	at := s.now()
	// 每一发都要落样本：上传者视图靠它证明「我们只拿你的凭据打了 ping」。
	trace.sample.Gateway, trace.sample.Status, trace.sample.At = res.Gateway, res.Status, at
	trace.sample.Err = types.SampleErr(res.ErrBody, err)
	trace.sample.DurationMS = at.Sub(sampleStarted).Milliseconds()

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

	if err != nil && (res.Status == 0 || res.Status == 200) {
		// 包一层 ErrMintFailed 但不改这句话：底下可能还裹着速率闸 / 出口熔断，
		// 调用方按**更靠内**的那几个先分流，剩下的才算「铸票没成」。
		// 已有非200响应时优先走下面的状态分类：401/429没有__oailb时同样会报
		// ErrNoLanding，不能因此跳过停用/Retry-After，下一轮继续白打。
		trace.failure = "transport"
		if res.Status == 200 {
			trace.failure = "invalid_response"
		}
		return minted{}, fmt.Errorf("%w: %w", ErrMintFailed, err)
	}
	if res.Status != 200 {
		trace.failure = "upstream"
		if s.opt.RefreshExpired != nil && oauth.TokenExpired(res.Status, res.ErrBody) {
			return minted{}, fmt.Errorf("%w: %w", ErrUpstreamRejected, oauth.ErrAccessExpired)
		}
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
		trace.failure = "invalid_response"
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
	if seeded {
		// 即使机房恰好在同一国家，也不能把 cookie 定向冒充实际出口的可达证据。
		reached = types.Exit{}
	} else if !e.MatchesObserved(obs) {
		reached = types.Exit{}
		s.opt.Log.Info("实际出口未确认匹配，不记可达出口或区域种子",
			"account", a.ID, "want_country", e.Region, "want_state", e.State, "want_city", e.City,
			"country", obs.Country, "state", obs.State, "city", obs.City, "gateway", res.Gateway)
	}
	// 落点入网关表。裸铸（预铸、发现）**必须**有这一步：Gateways() 是候选清单的
	// 唯一来源，不在表上的网关哪怕有票也永远不会被挑中。定向铸票走到这儿是幂等的，
	// 顺带把这个请求过的出口补进 ReachableExits（「下次想回来该怎么请求」）。
	if _, err := s.st.UpsertGateway(res.Gateway, a.ID, reached); err != nil {
		trace.committed, trace.failure = true, "persistence"
		return minted{}, fmt.Errorf("%w: 记网关: %w", ErrMintFailed, err)
	}
	// 记进 (账号 × 区域) 台账：挑下一个铸票者时「这个号哪个区域还没走过」、
	// 以及「再派这个组合会落到哪」全靠它。
	//
	// 区域 key 一定非空：pickExit 只给九个大区的代表出口，裸铸和扫描直接用 types.Regions。
	// 种子铸票照记（见上面那段）：它的出口国家对不上是常态，但大区是 `__cflb` 钉死的。
	if seeded || reached != (types.Exit{}) {
		if err := s.st.NoteRegionMint(a.ID, s.cfg().RegionKeyOf(e), res.Gateway, at); err != nil {
			trace.committed, trace.failure = true, "persistence"
			return minted{}, fmt.Errorf("%w: 记区域台账: %w", ErrMintFailed, err)
		}
	}

	p := cur
	var gain types.StockGain
	if res.NewPair != "" {
		// 上游下发了新 cookie，顶掉旧的。没下发则说明它接受了送进去的 pair，
		// 网关没变，旧记录照旧有效（SPEC 第 2 节，别当失败）。
		//
		// 寿命按**路由**算，不按 __oailb 算（见 types.MintResult.RouteDeadline）：
		// 把 cur 的 __cflb 死期带进去是关键的一条——上游常只补发 __oailb，
		// 那时旧 __cflb 原样留在 cookie 里，新 __oailb 的 65 分钟盖不住它。
		route, cflb := res.RouteDeadline(cur.CflbExpiresAt)
		regionKey := ""
		if seeded || reached != (types.Exit{}) {
			regionKey = s.sampleRegion(e)
		}
		p = types.Pair{Gateway: res.Gateway, Cookie: res.NewPair,
			ExpiresAt: route, CflbExpiresAt: cflb, MintedBy: a.ID,
			// 区域取**这一发实际用的出口**，和上面那条区域台账同一个口径。
			// 走 sampleRegion 的兜底：续期型铸票的出口取自 ReachableExits，不保证
			// 正好等于 Regions 里那九个代表出口，RegionKeyOf 会回空串 ⇒ 消费端整片
			// 画成「未归类」。退回「国家[:城市]」比空着能认。
			RegionKey: regionKey}
		// Comparison, donor retention and publication share the Store commit
		// lock. A drift must not overwrite longer stock at its actual landing.
		committed, err := trace.commit(p)
		if err != nil {
			s.opt.Log.Warn("写 pair 失败", "gateway", p.Gateway, "err", err)
			return minted{}, fmt.Errorf("%w: 写 pair: %w", ErrMintFailed, err)
		}
		p, gain = committed.Pair, committed.Gain
	} else {
		if _, err := trace.commit(types.Pair{}); err != nil {
			return minted{}, fmt.Errorf("%w: route evidence: %w", ErrMintFailed, err)
		}
	}
	if res.Gateway != want && want != "" {
		return minted{pair: p, gain: gain}, fmt.Errorf("%w：%w，想要 %s，实到 %s", ErrMintFailed, errLandingDrift, want, res.Gateway)
	}
	return minted{pair: p, gain: gain}, nil
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

// RegionEnabled 供延迟执行的任务复核运行配置，不能只认入队快照。
// PickMinter serves the administrator's discovery queue, not consumer delivery.
func (s *Scheduler) PickMinter(exclude int64) (types.Account, types.Region, bool) {
	return s.minterFor(types.Exit{}, exclude, 0, nil)
}

func (s *Scheduler) RegionEnabled(region types.Region) bool {
	current, ok := s.cfg().RegionByKey(region.Key)
	return ok && current.Enabled && geography.Supported(current.Country) && current.Exit() == region.Exit
}

// premintPolicy 是预铸给挑选器的候选约束和偏好。交付路径传 nil（见 minterFor）。
//
// 合成一个结构而不是两个函数参数：这两条是同一件事的两半（哪些组合不要、哪些区域先要），
// 而 minterFor 的参数表已经够长了。
type premintPolicy struct {
	// skip 说这个 (账号, 区域) 组合这一轮不要。
	skip func(accountID int64, regionKey string) bool
	// prefer 说这个区域要优先（同一档防封规矩之内）。
	prefer func(regionKey string) bool
	// rank only breaks ties after quota/fairness and existing preferences. It
	// cannot create eligibility or replace a proven recovery route.
	rank  func(types.Account, types.Region) int
	route func(types.Account, types.Region) bool
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
// pol 非 nil 时按预铸的偏好再筛/再排一道。交付路径传 nil：那边有人在等票，
// 少一个候选就可能直接变成 503。
func (s *Scheduler) minterFor(want types.Exit, exclude int64, throttle time.Duration,
	pol *premintPolicy) (types.Account, types.Region, bool) {
	now := s.now()
	regions := s.dynamicRegions(want)
	byKey := make(map[string]types.Region, len(regions))
	for _, region := range regions {
		byKey[region.Key] = region
	}
	s.mu.Lock()
	lastUsed := make(map[int64]time.Time, len(s.lastMintedAt))
	for k, v := range s.lastMintedAt {
		lastUsed[k] = v
	}
	s.mu.Unlock()

	type cand struct {
		a      types.Account
		r      types.Region
		sent   int  // 本小时总发数
		reseed bool // 挑中的那个区域此刻没有种子票
		unseen bool // 挑中的那个区域终身还没铸过
		used   time.Time
		rank   int
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
		var rank func(types.Region) int
		if pol != nil {
			if pol.skip != nil || pol.route != nil {
				drop = func(key string) bool {
					return (pol.skip != nil && pol.skip(a.ID, key)) ||
						(pol.route != nil && !pol.route(a, byKey[key]))
				}
			}
			prefer = pol.prefer
			if pol.rank != nil {
				rank = func(region types.Region) int { return pol.rank(a, region) }
			}
		}
		pick, picked, reseed, ok := bestRegionRanked(regions, perRegion, limit, s.cfg().AccountAttemptLimit(), s.countryAttemptCounts(a), drop, prefer, rank)
		if !ok {
			continue // 九个区域（或点名的那一个）本小时都打满了、或者都被 skip 排掉了
		}
		candidate := cand{a: a, r: pick, sent: total, reseed: reseed,
			unseen: picked == 0, used: lastUsed[a.ID]}
		if rank != nil {
			candidate.rank = rank(pick)
		}
		cs = append(cs, candidate)
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
		if cs[i].reseed != cs[j].reseed {
			return cs[i].reseed
		}
		if cs[i].rank != cs[j].rank {
			return cs[i].rank > cs[j].rank
		}
		if !cs[i].used.Equal(cs[j].used) {
			return cs[i].used.Before(cs[j].used)
		}
		return cs[i].a.ID < cs[j].a.ID
	})
	return cs[0].a, cs[0].r, true
}

func bestRegionRanked(regions []types.Region, perRegion map[string]int, limit, accountLimit int, lifetime map[string]int64,
	drop, prefer func(regionKey string) bool, rank func(types.Region) int) (types.Region, int64, bool, bool) {
	if types.MintBudgetFull(perRegion, limit, accountLimit, "") {
		return types.Region{}, 0, false, false
	}
	var best types.Region
	bestCount, bestPrefer, found := int64(0), false, false
	bestRank := 0
	for _, r := range regions {
		country := geography.CountryKey(r.Exit.Region)
		if limit > 0 && perRegion[country] >= limit {
			continue
		}
		if drop != nil && drop(r.Key) {
			continue
		}
		p := prefer != nil && prefer(r.Key)
		n := lifetime[country]
		value := 0
		if rank != nil {
			value = rank(r)
		}
		// Failed attempts advance the country just like success. Seed preference
		// only breaks ties, so a persistently failing exit cannot starve new ones.
		switch {
		case !found, n < bestCount:
		case n == bestCount && p && !bestPrefer:
		case n == bestCount && p == bestPrefer && value > bestRank:
		default:
			continue
		}
		best, bestCount, bestPrefer, found = r, n, p, true
		bestRank = value
	}
	return best, bestCount, bestPrefer, found
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

func (s *Scheduler) sample(sm types.Sample) {
	if err := s.st.AppendSample(sm); err != nil {
		s.opt.Log.Warn("写样本失败", "account", sm.AccountID, "err", err)
	}
}

// ---------- 退避 ----------

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

func (s *Scheduler) markDemand() {
	s.mu.Lock()
	s.lastDeliver = s.now()
	s.mu.Unlock()
	if s.opt.Mode == types.ModePool {
		select {
		case s.inventoryWake <- struct{}{}:
		default:
		}
	}
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

func exitKey(e types.Exit) string {
	return e.Label()
}

// sampleRegion 是样本里那一列区域 key。
//
// 反查不到就退回「国家[:城市]」：账本和页面是给人看的，留个能认的字比空着好
// （而速率闸那边刻意不这么兜底——配额必须按真区域分桶，见 mint.rateKey）。
func (s *Scheduler) sampleRegion(e types.Exit) string {
	if k := s.cfg().RegionKeyOf(e); k != "" {
		return k
	}
	return exitKey(e)
}

// ErrNoRelease 是「这张票没得还」：过了受理窗口、找不到、或者已经还过了。
var ErrNoRelease = errors.New("没有可归还的交付")
