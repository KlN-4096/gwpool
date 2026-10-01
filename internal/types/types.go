// Package types holds the shared vocabulary of gwpool.
//
// 这些签名由架构方定稿，实现方不要改。改了会让并行开发的几个包对不上。
// 领域含义见 SPEC.md 第 1 节。
package types

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Creds 是一个托管账号的 Codex 凭据。两个 token 都绝不进日志、页面或 API 响应。
type Creds struct {
	AccessToken string
	AccountID   string
	// RefreshToken 可空。有它池子才能自己换票——access_token 只活约九天，
	// 没有它到期就是一个 401、账号自动停用、等人重新上传。
	RefreshToken string
}

// String / GoString / LogValue 把 AccessToken 挡在所有打印路径外：
// %v %+v %s 走 String，%#v 走 GoString，slog 任意 struct 走 LogValue。
// 唯一防线是「没人写错」时，将来一句 log.Info("acct", "a", a) 就把 token 打进日志。
//
// **刻意不实现 MarshalJSON**：state.json 要靠 json 原样存回 AccessToken，
// 打码的 MarshalJSON 会让凭据在第一次保存时静默变成 "***"，重启后全部账号失效。
// API 响应里不许出现凭据这条，由 httpapi 自己不序列化 Account 来保证。
func (c Creds) String() string {
	// 「有没有 refresh」要说出来：能不能自己换票决定了这个号到期后是续上还是停掉，
	// 排查时需要知道，而说「有」不泄露任何内容。
	tail := "}"
	if c.RefreshToken != "" {
		tail = ",refresh=***}"
	}
	if c.AccessToken == "" {
		return "Creds{" + c.AccountID + ",无 token" + tail
	}
	return "Creds{" + c.AccountID + ",token=***" + tail
}

func (c Creds) GoString() string { return c.String() }

func (c Creds) LogValue() slog.Value { return slog.StringValue(c.String()) }

// Exit 是一个 novproxy 出口。City 可空。
// 注意：城市库存不足时上游会回退到别的国家，所以这只是**请求的**出口，
// 实际落在哪要看 Observed。
type Exit struct {
	Region string // ISO-2，如 "US"
	City   string // 可空，如 "Seattle"
}

// Observed 是一次请求实际用到的出口地理，由 ipinfo 查得。
// 一切统计都按它来，不按 Exit。
type Observed struct {
	IP      string
	Country string
	City    string
	ASN     string
	ISP     string
}

// MintResult 是一次铸票的结果。
//
// Gateway 的语义是本项目最容易写错的地方，照 SPEC.md 第 2 节：
//   - 响应里有新的 set-cookie __oailb  → Gateway = 该 JWT 的 host 第三段
//   - 没有新 __oailb 且本次发送了 pair  → Gateway = 发送的那个 pair 的网关（上游接受了）
//   - 没有新 __oailb 且本次没发 pair    → 视为失败，Gateway 为空
//
// NewPair 为空表示上游没下发新 cookie；此时若 Gateway 非空，说明沿用了送进去的 pair。
type MintResult struct {
	Status      int
	Gateway     string    // "unified-167"
	NewPair     string    // 新下发的完整 cookie 头；没下发则为空
	TurnState   string    // x-codex-turn-state
	PairExpires time.Time // 新 pair 的 __oailb exp；没下发则零值
	// ErrBody 是非 2xx 时上游回的那段 JSON（截断）。只在失败路径填。
	// 没有它的话一个 400 什么都不告诉人——2026-10-01 首扫全军覆没就查了半天。
	ErrBody string
}

// Verdict 是满血判定。nil 表示不下结论（HTTP 非 200 等）。
// state-echo **有假阴、无假阳**：Full=true 可信，Full=false 可能是误判。
type Verdict struct {
	Full   bool
	Status int
	Note   string
}

// Minter 经指定出口向上游铸票。sentPair 为空表示裸打。
//
// 实现要点：
//   - 铸票与随后的 state-echo 必须复用**同一个 sid**，否则可能换出口 IP、换网关
//   - 单账号并发硬性为 1，由调用方用 per-account mutex 保证
type Minter interface {
	Mint(ctx context.Context, c Creds, e Exit, sid, sentPair string) (MintResult, error)

	// VerifyFull 带一张活票再打一发，只读响应头。必须复用铸票时的 sid 和 pair。
	VerifyFull(ctx context.Context, c Creds, e Exit, sid, pair, ticket string) (*Verdict, error)

	// Geo 查这个 sid 当前的出口地理。与 Mint 复用同一 sid 才有意义。
	Geo(ctx context.Context, e Exit, sid string) (Observed, error)
}

// Account 是一个托管账号。
type Account struct {
	ID        int64
	Label     string
	PlanType  string // "plus" / "pro" / ...，取自 access_token 的声明
	Creds     Creds
	ViewToken string
	Enabled   bool
	CreatedAt time.Time
	// DisabledReason 是自动停用的原因（上游那段话）。手动停用和启用时都清空。
	// 没有它的话页面上只有一个「已停用」，没人知道为什么，只会被重新打开再废一轮。
	DisabledReason string
}

// RejectedByUpstream 判断这个状态码是不是「上游说这个账号不行」。
//
// 400/401/403 是**对这个账号的否决**，重试多少次都一样：票过期了、被撤销了、
// 或者这个账号没有所配模型的权限（2026-10-01 实测：free 账号打 gpt-6-astra
// 恒回 400，九个区域每个重试三次，二十七发全废）。遇到就停用，别再轮询。
//
// 429 和 5xx **刻意不算**：那是「现在不行」，归退避管，停用一个被限流的号
// 等于把一次限流变成永久损失。
func RejectedByUpstream(status int) bool {
	switch status {
	case 400, 401, 403:
		return true
	}
	return false
}

// Sample 是一次扫描的落点记录。
type Sample struct {
	AccountID int64
	RegionKey string // SPEC.md 第 4 节的区域 key
	Requested Exit
	Observed  Observed
	Gateway   string
	Status    int
	At        time.Time
	// Err 是这一发失败的具体原因（上游那段 JSON，或本地的错误文本），成功时为空。
	// 没有它的话 samples.jsonl 里一行 status=400 什么都不告诉人：2026-10-01 首扫
	// 二十发全 400，真正的原因（账号清单里没有 gpt-6-astra）要单独打一发才看得见。
	Err string
}

// Gateway 是池子里的一个网关。
type Gateway struct {
	Name           string // "unified-142"
	FirstSeenBy    int64
	FirstSeenAt    time.Time
	ReachableExits []Exit

	// RenewFails 是这个网关的 pair **连续**续期失败次数，续上一次就清零。
	// 连败到 RenewFailBudget 就停摆到 ParkedUntil：一个续不上的网关每轮都重试，
	// 等于拿全局速率预算去喂一个已经不通的落点。
	RenewFails  int
	ParkedUntil time.Time
}

// Parked 报这个网关此刻是不是在停摆里。
func (g Gateway) Parked(now time.Time) bool { return now.Before(g.ParkedUntil) }

// 槽位判定的三个取值。VerdictUnknown 不只是「还没验」，也是客户端回报里**唯一
// 合法的无信息值**：转发路径上没有可用的降智判据，所以它不许覆盖已有判定。
const (
	VerdictFull     = "full"
	VerdictDegraded = "degraded"
	VerdictUnknown  = "unknown"
)

// Slot 是 (账号 × 网关)，降智的作用单位。调度就是在这张表上取歇得最久的。
type Slot struct {
	AccountID    int64
	Gateway      string
	LastTouch    time.Time
	LastVerdict  string // VerdictFull / VerdictDegraded / VerdictUnknown
	BackoffUntil time.Time
}

// Pair 是池子里一张活的路由 cookie。
//
// __oailb 的 exp-iat 恒为 3900s（65 分钟），硬过期。
// 剩余不足 RenewLead 就续；剩余不足 DeliverFloor 不交付。
type Pair struct {
	Gateway   string
	Cookie    string
	ExpiresAt time.Time
	MintedBy  int64

	// 租约：同一张 pair 同一时刻只交给一个消费者。
	//
	// 两个不同账号**同时**带着同一张路由 cookie 到同一个网关，这个模式本身就是
	// 异常特征——跨账号共享 cookie 正是本项目在做的事，并发做就把它暴露出来了。
	// 所以交付时优先挑没被租走的网关；只有池子深度不够（全部在租）才退化成共享，
	// 并且必须 log 出来，否则「池子太浅」会被当成「一切正常」。
	//
	// 租约到期即失效，不需要显式归还（消费者可能崩了）。LeasedBy == 0 表示空闲。
	LeasedBy   int64
	LeaseUntil time.Time
}

// Leased 报告这张 pair 此刻是否被别人占着。by 传调用方账号；自己续租不算占用。
func (p Pair) Leased(now time.Time, by int64) bool {
	return p.LeasedBy != 0 && p.LeasedBy != by && now.Before(p.LeaseUntil)
}

// 时间常量，依据 2026-10-01 实测。改之前先看 SPEC.md。
const (
	PairLifetime = 3900 * time.Second // __oailb exp-iat，上游定死
	FullWindow   = 183 * time.Second  // 首次接触后的满血窗口，静默失效
	DeliverTTL   = 150 * time.Second  // 交付给消费者的有效期，FullWindow 留 18% 余量
	RenewLead    = 600 * time.Second  // pair 剩余不足这么多就续
	DeliverFloor = 600 * time.Second  // pair 剩余不足这么多不交付
)

// SpareTarget 是池子要比「最忙的那个号」多留几张活 pair。
//
//	目标活 pair 数 = max(每个号碰过的网关数) + SpareTarget
//
// 「碰过的网关数」= 那个号 slot 表里 LastTouch 非零的行数，也就是它已经烧掉的落点。
// 多留两张的意思是：最忙的那个号手上**永远还有两个没碰过、且已经有活 pair 的网关**，
// 来取票时不用现等铸票（一发上游要几秒到几十秒）。
//
// 这个目标天然被已知网关数封顶：最忙的号把所有网关都碰过之后，缺口只能靠**发现新
// 网关**来补，再铸多少张 pair 都没用（pair 是路由，烧掉的是槽位）。那条路是
// httpapi 在 ErrNoFullSlot 上挂的探索扫描。
const SpareTarget = 2

// RenewFailBudget 是一个网关的 pair 连续续期失败几次就停摆。
// GatewayParkFor 是停摆多久——按 pair 的寿命算：一小时之后那张旧 pair 本来也死了，
// 再碰它是一次全新的尝试，而不是同一场失败的第四次重试。
const (
	RenewFailBudget = 3
	GatewayParkFor  = time.Hour
)

// ErrRateLimited 是全局速率闸拦下来的。
//
// 必须能被单独认出来：它**不是这个槽位的错**，也不是这个账号的错。当成普通失败处理
// 会把一次限流变成「给所有试过的槽位各记一次退避」，等于拿速率纪律去惩罚供给。
var ErrRateLimited = errors.New("全局速率闸：这一小时的上游请求配额用完了")

// SlotCooldown 是一个 (账号 × 网关) 烧掉之后要歇多久才重新给满血。
//
// **这是估算值，至今没测准**（只知道 ≥4h）。它只用来算下面那个开放门槛，
// 不参与任何调度决策——调度看的是 Slot.LastTouch 和实测判定，不看这个数。
const SlotCooldown = 4 * time.Hour

// GatewaysForContinuous 是「一个账号要不停转」所需的网关数。
//
//	每张票只给 DeliverTTL 这么长的可用时间，用完这个 (账号 × 网关) 就得歇
//	SlotCooldown。所以要连续供上，手里的网关数 = 冷却 ÷ 单张可用时长。
//
// 按 4h ÷ 150s 算是 96 个。这就是「门槛」该有的量级——之前写死的 6 是个
// 随手填的数，6 个网关只够一个账号转 6×150s = 15 分钟，远不是「稳定使用」。
//
// 它同时也解释了这个项目的结构性难点：实际只摸到个位数到十几个网关，
// 离连续供应差一个量级。所以门槛的意义更像是一道**诚实的闸门**——
// 够不到就别对外开放，而不是「调一个小点的数好让它看起来开着」。
func GatewaysForContinuous() int {
	n := int(SlotCooldown / DeliverTTL)
	if SlotCooldown%DeliverTTL != 0 {
		n++
	}
	return n
}

// Mode 是运行模式。见 SPEC.md 第 7 节。
//
// 单账号模式下**绝不保温**：保温那一发恰好烧掉自己要用的窗口。
type Mode string

const (
	ModeSingle Mode = "single" // 扫描者 = 消费者：按需现铸，不保温
	ModePool   Mode = "pool"   // 扫描者 ≠ 消费者：扫描账号保温，消费账号取用
)

// Delivery 是 /cookie 的返回。
type Delivery struct {
	Gateway      string    `json:"gateway"`
	Cookie       string    `json:"cookie"`
	ExpiresAt    time.Time `json:"expires_at"`
	ValidForS    int       `json:"valid_for_s"`
	VerifiedFull bool      `json:"verified_full"`
}
