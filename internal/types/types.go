// Package types holds the shared vocabulary of gwpool.
//
// 这些签名由架构方定稿，实现方不要改。改了会让并行开发的几个包对不上。
// 领域含义见 SPEC.md 第 1 节。
package types

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
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
	State  string // 供应商的州/省代码；与 City 分开，不能互相冒充。
	City   string // 可空，如 "Seattle"
}

// Observed 是一次请求实际用到的出口地理，由 ipinfo 查得。
// 一切统计都按它来，不按 Exit。
type Observed struct {
	IP      string
	Country string
	State   string
	City    string
	ASN     string
	ISP     string
}

// Region 是一个区域及其代表出口。
//
// 区域划分本身随 cohort 变（账号B 把南亚和中东合并了），所以这只是**清单**，
// 不是数据模型里的强约束（SPEC 第 4 节）。
type Region struct {
	Key  string
	Exit Exit
}

// Regions 是 SPEC 第 4 节那张表，代表出口照抄，不要换成「更有代表性」的国家：
// 这些是 2026-10-01 的 170+ 样本里跑出来的，换一个国家可能落到同一个网关、白打一发。
//
// 放在 types 而不是 scan：铸票者的选择、速率闸的分桶、台账的格子都按区域算，
// 它早就不只是扫描清单了。
var Regions = []Region{
	{"us-east", Exit{Region: "US"}},
	{"us-west", Exit{Region: "US", City: "Seattle"}},
	{"south-america", Exit{Region: "BR"}},
	{"west-europe", Exit{Region: "ES"}},
	{"europe", Exit{Region: "IT"}},
	{"east-asia", Exit{Region: "JP"}},
	{"oceania", Exit{Region: "AU"}},
	{"south-asia", Exit{Region: "IN"}},
	{"middle-east", Exit{Region: "SA"}},
}

// RegionKeyOf 把一个出口反查成区域 key，对不上回空串。
//
// **精确匹配 Region+City**，不按国家模糊归并：us-east 和 us-west 的国家都是 US，
// 只有城市分得开它们，而那两个区域在实测里落的是不同网关（167 / 126）。
//
// 续期型铸票的出口取自网关的 ReachableExits，区域 key 就靠它反查出来（配额记到那一格上）。
func RegionKeyOf(e Exit) string {
	for _, r := range Regions {
		if r.Exit == e {
			return r.Key
		}
	}
	return ""
}

// RegionMint 是 (账号 × 区域) 铸票台账的一格。
//
// **终身计数不是滑窗**：它回答的是「这个号在这个区域铸过没有」，用来让铸票者优先去
// 没走过的区域（新区域才可能发现新网关）。本小时的配额是另一回事，归 mint 的速率闸。
type RegionMint struct {
	AccountID int64
	RegionKey string
	// Count 只数**成功**的那几发：失败什么都没告诉我们，记上去会把一个打不通的
	// 区域标成「走过了」，于是永远不再去试。
	Count  int
	LastAt time.Time
	// Gateway 是**上一次**成功铸票落到的网关，空串 = 这个组合还没铸过。
	//
	// 它是预铸挑组合的依据：(账号 × 大区) 基本是一对一的（同账号同出口连铸 5 发落点
	// 逐字相同），所以上次落点就是「再派这个组合会落到哪」的预测。预铸据此跳过
	// 「预测落点已经有活票」的组合，不必真打一发才发现白打。
	//
	// **只是提示，不是约束**：区域划分随 cohort 变（SPEC 第 4 节），同一个号同一个
	// 大区实测见过落两个网关。所以铸完一律按实际落点覆盖它，别拿它当硬判据。
	Gateway string
}

// MintResult 是一次铸票的结果。
//
// Gateway 的语义是本项目最容易写错的地方，照 SPEC.md 第 2 节：
//   - 响应里有新的 set-cookie __oailb → Gateway = 该 JWT 的 host 第三段
//   - 没有新 __oailb、本次发了 pair、且那张的 __cflb **可证还活着** → Gateway = 那张
//     pair 的网关（上游接受了它，路由没变）
//   - 没有新 __oailb、本次发了 pair，但那张的 __cflb 已经不保证活着 → **视为失败**，
//     Gateway 为空。钉路由的是 __cflb：它死了这一发就是按出口地理随机路由走的，
//     而 __oailb 还能读出那个过期的名字 ⇒ 认它就是把触碰记在没打到的网关上
//   - 没有新 __oailb 且本次没发 pair → 视为失败，Gateway 为空
//
// NewPair 为空表示上游没下发新 cookie；此时若 Gateway 非空，说明沿用了送进去的 pair。
type MintResult struct {
	Status      int
	Gateway     string    // "unified-167"
	NewPair     string    // 新下发的完整 cookie 头；没下发则为空
	TurnState   string    // x-codex-turn-state
	PairExpires time.Time // 新下发的 __oailb 的 exp；没下发则零值
	// CflbExpires 是这一发下发的 __cflb 的死期（Set-Cookie 的 Max-Age / Expires）。
	//
	// **零值 = 这一发没补发 __cflb**（上游常只补发变动的那一项）：送进去那张的
	// __cflb 被原样留在 pair 里，而它是 AEAD 密文、读不出时间 ⇒ 它的死期只有账本
	// 上的旧值知道。所以寿命一律走 RouteDeadline，别直接用这两个字段。
	CflbExpires time.Time
	// ErrBody 是非 2xx 时上游回的那段 JSON（截断）。只在失败路径填。
	// 没有它的话一个 400 什么都不告诉人——2026-10-01 首扫全军覆没就查了半天。
	ErrBody string
	// RetryAfter 是上游 Retry-After 头折算出来的时长，零值 = 它没给。
	// 429 的账号级退避要听它的：不听 Retry-After 正是被封的典型走法。
	RetryAfter time.Duration
}

// RouteDeadline 算这一发之后那张 pair 的两个死期：钉路由那个 cookie 自己的（cflb），
// 和路由**真正**失效的时刻（route = 两个组件取小，就是 Pair.ExpiresAt）。
//
// prevCflb 是送进去那张 pair 的 Pair.CflbExpiresAt（裸铸传零值）。三级取值：
//  1. 这一发补发了 __cflb ⇒ 用它自己的 Max-Age
//  2. 没补发 ⇒ 旧的那张原样留在 cookie 里 ⇒ 用账本上它的死期。**这一条不能漏**：
//     漏了就会让新 __oailb 的 65 分钟盖掉旧 __cflb 的死期，虚报量无上限
//  3. 两样都没有（Max-Age 读不出的裸铸）⇒ FallbackCflbExpiry
func (r MintResult) RouteDeadline(prevCflb time.Time) (route, cflb time.Time) {
	cflb = r.CflbExpires
	if cflb.IsZero() {
		cflb = prevCflb
	}
	if cflb.IsZero() {
		cflb = FallbackCflbExpiry(r.PairExpires)
	}
	route = r.PairExpires
	if !cflb.IsZero() && cflb.Before(route) {
		route = cflb
	}
	return route, cflb
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
//   - sentCflbExp = 送进去那张 pair 的 Pair.CflbExpiresAt（裸打传零值）。
//     「上游没下发新 cookie」只有在它**可证还活着**时才意味着「路由没变」，
//     否则这一发其实是随机路由走的、落点读不出来（见 MintResult.Gateway）
type Minter interface {
	Mint(ctx context.Context, c Creds, e Exit, sid, sentPair string, sentCflbExp time.Time) (MintResult, error)

	// VerifyFull 带一张活票再打一发，只读响应头。必须复用铸票时的 sid 和 pair。
	VerifyFull(ctx context.Context, c Creds, e Exit, sid, pair, ticket string) (*Verdict, error)

	// Geo 查这个 sid 当前的出口地理。与 Mint 复用同一 sid 才有意义。
	Geo(ctx context.Context, e Exit, sid string) (Observed, error)
	// Provider returns only the bound provider kind, never its URL or credentials.
	Provider(sid string) string
	SupportsDynamicExit(Exit) bool
}

// Owner 是一个上传者。一个人可能先后交上来好几份凭据，owner 把它们归到一页
// （GET /u/{token}）上——没有它，每份凭据只剩一个孤零零的 /a/ 链接，丢一个少一个。
//
// 和 Account.ViewToken 的分工：view_token 认**一个账号**，owner token 认**一个人**。
// 两个都是 128 bit 的 URL 令牌，所以也都是「谁拿到谁就是他」。刻意不做邮箱密码登录：
// 这个服务没有会话层，为一个只读页面引进一套身份系统是本末倒置。
type Owner struct {
	ID        int64
	Token     string
	Label     string
	CreatedAt time.Time
}

// Account 是一个托管账号。
type Account struct {
	ID        int64
	Label     string
	PlanType  string // "plus" / "pro" / ...，取自 access_token 的声明
	Creds     Creds
	ViewToken string
	// LegacyConsumerKey marks credentials present before owner-level keys.
	// Their existing machine keys remain valid, but are never displayed again.
	LegacyConsumerKey bool `json:",omitempty"`
	Enabled           bool
	CreatedAt         time.Time
	// OwnerID 是这份凭据挂在哪个 Owner 名下。
	//
	// 0 = 没有归属。两种情形：匿名占位行（没凭据、不是谁托管的）**永远**是 0；
	// 托管行的 0 只会出现在加这个字段之前写下的老 state.json 里，由 store.Open
	// 的一次性迁移收口。
	//
	// **归属一旦定下就不改。** 改归属的入口等于「谁手上有一份公开流传的凭据，
	// 就能把别人的账号搬到自己页面下」，而那一页上有对方每个账号的 consumer key。
	OwnerID int64
	// DisabledReason 是自动停用的原因（上游那段话）。手动停用和启用时都清空。
	// 没有它的话页面上只有一个「已停用」，没人知道为什么，只会被重新打开再废一轮。
	DisabledReason string
	// Via 只对匿名占位行有意义：最近一次**取到票**是拿谁的 key（池内账号 ID）。
	// 0 = 不带 key 取的，或这个字段加上之前的记录。占位行没凭据、没归属，不记这个的话
	// 管理页上就只剩一串上游 ID，回答不了「这一行是谁在用」。
	Via int64
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
	AccountID   int64
	RegionKey   string // SPEC.md 第 4 节的区域 key
	Requested   Exit
	Observed    Observed
	Gateway     string
	Status      int
	At          time.Time
	Provider    string `json:",omitempty"`
	Purpose     string `json:",omitempty"`
	TargetMatch string `json:",omitempty"`
	DurationMS  int64  `json:",omitempty"`
	// Err 是这一发失败的具体原因（上游那段 JSON，或本地的错误文本），成功时为空。
	// 没有它的话 samples.jsonl 里一行 status=400 什么都不告诉人：2026-10-01 首扫
	// 二十发全 400，真正的原因（账号清单里没有 gpt-6-astra）要单独打一发才看得见。
	Err string
}

// SampleErrMaxRunes 截断样本里的原因：它进的是账本和网页，不是日志系统。
const SampleErrMaxRunes = 512

// SampleErr 按「上游原话优先、本地错误兜底」拼 Sample.Err。
//
// 上游那段 JSON 优先——它说的是「为什么」（"The 'gpt-6-astra' model is not supported…"）。
// 传输层失败时上游什么都没回，这时候退回本地错误文本，否则这一行只剩 status=0。
// 按**字符**截，别把一个汉字劈成半个落进 samples.jsonl。
//
// 三条路都要用它（首扫、铸票、state-echo）：2026-10-01 的教训是铸票那条路忘了填，
// 于是出口熔断那几分钟 samples.jsonl 里只剩一串 status=0 的空行，
// 「是连不上代理、认证被拒、还是出口中途断开」在事后完全查不出来。
func SampleErr(errBody string, err error) string {
	s := strings.TrimSpace(errBody)
	if s == "" && err != nil {
		s = err.Error()
	}
	if r := []rune(s); len(r) > SampleErrMaxRunes {
		s = string(r[:SampleErrMaxRunes])
	}
	return s
}

// Gateway 是池子里的一个网关。
type Gateway struct {
	Name           string // "unified-142"
	FirstSeenBy    int64
	FirstSeenAt    time.Time
	ReachableExits []Exit
}

// 槽位判定的三个取值。VerdictUnknown 不只是「还没验」，也是客户端回报里**唯一
// 合法的无信息值**：转发路径上没有可用的降智判据，所以它不许覆盖已有判定。
const (
	VerdictFull     = "full"
	VerdictDegraded = "degraded"
	VerdictUnknown  = "unknown"
)

// Slot 是 (账号 × 网关)，降智的作用单位。调度就是在这张表上取歇得最久的。
type Slot struct {
	// LastDeliver 是这个账号上次在这个网关上**拿到票**的时刻（两条交付路径都记）。
	//
	// 和 LastTouch 刻意分开：LastTouch 什么接触都记——扫描、保温、失败的续期，
	// 因为那些都真的烧了窗口。而「有人真的用掉了一张票」是另一件事，它才是
	// 「池子该备多深」的依据。拿 LastTouch 当依据会自激：保温自己也 touch，
	// 于是铸一张让目标 +1、再铸一张再 +1，一路把所有网关铸满。
	LastDeliver time.Time

	AccountID    int64
	Gateway      string
	LastTouch    time.Time
	LastVerdict  string // VerdictFull / VerdictDegraded / VerdictUnknown
	BackoffUntil time.Time

	// PrevDeliver / PrevTouch 是交付那一刻存下的**交付之前**那两个时刻，
	// 供「票取了但一发没打」的归还（/release）还原用。
	//
	// 为什么是存旧值而不是归还时清零：清零会把一次真实的历史接触抹成「从没碰过」，
	// 而那个槽位可能在更早的某轮里真的被烧过——抹掉之后它会被当成首次接触再发一遍。
	//
	// 两个字段必须在**同一次**读-改-写里和 LastDeliver / LastTouch 一起写
	// （见 sched.noteDeliver）：分两次写就会把这次交付自己写进去的值当成旧值存下来，
	// 还原完槽位看起来还是刚烧过的。
	PrevDeliver time.Time
	PrevTouch   time.Time

	// LastDeliverVersion 是上一次交付**真正发出去的那张票**的 cookie_version。
	//
	// 单有 LastDeliver（时刻）不够：/pair/renew 要回答「这张票是不是交付给你的」，
	// 而拿「你报的号 == 池子现持那张的号」当判据是**假判据**——现持那张的号谁都查得到
	// （自己取一张同网关的票就知道），于是一次交付能换到那张 pair 在整个窗口里被
	// 无限次改写（2026-10-02 复审实测 5/5）。记下交付出去的那一张，一次交付就只换到
	// 一次改写：改写之后池子现持的已经不是它了，再报同一个号就过不了票号闸。
	//
	// PrevDeliverVersion 和 PrevDeliver / PrevTouch 同理，归还时一起还原——
	// 漏掉它的话 /release 还原完，票号仍指向一张已经不在的票。
	//
	// 老 state.json 里没有这两个键 ⇒ 零值 ⇒ 续期一律拒，直到下一次交付把它写上。
	// 方向是安全的那边，不需要迁移。
	LastDeliverVersion string
	PrevDeliverVersion string
}

// Pair 是池子里一张活的路由 cookie。
//
// 寿命看 PairLifetime（= 钉路由那个 __cflb 的 Max-Age），不是 __oailb 的 3900 s。
// 下面所有门槛量的都是 ExpiresAt（两个组件取小）：剩余不足 RenewLead 就续；
// 剩余不足 DeliverFloor 不交付。
//
// **刻意没有租约字段。** 曾经有过 LeasedBy/LeaseUntil，理由是「两个不同账号同时带着
// 同一张路由 cookie 到同一个网关」这个形状本身就是跨账号共享的特征，所以同一张 pair
// 同一时刻只许交给一个消费者。2026-10-01 实测把这个假设推翻了：
//
//	一张票 + 一个出口 IP（同 sid，并发两查 ipinfo 确认两发真是同一个 IP）
//	+ 两个账号（pro#1 和已知降智的 plus#2）真并发各打一发。
//	两次独立试验（票分别由 pro 和 plus 铸，落 unified-126 / unified-123，
//	连接重叠 2.0s / 3.0s）：4 发全部 HTTP 200、全部**没有**下发新 __oailb、
//	全部仍落在那张 pair 的网关上；31 个响应头的名字集合 10/10 发逐项一致，
//	x-codex-* 的差异只按账号走（plan-type / credits / limits），没有 retry-after，
//	没有任何只在并发那两发里出现的头。随后两个号各自单打一发做对照，读数一模一样。
//
// 也就是说上游不介意一张 pair 被几个号同时用，互斥只是白白把池子深度砍半。
// 要加回来先拿出比这个更强的实测。
type Pair struct {
	Gateway string
	Cookie  string
	// ExpiresAt 是**路由失效的时刻**，不是某一个 cookie 的 exp：两个组件里先死的那个
	// （几乎总是 __cflb，它比 __oailb 早死 300 秒，见 PairLifetime / OailbLifetime）。
	// 全池判「这张票还能不能发」只读这一个字段。
	ExpiresAt time.Time
	// CflbExpiresAt 是钉路由那个 cookie 自己的死期。单独留着有两个用处：
	// 续期**只补发 __oailb** 时寿命得按它算（新 __oailb 的 65 分钟盖不住旧 __cflb，
	// 而 __cflb 是 AEAD 密文、读不出时间，只有这个字段知道它什么时候死）；
	// 以及零值能认出「老 state.json 写的记录」——那时候 ExpiresAt 存的是 __oailb 的 exp。
	// 零值**绝不能**当成「不过期」，见 store.Open 里的兜底。
	CflbExpiresAt time.Time
	MintedBy      int64
	// RegionKey 是**铸这张票时走的那个出口**所属的区域（Regions 里的 key）。
	//
	// 为什么要存下来而不是事后反查：网关 = (区域 × 账号)，同一个网关名在不同账号
	// 眼里可能来自不同区域 ⇒ 「这张票属于哪个区域」只有铸它的那一刻知道。
	//
	// 旧记录可能是 sampleRegion 兜底的「国家[:城市]」或空串，展示仍兼容。
	// 当前种子续票只认九个标准 key，并用本字段选择逻辑区域；不能从网关的
	// ReachableExits 随机反推，否则会把新票和配额记到另一区。
	RegionKey string
}

// CookieVersionLen 是 cookie_version 的长度（十六进制字符数）。
//
// 12 个字符 = 48 bit。池子里同时活着的票是十几张量级，撞一次的概率在这个量级下
// 可以忽略，而这个值要被人贴进 exclude_versions 里，短一点好用。
const CookieVersionLen = 12

// CookieVersion 是一张具体的票的不透明稳定身份：sha256(cookie) 的前 12 个十六进制字符。
//
// **算出来的，不存状态**：调用方拿它做 exclude_versions 和 /release 的引用，所以
// 重启后必须还是同一个值；而池子里唯一稳定标识一张票的东西就是 cookie 本体，
// 而 cookie 本体一个字都不许出现在响应、页面、日志里（SPEC 红线 1）——
// 哈希正好两边都满足。
//
// 空 cookie 回空串：没有票就没有身份，让调用方自己判，别给它一个「空串的哈希」。
func CookieVersion(cookie string) string {
	if cookie == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(cookie))
	return hex.EncodeToString(sum[:])[:CookieVersionLen]
}

// 时间常量，依据 2026-10-01 实测。改之前先看 SPEC.md。
const (
	// PairLifetime 是**一张票能用多久** = 路由寿命 = __cflb 的 Set-Cookie Max-Age。
	// 铺一张新鲜 pair 的地方一律用它。
	//
	// **钉路由的是 __cflb，不是 __oailb。** 2026-10-01 的干净隔离复验：只带一个 ES 的
	// __cflb 就能把一发 US 直连请求从 unified-126 拽到 183；只带 __oailb 则连落点都
	// 读不出来（上游不认，重新分配）。更早那个「__oailb 签名强制校验」的结论是被
	// 真实 __cflb 污染出来的假结论。__oailb 的作用只是让我们**读出**落到了哪儿。
	//
	// __oailb 还能多活 300 秒（见 OailbLifetime），但那 300 秒**钉不住路由**：
	// 按它算寿命的话，「刚好够交付」那张票（DeliverFloor = 300 s）的路由恰好已经
	// 死了——亲和失效 ⇒ 上游按出口地理重新分配 ⇒ 现场看到的就是
	// 「落点漂移：想要 unified-194，实到 unified-150」。
	PairLifetime = 3600 * time.Second
	// OailbLifetime 是 __oailb 的 exp−iat，上游定死。
	//
	// **它不是「一张票能用多久」**，只有两处真的需要这个跨度：算 __cflb 的兜底死期
	// （FallbackCflbExpiry，差值就是它减 PairLifetime），和构造 / 伪造 __oailb 的时候。
	// 拿它铺场景就会铺出一个生产永远不会出现的状态，而那 300 秒的带子正是出过 bug 的地方。
	OailbLifetime = 3900 * time.Second
	FullWindow    = 183 * time.Second // 首次接触后的满血窗口，静默失效
	DeliverTTL    = 150 * time.Second // 交付给消费者的有效期，FullWindow 留 18% 余量
	// DeliverFloor 是交付的最低 pair 剩余量，量的是 Pair.ExpiresAt（= 路由死期，
	// 也就是 __cflb 那个）。
	//
	// 300 s 的含义是「消费者手上这张至少还能活过两个交付窗口」。
	//
	// 它和 OailbLifetime−PairLifetime 都是 300 s 纯属巧合，别把两件事绑在一起：
	// 前者是「留给消费者的余量」，后者是两个 cookie 的寿命差。
	DeliverFloor = 300 * time.Second
	// RenewAccept 是 /pair/renew 的写放大**兜底**闸：池子手上那张还有
	// 多于 PairLifetime−RenewAccept 的寿命（= 刚换过不到这么久）时受理但不落盘。
	//
	// 刻意只是兜底，不是机制：正常节奏由消费端定——它只在「取票后的第一发业务
	// 请求」里续一次（2026-10-02 口径）。这条防的是异常：重试风暴，或者两个消费者
	// 拿着同一张票各续一次。state.json 是整份原子重写，每次落盘都带着全部账号和槽位。
	RenewAccept = 60 * time.Second
)

// FallbackCflbExpiry 在读不出 __cflb 死期时按实测差值从 __oailb 的 exp 倒推。
//
// 依据：两个 cookie 是**同一个响应**下发的，Max-Age=3600 对 exp−iat=3900 ⇒ 差 300 秒。
// 用在两处：Set-Cookie 里没给 Max-Age/Expires 的裸铸，以及老 state.json 的记录
// （那时候只记了 __oailb 的 exp）。零值进零值出——零值得由调用方当成「不知道」处理，
// 绝不能当成「不过期」。
func FallbackCflbExpiry(oailbExp time.Time) time.Time {
	if oailbExp.IsZero() {
		return time.Time{}
	}
	return oailbExp.Add(-(OailbLifetime - PairLifetime))
}

// AnonPrefix 是外部消费者占位行的 account_id 前缀。
//
// 跨账号取票（一把 key 替别的上游账号取）和匿名取票的槽位都记在 AnonPrefix + 他报的
// account_id 那一行上。加前缀是为了隔开：直接拿他报的 ID 当键，他只要填一个上传者的 ID
// 就能把那个上传者的槽位标成「刚烧过」。**读这本账的地方必须照同一个键找**，不然
// used_by_you 永远是 false。
//
// **「乱填只害他自己」这条推理只对取票成立。** 取票是往账本里**多**记一次接触，
// 方向保守（被冒名的人少拿几张票）；而**少**记的操作方向相反——归还（/release）
// 把一行还原成「从没碰过」会让它排到挑选队列最前，于是被冒名的人立刻拿回自己刚烧过
// 的落点，是降智的票而且无从察觉。占位行的命名空间谁都能填，没有任何归属证据，
// 所以少记那一类操作**一律不许落在占位行上**（见 httpapi.handleRelease）。
const AnonPrefix = "anon:"

// SpareTarget 是池子要比「最忙的那个号」多留几张活 pair。
//
//	目标活 pair 数 = min(max(每个号用过的网关数) + SpareTarget, GatewayCeiling)
//
// 「用过的网关数」= 那个号 slot 表里 LastDeliver 非零的行数，也就是真有人取走过票的落点。
// 多留两张的意思是：最忙的那个号手上**永远还有两个没碰过、且已经有活 pair 的网关**，
// 来取票时不用现等铸票（一发上游要几秒到几十秒）。
//
// 目标超过**已知**网关数时，缺口靠裸铸和探索扫描摸新网关来补；超过**上游网关总数**
// 的那部分永远补不上，所以封顶在 GatewayCeiling。SpareTarget 自己填得比 GatewayCeiling
// 还大 = 停止预铸（不报错）。
const SpareTarget = 2

// GatewayCeiling 是上游网关总数的保守上限：活 pair 数（一个网关一张）不可能超过它。
//
// DNS 枚举 chat.gateway.unified-N.api.openai.com 得 201 个，别人统计是 215 个里 14 个
// 黑洞；按 190 算，给走不到的那几个留余量。不封顶的话，最忙的号用遍全部网关之后
// 目标就永远够不着，每轮保温都会为它白打几发上游（一轮的失手预算 = 缺口，见 premintBare）。
const GatewayCeiling = 190

// ErrRateLimited 是速率闸拦下来的。闸是**按上游账号**算的，不是全局：
// 一个号打太猛不该让别的号跟着挨饿。
//
// 必须能被单独认出来：它**不是这个槽位的错**。当成普通失败处理会把一次限流变成
// 「给所有试过的槽位各记一次退避」，等于拿速率纪律去惩罚供给。
var ErrRateLimited = errors.New("速率闸：这个账号这一小时的上游请求配额用完了")

// ErrNoExit 是「能用的出口一个都没有」：全被熔断跳过了，或者配置里没有符合要求的。
//
// 放在 types 而不是 mint，是因为**调度要认它**：出口没了是池子自己的基础设施故障，
// 给每个试过的槽位记退避等于故障恢复后还让它们挨个瞎等一轮
// （2026-10-01 现网实测：出口恢复了，账号 3 的 19 个槽位还全躺在退避里）。
var ErrNoExit = errors.New("没有可用的出口代理")

// SlotCooldown 是一个 (账号 × 网关) 烧掉之后要歇多久才重新给满血的**默认值**。
//
// **至今没测准**：用户实测见过 30 分钟就好了的，也见过 3 小时还没好的，4h 是保守估算。
// 所以真正生效的是配置项 slot_cooldown_minutes，这个常量只当它的默认值用
// （以及测试里没接配置时的兜底）。要调就调配置，别改这里。
const SlotCooldown = 4 * time.Hour

// GatewaysForContinuous 是「一个账号要不停转」所需的网关数。
//
//	每张票只给 DeliverTTL 这么长的可用时间，用完这个 (账号 × 网关) 就得歇
//	cooldown。所以要连续供上，手里的网关数 = 冷却 ÷ 单张可用时长。
//
// 按 4h ÷ 150s 算是 96 个。这就是「门槛」该有的量级——之前写死的 6 是个
// 随手填的数，6 个网关只够一个账号转 6×150s = 15 分钟，远不是「稳定使用」。
//
// 它同时也解释了这个项目的结构性难点：实际只摸到个位数到十几个网关，
// 离连续供应差一个量级。所以门槛的意义更像是一道**诚实的闸门**——
// 够不到就别对外开放，而不是「调一个小点的数好让它看起来开着」。
//
// 冷却是参数而不是常量：它是实测没定下来的那个数，改一次门槛就得跟着动。
func GatewaysForContinuous(cooldown time.Duration) int {
	if cooldown <= 0 {
		cooldown = SlotCooldown
	}
	n := int(cooldown / DeliverTTL)
	if cooldown%DeliverTTL != 0 {
		n++
	}
	return n
}

// AccountsForContinuous 是对外开放的门槛：要托管多少个账号才够连续供上。
// 只供 min_accounts **没显式写**时推导默认值（见 config.Load）。
//
// **一个账号能摸到多少落点和大区数无关 ⇒ 这里不再做容量除法。** 原先写的是
//
//	一个账号最多能摸到 len(Regions) 个落点 ⇒ 账号数 = 所需网关数 ÷ 区域数，再 +1
//
// 2026-10-03 核了现网 state.json 的槽位台账：130 个有槽位的账号里，碰过网关最多的
// 三个分别摸到 105 / 105 / 103 个，而池子一共就只认得 105 个 —— 单号的触达面是
// **整张网关表**，不是九个，那个除法高估了门槛约 11 倍。根因是「同一个大区持续出新
// 落点」（us-east 一个大区 253 发摸到 36 个不同落点），所以「一个大区一个落点」
// 从来不成立。
//
// 触达面既然不是瓶颈，容量这一问就已经被满足了（105 个可达网关 ≥ 所需的 96），
// 剩下的只有**冗余**：一个号被上游否决会当场停用（见 sched.mintOnce 的
// RejectedByUpstream），429 会把整个号按住最多两小时（holdAccount）——
// 只托管一个号时这两件事里任何一件都等于整池停摆。所以门槛 = 2。
//
// **为什么门槛按账号数算，而不按「此刻有几张活 pair」。** 改成按需铸票之后池子
// 根本不囤票（线上长期就是个位数）⇒ 按活 pair 数判等于把对外开放结构性焊死。
// 账号数量的是**池子够不够格**，活 pair 数量的是**货架上还剩几张**；门槛问的是前者。
// 后者不用在这儿判：没有现成票的匿名请求本来就会 503（DeliverAnonymous 刻意不现铸）。
//
// 真想按容量判就数网关：len(Gateways()) >= GatewaysForContinuous(cooldown)
// （现网 105 >= 96，已经成立）。那是实测的同一件事，而且不用猜单号触达面——
// 但它换的是 min_accounts 这个旋钮的语义，所以留给运营方自己改配置，不在这儿偷偷换。
//
// cooldown 留在签名里但不再参与计算：容量那一问已经不由它决定了（见上）。
// 调用方照旧传配置里那个冷却，这样测准之后要改回容量判据时签名不用动。
func AccountsForContinuous(cooldown time.Duration) int {
	_ = cooldown
	return 2
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
	Gateway string `json:"gateway"`
	// Region 是交付出去这张票的 Pair.RegionKey：**铸它的出口**属于哪个区域。
	// 空 = 池子不知道（老记录 / 反查不到），消费端按「未归类」处理，不要猜。
	//
	// 注意它不是「消费端这一发会落在哪个区域」：cflb 跨出口回放会漂（SPEC 第 4 节），
	// 真实落点只有响应里的新 __oailb 说得准。这个字段只回答「这张票是哪儿铸的」。
	Region       string    `json:"region,omitempty"`
	Cookie       string    `json:"cookie"`
	ExpiresAt    time.Time `json:"expires_at"`
	ValidForS    int       `json:"valid_for_s"`
	VerifiedFull bool      `json:"verified_full"`
}
