package httpapi

import (
	"net/url"

	"gwpool/internal/config"
	"gwpool/internal/sched"
	"gwpool/internal/types"
)

// 视图结构全是**预算好的字符串**，模板里不做运算也不注册函数：
// 页面逻辑留在 Go 里能被编译器看住，模板只管排版。

type base struct {
	Title      string
	Mode       types.Mode
	Admin      bool
	AdminPage  string
	Restricted bool
}

// ---------- 门禁页 ----------

type gateView struct {
	base
	Wrong bool // 带了码但不对；只影响文案，状态码恒为 403
	// **刻意不带「输完码回原页」的目标路径**：那要把 r.URL 反射进门禁页正文，而
	// /a/<view_token> 和 /u/<owner_token> 的路径本身就是访问凭证 —— 门禁页是未鉴权
	// 的 403，不该有任何来自请求的内容（TestGateBlocksHumanPages 钉着这一点）。
	// 代价是别人发来 /a/… 链接时，输完码落在首页，得自己回头再点一次。
}

// ---------- 公开页 ----------

// uploadForm 是上传表单（layout.html 里的 "upload"）的参数。两个都空 ⇒ 给「挂到已有
// 账号页」输入框。
//
// PageToken：/u/ 页自己的令牌，进隐藏字段 ⇒ 在这一页传的都挂到这一页。
// KnownOwner：首页认出了浏览器的身份，**只用来给个链接、不进表单**——服务端本来就按
// cookie 挂；写进表单的话，在别的标签页登出之后，这个旧标签页还会把凭据挂回去
// （审查 2026-10-02）。
type uploadForm struct {
	PageToken  string
	KnownOwner string
}

type poolView struct {
	base
	Upload uploadForm
	// 对外开放状态是承诺，得让人看得见。
	//
	// OpenAccounts按调度原有开放资格统计，不等于展示侧的凭据有效性读数。
	PublicEnabled bool
	PublicOpen    bool
	OpenAccounts  int
	Threshold     int
	PublicKey     string
	PublicLimits  config.PublicWhenSurplus

	// Gateways 是**全部**已知网关：上面那几个 KPI 和页签计数都数它，筛选时不能跟着缩水
	// （筛着欧洲时顶上写「已知网关 2」是在说池子只剩两个）。表里实际列出来的是 Shown。
	Gateways []gatewayRow
	// Shown 是表里列出来的那几行：没筛选时等于 Gateways，筛了就是那个区域的。
	Shown []gatewayRow
	// FilterLabel 非空 = 正在只看某一格，页面上要如实说、并给一条退出去的路。
	FilterLabel string
	// Regions 按已发现网关的历史机房归属分区，每个网关只计入一格。
	// 不提供目录总量、缺项或未发现名称；Total仅是本池已经发现的数量。
	//
	// 每一格都是个链接（筛选走查询串、服务端过滤）：**不写 JS**，关掉 JS 也能用，
	// 而且表格那套 data-grid 的搜索/分面/计数全都照常工作——它只是拿到了更少的行。
	Regions []poolRegionCell
	// Mints 是最近几发铸票，新的在前（见 poolMintRows）。
	//
	// 做成**历史**而不是「此刻在铸的」：一发铸票只有几秒，而这一页是服务端渲染的
	// ⇒ 在飞快照大概率恒为空，看不出池子到底在不在干活。
	Mints []poolMintRow
	// TotalMints 是账本里一共多少发（页签上那个计数），不是 Mints 的长度 —— 表里只列最近几十行。
	TotalMints int

	// ReadyCount 在服务端算好。原来这张卡片靠 JS 数表格行，关掉 JS 就只剩「—」。
	ReadyCount int
	Health     accountHealth
	Model      string
	Effort     string
	// OwnerToken 非空 = 这个浏览器带着身份 cookie，直接给一个进总览页的按钮。
	// 空则只给贴令牌的小表单。
	OwnerToken string
	// EnterErr 是上一次贴令牌没认出来。**只有一个 bool**：区分「不存在」和
	// 「格式不对」等于给人一个枚举令牌的工具。
	EnterErr bool
	// BaseURL 是要填进 sub2api 的那个根地址（config.PublicBaseURL）。
	// 空 = 没配，页面上如实说「问管理员要」，绝不拿请求头猜一个。
	BaseURL string
	// Supply 是「可交付网关这个数接下来会涨还是会降」的账。
	Supply supplyLedger
}

// supplyLedger keeps inventory and request activity separate, without forecasting.
type supplyLedger struct {
	WindowMin, FloorMin, Ready, Expiring, Target, Gap int
	Attempts, Landed                                  int
	Unavailable                                       bool
}

// poolMintRow 是公开页「铸票记录」里的一行。
//
// 字段是**白名单**，不是「把 adminSampleRow 抄过来删几个」：采样行里带出口国家/城市
// （geoLabel）、ASN 和上游原始错误正文，三样都不许上这一页 —— 前两样用户定过只给管理员看，
// 错误正文是上游回的 JSON，里面可能带账号信息。
type poolMintRow struct {
	At      string
	Account string // 遮过的备注，见 maskLabel
	Region  string
	Gateway string // 落点；裸铸读不出来时是「—」
	OK      bool
	// Status 是上游 HTTP 状态码。0 = 一发都没出站（传输失败/本地拦下），
	// 模板据此显示「失败」而不是「HTTP 0」。
	Status int
}

type gatewayRow struct {
	Name      string
	Exits     int
	PairReady bool   // 池里有剩余 ≥ DeliverFloor 的 pair
	PairLeft  string // 人读的剩余时间；**不含 cookie 本体**
	// Regions 是该已发现网关的机房区域（最多一个），给行的悬停用。
	// 空 = 机房归属未知，不能从代理出口推断。
	Regions []string
	// regionKeys 是上面那几个区域的 key（含未归类的空串），只给筛选用，模板不读。
	regionKeys []string
}

// poolRegionCell 是号池页九宫格的一格：这个区域已知哪些网关、其中几个此刻可交付。
//
// 末尾可能多出一格「未归类」：机房归属未知的网关要有地方落，否则九格加起来
// 对不上页面自己写的网关总数，而那种对不上没人查得出来。
type poolRegionCell struct {
	poolRegionFilter
	Key   string
	Label string
	Total int
	Ready int
	Note  string
	// All = 这是「全部」那一格（退出筛选用）。它不列网关名：几十个名字堆在一格里
	// 什么也读不出来，那正是下面那张表的活。
	All bool
	// Gateways 按名字排，可交付的在前——这一格最常被问的是「现在这儿能不能拿到票」。
	Gateways []poolRegionGateway
}

type poolRegionGateway struct {
	Name  string
	Ready bool
}

// poolRegionFilter 是九宫格每一格的点击目标。
//
// Href 在服务端拼好：模板不注册函数（见本文件顶上的注释），把 url.Values 的拼接
// 散进模板就得给它加一个转义规则，而那正是最容易写漏的地方。
type poolRegionFilter struct {
	Active bool
	Href   string
}

// ---------- 上传者页 ----------

type accountView struct {
	base
	Activity accountActivity
	// ViewToken 只回给持有它的人自己（这一页就是靠它认人的），用来拼本页上的表单地址。
	ViewToken string
	Label     string
	Plan      string
	Enabled   bool
	// DisabledReason 是被自动停用的原因（上游原话）。手动停用时为空。
	DisabledReason string
	Since          string
	Requests       int
	NewGateways    int
	Model          string
	Effort         string
	Rows           []sampleRow
	Truncated      int
	// Regions 和总览页那一格同一份读数（regionCells）：拿着 /a/ 链接的人也该看得见
	// 「我这个号在哪几个区域取到过票」—— 那是他判断自己这份凭据有没有在干活的最直观读数，
	// 而他手上可能只有这一个链接（总览令牌只在自己浏览器里认得出他时才显示）。
	Regions regionGrid
	// Fails 是最近几次没给出票的交付：客户端那头只看到一个 502/503。
	Fails []failRow
	// OwnerToken 只在**请求自己带着**这个账号 owner 的 cookie 时才填（见 handleAccount）：
	// 这一页是转发得出去的，而这个令牌能打开这个人所有账号的页面。
	OwnerToken string
	// BaseURL 见 poolView.BaseURL。
	BaseURL string
}

// ---------- 上传者总览页（/u/{token}） ----------

type ownerView struct {
	base
	AccountKey string
	KeyActive  bool
	Upload     uploadForm
	// Token 是这一页自己的地址令牌，用来拼页内链接。
	Token string
	Label string
	Since string
	// Requests / NewGateways 是名下所有账号的合计。
	Requests    int
	NewGateways int
	// LastFetch 是名下所有账号里最近的那次取票（人读），空 = 一次都没有。
	LastFetch string
	Model     string
	Effort    string
	// BaseURL 见 poolView.BaseURL。
	BaseURL string
	// CanLogout：访客的身份 cookie 正好是这个 owner，给一个「登出」按钮。
	CanLogout bool
	Accounts  []ownerAccountRow
}

type ownerAccountRow struct {
	ID      int64
	Label   string
	Plan    string
	Enabled bool
	// DisabledReason 是自动停用的原因（上游原话）。手动停用时为空。
	DisabledReason string
	Since          string
	// ViewToken 用来拼进这个账号详情页的链接。同一个 owner 名下的账号，这一页
	// 本来就都看得见，不是额外的泄漏面。
	ViewToken   string
	Requests    int
	NewGateways int
	// LastFetch 是最近一次拿这个号的接入 key 取到票（人读的「12 分钟前」），空 = 还没取到过。
	// 上传者拿它判断自己那边 sub2api 对接通没通：探测请求数只说明池子在用他的号。
	LastFetch   string
	LastFetchAt string // 同一时刻的绝对时间（UTC），给悬停
	// Regions 恒为 types.Regions 那九个，顺序一致。
	Regions regionGrid
	Fails   []failRow
}

// regionCell 是九宫格的一格。
//
// Note 是给人读的那句（「铸过 3 次，最近 … UTC」）：纯靠红绿传达信息对色盲不友好，
// 所以格子里必须有文字，而不只是 title 属性。
// regionGrid 是九宫格整块。窗口文案在格子外面说一次，不在九个格子里各写一遍。
type regionGrid struct {
	// Window 是「最近多久」的人读文案（跟着 slot_cooldown 走，默认 4 小时）。
	Window                   string
	Cells                    []regionCell
	Total, Recorded, Enabled int
}

type regionCell struct {
	Key     string
	Label   string
	Enabled bool
	// Fresh = **窗口内**在这个区域取到过路由 Cookie。
	//
	// 刻意不是终身计数：这张卡要回答的是「此刻这个号还能去哪个区域铸票」，而终身绿
	// 会把一个三天前摸过、现在早凉了的区域画成「已探测」，正好把答案盖掉。
	// 终身读数降级进 Note（悬停还看得到）。
	Fresh bool
	// Ago 是最近一次取到票距今多久（Fresh 时才有意义）。
	Ago  string
	Note string
	// Gateways 是这个账号在这个区域**历史上**见过的网关名（样本里的，台账只有计数）。
	// 这一列刻意是终身的：它回答的是「这个区域能给到哪些落点」，窗口内没铸过的格子
	// 照样该看得见——否则最该被派出去的那几格永远空着。
	Gateways []regionGateway
}

// regionGateway 是九宫格某一格里见过的一个网关。
//
// Dup = 这一格没给池子添新落点：这个网关要么记在别人名下（池子里先有了），
// 要么这个账号自己在更早的区域已经撞到过。标出来是为了让人看出「还值得往哪个
// 区域派」——九个格子全绿但全是重复的号，再派也只是白烧窗口。
type regionGateway struct {
	Name string
	// Short 是去掉恒定前缀 unified- 之后的编号：九个格子排一行，每格只放得下数字。
	// 完整名字进悬停提示。
	Short string
	Dup   bool
}

// failRow 是「为什么没给票」。Detail 只在管理页填。
type failRow struct {
	At        string
	AccountID int64
	Account   string // 「#3 xxx」，只有管理页填
	Gateway   string
	// Code 是回给客户端的那个闭集错误码；"release" 表示这行是一次归还。
	Code   string
	Reason string
	Detail string
}

type sampleRow struct {
	At                                  string
	Where                               string // 观测到的国家/城市
	Gateway                             string
	Status                              int
	Provider, Purpose, Requested, Match string
	DurationMS                          int64
}

// ---------- 管理页 ----------

type adminView struct {
	// Config 是「能改的旋钮」那一节。
	Config configView

	MintAllAvailable  bool
	MintAll           sched.MintAllStatus
	MintAllStarted    string
	MintAllFinished   string
	CooldownStats     []types.CooldownStats
	ContactStats      []types.ContactStats
	Health            accountHealth
	Supply            supplyLedger
	Directory         directoryView
	UnauthorizedCount int

	base
	Proxies    []proxyRow
	ProxyPools []proxyPoolRow
	Owners     []ownerRow
	Accounts   []accountRow // 托管着凭据的
	// AccountFacets 是账号表的几组筛选（状态 / 套餐 / 区域覆盖 / 上传者），组之间用 ";" 隔开，
	// 格式见 layout.html 里的 grid()。服务端拼好：上传者名字里的 ":" "|" ";" 会把它拆坏。
	AccountFacets string
	External      []accountRow // 匿名占位行：池里没凭据、只来取票的上游账号
	ExternalUsage []externalUsageRow
	Gateways      []adminGatewayRow
	// SlotAccounts 是「账号×网关」按账号汇总：明细一千多行，平铺会占掉整页一大半体积。
	SlotAccounts []slotAccountRow
	SlotTotal    int
	SlotVerdicts [3]int // 全部槽位最近一次校验：正常 / 降级 / 未知
	Samples      []adminSampleRow
	TotalSamples int
	Fails        []failRow
	// Started 是这个进程什么时候起的（UTC）。取票失败只在内存里，重启就清空——
	// 不写明的话，空表会被读成「最近没失败」。
	Started string
	// Rescan 是刚点过「找新网关」按钮之后的回话："queued" = 真排进去了，
	// 其余非空串是没派出去的原因。空 = 这一次打开不是从那个按钮跳回来的。
	Rescan string
	// ProxyEditable：接了配置文件和出口代理，出口代理那一节才有「添加」表单和停用按钮。
	ProxyEditable bool
	// ProxyNote / ProxyErr 是刚加完、刚停用完代理跳回来时的回话。
	ProxyNote, ProxyErr string
}

type externalUsageRow struct {
	ID string
	types.ExternalConsumerUsage
}

// ownerRow 是一个上传者。**Token 在这一页是故意给的**：迁移出来那个 owner 的令牌
// 没人见过，不显示就等于现有账号的总览页谁都进不去，而这一页已经要 admin key。
type ownerRow struct {
	ID       int64
	Label    string
	Token    string
	Accounts int
	Since    string
}

type accountRow struct {
	ID      int64
	Label   string
	Plan    string
	Enabled bool
	Health  string
	// OwnerID 是归属的上传者。0 = 匿名占位行（不是谁托管的）。
	OwnerID int64
	// DisabledReason 让管理员一眼看出是「上游否决」还是「上传者自己关的」。
	DisabledReason string
	// Held 是「上游 429 按住了它，还要等 X」。空 = 没被按住。
	// 和 DisabledReason 分开：被按住的号**仍然是启用的**，等额度窗口过去自己就好，
	// 不需要人去点恢复。没有这一格的话它在表里就是个「健康但不干活」的号。
	Held      string
	Requests  int
	Found     int
	ViewToken string
	Since     string
	Upstream  string // 上游账号标识；外部账号去掉了 anon: 前缀
	Owner     string // 「#6 xxx」：上传者
	Covered   string // 「4/9」：九个区域里铸出过票的有几个
	Facet     string // 账号表筛选用的标签，空格分隔（见 adminView.AccountFacets）
	// 下面几个只给外部账号（匿名占位行）：它不探测，「请求数」「发现网关」恒为 0，
	// 有用的是谁在替它取票、取了多少、最近一次在什么时候。
	Via         string // 「#20 free-kl1」；空 = 不带 key 取的或没记录到
	ViaToken    string // Via 那个账号的 /a/ 令牌；空 = 那个账号已删除
	SameAs      string // 报上来的上游 ID 正好是池里某个托管账号的：「#3 xxx（已停用）」
	Delivered   int    // 取过票的网关数（槽位台账里 LastDeliver 非零的）
	LastDeliver string // 最近一次取票（UTC）；空 = 一张都没取到过
	// Card 是 /u/ 那张卡的同一份读数（ownerAccountRows）：最近取票、接入 key、九区域格子、
	// 取票失败。只给托管着凭据的号——管理页账号页签一号一张紧凑卡，就是拿它画的。
	Card ownerAccountRow
}

// proxyRow 是一条出口供应商。**没有密码字段**，用户名只给前 4 个字符：
// novproxy 的用户名里嵌着账号标识，整串贴出来等于泄一半凭据。
type proxyRow struct {
	Name      string
	Kind      string
	Host      string
	UserMask  string
	Enabled   bool
	Sticky    bool
	Fails     int
	SkipLeft  string // 熔断剩余，空 = 没被熔断
	BoundSIDs int
}

type proxyPoolRow struct {
	ID, Title, Description string
	RenewOnly              bool
	Rows                   []proxyRow
}

type adminGatewayRow struct {
	Name        string
	FirstSeenBy string // 「#2 xxx」
	FirstSeen   string
	Exits       int    // 请求过的出口数，和首页那个「出口 N」是同一个数
	ExitLabels  string // 那几个出口本身，给悬停
	// 这个网关手上那张路由 Cookie（原来单独一个页签，一个网关最多一张，就并进来了）。
	// PairState: ready / short / expired / none; never expose cookie contents.
	PairState string
	PairLeft  string
	MintedBy  string
	// Places 是实际从哪些国家/城市到达过这个网关，只给管理员看（首页只给出口数）。
	Places                   []gatewayPlace
	Contacts                 []types.ContactStats
	Full, Refreshed, Unknown int
	Cooldown                 types.CooldownStats
	CooldownLabel            string
}

// gatewayPlace 是到达过某个网关的一个国家，和从那个国家出去的城市。
type gatewayPlace struct {
	Country string
	Cities  []string
	Known   int // 城市名已知的有几个（「城市不明」那一项列出来但不算数）
}

// slotAccountRow 是「账号×网关」按账号汇总的一行，明细（每个网关一句话）点开才看。
type slotAccountRow struct {
	Account     string // 「#3 xxx」
	Touched     int    // 碰过的网关数
	Cooling     int    // 交付过、还在冷却期里的
	Backoff     int    // 退避中的
	Full        int    // 最近一次校验：正常
	Degraded    int    // 降级
	LastDeliver string // 最近一次取票（UTC）；空 = 一张都没取过
	Gateways    []string
}

type adminSampleRow struct {
	At                                  string
	AccountID                           int64
	Account                             string // 「#3 xxx」
	Region                              string
	Where                               string
	ASN                                 string
	Gateway                             string
	Status                              int
	Provider, Purpose, Requested, Match string
	DurationMS                          int64
}

// parseForm 解表单体。自己解而不用 r.ParseForm：请求体已经被 MaxBytesReader 读走了。
func parseForm(raw []byte) (url.Values, error) {
	return url.ParseQuery(string(raw))
}
