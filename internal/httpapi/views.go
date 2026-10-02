package httpapi

import (
	"net/url"

	"gwpool/internal/types"
)

// 视图结构全是**预算好的字符串**，模板里不做运算也不注册函数：
// 页面逻辑留在 Go 里能被编译器看住，模板只管排版。

type base struct {
	Title string
	Mode  types.Mode
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

type poolView struct {
	base
	// 对外开放状态是承诺，得让人看得见。
	//
	// OpenAccounts/Threshold 数的是**托管账号数**，不是活 pair 数——按需铸票的池子
	// 不囤票，按货架存量判等于永远关着（见 types.AccountsForContinuous）。
	// 和下面那个 Accounts 是同一个数，分开两个字段是因为一个在讲「开放门槛」、
	// 另一个在讲「池子规模」，哪天门槛改成别的判据时不该牵连那张概览卡片。
	PublicEnabled bool
	PublicOpen    bool
	OpenAccounts  int
	Threshold     int

	Gateways []gatewayRow
	// ReadyCount 在服务端算好。原来这张卡片靠 JS 数表格行，关掉 JS 就只剩「—」。
	ReadyCount  int
	Accounts    int
	RestedLabel string
	Model       string
	Effort      string
	// OwnerToken 非空 = 这个浏览器带着身份 cookie，直接给一个进总览页的按钮。
	// 空则只给贴令牌的小表单。
	OwnerToken string
	// EnterErr 是上一次贴令牌没认出来。**只有一个 bool**：区分「不存在」和
	// 「格式不对」等于给人一个枚举令牌的工具。
	EnterErr bool
	// BaseURL 是要填进 sub2api 的那个根地址（config.PublicBaseURL）。
	// 空 = 没配，页面上如实说「问管理员要」，绝不拿请求头猜一个。
	BaseURL string
}

type gatewayRow struct {
	Name        string
	Exits       int
	PairReady   bool   // 池里有剩余 ≥ DeliverFloor 的 pair
	PairLeft    string // 人读的剩余时间；**不含 cookie 本体**
	RestedSlots int
}

// ---------- 上传者页 ----------

type accountView struct {
	base
	// ViewToken 只回给持有它的人自己（这一页就是靠它认人的），用来拼本页上的表单地址。
	ViewToken string
	Label     string
	Plan      string
	Enabled   bool
	// DisabledReason 是被自动停用的原因（上游原话）。手动停用时为空。
	DisabledReason string
	Since          string
	ConsumerKey    string // 机器凭证；为空说明服务端没配 consumer_key
	Requests       int
	NewGateways    int
	Model          string
	Effort         string
	Rows           []sampleRow
	Truncated      int
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
	// Token 是这一页自己的地址令牌，用来拼页内链接。
	Token string
	Label string
	Since string
	// Requests / NewGateways 是名下所有账号的合计。
	Requests    int
	NewGateways int
	Model       string
	Effort      string
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
	ConsumerKey string
	Requests    int
	NewGateways int
	// Regions 恒为 types.Regions 那九个，顺序一致。
	Regions []regionCell
	Fails   []failRow
}

// regionCell 是九宫格的一格。
//
// Note 是给人读的那句（「铸过 3 次，最近 … UTC」）：纯靠红绿传达信息对色盲不友好，
// 所以格子里必须有文字，而不只是 title 属性。
type regionCell struct {
	Key    string
	Label  string
	Minted bool
	Count  int
	Note   string
}

// regionLabels 把区域 key 翻成人话。
//
// 只放在视图层：types.Regions 是**扫描清单**（区域划分本身随 cohort 变，见那边的
// 注释），给它挂一列中文名会让人以为那是数据模型的一部分。
var regionLabels = map[string]string{
	"us-east":       "美国东部",
	"us-west":       "美国西部",
	"south-america": "南美",
	"west-europe":   "西欧",
	"europe":        "欧洲",
	"east-asia":     "东亚",
	"oceania":       "大洋洲",
	"south-asia":    "南亚",
	"middle-east":   "中东",
}

// regionLabel 查不到就回 key 本身：往 types.Regions 里加一个区域不该让页面出现空格子。
func regionLabel(key string) string {
	if v, ok := regionLabels[key]; ok {
		return v
	}
	return key
}

// failRow 是「为什么没给票」。Detail 只在管理页填。
type failRow struct {
	At        string
	AccountID int64
	Gateway   string
	// Code 是回给客户端的那个闭集错误码；"release" 表示这行是一次归还。
	Code   string
	Reason string
	Detail string
}

type sampleRow struct {
	At      string
	Where   string // 观测到的国家/城市
	Gateway string
	Status  int
}

// ---------- 管理页 ----------

type adminView struct {
	// Config 是「能改的旋钮」那一节。
	Config configView

	base
	Proxies      []proxyRow
	Owners       []ownerRow
	Accounts     []accountRow
	Gateways     []adminGatewayRow
	Pairs        []pairRow
	Slots        []slotRow
	Samples      []adminSampleRow
	TotalSamples int
	Fails        []failRow
	// Rescan 是刚点过「找新网关」按钮之后的回话："queued" = 真排进去了，
	// 其余非空串是没派出去的原因。空 = 这一次打开不是从那个按钮跳回来的。
	Rescan string
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
	// OwnerID 是归属的上传者。0 = 匿名占位行（不是谁托管的）。
	OwnerID int64
	// DisabledReason 让管理员一眼看出是「上游否决」还是「上传者自己关的」。
	DisabledReason string
	Requests       int
	Found          int
	ViewToken      string
	Since          string
	HasCreds       bool   // 只说有没有，不说是什么
	Upstream       string // 上游账号标识，匿名消费者按它认账号
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

type adminGatewayRow struct {
	Name        string
	FirstSeenBy int64
	FirstSeen   string
	Exits       string
}

type pairRow struct {
	Gateway  string
	Left     string
	MintedBy int64
}

type slotRow struct {
	AccountID int64
	Gateway   string
	Rested    string
	Verdict   string
	Backoff   string
}

type adminSampleRow struct {
	At        string
	AccountID int64
	Region    string
	Where     string
	ASN       string
	Gateway   string
	Status    int
}

// parseForm 解表单体。自己解而不用 r.ParseForm：请求体已经被 MaxBytesReader 读走了。
func parseForm(raw []byte) (url.Values, error) {
	return url.ParseQuery(string(raw))
}
