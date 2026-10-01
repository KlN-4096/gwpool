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
}

// ---------- 公开页 ----------

type poolView struct {
	base
	// 对外开放状态是承诺，得让人看得见。
	PublicEnabled bool
	PublicOpen    bool
	FreeGateways  int
	Threshold     int

	Gateways []gatewayRow
	// ReadyCount 在服务端算好。原来这张卡片靠 JS 数表格行，关掉 JS 就只剩「—」。
	ReadyCount  int
	Accounts    int
	RestedLabel string
	Model       string
	Effort      string
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
}

// failRow 是「为什么没给票」。Detail 只在管理页填。
type failRow struct {
	At        string
	AccountID int64
	Gateway   string
	Reason    string
	Detail    string
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
	Accounts     []accountRow
	Gateways     []adminGatewayRow
	Pairs        []pairRow
	Slots        []slotRow
	Samples      []adminSampleRow
	TotalSamples int
	Fails        []failRow
}

type accountRow struct {
	ID      int64
	Label   string
	Plan    string
	Enabled bool
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
