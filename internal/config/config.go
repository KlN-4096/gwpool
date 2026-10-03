// Package config 从一个 JSON 文件加载运行配置。
//
// 为什么不走命令行参数和环境变量：代理凭据和 access_token 同级敏感，
// argv 会被 ps 看见（SPEC 第 5 节）。只从文件读，一个来源，好审计。
package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gwpool/internal/types"
)

// Secret 是不该被打印的字符串。
//
// types.Creds 不能实现 MarshalJSON（state.json 要原样存回 token），但配置只读不存，
// 所以这边连 JSON 一起打码：String / GoString / MarshalJSON 全打码，于是
// %v %s %q %#v 和任何 json.Marshal（配置被塞进调试接口、日志行、错误消息）都漏不出去。
// 真要用值时显式 Reveal()，调用点一眼能查（SPEC 红线 1）。
type Secret string

func (s Secret) String() string {
	if s == "" {
		return ""
	}
	return "***"
}

func (s Secret) GoString() string { return `"` + s.String() + `"` }

func (s Secret) MarshalJSON() ([]byte, error) { return json.Marshal(s.String()) }

// Reveal 取出真值。只在拼代理 URL、比对鉴权头这种出口上调用。
func (s Secret) Reveal() string { return string(s) }

// ProxyKind 选内置的用户名构造器。加新供应商 = 加一个 case，不要搞模板语言。
type ProxyKind string

const (
	// KindNovproxy 住宅代理，粘性会话：<user>-region-<CC>[-city-<City>]-sid-<SID>-t-<N>
	KindNovproxy ProxyKind = "novproxy"
	// KindWebshare 机房代理，每连接换 IP：<user>-<CC>-rotate
	KindWebshare ProxyKind = "webshare"
	// KindSeed 是我们自己的普通机房代理，用户名里不带任何定向段，原样送出去。
	//
	// 它**选不了地理**，所以只做一件事：带着某个大区的种子 `__cflb` 去摸这个账号
	// 在那个大区的网关（2026-10-03 实测，见 docs 和 mint.Client.NewSeedSID）。
	// 不参与住宅轮转——`Sticky()` 返回 false 把它挡在 StickyProxies 外面。
	KindSeed ProxyKind = "seed"
)

// Proxy 是一个出口代理供应商。
//
// 两种 kind 不是主备关系，是**互补的发现手段**：webshare 的机房 IP 打到过
// unified-121 和 unified-71，而 novproxy 的 117 个住宅样本一次都没碰到这两个。
//
// novproxy 的主机只能用 us.novproxy.io:1000。面板建议亚洲走 sg.novproxy.io:1000，
// 但那台是死的（2026-10-01 实测 14 发亚洲样本全是空响应）；us 打亚洲正常。
type Proxy struct {
	Name    string    `json:"name"` // 日志和轮转里标识这条出口，不含凭据
	Kind    ProxyKind `json:"kind"`
	Host    string    `json:"host"` // "us.novproxy.io:1000"
	User    string    `json:"user"`
	Pass    Secret    `json:"pass"`
	Enabled bool      `json:"enabled"`
}

// Sticky 说明同一个 sid 能不能钉住同一个出口 IP。
//
// 这是能力边界不是优化：铸票和随后的 state-echo 必须同一个出口 IP，否则可能换网关，
// 所以**只有粘性供应商能做需要配对的事**（扫描时的 Geo+Mint 配对、交付前的铸票+echo）。
// 非粘性的只能做粗粒度国家扫描：拿落点，不做质量判定。
// 续期不挑供应商——续期带着 pair 打，路由由 cookie 决定，出口无所谓。
// Sticky 报告这个供应商能不能把同一个 sid 钉在同一个出口 IP 上。
//
// webshare 也能：用户名尾段给个**纯数字**会话号就固定 IP（2026-10-01 实测
// 同号两发同 IP、换号换 IP；字母数字的会话号认证直接被拒，见 mint.webshareSession）。
// 它粘住多久在 webshare 面板里设，不在用户名里。
//
// 这件事不是锦上添花：只有一家粘性供应商时，它一熔断整池立刻停摆
// （2026-10-01 现网两次全池 502 都是这么来的）。
func (p Proxy) Sticky() bool { return p.Kind == KindNovproxy || p.Kind == KindWebshare }

// Fingerprint 选出站报文形状。见 internal/mint。
type Fingerprint string

const (
	// FingerprintSub2api 照 sub2api 实际出站的形状：真客户端的头全集 + 身份体。默认。
	FingerprintSub2api Fingerprint = "sub2api"
	// FingerprintMint 是 relay 作者那套极简形状。有 170+ 真实样本实证，是已知能用的后路。
	FingerprintMint Fingerprint = "mint"
)

// Config 是全量配置。字段缺省时由 Defaults 填。
type Config struct {
	Mode    types.Mode `json:"mode"`
	DataDir string     `json:"data_dir"`
	Listen  string     `json:"listen"`

	AdminKey    Secret `json:"admin_key"`
	ConsumerKey Secret `json:"consumer_key"`
	// SiteKey 给页面加一道访问码：想分享给谁就把码给谁。空 = 不设门禁。
	SiteKey Secret `json:"site_key"`

	// PublicBaseURL 是这个服务对外的根地址（"https://pool.example.com"），
	// 页面上拿它告诉人「填进 sub2api 的是这个，不是你的个人页地址」。
	//
	// **只能配置，不许从请求头推。** Host / X-Forwarded-Host 是调用方可控的输入，
	// 照它拼出来的地址会把这几个页面变成一台反射式的钓鱼地址分发器：攻击者带一个
	// 自己的 Host 来访问，受害者在页面上读到的「接口地址」就指向攻击者的域名。
	// 空就如实说「问管理员要」——猜一个比不说更糟。
	//
	// Load 会把它规范化成 scheme+主机名（去掉末尾斜杠、拒掉带路径的）：
	// 页面上要拼 "<base>/cookie"，多一个斜杠就是个 404。
	PublicBaseURL string `json:"public_base_url"`

	Proxies     []Proxy     `json:"proxies"`
	Fingerprint Fingerprint `json:"fingerprint"`

	PublicWhenSurplus PublicWhenSurplus `json:"public_when_surplus"`

	// StickyMinutes 是 novproxy 的 -t-，同 sid 在这么多分钟内固定同一出口 IP。
	StickyMinutes int `json:"sticky_minutes"`

	// Models 是铸票/验满血可以用的模型，**第一个是默认**。
	//
	// **不写死**：路由 cookie 钉的是网关 host，里面没有一个字节和模型有关，所以
	// 换模型不影响池子的产出——2026-10-01 实测过了：gpt-6-astra 铸的 pair 带着去打
	// gpt-5.6-sol，两个号各一次（pro#1 → unified-126、plus#2 → unified-123）都是
	// 200、仍落在那张 pair 的网关上、上游**一个新 __oailb 都没下发**；而同 sid（同出口
	// IP）裸打 gpt-5.6-sol 作对照，每次都下发了新 __oailb。也就是「没换路由」是那张
	// cookie 起的作用，不是 IP 碰巧落回去。⇒ pair 不用按模型分开存。
	// 而账号能用哪些模型是上游随时在动的——2026-10-01
	// 实测 pro1 的 /backend-api/codex/models 里已经没有 gpt-6-astra，打它只会换回
	// 400 "The 'gpt-6-astra' model is not supported when using Codex with a ChatGPT
	// account."，二十个区域全军覆没。拿账号自己清单里有的填这里。
	//
	// 多个的意义是**候选**，不是并存：同一时刻池子只用一个（第一个），其余是给你在
	// 管理页上随手切、以及给消费者点名用的。槽位**不按模型分**——它仍然是
	// (账号 × 网关)，所以别指望「astra 烧了但 luna 还满血」这种粒度。
	Models []string `json:"models"`

	// Model 是默认模型（= Models[0]）的**派生镜像**，Load 结束时由它自己填。
	//
	// 为什么留着它：老配置文件写的是单值 `"model"`，Load 会把它折进 Models；而代码里
	// 已经有几处直接读 Model。两边都兼容掉比全局改名更省事。
	// **不要手写它**——写了也会被 Models[0] 盖掉。Save 也不会把它写回文件。
	Model string `json:"model,omitempty"`

	// UpstreamPerRegionHour 是**每个上游账号、每个区域**每小时最多打多少发上游。
	// 0 时按 Defaults。它是安全阀：任何路径上的 bug 导致疯狂重铸，都在这里被兜住，
	// 而不是在上游变成封号。
	//
	// 按区域分桶而不是按账号算总量：九个区域各 6 发合起来正好是原先的 54 发/小时，
	// 但一个区域打不通时的疯狂重试再也吃不掉另外八个区域的预算了——而那八个区域
	// 才是发现新网关的地方。
	UpstreamPerRegionHour int `json:"upstream_per_region_hour"`

	// VerifyFullAtDeliver 决定交付前要不要用 state-echo 验一次满血。**默认关**。
	//
	// **2026-10-02 修掉的一条过时理由**：这里原先写「验证动作自己就把要交付的那个窗口烧掉了，
	// 交出去的已经是个二手落点」。那是错的 —— 当天实测**满血窗口内连打多发都是满血**
	// （窗口 ≥200s、窗口内 7/7），所以那两发不烧窗口，只是把窗口的计时提前启动、
	// 消费方少拿几十秒。
	//
	// 所以开不开是**成本取舍**，不是「验了就废」：代价是每次交付多 2 发上游请求，
	// 而 state-echo 和铸票共用「每区每小时 6 发」那个预算（UpstreamPerRegionHour）。
	// 加上 state-echo 有假阴无假阳（SPEC 第 3 节），关掉只损失一个读数、不会把降智的东西
	// 放出去，所以默认仍然关。
	VerifyFullAtDeliver bool `json:"verify_full_at_deliver"`

	// SlotCooldownMinutes 是 (账号 × 网关) 烧掉之后要歇多久，0 时按 types.SlotCooldown。
	//
	// 这个数至今没测准：实测见过 30 分钟就恢复的，也见过 3 小时还没恢复的，
	// 默认的 4 小时是保守估算。做成配置项就是为了能按当天的观测调。
	SlotCooldownMinutes int `json:"slot_cooldown_minutes"`

	// MaxTriesPerDeliver 是一次 /cookie 最多试几个网关，0 时按 Defaults（2）。
	//
	// 它是**请求数的乘数**：每多试一个网关，失败也要花一发上游（没票就得现铸）。
	// 2 的意思是「给你一次改口的机会，然后老实认 503」——试不出来就把供给
	// 留给下一个人，而不是一个倒霉的消费者把整池摸空（2026-10-01 实测连试 5 个、
	// 耗 90 秒，最后还是 503）。
	MaxTriesPerDeliver int `json:"max_tries_per_deliver"`

	// SpareTarget 是池子要比「最忙的那个号用过的 pair 数」多留几张活 pair。
	//
	// **0 是合法值，意思是全按需、不预铸**（而不是「用默认值」——只有键缺着才回默认 2）。
	// 这个旋钮买的东西很具体：首次取票不用现等铸票（一发上游要几秒到几十秒）。
	// 代价同样具体：预铸出来没人取、65 分钟后自然过期的那几张就是纯浪费的请求和槽位。
	// 想把上游请求压到最低就调 0。
	SpareTarget int `json:"spare_target"`

	// MinPairs 是活 pair 数的**下界**：目标深度取 max(最忙那个号用过的张数 + SpareTarget, MinPairs)。
	//
	// 和 SpareTarget 的区别是它**不跟着负载走**。SpareTarget 是相对量（比最忙那个号多留几张），
	// 所以池子空闲下来时目标会跟着塌到 SpareTarget 本身；真有人来的第一发就要现等铸票
	// （一发上游几秒到几十秒）。MinPairs 回答的是「不管有没有人用，手上至少备着这么多张」。
	//
	// 0 = 不设下界（默认，行为与接这个键之前逐字一致）。
	// 填得比 GatewayCeiling 大按 GatewayCeiling 算 —— 那个数本来就铸不出来，而这里
	// **不走 SpareTarget 那条「超了就停止预铸」的路**：下界的语义是「至少」，
	// 把「要得太多」解释成「一张都不要」正好反过来。
	MinPairs int `json:"min_pairs"`

	// RenewPairs 打开「给快到点的票续命」（见 sched.renewExpiring）。
	//
	// **这个键 2026-10-03 删过又加回来，不是同一个东西。** 删掉的那版用 minterFor 挑
	// 「本小时打得最少的号」去续别人的网关 —— 而网关 =(大区 × 账号)、`__cflb` 只钉大区
	// ⇒ 落点是那个号自己的网关，实测 51 发漂 44 发，于是保温失败刷屏、把裸铸挤掉。
	// 现在这版拿的是**铸出这张票的那个号**和**票自己的 `__cflb`**，落点确定。
	//
	// 买的东西：难摸到的网关不会因为票过期而丢 —— 裸铸再碰到它的概率只是「那几个能落到
	// 它的号恰好被挑中」。花的钱：每张续票一发真实上游请求，吃那个号那个区域的小时预算。
	// 每个号在一个大区只有一个网关 ⇒ 每号每小时约一发，离 upstream_per_region_hour 很远。
	//
	// 默认关：它会在没人要票时自己花额度，开之前先看一眼 `续票一轮结束` 那行日志。
	RenewPairs bool `json:"renew_pairs"`

	// MintConcurrency 是预铸同时派几个**不同账号**去铸票，0 时按 Defaults（1 = 串行）。
	//
	// 并发是**跨账号**的：同一个号仍然串行（store.LockAccount，SPEC 红线 2），而且同一个号
	// 不许同时两发裸铸走同一个大区（sched.claimRegion 按 (账号 × 大区) 拦）。
	// 几个**不同**号同时走同一个大区是允许的 —— 实测它们大概率落到不同网关。
	// 调大它买的是「一轮就把缺口补完」，代价是上游请求更密，而打得密本身就是封号特征
	// （见 sched.Options.RenewGap）。
	MintConcurrency int `json:"mint_concurrency"`
}

// defaultMintConcurrency 是 mint_concurrency 缺键时的取值。写成常量是因为 Defaults
// 和 MintWorkers 都要用它：分开写的话老配置文件（没这个键）走的是 MintWorkers 那一份，
// 两边分叉没人会发现。
const defaultMintConcurrency = 1

// MintWorkers 是当前生效的预铸并发数。
func (c Config) MintWorkers() int {
	if c.MintConcurrency > 0 {
		return c.MintConcurrency
	}
	return defaultMintConcurrency
}

// SlotCooldown 是当前生效的槽位冷却时长。
func (c Config) SlotCooldown() time.Duration {
	if c.SlotCooldownMinutes > 0 {
		return time.Duration(c.SlotCooldownMinutes) * time.Minute
	}
	return types.SlotCooldown
}

// PublicWhenSurplus：池子够格时自动对没上传凭据的人开放 /cookie，不够格自动关上。
//
// 门槛按**托管账号数**算，不按「此刻有几张活 pair」——理由见
// types.AccountsForContinuous（按需铸票的池子不囤票，按活 pair 数判等于永远关着）。
// 0 时按 types.AccountsForContinuous 推导，推导依据是配置里那个冷却。
//
// 旧字段 min_free_gateways 已经没了：Load 不是严格模式（json.Unmarshal），
// 旧配置里残留的那个键会被**静默忽略**然后落到推导值上。线上那份要手工改键名，
// 不然页面上的门槛数会跟你记的不一样。
type PublicWhenSurplus struct {
	Enabled     bool `json:"enabled"`
	MinAccounts int  `json:"min_accounts"`
}

// Defaults 是未配置字段的取值。
//
// Mode 默认 single：多账号模式会保温，保温那一发恰好烧掉自己要用的窗口，
// 所以池子模式必须由人显式打开（SPEC 红线 5）。
func Defaults() Config {
	return Config{
		Mode:          types.ModeSingle,
		DataDir:       "data",
		Listen:        "127.0.0.1:8099",
		Fingerprint:   FingerprintSub2api,
		StickyMinutes: 5,
		// pro1 清单里的头一个。换账号多半要改，所以 Load 里会校验它非空。
		Models: []string{"gpt-6-luna"},
		// 默认不对外开放：开放是运营决策，不该由默认值替人做主。
		// 门槛取「连续供上需要多少个托管账号」，不是随手填一个小数字让它看起来开着。
		// 冷却改过的话这个默认值会在 Load 里按**配置里那个冷却**重算。
		PublicWhenSurplus: PublicWhenSurplus{
			MinAccounts: types.AccountsForContinuous(types.SlotCooldown)},
		// 九个区域各 6 发 = 原先按账号算的 54 发/小时，拆开之后一个区域的重试
		// 吃不掉别的区域的预算。
		UpstreamPerRegionHour: 6,
		SlotCooldownMinutes:   int(types.SlotCooldown / time.Minute),
		MaxTriesPerDeliver:    2,
		SpareTarget:           types.SpareTarget,
		MintConcurrency:       defaultMintConcurrency,
		// 这两个开关默认关，理由在字段注释里：两个都是「花一个满血窗口换一个读数」。
		VerifyFullAtDeliver: false,
	}
}

// hasNestedKey 报告文件里显式写了 obj.key 没有。Load 为别的键做过同一件事
// （那份 raw），嵌套对象只是多解一层。
func hasNestedKey(raw map[string]json.RawMessage, obj, key string) bool {
	var inner map[string]json.RawMessage
	if json.Unmarshal(raw[obj], &inner) != nil {
		return false
	}
	_, ok := inner[key]
	return ok
}

// StickyProxies 返回启用着的粘性供应商。
func (c Config) StickyProxies() []Proxy {
	var out []Proxy
	for _, p := range c.Proxies {
		if p.Enabled && p.Sticky() {
			out = append(out, p)
		}
	}
	return out
}

// SeedProxy 返回启用着的那条普通机房代理（没有则 ok=false）。
//
// 只取第一条：它不做地理定向，多配几条也没有轮转的意义，谁在第一位就用谁。
func (c Config) SeedProxy() (Proxy, bool) {
	for _, p := range c.Proxies {
		if p.Enabled && p.Kind == KindSeed {
			return p, true
		}
	}
	return Proxy{}, false
}

// EnabledProxies 返回启用着的全部供应商。
func (c Config) EnabledProxies() []Proxy {
	var out []Proxy
	for _, p := range c.Proxies {
		if p.Enabled {
			out = append(out, p)
		}
	}
	return out
}

// Load 读配置文件。文件是系统边界，这里做校验；其它包拿到的 Config 直接信。
func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("读配置 %s: %w", path, err)
	}
	cfg := Defaults()
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("解析配置 %s: %w", path, err)
	}
	switch cfg.Mode {
	case types.ModeSingle, types.ModePool:
	default:
		return Config{}, fmt.Errorf("mode 只能是 %q 或 %q，给的是 %q",
			types.ModeSingle, types.ModePool, cfg.Mode)
	}
	switch cfg.Fingerprint {
	case FingerprintSub2api, FingerprintMint:
	default:
		return Config{}, fmt.Errorf("fingerprint 只能是 %q 或 %q，给的是 %q",
			FingerprintSub2api, FingerprintMint, cfg.Fingerprint)
	}
	if cfg.DataDir == "" {
		return Config{}, fmt.Errorf("data_dir 不能为空")
	}
	if cfg.StickyMinutes <= 0 {
		return Config{}, fmt.Errorf("sticky_minutes 必须为正")
	}
	if cfg.PublicBaseURL, err = normalizeBaseURL(cfg.PublicBaseURL); err != nil {
		return Config{}, err
	}
	// 负数会让速率闸变成「一发都不许打」，而那看起来和「上游全挂了」一模一样。
	// 0 是合法的「用默认值」。
	if cfg.UpstreamPerRegionHour < 0 {
		return Config{}, fmt.Errorf("upstream_per_region_hour 不能为负（0 = 用默认值）")
	}
	// 负数冷却会让「出冷却了吗」恒为真，于是刚烧过的槽位立刻又被挑出来发。
	if cfg.SlotCooldownMinutes < 0 {
		return Config{}, fmt.Errorf("slot_cooldown_minutes 不能为负（0 = 用默认值）")
	}
	if cfg.MaxTriesPerDeliver < 0 {
		return Config{}, fmt.Errorf("max_tries_per_deliver 不能为负（0 = 用默认值）")
	}
	if cfg.MintConcurrency < 0 {
		return Config{}, fmt.Errorf("mint_concurrency 不能为负（0 = 用默认值 %d = 串行）", defaultMintConcurrency)
	}
	// 封顶是**防手滑**，不再是物理上限：claimRegion 2026-10-03 改成按 (账号 × 大区) 拦
	// 之后，并发的真上限是「账号数 × 大区数」，Load 的时候数不出来。留着这个数是因为
	// 打得密本身就是封号特征（见 sched.Options.RenewGap），而九路并发已经远超实际缺口
	// （spare_target 量级是 20~30，premintWorkers 还会再按缺口收一次）。
	if n := len(types.Regions); cfg.MintConcurrency > n {
		return Config{}, fmt.Errorf("mint_concurrency 最多 %d：再大也只是把上游请求打得更密，"+
			"而密集铸票是封号特征；真要放开先改这里", n)
	}
	if cfg.MaxTriesPerDeliver == 0 {
		cfg.MaxTriesPerDeliver = Defaults().MaxTriesPerDeliver
	}
	// spare_target 的 0 是**真的 0**（全按需、不预铸），所以这里只拦负数——
	// 负数会让目标深度低于「最忙的号用过的张数」，预铸永远不触发，而那不是
	// 「不预铸」而是「连已经有人在用的那几张也不补」。
	if cfg.SpareTarget < 0 {
		return Config{}, fmt.Errorf("spare_target 不能为负（0 = 不预铸、全按需）")
	}
	// min_pairs 的 0 是「不设下界」。只拦负数：负数对 max() 恒无效，填出来一定是手滑或
	// 以为它是别的意思，静默忽略会让人以为下界生效了。
	if cfg.MinPairs < 0 {
		return Config{}, fmt.Errorf("min_pairs 不能为负（0 = 不设下界，目标深度只看 spare_target）")
	}
	// 文件里到底显式写了哪些键——两处要用：折 model 时要分清「真写了 models」和
	// 「这是 Defaults 填的」，以及下面拦改过名的旧键。
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(b, &raw)

	// 改过名的旧键要**明着拒**，不能跟着 encoding/json 的默认行为静默忽略。
	//
	// 这个文件是手改的（部署时直接编辑 config.json）：运维把 upstream_per_hour 从 54
	// 调到 108 以为放宽了配额，实际一直按每区 6 发跑，而「配额没生效」的现象和
	// 「上游在限流」长得一模一样，查起来要先怀疑一圈上游。启动即拒，一眼就知道改哪。
	// 键名带点号的是嵌套键——**必须认它**：公开门槛那个键就嵌在 public_when_surplus 里，
	// 只扫顶层的话这道闸对它等于不存在，而它恰好是「旧值照搬过去会错两个量级」的那种。
	for _, k := range renamedKeys {
		var present bool
		if obj, key, nested := strings.Cut(k.old, "."); nested {
			present = hasNestedKey(raw, obj, key)
		} else {
			_, present = raw[k.old]
		}
		if present {
			return Config{}, fmt.Errorf("配置键 %s 已经改名为 %s（%s），请手动改掉", k.old, k.now, k.why)
		}
	}
	for _, k := range removedKeys {
		if _, present := raw[k.key]; present {
			return Config{}, fmt.Errorf("配置键 %s 已经没有了（%s），请从配置文件里删掉这一行", k.key, k.why)
		}
	}

	// 老配置的单值 model 折进 Models，放在最前面（它原本就是默认值）。
	//
	// 只写了 model 的老配置里，Models 还停在 Defaults 填的那个默认值上——不清掉的话
	// 会凭空多出一个谁都没要的模型。所以得分清「文件里真写了 models」和「这是默认值」。
	if cfg.Model != "" {
		if _, explicit := raw["models"]; !explicit {
			cfg.Models = nil
		}
		cfg.Models = append([]string{cfg.Model}, cfg.Models...)
		cfg.Model = ""
	}
	cfg.Models = dedup(cfg.Models)
	// 空模型名会让上游回 400，而那是扫完二十个区域之后才看得见的。
	// 顺手卡住能进 JSON 请求体的内容：它来自配置文件，是系统边界。
	if len(cfg.Models) == 0 {
		return Config{}, fmt.Errorf("models 不能为空；" +
			"账号能用哪些看 https://chatgpt.com/backend-api/codex/models?client_version=<版本>")
	}
	for _, m := range cfg.Models {
		if !isModelName(m) {
			return Config{}, fmt.Errorf("models 里的 %q 不像模型名（只允许字母数字和 .-_）", m)
		}
	}
	cfg.Model = cfg.Models[0] // 派生镜像，见字段注释
	// min_accounts 没显式写时按**配置里的**冷却算，不按那个常量：冷却调成 30 分钟时
	// 连续供上只要 3 个账号，留着按 4 小时算出来的 12 会让公开池难开好几倍
	// （而那个函数自己的注释就说冷却是参数不是常量）。
	//
	// 0（和没写一样走推导）而不是报错：同一张管理表单上 upstream_per_region_hour、
	// slot_cooldown_minutes、max_tries_per_deliver 的提示都是「0 = 用默认值」，而那一格
	// 留空时 atoiField 回的就是下限 ⇒ 这一格必须也认 0，否则「留空 = 用默认值」这个
	// 全表一致的直觉在**唯一一个往开那边失败的格子**上不成立。
	if n := cfg.PublicWhenSurplus.MinAccounts; n <= 0 || !hasNestedKey(raw, "public_when_surplus", "min_accounts") {
		cfg.PublicWhenSurplus.MinAccounts = types.AccountsForContinuous(cfg.SlotCooldown())
	}
	if cfg.PublicWhenSurplus.Enabled && cfg.PublicWhenSurplus.MinAccounts <= 0 {
		return Config{}, fmt.Errorf("public_when_surplus.min_accounts 必须为正，" +
			"否则一个托管账号都没有也会放匿名进来")
	}
	seen := map[string]bool{}
	for i, p := range cfg.Proxies {
		switch {
		case strings.TrimSpace(p.Name) == "":
			return Config{}, fmt.Errorf("proxies[%d] 缺 name", i)
		case seen[p.Name]:
			return Config{}, fmt.Errorf("proxies 里 name %q 重复", p.Name)
		case p.Kind != KindNovproxy && p.Kind != KindWebshare && p.Kind != KindSeed:
			return Config{}, fmt.Errorf("proxies[%s] 的 kind %q 不认，只有 %q、%q 和 %q",
				p.Name, p.Kind, KindNovproxy, KindWebshare, KindSeed)
		case p.Host == "" || p.User == "" || p.Pass == "":
			return Config{}, fmt.Errorf("proxies[%s] 的 host/user/pass 都必填", p.Name)
		}
		seen[p.Name] = true
	}
	// 一个粘性供应商都没有时，铸票和 state-echo 配不成对，整个交付路径是坏的。
	// 启动即拒，别等到第一次交付才发现（SPEC 第 5 节）。
	if len(cfg.StickyProxies()) == 0 {
		return Config{}, fmt.Errorf("至少要有一个启用着的粘性代理（kind=%q 或 %q），"+
			"否则铸票与 state-echo 配不成同一个出口 IP", KindNovproxy, KindWebshare)
	}
	return cfg, nil
}

// renamedKeys 是改过名的配置键：旧名 → 新名。
//
// 只收**语义也变了**的那种。单纯挪位置的不用进来——静默忽略一个没人读的键无害，
// 而静默忽略一个「我以为我改了配额」的键会让人去怀疑上游。
// 键名用点号表示嵌套（"public_when_surplus.min_accounts"）。why 要写清**语义**
// 怎么变了——只说「改名了」的话，运维会直接把值照搬到新键上，而值的含义已经不同。
var renamedKeys = []renamedKey{
	{old: "upstream_per_hour", now: "upstream_per_region_hour",
		why: "语义也变了：现在是**每个区域**每小时的发数，九个区域各一份"},
	{old: "public_when_surplus.min_free_gateways", now: "public_when_surplus.min_accounts",
		why: "语义也变了：数的是**托管账号数**不是空闲网关数（见 types.AccountsForContinuous）。" +
			"按 4 小时冷却推导出来是 12，别把原来那个 96 照搬过去"},
}

type renamedKey struct{ old, now, why string }

// removedKeys 是**功能已经删掉**的配置键。和 renamedKeys 同一个理由：这个文件是手改的，
// 静默忽略一个「我以为我打开了某个功能」的键，现象和上游出问题长得一样。
//
// 代价是**部署时必须先把这些键从 config.json 里删掉**，否则 gwpool 起不来。这是刻意的：
// 留着一个没人读的开关，下一个人迟早会去拨它，然后花半天查「为什么拨了没反应」。
var removedKeys = []removedKey{}

type removedKey struct{ key, why string }

// normalizeBaseURL 校验并规范化 public_base_url：空串照过，非空必须是
// scheme + 主机名，回出来一律没有末尾斜杠。
//
// 为什么拒带路径的：页面上拼的是 "<base>/cookie"，而人很自然会把自己那个
// /a/<令牌> 地址填进来（这就是这个配置项要解决的那个坑），拼出来是
// "/a/<令牌>/cookie" ⇒ 404。在配置边界上拒掉，比在每个页面上提醒管用。
func normalizeBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("public_base_url 解不开: %w", err)
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return "", fmt.Errorf("public_base_url 要以 http:// 或 https:// 开头，给的是 %q", raw)
	case u.Host == "":
		return "", fmt.Errorf("public_base_url 缺主机名: %q", raw)
	case u.Path != "" && u.Path != "/", u.RawQuery != "", u.Fragment != "", u.User != nil:
		return "", fmt.Errorf("public_base_url 只要根地址（如 https://pool.example.com），"+
			"不要路径、查询串或凭据，给的是 %q", raw)
	}
	return u.Scheme + "://" + u.Host, nil
}

// isModelName 卡住能进 JSON 请求体和日志的模型名。上游的拼法只用这几类字符
// （gpt-6-luna、gpt-5.6-terra、codex-auto-review），放宽没有好处。
func isModelName(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.' || r == '-' || r == '_':
		default:
			return false
		}
	}
	return true
}

// Model 是默认模型：Models 的第一个。
func (c Config) DefaultModel() string {
	if len(c.Models) == 0 {
		return ""
	}
	return c.Models[0]
}

// AllowsModel 报这个模型在不在候选清单里。消费者点名的模型要过这一关：
// 它来自 HTTP 查询串，是系统边界，不能直接塞进发给上游的请求体。
func (c Config) AllowsModel(m string) bool {
	for _, x := range c.Models {
		if x == m {
			return true
		}
	}
	return false
}

// dedup 去重且保序。配置文件是人手写的，重复项不该变成「同一个模型排两次」。
func dedup(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// Patch 只改配置文件里指定的那几个键，别的**原样留着**，然后校验一遍。
//
// **刻意不是 Save(Config)**：Secret 的 MarshalJSON 会把密钥打码成 "***"，把整个
// Config 序列化回文件等于第一次保存就抹掉 admin key、consumer key 和每一条 socks
// 口令。按键打补丁的话那些字段根本不经过 Secret——它们以 json.RawMessage 的形式
// 原封不动待在那儿。顺带也修掉了「页面上没出现的字段被一次保存抹掉」整类问题。
//
// 校验不过就把原内容写回去：绝不在磁盘上留一份启动不起来的配置。
func Patch(path string, changes map[string]any) error {
	orig, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("读配置: %w", err)
	}
	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal(orig, &raw); err != nil {
		return fmt.Errorf("解析配置: %w", err)
	}
	for k, v := range changes {
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("序列化 %s: %w", k, err)
		}
		raw[k] = b
	}
	// 派生字段不该留在文件里：它是 Models[0] 的镜像，下次 Load 会把它当成「又排一次」。
	// **只在这次改了 models 时删**：只改 proxies 的补丁（管理页加代理、停用代理）删了它，
	// 只写了老式单值 model 的配置就会悄悄回落到默认模型。
	if _, ok := changes["models"]; ok {
		delete(raw, "model")
	}

	out, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(path, out); err != nil {
		return err
	}
	if _, err := Load(path); err != nil {
		// 回滚。回滚本身再失败就只能把两件事都说出来——这时候磁盘上那份是坏的。
		if rerr := writeFileAtomic(path, orig); rerr != nil {
			return fmt.Errorf("新配置不合法（%w），而且回滚也失败了：%v", err, rerr)
		}
		return fmt.Errorf("新配置不合法，已回滚: %w", err)
	}
	return nil
}

// writeFileAtomic 写临时文件再改名。配置文件里有密钥和 socks 口令，权限收到 0600。
func writeFileAtomic(path string, b []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
