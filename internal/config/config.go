// Package config 从一个 JSON 文件加载运行配置。
//
// 为什么不走命令行参数和环境变量：代理凭据和 access_token 同级敏感，
// argv 会被 ps 看见（SPEC 第 5 节）。只从文件读，一个来源，好审计。
package config

import (
	"encoding/json"
	"fmt"
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
	// 关掉的理由不是「不想验」，是**验不起**：账号在一个它没碰过的网关上本来就是满血，
	// 而验证那一发是用它自己的凭据打的 ⇒ 验证动作自己就把要交付的那个窗口烧掉了。
	// 加上 state-echo 有假阴无假阳（SPEC 第 3 节），关掉只损失一个读数，
	// 换回来的是一个真正没被碰过的落点。
	VerifyFullAtDeliver bool `json:"verify_full_at_deliver"`

	// RenewPairs 决定保温要不要「带着旧 pair 去续同一个网关」。**默认关**。
	//
	// 续期能让一张 pair 一直活着，但每续一发都要烧掉执行者在那个网关上的窗口，
	// 而池子真正缺的是**落点**不是票。关掉之后保温只做裸铸（落到哪个网关算哪个），
	// 同样的上游预算花在扩大触达面上。
	RenewPairs bool `json:"renew_pairs"`

	// SlotCooldownMinutes 是 (账号 × 网关) 烧掉之后要歇多久，0 时按 types.SlotCooldown。
	//
	// 这个数至今没测准：实测见过 30 分钟就恢复的，也见过 3 小时还没恢复的，
	// 默认的 4 小时是保守估算。做成配置项就是为了能按当天的观测调。
	SlotCooldownMinutes int `json:"slot_cooldown_minutes"`
}

// SlotCooldown 是当前生效的槽位冷却时长。
func (c Config) SlotCooldown() time.Duration {
	if c.SlotCooldownMinutes > 0 {
		return time.Duration(c.SlotCooldownMinutes) * time.Minute
	}
	return types.SlotCooldown
}

// PublicWhenSurplus：票富裕时自动对没上传凭据的人开放 /cookie，供给回落自动关上。
//
// 空闲网关 = 有活 pair（剩余 ≥ types.DeliverFloor）且当前没被租走的网关。
// 门槛是**给上传者留的余量**：跌破它的那一刻就拒匿名，哪怕还剩一张。
type PublicWhenSurplus struct {
	Enabled         bool `json:"enabled"`
	MinFreeGateways int  `json:"min_free_gateways"`
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
		// 门槛取「一个账号能不停转」所需的网关数（冷却 ÷ 单张可用时长），
		// 不是随手填一个小数字让它看起来开着。
		PublicWhenSurplus: PublicWhenSurplus{
			MinFreeGateways: types.GatewaysForContinuous(types.SlotCooldown)},
		// 九个区域各 6 发 = 原先按账号算的 54 发/小时，拆开之后一个区域的重试
		// 吃不掉别的区域的预算。
		UpstreamPerRegionHour: 6,
		SlotCooldownMinutes:   int(types.SlotCooldown / time.Minute),
		// 这两个开关默认关，理由在字段注释里：两个都是「花一个满血窗口换一个读数」。
		VerifyFullAtDeliver: false,
		RenewPairs:          false,
	}
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
	// 负数会让速率闸变成「一发都不许打」，而那看起来和「上游全挂了」一模一样。
	// 0 是合法的「用默认值」。
	if cfg.UpstreamPerRegionHour < 0 {
		return Config{}, fmt.Errorf("upstream_per_region_hour 不能为负（0 = 用默认值）")
	}
	// 负数冷却会让「出冷却了吗」恒为真，于是刚烧过的槽位立刻又被挑出来发。
	if cfg.SlotCooldownMinutes < 0 {
		return Config{}, fmt.Errorf("slot_cooldown_minutes 不能为负（0 = 用默认值）")
	}
	// 老配置的单值 model 折进 Models，放在最前面（它原本就是默认值）。
	//
	// 只写了 model 的老配置里，Models 还停在 Defaults 填的那个默认值上——不清掉的话
	// 会凭空多出一个谁都没要的模型。所以得分清「文件里真写了 models」和「这是默认值」。
	if cfg.Model != "" {
		var raw map[string]json.RawMessage
		if json.Unmarshal(b, &raw) == nil {
			if _, explicit := raw["models"]; !explicit {
				cfg.Models = nil
			}
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
	if cfg.PublicWhenSurplus.Enabled && cfg.PublicWhenSurplus.MinFreeGateways <= 0 {
		return Config{}, fmt.Errorf("public_when_surplus.min_free_gateways 必须为正，" +
			"否则一张空闲票都没有也会放匿名进来")
	}
	seen := map[string]bool{}
	for i, p := range cfg.Proxies {
		switch {
		case strings.TrimSpace(p.Name) == "":
			return Config{}, fmt.Errorf("proxies[%d] 缺 name", i)
		case seen[p.Name]:
			return Config{}, fmt.Errorf("proxies 里 name %q 重复", p.Name)
		case p.Kind != KindNovproxy && p.Kind != KindWebshare:
			return Config{}, fmt.Errorf("proxies[%s] 的 kind %q 不认，只有 %q 和 %q",
				p.Name, p.Kind, KindNovproxy, KindWebshare)
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
	delete(raw, "model")

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
