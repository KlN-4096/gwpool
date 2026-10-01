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
func (p Proxy) Sticky() bool { return p.Kind == KindNovproxy }

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

	// Model 是铸票那一发用的模型。
	//
	// **不写死**：路由 cookie 钉的是网关 host，里面没有一个字节和模型有关，所以
	// 换模型理论上不影响池子的产出。而账号能用哪些模型是上游随时在动的——2026-10-01
	// 实测 pro1 的 /backend-api/codex/models 里已经没有 gpt-6-astra，打它只会换回
	// 400 "The 'gpt-6-astra' model is not supported when using Codex with a ChatGPT
	// account."，二十个区域全军覆没。拿账号自己清单里有的填这里。
	Model string `json:"model"`
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
		Model: "gpt-6-luna",
		// 默认不对外开放：开放是运营决策，不该由默认值替人做主。
		// 门槛取「一个账号能不停转」所需的网关数（冷却 ÷ 单张可用时长），
		// 不是随手填一个小数字让它看起来开着。
		PublicWhenSurplus: PublicWhenSurplus{MinFreeGateways: types.GatewaysForContinuous()},
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
	// 空模型名会让上游回 400，而那是扫完二十个区域之后才看得见的。
	// 顺手卡住能进 JSON 请求体的内容：它来自配置文件，是系统边界。
	if !isModelName(cfg.Model) {
		return Config{}, fmt.Errorf("model %q 不像模型名（只允许字母数字和 .-_，不能为空）；"+
			"账号能用哪些看 https://chatgpt.com/backend-api/codex/models?client_version=<版本>", cfg.Model)
	}
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
		return Config{}, fmt.Errorf("至少要有一个启用着的粘性代理（kind=%q），"+
			"否则铸票与 state-echo 配不成同一个出口 IP", KindNovproxy)
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
