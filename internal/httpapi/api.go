// Package httpapi 是 gwpool 的全部对外表面：四个视图 + 两个机器接口。
//
// 分级就是这个服务的信任模型，别混（SPEC 第 10、12 节）。
// 公开页只给聚合读数，**绝不下发 pair 或 turn-state 本体**；
// 上传者页是给托管凭据的人看的**信任凭证**，不是运维面板；
// /cookie 要 consumer key，/admin 要 admin key。
// 任何视图都不出现 access_token 和代理密码（SPEC 红线 1）。
package httpapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/mint"
	"gwpool/internal/oauth"
	"gwpool/internal/sched"
	"gwpool/internal/types"
	"gwpool/web"
)

// Pool 是调度器那一侧的能力。
//
// 两个方法都带 accountID：槽位是 (账号 × 网关) 二元组，池子必须知道是谁在要、
// 是谁在回报，否则调度和账本都会串号。
type Pool interface {
	// req 带着调用方说得上话的那几件事（点名网关、force、两个排除名单、最低剩余寿命）。
	// 它们全是外部输入，校验在 parseCookieRequest 做完（信任边界只有一道）。
	Deliver(ctx context.Context, accountID int64, req sched.Request) (types.Delivery, error)
	// DeliverAnonymous 给池子**手上没有凭据**的那个上游账号交付。upstreamAccountID 是
	// 他自己的上游账号标识，槽位照常按它记——调度单位永远是 (账号 × 网关)。
	//
	// authed 为真 = 调用方自己是鉴权过的，只是替别的上游账号取票。那种调用方不受
	// public_when_surplus 门槛约束（门槛是给上传者留的余量，不该挡住上传者本人）。
	DeliverAnonymous(ctx context.Context, upstreamAccountID string, req sched.Request, authed bool) (types.Delivery, error)
	// Release 还原一次「票取了但一发没打」的交付，返回还原了几个槽位。
	Release(accountID int64, cookieVersion string) (int, error)
	// DeliveredTo 报告这张票确实被交付给过这一行。/pair/renew 的归属闸用它，
	// 而那是那条路上**唯一真正的**安全边界（交回来的 cookie 本体验不了签）。
	// 判据归调度——它和 /release 共用同一份比对，HTTP 这层绝不自己算一份。
	DeliveredTo(accountID int64, cookieVersion string) bool
	// HasCandidate 是 wait 轮询的判据：此刻有没有一张**这个账号**拿得到、而且不用铸
	// 的票。判据必须和交付是同一把尺子，所以归调度（它才看得见冷却），HTTP 这层
	// 绝不自己算一份。
	HasCandidate(accountID int64, req sched.Request) bool
	// Public 是此刻的对外开放状态，现算不缓存。
	Public() sched.PublicState
	// PairTarget 是目标活 pair 数，只给 /gateways 的读数用。
	PairTarget() int
	// AccountHolds 是被上游 429 按住的账号 → 还要等多久。只给管理页的读数用：
	// 被按住的号在表里看起来健康且启用，却一发都不铸。
	AccountHolds() map[int64]time.Duration
	Mode() types.Mode
}

// Store 是视图需要的读写面。
type Store interface {
	Gateways() map[string]types.Gateway
	Pairs() map[string]types.Pair
	Slots() map[string]types.Slot
	Accounts() []types.Account
	AccountByViewToken(tok string) (types.Account, bool)
	AddAccount(a types.Account) (types.Account, error)

	// owner 层。/u/{token} 的鉴权就是 OwnerByToken，和 /a/{token} 同一个套路。
	OwnerByToken(tok string) (types.Owner, bool)
	AccountsByOwner(ownerID int64) []types.Account
	// AddOwner 建一个上传者。归属在**这一层**定好再传给 AddAccount：让存储层
	// 兜底铸一个的话，回来那个 OwnerID 分不清是「刚为我铸的」还是「去重命中了
	// 别人的账号」，而那两件事的处置完全相反。
	AddOwner(label string) (types.Owner, error)
	// PruneEmptyOwners 丢掉名下零账号的 owner。上传新建了 owner 但 AddAccount 失败时
	// （撞上限、并发竞争）清掉那个孤儿，否则 owner 表会被反复上传刷到无上限。
	PruneEmptyOwners() (int, error)
	// Owners 只给管理页：迁移出来那个 owner 的 token 不显示的话，现有账号的
	// 总览页谁都进不去。
	Owners() []types.Owner

	// RegionMints 是 (账号 × 区域) 的终身铸票台账，/u/ 的九宫格按它画。
	RegionMints() map[string]types.RegionMint
	// SetAccountEnabled 只翻一个 bool。给上传者自己停用/启用用。
	SetAccountEnabled(id int64, enabled bool) error
	// SetAccountVia 记匿名占位行最近是拿谁的 key 取的票（管理页靠它认人）。
	SetAccountVia(id, via int64) error
	// PutPair 覆盖一个网关的 pair。只有 /pair/renew 用它（消费端自己续出来的那张
	// 交回来），所以这一层写盘只会改 Cookie 和两个死期，不碰 MintedBy。
	PutPair(p types.Pair) error

	// DeleteAccount 删掉账号连同它的槽位，不动已经铸出来的 pair。
	DeleteAccount(id int64) error
	Samples() ([]types.Sample, error)
}

// Scanner 排一次「去找新落点」的裸铸。
//
// **刻意没有「上传就扫一遍」那个入口**：九区域首扫 2026-10-01 删掉了——它在没有
// 任何需求的时候就打九发上游（成本不为零），还顺手烧掉执行者在那九个落点上的窗口。
// 需要新落点的时刻是「槽位烧完了」，那时候再按 (账号 × 区域) 台账派一发。
type Scanner interface {
	// EnqueueDiscovery 去找这个账号**从没碰过**的新网关，排一发裸铸。
	//
	// exclude 是要票的那个账号（0 = 没有特定的人）：铸票那一发会烧掉执行者在新网关
	// 上的窗口，让要票的人自己去等于白跑一趟（它找到的每个新网关都当场被它自己烧掉）。
	// 所以派**别人**去，铸出来的 pair 是账号无关的路由，要票的人拿到时仍然是对那个
	// 网关的首次接触——这才有满血窗口。
	//
	// 实现方负责挑人（按 (账号 × 区域) 台账）和限流。
	//
	// 返回空串 = 真的排进去了；非空 = 没派出去，内容是给人看的原因。管理页那个
	// 「手动踢一次」的按钮据此如实回话：挑不出人（单账号冷池子）、九个区域本小时
	// 都打满、挑中的号还在 30 分钟发现冷却里——这三种从前都回「已排队」，
	// 只有服务端日志说了实话，而这个按钮恰恰是冷启动时唯一的出路。
	EnqueueDiscovery(exclude int64) string
}

// Options 是服务端参数。两个 key 为空时对应路由**全拒**，不是全放。
type Options struct {
	// ConsumerKey 有两个身份：管理员兜底凭证，以及**每账号 key 的派生密钥**。
	// 换掉它等于把所有账号的 key 一起换掉。见 consumerKeyFor。
	ConsumerKey string
	AdminKey    string
	// SiteKey 是页面门禁：拿到它的人才看得见池子。**空 = 不设门禁**，
	// 和上面两个「空则全拒」的 key 刻意相反——门禁是可选的运营手段，
	// 没配就该维持原样开放，而不是把自己锁在门外。
	SiteKey string

	// ProxyStates 报各出口供应商的状态，给管理页看。nil = 管理页不显示这一节。
	// 用函数而不是接口：只有一个实现（mint.Client），为它造个接口是白开销。
	ProxyStates func() []mint.ProxyState
	// AddProxy / SetProxyEnabled 在运行中往末尾加出口、改启用标志；ProbeProxy 是加之前那一发试通。
	// 这三个和 ProxyStates 有一个是 nil，管理页的出口代理就只能看（测试和没接出口的实例）。
	AddProxy        func(config.Proxy) error
	SetProxyEnabled func(name string, on bool) error
	ProbeProxy      func(ctx context.Context, p config.Proxy) (types.Observed, error)

	// Refresh 换票。nil = 用 oauth.Refresh 直连上游。
	// 存在的唯一理由是测试：测试绝不打真实上游（SPEC 红线 8），而换票是
	// parseCreds 里唯一会出网的一步。生产永远不设它。
	Refresh func(ctx context.Context, refreshToken string) (access, newRefresh string, err error)

	// Live 非 nil 时管理页可以改配置并热重载；为空则那一节只读（测试走这条路）。
	Live *config.Live

	// Model 是铸票那一发用的模型，照 config.Model 传进来。
	// 上传者视图要向托管者如实交代用了他的凭据打什么，所以不能写死。
	Model string

	// PublicBaseURL 是启动时那份 config.PublicBaseURL，给没接 Live 的实例（测试）用。
	// 接了 Live 的现读它（管理页改完立刻生效），见 Server.publicBaseURL。
	PublicBaseURL string

	Log *slog.Logger
}

type Server struct {
	pool Pool
	st   Store
	scan Scanner
	opt  Options

	// 账本缓存。回读 samples.jsonl 是全量的，而上传者页对外、谁都能反复刷，
	// 每刷一次就在存储的读锁里压一次全量读 ⇒ 刷页即可堵住所有写。
	// 账本晚几秒无所谓，这几秒换的是「刷不垮」。
	smu  sync.Mutex
	sm   []types.Sample
	smAt time.Time

	// 最近的交付失败，给页面回答「为什么客户端报 502」。
	fails failLog
	// started 写在管理页那张失败表上：失败只存在内存里，重启前的已经没了。
	started time.Time

	tplPool, tplAccount, tplOwner, tplAdmin, tplGate *template.Template
}

// probeEffort 必须跟 mint 包实际发出去的 payload 一致（SPEC 第 2 节）：
// 上传者视图拿它向托管者交代「我们只用你的凭据打了 ping」。
// 模型不是常量——它由 config.Model 决定，走 Options.Model 传进来，否则页面上
// 那句交代就是假的（这正好是「必须一致」的意思）。
const probeEffort = "low"

// probeModel 是页面上要向托管者交代的模型名。
//
// 先读热配置：管理页那个按钮写着「保存并立即生效」，铸票确实立刻换模型
// （mint 走 Live.Get().DefaultModel()），而 Options.Model 是启动快照 ⇒ 只读它的话，
// 改完模型到下次重启之间，首页和 /a/、/u/ 三处都在向托管者交代一个**已经不用了**的
// 模型名。上传者页是信任凭证，上面那句注释「模型不是常量，否则页面上那句交代就是假的」
// 要靠这一步才成立。写法与 publicBaseURL 一致（Live 为 nil 时 Get 回零值配置）。
func (s *Server) probeModel() string {
	if v := s.opt.Live.Get().DefaultModel(); v != "" {
		return v
	}
	if s.opt.Model != "" {
		return s.opt.Model
	}
	return "(未配置)"
}

func New(p Pool, st Store, sc Scanner, opt Options) (*Server, error) {
	if opt.Log == nil {
		opt.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s := &Server{pool: p, st: st, scan: sc, opt: opt, started: time.Now().UTC()}
	var err error
	// 每个页面单独和 layout 一起解析：三份 "content" 定义放一个集合里会撞名。
	if s.tplPool, err = parse("pool.html"); err != nil {
		return nil, err
	}
	if s.tplAccount, err = parse("account.html"); err != nil {
		return nil, err
	}
	if s.tplOwner, err = parse("owner.html"); err != nil {
		return nil, err
	}
	if s.tplAdmin, err = parse("admin.html"); err != nil {
		return nil, err
	}
	if s.tplGate, err = parse("gate.html"); err != nil {
		return nil, err
	}
	return s, nil
}

func parse(page string) (*template.Template, error) {
	return template.ParseFS(web.Templates, "templates/layout.html", "templates/"+page)
}

// Handler 建路由。用标准库 ServeMux 的方法+路径模式（Go 1.22 起），不引第三方路由。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// 门禁只罩**给人看的那几个页面**。/cookie 是机器面、自带 key，
	// 罩上去等于把 sub2api 一起关在门外；/admin/ 有自己的 admin key。
	// 给人看的页面罩门禁；**改状态的 POST 还要再罩一层 sameOrigin**（CSRF，见那个函数
	// 的注释）。机器端点（/cookie、/release、/pair/renew、/gateways）不罩 sameOrigin：
	// 它们按定义就是别的服务从服务端调的，没有浏览器、也没有可被利用的环境凭证。
	mux.Handle("GET /{$}", s.gate(http.HandlerFunc(s.handlePool)))
	mux.Handle("POST /upload", s.gate(s.sameOrigin(http.HandlerFunc(s.handleUpload))))
	mux.Handle("GET /a/{token}", s.gate(http.HandlerFunc(s.handleAccount)))
	mux.Handle("POST /a/{token}/enabled", s.gate(s.sameOrigin(http.HandlerFunc(s.handleAccountEnabled))))
	// owner 总览页和「贴令牌进来」的入口也是给人看的，和上面几个一起罩门禁。
	mux.Handle("GET /u/{token}", s.gate(http.HandlerFunc(s.handleOwner)))
	mux.Handle("POST /enter", s.gate(s.sameOrigin(http.HandlerFunc(s.handleEnter))))
	mux.Handle("POST /logout", s.gate(s.sameOrigin(http.HandlerFunc(s.handleLogout))))
	mux.HandleFunc("GET /cookie", s.handleCookie)
	// /release 和 /cookie 同一把钥匙、同一套 account 语义（见 handleRelease）。
	mux.HandleFunc("POST /release", s.handleRelease)
	// /pair/renew 同上。它只把消费端自己续出来的那张记进账本，一发上游都不打。
	mux.HandleFunc("POST /pair/renew", s.handleRenew)
	mux.HandleFunc("GET /gateways", s.handleGateways)
	// 管理面的 POST 也要罩 sameOrigin：authAdmin 走 HTTP Basic，而浏览器一旦缓存了
	// 这个 realm 的凭据，**跨站请求照样会自动带上**——和 cookie 一样是「环境凭证」。
	// 不罩的话一个第三方页面就能让管理员的浏览器替他删号或改配置。
	mux.Handle("POST /admin/rescan", s.sameOrigin(http.HandlerFunc(s.handleAdminRescan)))
	mux.Handle("POST /admin/accounts/{id}/delete", s.sameOrigin(http.HandlerFunc(s.handleAdminDeleteAccount)))
	mux.Handle("POST /admin/accounts/{id}/enabled", s.sameOrigin(http.HandlerFunc(s.handleAdminAccountEnabled)))
	mux.Handle("POST /admin/config", s.sameOrigin(http.HandlerFunc(s.handleAdminConfig)))
	mux.Handle("POST /admin/proxies", s.sameOrigin(http.HandlerFunc(s.handleAdminAddProxy)))
	mux.Handle("POST /admin/proxies/enabled", s.sameOrigin(http.HandlerFunc(s.handleAdminProxyEnabled)))
	// 原来的「全部账号」卡片页并进了管理页账号页签，旧链接跳过去。
	mux.Handle("GET /admin/accounts", http.RedirectHandler("/admin/#accounts", http.StatusSeeOther))
	mux.HandleFunc("GET /admin/", s.handleAdmin)
	return noStore(mux)
}

// gateCookie 是放行凭证的 cookie 名。
const gateCookie = "gwp_gate"

// gate 是页面门禁。
//
// 为什么存 cookie 而不是每次都带 ?k=：URL 里的 key 会进浏览器历史、进书签、
// 进用户随手贴出来的截图。所以 ?k= 只当一次性入场券，认完立刻 303 到干净路径。
// （Referer 外泄另有 noStore 里的 Referrer-Policy: no-referrer 兜着。）
func (s *Server) gate(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.opt.SiteKey == "" {
			h.ServeHTTP(w, r)
			return
		}
		if k := r.URL.Query().Get("k"); k != "" {
			if !keyOK(s.opt.SiteKey, k) {
				s.renderGate(w, true)
				return
			}
			http.SetCookie(w, &http.Cookie{
				Name:     gateCookie,
				Value:    s.opt.SiteKey,
				Path:     "/",
				HttpOnly: true,
				Secure:   forwardedHTTPS(r),
				SameSite: http.SameSiteLaxMode,
				MaxAge:   30 * 24 * 3600,
			})
			u := *r.URL
			q := u.Query()
			q.Del("k")
			u.RawQuery = q.Encode()
			http.Redirect(w, r, u.RequestURI(), http.StatusSeeOther)
			return
		}
		c, err := r.Cookie(gateCookie)
		if err != nil || !keyOK(s.opt.SiteKey, c.Value) {
			s.renderGate(w, err == nil)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// sameOrigin 挡住跨站发起的改状态请求（CSRF）。
//
// **这条闸不能用「POST 比 GET 安全」来替代**——第三方页面放一个自动提交的表单就能发出
// 任意 POST。SameSite=Lax 也替代不了它：Lax 只约束 cookie 的**发送**，不约束响应里
// Set-Cookie 的**写入**，所以一个跨站 POST /enter 照样能把受害者的身份 cookie 换成
// 攻击者的令牌，之后他上传的凭据和接入 key 全落到攻击者的 /u/ 页下，全程不用猜任何令牌。
//
// 站点码开着时这条路恰好被堵住（门禁 cookie 是 Lax，跨站 POST 不带它 ⇒ 403），
// 但那是**巧合不是设计**：site_key 是可以留空的（设计上就支持全公开），
// 一个配置开关之外就是凭据窃取。所以显式挡。
//
// 判据按「浏览器一定会说实话」的两个头来，不引第三方库、不发 CSRF 令牌：
//   - Sec-Fetch-Site：现代浏览器必带。same-origin/none（地址栏直接访问）放行，
//     cross-site/same-site 拒。
//   - 没有这个头说明不是现代浏览器 ⇒ 退回看 Origin。浏览器对跨源 POST 一定带 Origin，
//     带了就必须和本站对得上。两个都没有的是 curl/脚本，它们不是 CSRF 的载体
//     （攻击者没法让受害者的 curl 带上受害者的 cookie），放行——/upload 本来就支持脚本调用。
func (s *Server) sameOrigin(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !sameOriginRequest(r) {
			s.opt.Log.Warn("拒掉跨站请求", "path", r.URL.Path,
				"sec_fetch_site", r.Header.Get("Sec-Fetch-Site"), "has_origin", r.Header.Get("Origin") != "")
			http.Error(w, "跨站请求被拒绝：请从本站页面上操作", http.StatusForbidden)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func sameOriginRequest(r *http.Request) bool {
	switch r.Header.Get("Sec-Fetch-Site") {
	case "same-origin", "none":
		return true
	case "":
		// 老浏览器或非浏览器客户端，往下看 Origin。
	default:
		return false // cross-site / same-site
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	// r.Host 在反代后面是对外那个主机名（Caddy 原样透传），和 Origin 同口径。
	return strings.EqualFold(u.Host, r.Host)
}

// ownerCookie 记住「这个浏览器是哪个上传者」。
//
// 和门禁 cookie 刻意分开：门禁是「这个站给不给你看」，这个是「你是谁」。
// 一台共用的浏览器上两件事的取舍不一样，混成一个就没法只清掉身份。
const ownerCookie = "gwp_owner"

// ownerCookieDays 比门禁那个 30 天长得多：这是一个人回到自己账号页的唯一一条路，
// 而令牌本体只在上传那一次（以及管理页上）露过面。过期等于把人锁在外面。
const ownerCookieDays = 365

// setOwnerCookie 下发身份 cookie。属性照门禁那个来（HttpOnly + Lax + 反代感知的
// Secure）：HttpOnly 挡住页面上任何脚本读它，Lax 挡住跨站请求带上它。
func (s *Server) setOwnerCookie(w http.ResponseWriter, r *http.Request, tok string) {
	http.SetCookie(w, &http.Cookie{
		Name:     ownerCookie,
		Value:    tok,
		Path:     "/",
		HttpOnly: true,
		Secure:   forwardedHTTPS(r),
		SameSite: http.SameSiteLaxMode,
		MaxAge:   ownerCookieDays * 24 * 3600,
	})
}

// clearOwnerCookie 清掉身份 cookie（MaxAge<0 让浏览器立刻删）。共用机器上前一个人
// 留下的一年期 cookie 会让下一个人的上传默默挂到他名下，总得有个「这不是我」的出口。
func (s *Server) clearOwnerCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name: ownerCookie, Value: "", Path: "/",
		HttpOnly: true, Secure: forwardedHTTPS(r), SameSite: http.SameSiteLaxMode,
		MaxAge: -1,
	})
}

// ownerFromCookie 认出带着身份 cookie 来的上传者。认不出就当没带。
func (s *Server) ownerFromCookie(r *http.Request) (types.Owner, bool) {
	c, err := r.Cookie(ownerCookie)
	if err != nil {
		return types.Owner{}, false
	}
	return s.st.OwnerByToken(c.Value)
}

// forwardedHTTPS 判断对外那一跳是不是 https。进程只监听回环、由 Caddy 反代，
// 直接看 r.TLS 永远是 nil，Secure 就永远加不上。
func forwardedHTTPS(r *http.Request) bool {
	return r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
}

// noStore 挡住两类泄漏：view_token 在 URL 里，别让它经 Referer 漏给外站，
// 也别让中间缓存留下带 cookie 的 /cookie 响应。
func noStore(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		h.ServeHTTP(w, r)
	})
}

// ---------- 鉴权 ----------

// bearer 取请求里的 key：Authorization: Bearer / X-Api-Key。
// 刻意不收 query 参数——密钥进 URL 就会进访问日志。
func bearer(r *http.Request) string {
	if v := r.Header.Get("Authorization"); v != "" {
		if tok, ok := strings.CutPrefix(v, "Bearer "); ok {
			return strings.TrimSpace(tok)
		}
	}
	return strings.TrimSpace(r.Header.Get("X-Api-Key"))
}

func keyOK(want, got string) bool {
	// want 为空 ⇒ 没配密钥 ⇒ 一律拒绝。失败要往关的方向失败。
	if want == "" || got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(want), []byte(got)) == 1
}

// ConsumerKeyFor 为一个账号派生它的机器凭证。
//
// 为什么是派生而不是存一份：types.Account 签名冻结、加不了字段，而另起一个
// 「key → 账号」的文件就是第二份迟早和账本走散的状态。HMAC(配置密钥, 账号身份)
// 随时能重算，换掉配置里的密钥就等于一次性吊销所有人的 key。
// 单独吊销某一个人：服务没有这个入口，他自己在 ChatGPT 设备列表里取消登录即可
// （见 store.AddAccount 上方那段），令牌一失效这把 key 就换不到任何东西。
//
// 和 view_token 刻意是两个值：view_token 进 URL、进浏览器历史，是给人看页面的；
// 这把是机器凭证，只走 Authorization 头。
// 没有 access_token 的占位账号（只回报、不托管凭据）不发 key：它换不到 pair。
func (s *Server) ConsumerKeyFor(a types.Account) string {
	if s.opt.ConsumerKey == "" || a.Creds.AccessToken == "" {
		return ""
	}
	m := hmac.New(sha256.New, []byte(s.opt.ConsumerKey))
	_, _ = fmt.Fprintf(m, "gwpool-consumer-key|%d|%s", a.ID, a.Creds.AccountID)
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil)[:18])
}

// caller 是认出来的调用方。admin 为真时 acct 是零值，必须由请求显式指明替谁办事。
type caller struct {
	acct  types.Account
	admin bool
}

// resolveCaller 按 key 认人。管理员密钥先看，再逐个账号比对派生 key。
// 账号数是几十量级，线性扫一遍不值得为它建索引。
func (s *Server) resolveCaller(r *http.Request) (caller, bool) {
	got := bearer(r)
	if got == "" {
		return caller{}, false
	}
	if keyOK(s.opt.AdminKey, got) || keyOK(s.opt.ConsumerKey, got) {
		return caller{admin: true}, true
	}
	for _, a := range s.st.Accounts() {
		if !a.Enabled {
			continue
		}
		k := s.ConsumerKeyFor(a)
		if k != "" && subtle.ConstantTimeCompare([]byte(k), []byte(got)) == 1 {
			return caller{acct: a}, true
		}
	}
	return caller{}, false
}

// accountRefMatches：账号引用可以是池内 ID（纯数字），也可以是上游账号标识。
func accountRefMatches(a types.Account, ref string) bool {
	if ref == "" {
		return false
	}
	if a.Creds.AccountID != "" && a.Creds.AccountID == ref {
		return true
	}
	return strconv.FormatInt(a.ID, 10) == ref
}

func (s *Server) findAccount(ref string) (types.Account, bool) {
	for _, a := range s.st.Accounts() {
		if accountRefMatches(a, ref) {
			return a, true
		}
	}
	return types.Account{}, false
}

// accountByUpstream 按**上游账号标识精确匹配**，不收池内 ID 那种写法。
//
// 和 findAccount 分开是因为它要和 store.AddAccount 的去重判据逐字一致：那边只比
// Creds.AccountID。顺手接受池内 ID 的话，一个长得像数字的上游标识就会让这边以为
// 「已经有行了」而那边照样新建。
func (s *Server) accountByUpstream(id string) (types.Account, bool) {
	if id == "" {
		return types.Account{}, false
	}
	for _, a := range s.st.Accounts() {
		if a.Creds.AccountID == id {
			return a, true
		}
	}
	return types.Account{}, false
}

func (s *Server) authAdmin(w http.ResponseWriter, r *http.Request) bool {
	got := bearer(r)
	if got == "" {
		// 浏览器开管理页用得上 Basic：用户名随意，口令就是 admin key。
		// 比 ?key= 好——密钥不进 URL、不进 Referer、不进访问日志。
		if _, pass, ok := r.BasicAuth(); ok {
			got = pass
		}
	}
	if keyOK(s.opt.AdminKey, got) {
		return true
	}
	w.Header().Set("WWW-Authenticate", `Basic realm="gwpool admin", charset="UTF-8"`)
	http.Error(w, "需要 admin key", http.StatusUnauthorized)
	return false
}

// ---------- 公开池视图 ----------

func (s *Server) handlePool(w http.ResponseWriter, r *http.Request) {
	gws := s.st.Gateways()
	pairs := s.st.Pairs()
	accounts := s.st.Accounts()
	now := time.Now().UTC()

	// 只数**托管**账号：匿名调用方的占位行（sched.anonAccount）没有凭据，既铸不了票
	// 也不是谁托管的。算进来的话这张对外的页面上「托管账号」会虚高一大截。
	hosted := 0
	for _, a := range accounts {
		if a.Creds.AccessToken != "" {
			hosted++
		}
	}

	v := poolView{base: base{Title: "池子", Mode: s.pool.Mode()}}
	for name, g := range gws {
		row := gatewayRow{Name: name, Exits: len(g.ReachableExits), PairLeft: "—"}
		if p, ok := pairs[name]; ok {
			left := p.ExpiresAt.Sub(now)
			// 公开页只说「还能不能交付」，绝不吐 cookie 本体（SPEC 红线 2）。
			row.PairReady = left >= types.DeliverFloor
			if left > 0 {
				row.PairLeft = shortDur(left)
			}
		}
		v.Gateways = append(v.Gateways, row)
	}
	sort.Slice(v.Gateways, func(i, j int) bool { return v.Gateways[i].Name < v.Gateways[j].Name })
	for _, g := range v.Gateways {
		if g.PairReady {
			v.ReadyCount++
		}
	}
	// 格子先算（它们报的是**整池**的读数，筛选时不许跟着缩水），再按查询串筛表。
	v.Regions = poolRegionCells(v.Gateways, gws)
	want := sanitizeLabel(r.URL.Query().Get("region"), "")
	v.Shown, v.FilterLabel = filterPoolGateways(v.Gateways, v.Regions, want)
	markPoolRegionLinks(v.Regions, want)
	ps := s.pool.Public()
	v.PublicEnabled, v.PublicOpen = ps.Enabled, ps.Open
	v.OpenAccounts, v.Threshold = ps.Accounts, ps.Threshold
	v.Accounts = hosted
	v.Model, v.Effort = s.probeModel(), probeEffort
	v.BaseURL = s.publicBaseURL()
	// 带着身份 cookie 来的人直接给按钮；没带的只给一个贴令牌的小表单。
	if o, ok := s.ownerFromCookie(r); ok {
		v.OwnerToken = o.Token
	}
	v.Upload.KnownOwner = v.OwnerToken
	v.EnterErr = r.URL.Query().Get("enter") == "bad"
	s.render(w, s.tplPool, v)
}

// poolRegionCells 把已知网关按出口区域摊成九格（出口反查不到区域的另起一格「未归类」）。
//
// 顺带回填每一行的 Regions 那一列，所以它接的是 rows 的**切片**（元素可改）而不是副本。
// 两处读数必须出自同一次计算：表里写着「欧洲」而九宫格的欧洲格子里没有它，没人查得出
// 是哪一边错了。
func poolRegionCells(rows []gatewayRow, gws map[string]types.Gateway) []poolRegionCell {
	byRegion := map[string][]poolRegionGateway{}
	for i := range rows {
		item := poolRegionGateway{Name: rows[i].Name, Ready: rows[i].PairReady}
		seen := map[string]bool{}
		for _, e := range gws[rows[i].Name].ReachableExits {
			// 反查不到就归「未归类」（key 为空）：这些出口是代理商给的，不在
			// types.Regions 那张表里（精确匹配 Region+City，见那边的注释）。
			k := types.RegionKeyOf(e)
			if seen[k] {
				continue
			}
			seen[k] = true
			byRegion[k] = append(byRegion[k], item)
			rows[i].regionKeys = append(rows[i].regionKeys, k)
			if k != "" {
				rows[i].Regions = append(rows[i].Regions, regionLabel(k))
			}
		}
		// 一个出口都没记过的网关也要有地方落，否则它在这一页上凭空消失。
		if len(gws[rows[i].Name].ReachableExits) == 0 {
			byRegion[""] = append(byRegion[""], item)
			rows[i].regionKeys = append(rows[i].regionKeys, "")
		}
	}
	out := make([]poolRegionCell, 0, len(types.Regions)+2)
	// 「全部」排在最前面：它是默认态，也是从某一格退出来的那条路。
	all := poolRegionCell{Label: "全部", All: true, Total: len(rows)}
	for _, r := range rows {
		if r.PairReady {
			all.Ready++
		}
	}
	switch {
	case all.Total == 0:
		all.Note = "还没发现过网关"
	case all.Ready == 0:
		all.Note = fmt.Sprintf("已知 %d 个网关，此刻一个都交付不了", all.Total)
	default:
		all.Note = fmt.Sprintf("已知 %d 个网关，其中 %d 个此刻可交付", all.Total, all.Ready)
	}
	out = append(out, all)

	keys := make([]string, 0, len(types.Regions)+1)
	for _, r := range types.Regions {
		keys = append(keys, r.Key)
	}
	if len(byRegion[""]) > 0 {
		keys = append(keys, "")
	}
	for _, k := range keys {
		c := poolRegionCell{Key: k, Label: regionLabel(k), Gateways: byRegion[k]}
		if k == "" {
			c.Label = "未归类"
		}
		sort.Slice(c.Gateways, func(i, j int) bool {
			if c.Gateways[i].Ready != c.Gateways[j].Ready {
				return c.Gateways[i].Ready
			}
			return c.Gateways[i].Name < c.Gateways[j].Name
		})
		c.Total = len(c.Gateways)
		for _, g := range c.Gateways {
			if g.Ready {
				c.Ready++
			}
		}
		switch {
		case c.Total == 0:
			c.Note = "还没在这个区域发现过网关"
		case c.Ready == 0:
			c.Note = fmt.Sprintf("已知 %d 个网关，此刻都没有可交付的路由 Cookie", c.Total)
		default:
			c.Note = fmt.Sprintf("已知 %d 个网关，其中 %d 个此刻可交付", c.Total, c.Ready)
		}
		out = append(out, c)
	}
	return out
}

// markPoolRegionLinks 给每一格配上「点它去哪儿」和「现在选的是不是它」。
//
// 链接都带 #tab-gateways：这一页是分页签的，不带锚点的话点一下会弹回第一个页签，
// 而人正看着的就是网关那一页。
func markPoolRegionLinks(cells []poolRegionCell, want string) {
	hit := false
	for i := range cells {
		c := &cells[i]
		switch {
		case c.All:
			c.Href = "/#tab-gateways"
		case c.Key == "":
			c.Href = "/?region=" + poolRegionQuery + "#tab-gateways"
		default:
			c.Href = "/?region=" + url.QueryEscape(c.Key) + "#tab-gateways"
		}
		if !c.All && (c.Key == want || (c.Key == "" && want == poolRegionQuery)) {
			c.Active, hit = true, true
		}
	}
	// 没选中任何一格（没带参数，或带了个认不出来的）⇒ 高亮「全部」，和表里列的内容一致。
	if !hit && len(cells) > 0 && cells[0].All {
		cells[0].Active = true
	}
}

// poolRegionQuery 是「未归类」那一格在查询串里的值。
//
// 不能用空串：空串就是「全部」（没带参数、或带了个空的），两者混在一起的话点「未归类」
// 会跳回全部，而那一格恰恰是最需要单独看的（那些网关的出口反查不到区域）。
const poolRegionQuery = "other"

// filterPoolGateways 按查询串挑出表里要列的行，并回一句「正在只看谁」。
//
// 认不出的值一律当「全部」，不报错：这个参数只影响看到哪几行，而把一个拼错的链接变成
// 一张错误页，比直接给他整张表更糟。
func filterPoolGateways(rows []gatewayRow, cells []poolRegionCell, want string) ([]gatewayRow, string) {
	if want == "" {
		return rows, ""
	}
	key := want
	if want == poolRegionQuery {
		key = ""
	}
	var label string
	for _, c := range cells {
		if !c.All && c.Key == key {
			label = c.Label
			break
		}
	}
	if label == "" {
		return rows, ""
	}
	out := rows[:0:0]
	for _, r := range rows {
		if slices.Contains(r.regionKeys, key) {
			out = append(out, r)
		}
	}
	return out, label
}

// publicBaseURL 是对外根地址。接了配置文件就现读（管理页改完立刻生效），
// 否则用启动时传进来的那个（测试走这条路）。Live 为 nil 时 Get 回零值配置。
//
// **绝不看 Host / X-Forwarded-Host**：见 config.Config.PublicBaseURL。
func (s *Server) publicBaseURL() string {
	if v := s.opt.Live.Get().PublicBaseURL; v != "" {
		return v
	}
	return s.opt.PublicBaseURL
}

// handleEnter 收人贴进来的令牌，认出来就把他送到对应的页面。
//
// owner 令牌和 view_token 都收：人手上可能只剩其中一个，而让他自己分辨「我这串是
// 哪种」毫无意义——两种都是 32 个十六进制字符，长得一模一样。
//
// **认不出来时不区分「不存在」和「格式不对」**：区分出来就等于给人一个枚举令牌的
// 工具（猜对格式回一种话、猜错回另一种，一次尝试就有了反馈）。统一回那一句。
func (s *Server) handleEnter(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<10))
	if err != nil {
		http.Error(w, "请求体太大", http.StatusRequestEntityTooLarge)
		return
	}
	form, err := parseForm(body)
	if err != nil {
		http.Error(w, "表单解不开", http.StatusBadRequest)
		return
	}
	tok := strings.TrimSpace(form.Get("token"))
	if o, ok := s.st.OwnerByToken(tok); ok {
		// 顺手记住他：下次进首页就是一个按钮，不用再贴一遍。
		s.setOwnerCookie(w, r, o.Token)
		http.Redirect(w, r, "/u/"+url.PathEscape(o.Token), http.StatusSeeOther)
		return
	}
	if a, ok := s.st.AccountByViewToken(tok); ok {
		http.Redirect(w, r, "/a/"+url.PathEscape(a.ViewToken), http.StatusSeeOther)
		return
	}
	// 令牌一个字都不回显（它会躺在 URL、历史和日志里），只回一个标记。
	http.Redirect(w, r, "/?enter=bad", http.StatusSeeOther)
}

// handleLogout 清掉身份 cookie。POST（表单按钮），不是 GET——GET 能被别人的页面
// 用 <img> 之类静默触发，而登出别人的身份也是一种骚扰。清完回首页。
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	s.clearOwnerCookie(w, r)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// ---------- 上传 ----------

// authUpload 是 codex 凭据文件的两种常见形状：顶层直放，或裹在 tokens 里。
type authUpload struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	AccountID    string `json:"account_id"`
	Tokens       *struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		AccountID    string `json:"account_id"`
	} `json:"tokens"`
}

// refresh 换票，默认直连上游。
func (s *Server) refresh(ctx context.Context, refreshToken string) (string, string, error) {
	if s.opt.Refresh != nil {
		return s.opt.Refresh(ctx, refreshToken)
	}
	return oauth.Refresh(ctx, nil, refreshToken)
}

// hasBlank 判断有没有空白字符。凭据里一个都不该有——有就是复制时带进来的，
// 直接拿去发请求会变成一个没头没尾的 401。
func hasBlank(s string) bool {
	return strings.ContainsAny(s, " \t\r\n")
}

// pick 把两层形状压平：顶层优先，缺的到 tokens 里找。
func (f authUpload) pick() (access, refresh, account string) {
	access, refresh, account = f.AccessToken, f.RefreshToken, f.AccountID
	if f.Tokens != nil {
		if access == "" {
			access = f.Tokens.AccessToken
		}
		if refresh == "" {
			refresh = f.Tokens.RefreshToken
		}
		if account == "" {
			account = f.Tokens.AccountID
		}
	}
	return strings.TrimSpace(access), strings.TrimSpace(refresh), strings.TrimSpace(account)
}

// parseCreds 是信任边界：外面贴进来的东西在这里校验完，往后各包直接信。
// 报错绝不回显内容——那整段就是一个 access_token。
//
// 三种贴法都收（上传页上让人自己挑）：
//   - 整份 auth.json：access_token + refresh_token + account_id
//   - 只有 access_token：account_id 从票里的声明读，不用人再贴一遍
//   - 只有 refresh_token：当场换一张 access_token，其余同上
//
// account_id **不再要求人填**：它本来就写在票里，让人另贴一遍只是多一个贴错的机会
// ——2026-10-01 就这么进来一张标着 pro1 实际是另一个 free 账号的票，两个字段自洽、
// 上游照收，然后二十个区域全回 400。填了就必须和票里一致。
//
// 返回的第二个值是票里的声明（plan、邮箱……），空字段表示票里没写。
func (s *Server) parseCreds(ctx context.Context, raw []byte) (types.Creds, oauth.Claims, error) {
	var f authUpload
	if err := json.Unmarshal(raw, &f); err != nil {
		return types.Creds{}, oauth.Claims{}, errors.New("不是合法 JSON")
	}
	access, refresh, account := f.pick()
	if access == "" && refresh == "" {
		return types.Creds{}, oauth.Claims{}, errors.New("access_token 和 refresh_token 至少要有一个")
	}
	if refresh != "" && (len(refresh) < 20 || hasBlank(refresh)) {
		return types.Creds{}, oauth.Claims{}, errors.New("refresh_token 形状不对")
	}
	// 只给了 refresh_token：当场换一张。换不出来就别建号——建了也是个废号，
	// 第一次扫描才发现，白打一轮。
	if access == "" {
		got, newRefresh, err := s.refresh(ctx, refresh)
		if err != nil {
			return types.Creds{}, oauth.Claims{}, fmt.Errorf("用 refresh_token 换 access_token 失败：%w", err)
		}
		access = got
		if newRefresh != "" {
			refresh = newRefresh
		}
	}
	if len(access) < 20 || hasBlank(access) {
		return types.Creds{}, oauth.Claims{}, errors.New("access_token 形状不对")
	}
	cl, err := oauth.Parse(access)
	if err != nil {
		return types.Creds{}, oauth.Claims{}, err
	}
	switch {
	case account == "" && cl.AccountID == "":
		return types.Creds{}, oauth.Claims{}, errors.New("票里没有 chatgpt_account_id，得自己贴一个 account_id")
	case account == "":
		account = cl.AccountID
	case cl.AccountID != "" && cl.AccountID != account:
		return types.Creds{}, oauth.Claims{}, fmt.Errorf(
			"account_id 和 access_token 里写的不是同一个账号（票里是 %s）——八成是两处凭据贴串了",
			cl.AccountID)
	}
	// 放在这儿而不是读票之前：票里读出来的那个同样是外面送进来的（票不验签）。
	// 「anon:」开头的是匿名占位行的命名空间（types.AnonPrefix），真账号是 UUID 不会这样；
	// 放进来就能认领一行占位行、拿到它的 key，再把那个外部号的槽位历史还原掉。
	if len(account) > 128 || hasBlank(account) || strings.HasPrefix(account, types.AnonPrefix) {
		return types.Creds{}, oauth.Claims{}, errors.New("account_id 形状不对")
	}
	if !cl.Expires.IsZero() && time.Now().After(cl.Expires) {
		if refresh == "" {
			return types.Creds{}, oauth.Claims{}, errors.New("access_token 已经过期了，换一张新的（或者贴 refresh_token）")
		}
		// 有 refresh 就自己续，不必让人回去重新导出。
		got, newRefresh, rerr := s.refresh(ctx, refresh)
		if rerr != nil {
			return types.Creds{}, oauth.Claims{}, fmt.Errorf("access_token 过期且换票失败：%w", rerr)
		}
		access = got
		if newRefresh != "" {
			refresh = newRefresh
		}
		if cl, err = oauth.Parse(access); err != nil {
			return types.Creds{}, oauth.Claims{}, err
		}
	}
	return types.Creds{AccessToken: access, AccountID: account, RefreshToken: refresh}, cl, nil
}

// wrapBareToken 把「光一个 token」包成 parseCreds 认的 JSON。
//
// 不是所有人都愿意把整份 auth.json 交出来（里面还有 id_token 和别的东西），
// 所以单贴一个 token 也要收。已经是 JSON 的原样放过。
//
// kind 来自上传页的单选。auto 只认两种前缀：`eyJ`（JWT 头 `{"` 的 base64）是
// access_token，`rt.` 是 refresh_token（2026-10-02 查了池子里全部 26 张，无一例外）。
// **不能数段数**：`rt.<…>.<…>` 也是点分三段，按段数猜会把它当 access_token 拆。
// 两种都不像就报错让人手动选，**不**默认当 refresh_token：终端折行切开的半截 JWT
// 会被送去上游换票，报一句不着边的「换票失败」。
func wrapBareToken(raw []byte, kind string) ([]byte, error) {
	if len(raw) == 0 || raw[0] == '{' {
		return raw, nil
	}
	tok, err := json.Marshal(string(raw))
	if err != nil { // 只会在非法 UTF-8 时发生，交给 parseCreds 去报「不是合法 JSON」
		return raw, nil
	}
	field := "refresh_token"
	switch kind {
	case "access":
		field = "access_token"
	case "refresh":
	default: // auto
		switch {
		case bytes.HasPrefix(raw, []byte("eyJ")):
			field = "access_token"
		case bytes.HasPrefix(raw, []byte("rt.")):
		default:
			return nil, errors.New("自动识别认不出这个 token（access_token 是 eyJ 开头，refresh_token 是 rt. 开头），在「凭据类型」里手动选一下")
		}
	}
	return []byte(`{"` + field + `":` + string(tok) + `}`), nil
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		http.Error(w, "请求体太大", http.StatusRequestEntityTooLarge)
		return
	}
	label, plan := r.URL.Query().Get("label"), r.URL.Query().Get("plan")
	raw := bytes.TrimSpace(body)
	wantJSON := strings.Contains(r.Header.Get("Content-Type"), "json")
	// 身份默认取 cookie；表单里贴了 owner 令牌就按贴的那个办（「我已经有账号页了，
	// 把这份凭据挂上去」——那是显式意图，压过浏览器里碰巧存着的那个）。
	cur, hadCur := s.ownerFromCookie(r)
	owner, hasOwner := cur, hadCur
	fromPage := false // 归属来自 /u/ 页的隐藏字段（见下面 page_owner）
	if !wantJSON {
		// 表单来的：凭据在 creds 字段里。
		form, ferr := parseForm(raw)
		if ferr != nil {
			http.Error(w, "表单解析失败", http.StatusBadRequest)
			return
		}
		raw = []byte(strings.TrimSpace(form.Get("creds")))
		if v := form.Get("label"); v != "" {
			label = v
		}
		if v := form.Get("plan"); v != "" {
			plan = v
		}
		// page_owner 是 /u/ 页表单的隐藏字段：令牌跟着链接走、不是这个人自己选的，所以
		// 只决定挂到哪一页，**不写身份 cookie**——否则别人发来一个 /u/ 链接、新人在那儿
		// 传一份，浏览器就记成了别人，之后在首页传的全挂过去（审查 2026-10-02 第二轮）。
		// owner_token 是首页那个手填的输入框：人自己贴的，挂上并记住。
		tok := strings.TrimSpace(form.Get("owner_token"))
		if v := strings.TrimSpace(form.Get("page_owner")); v != "" {
			tok, fromPage = v, true
		}
		if tok != "" {
			o, ok := s.st.OwnerByToken(tok)
			if !ok {
				// 和 /enter 同一个口径：不说「这个令牌不存在」。
				http.Error(w, "认不出这个账号总览令牌，留空就新建一个", http.StatusBadRequest)
				return
			}
			owner, hasOwner = o, true
		}
		var werr error
		if raw, werr = wrapBareToken(raw, form.Get("kind")); werr != nil {
			http.Error(w, "凭据有问题："+werr.Error(), http.StatusBadRequest)
			return
		}
	}
	creds, claims, err := s.parseCreds(r.Context(), raw)
	if err != nil {
		http.Error(w, "凭据有问题："+err.Error(), http.StatusBadRequest)
		return
	}
	// token 自己写的 plan 压过表单里手填的那个：手填的只是个标签，票里的是事实，
	// 而「这票是 free」正是会让整次扫描全军覆没的那件事，得在页面上看得见。
	if claims.Plan != "" {
		plan = claims.Plan
	}
	// 回执里报「新建」还是「更新」：批量导入时同一个号贴两遍、或拿新票来刷旧号，
	// 得让人看得出没多建一行。占位行（没凭据）升级成托管行算新建。
	ex, exists := s.accountByUpstream(creds.AccountID)
	existed := exists && ex.Creds.AccessToken != ""
	// 更新已有行时没填备注就传空，存储层会留着原来的：批量上传一定不带备注，拿新票刷
	// 旧号不该把人家起过的名字冲成邮箱。新建才用默认值。
	lbl := sanitizeLabel(label, "")
	if lbl == "" && !existed {
		// 没填备注就用票里的邮箱：一次传好几份时没法逐个起名，而 account_id 那串谁也认不出。
		lbl = sanitizeLabel(claims.Email, creds.AccountID)
	}

	// 上传即 both：产品规则是「池子的目的是共享 pair，上传了凭据的就可以请求到 pair」。
	// 没有角色之分，也就没有「谁能取 pair」这个问题。
	// view_token 留空，由存储层生成（它负责保证不重样），这边只负责交到上传者手里。
	//
	// 没带身份来的要现建一个 owner —— 除非这份凭据在池子里**已经有行而且那行已经
	// 属于别人**：存储层不会改归属（见 store.AddAccount），这次上传拿不到归属，
	// 建出来的只是一个谁都不用的空 owner 行。
	createdOwner := false // 这一发新建了 owner ⇒ AddAccount 失败时要把它清掉
	if !hasOwner {
		if !exists || ex.OwnerID == 0 {
			o, oerr := s.st.AddOwner(defaultOwnerLabel)
			if oerr != nil {
				s.opt.Log.Error("建上传者失败", "err", oerr)
				http.Error(w, "存储失败", http.StatusInternalServerError)
				return
			}
			owner, hasOwner, createdOwner = o, true, true
		}
	}
	ownerID := int64(0)
	if hasOwner {
		ownerID = owner.ID
	}
	a, err := s.st.AddAccount(types.Account{
		Label:     lbl,
		PlanType:  sanitizeLabel(plan, "unknown"),
		Creds:     creds,
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
		OwnerID:   ownerID,
	})
	if err != nil {
		// 这一发要是刚为它新建了一个 owner（没带身份来的那条路），AddAccount 又失败了
		// （撞上限、并发竞争），那个 owner 就成了孤儿。立刻清掉，别让它在账本上堆积
		// ——拿站点码反复贴新 account_id 本来能把 owner 表刷到无上限。
		if createdOwner {
			if _, perr := s.st.PruneEmptyOwners(); perr != nil {
				s.opt.Log.Error("清理孤儿 owner 失败", "err", perr)
			}
		}
		s.opt.Log.Error("建账号失败", "err", err)
		http.Error(w, "存储失败", http.StatusInternalServerError)
		return
	}
	// a.OwnerID 是**最终**归属：去重命中一个已经属于别人的账号时存储层刻意不改归属，
	// 所以它可能不是我们传进去的那个。对不上就什么都不做——不换 cookie、不回链接、
	// 更不去查那个 owner 的令牌（那一页上有对方每个账号的 consumer key）。
	mine := hasOwner && a.OwnerID == ownerID
	if mine {
		// 浏览器原来没身份、或者就是这个人，才写（顺手刷有效期）。**不拿表单里的令牌去
		// 顶掉另一个身份**：/u/ 页的上传表单把那一页的令牌放在隐藏字段里，令牌跟着链接走
		// ——A 把自己的页面链接发给带着身份的 V，V 在那儿传一份，从前这里就把 V 的身份
		// 换成了 A，之后 V 在首页传的全挂到 A 名下（审查 2026-10-02）。换身份只走 /enter。
		if !fromPage && (!hadCur || cur.ID == owner.ID) {
			s.setOwnerCookie(w, r, owner.Token)
		}
	} else {
		s.opt.Log.Info("上传的凭据已挂在别的上传者名下，归属不动",
			"account", a.ID, "owner", a.OwnerID, "asked_by", ownerID)
	}
	// **不排首扫**：新账号进来时没有任何需求，那九发上游纯属白花，而且会把它在
	// 九个落点上的首次接触全烧掉。等有人要票时按需去找落点（见 Scanner 的注释）。
	s.opt.Log.Info("收到新账号", "account", a.ID, "label", a.Label)

	// 归属没落到他头上 ⇒ **一把钥匙都不给**，连 view_token 和 303 的落点都不给。
	//
	// 这条路是真能被人走的：oauth.Parse 不验签（它自己的注释写着），所以知道别人
	// 上游 account_id 的人可以自制一张 JWT 打上来，让去重命中那一行。归属确实没被改，
	// 但从前这里照样把**那一行的** view_token 和 consumer key 原样回出去，表单路径还
	// 303 到 /a/<别人的 view_token>——拿到前者能开对方账号页、能 POST 停掉他的号，
	// 拿到后者能直接替他取票。等于归属守住了，钥匙从旁边递了出去。
	if !mine {
		// **409 而不是 200**：这不是一次成功的上传，而前端恰好把「200 就一定有 url」当成了
		// 契约（layout.html 里 `if (r.ok) location.assign((await r.json()).url)`）。回 200 +
		// 没有 url 的 JSON ⇒ `location.assign(undefined)` ⇒ 浏览器跳到 /undefined 的裸 404，
		// 下面这句话一个人都看不到。而走到这条路最常见的不是攻击，是「换了浏览器、总览令牌
		// 丢了，把同一份 auth.json 再贴一次想找回链接」。
		// 两条路都回同一个状态码和同一句话，钥匙照旧一把都不给（见上面那段注释）。
		http.Error(w, "这份凭据已经登记过了，页面地址请找它的上传者要。", http.StatusConflict)
		return
	}
	if wantJSON || strings.Contains(r.Header.Get("Accept"), "json") {
		status := "created"
		if existed {
			status = "updated"
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"status":       status,
			"label":        a.Label,
			"plan":         a.PlanType,
			"view_token":   a.ViewToken,
			"url":          "/a/" + a.ViewToken,
			"consumer_key": s.ConsumerKeyFor(a),
			// 换设备时靠这个令牌回来，所以要给得出来。
			"owner_token": owner.Token,
			"owner_url":   "/u/" + owner.Token,
		})
		return
	}
	// 传完回账号总览页，不钻进某一个号：一次常常传好几份，要看的是全貌（用户 2026-10-02 要的）。
	http.Redirect(w, r, "/u/"+owner.Token, http.StatusSeeOther)
}

// ---------- 上传者信任视图 ----------

func (s *Server) handleAccount(w http.ResponseWriter, r *http.Request) {
	tok := r.PathValue("token")
	// 空 token 一律拒：历史上没分配过 view_token 的账号行，空串会把它整条匹配出来。
	if tok == "" {
		http.Error(w, "没有这个视图链接", http.StatusNotFound)
		return
	}
	a, ok := s.st.AccountByViewToken(tok)
	if !ok {
		http.Error(w, "没有这个视图链接", http.StatusNotFound)
		return
	}
	samples, err := s.samples()
	if err != nil {
		s.opt.Log.Error("读样本失败", "err", err)
		http.Error(w, "读账本失败", http.StatusInternalServerError)
		return
	}
	v := accountView{
		base:      base{Title: "你的账号", Mode: s.pool.Mode()},
		ViewToken: tok,
		Label:     a.Label, Plan: a.PlanType, Enabled: a.Enabled,
		DisabledReason: a.DisabledReason,
		Since:          a.CreatedAt.Format(time.DateTime),
		Model:          s.probeModel(), Effort: probeEffort,
		ConsumerKey: s.ConsumerKeyFor(a),
		BaseURL:     s.publicBaseURL(),
	}
	// owner 令牌**只在浏览器已经带着它**时才回显。这一页的地址是转发得出去的
	// （页面上写着「不要分享」，人还是会分享），而 owner 令牌能打开这个人**所有**
	// 账号的页面、连带那几把 consumer key ⇒ 印在 HTML 里就是一次权限升级。
	// 拿 cookie 当判据的话，转发出去的链接在别人浏览器里什么都多不出来。
	// 令牌丢了的人去找管理员（管理页上列着）。
	if o, ok := s.ownerFromCookie(r); ok && o.ID == a.OwnerID {
		v.OwnerToken = o.Token
	}
	v.NewGateways = s.foundBy(a.ID)
	v.Regions = regionCells(a.ID, s.st.RegionMints(), samples, s.st.Gateways(), s.slotCooldown())
	for _, sm := range samples {
		if sm.AccountID != a.ID {
			continue
		}
		v.Requests++
		v.Rows = append(v.Rows, sampleRow{
			At: sm.At.Format(time.DateTime), Where: geoLabel(sm),
			Gateway: dash(sm.Gateway), Status: sm.Status,
		})
	}
	// Detail 不给上传者：里面是出口名、上游状态码这类内部读数。
	v.Fails = s.fails.failRows(a.ID, 20, false)
	// 新的在上面，太长就截断——这是给人看的凭证，不是日志系统。
	reverse(v.Rows)
	if len(v.Rows) > 500 {
		v.Truncated = len(v.Rows) - 500
		v.Rows = v.Rows[:500]
	}
	s.render(w, s.tplAccount, v)
}

// foundBy 数这个账号为池子发现了几个新网关。
func (s *Server) foundBy(accountID int64) int {
	n := 0
	for _, g := range s.st.Gateways() {
		if g.FirstSeenBy == accountID {
			n++
		}
	}
	return n
}

// ---------- 上传者总览（一个人的多账号页） ----------

// handleOwner 出 GET /u/{owner_token}：这个人名下每一份凭据各一张卡。
//
// 鉴权就是令牌本身，和 /a/{view_token} 同一个套路（store.OwnerByToken 挡掉空串）。
// **只读 AccountsByOwner 那一份名单**，绝不遍历全表再筛——筛漏一条就是把别人的
// consumer key 印在这一页上。
func (s *Server) handleOwner(w http.ResponseWriter, r *http.Request) {
	tok := r.PathValue("token")
	o, ok := s.st.OwnerByToken(tok)
	if !ok {
		// 和 /a/ 一个口径：不区分「没有这个令牌」和「格式不对」。
		http.Error(w, "没有这个总览链接", http.StatusNotFound)
		return
	}
	samples, err := s.samples()
	if err != nil {
		s.opt.Log.Error("读样本失败", "err", err)
		http.Error(w, "读账本失败", http.StatusInternalServerError)
		return
	}
	// **这一页一个字的身份 cookie 都不写。** 曾经写过，条件是「还没有身份时才记」，
	// 图的是「来都来了，下次进首页就是一个按钮」。那是一个身份固定漏洞：
	//
	//	攻击者把自己的 /u/<他的令牌> 发给一个新人（「这是我的后台，你也传一份」）
	//	⇒ 新人浏览器里没有 gwp_owner ⇒ 服务端给他种上**攻击者的**令牌
	//	⇒ 新人回首页上传，handleUpload 读到这个 cookie ⇒ 凭据以攻击者的 OwnerID 落库
	//	⇒ 攻击者的总览页上出现受害者的 consumer key 和 view_token（后者还能停对方的号），
	//	  而受害者自己连一个 owner 都没有，看不出任何异常。
	//
	// SameSite=Lax 挡不住：顶层 GET 导航照样带 cookie，响应里的 Set-Cookie 也照收。
	// 「只在没有身份时写」挡住的是**换掉**已有身份，挡不住**凭空建立**一个身份，
	// 而后者才是这条路上真正发生的事。
	//
	// 身份只在用户**自己动手**的两条 POST 上写：/enter（他贴了令牌）和 /upload
	// （他交了凭据）。一个点链接就能到达的 GET 不该改变「你是谁」。

	// 登出按钮只在「访客的身份 cookie 正好就是这个 owner」时给：这一页靠令牌认人，
	// 谁拿到链接都看得到，但登出清的是浏览器里那个 cookie——cookie 指向别人（或没有）时
	// 给按钮只会误导。
	cur, had := s.ownerFromCookie(r)
	v := ownerView{
		base:      base{Title: "我的账号", Mode: s.pool.Mode()},
		Token:     tok,
		Label:     o.Label,
		Since:     o.CreatedAt.Format(time.DateTime),
		Model:     s.probeModel(),
		Effort:    probeEffort,
		BaseURL:   s.publicBaseURL(),
		CanLogout: had && cur.ID == o.ID,
		Upload:    uploadForm{PageToken: tok},
	}
	v.Accounts, v.LastFetch = s.ownerAccountRows(s.st.AccountsByOwner(o.ID), samples)
	for _, row := range v.Accounts {
		v.Requests += row.Requests
		v.NewGateways += row.NewGateways
	}
	s.render(w, s.tplOwner, v)
}

// ownerAccountRows 把一串账号摊成卡片行。/u/{token} 和管理页账号页签共用：
// 两边要的是同一套读数，分两份写迟早只有一边加字段。
//
// 失败行两边都按**上传者口径**取（只要真失败、不带 Detail）：这张卡的标题就是
// 「最近的取票失败」，把归还和续期混进来会让人读错；管理员要的 Detail 在管理页
// 那张全量失败表里，带原始错误。
func (s *Server) ownerAccountRows(accs []types.Account, samples []types.Sample) ([]ownerAccountRow, string) {
	// 请求数按账号先汇总一遍：账号可能有好几个，每个都扫一遍全量样本是 O(n×m)。
	reqs := map[int64]int{}
	for _, sm := range samples {
		reqs[sm.AccountID]++
	}
	// 最近一次取票：自己的槽位，加上拿这个号的 key 替外部账号取的——那些记在占位行上，
	// 占位行的 Via 指回最近一次替它取票的那个号。两种都说明上传者那边的对接是通的。
	// ponytail: Via 只记最近一个号，一个外部账号先后被两把 key 取过时，早先那把 key 的
	// 号就看不到这几次；要准就得按 (占位行, key) 记取票时间。
	via := map[int64]int64{}
	for _, a := range s.st.Accounts() {
		if a.Via != 0 && a.Creds.AccessToken == "" {
			via[a.ID] = a.Via
		}
	}
	lastFetch := map[int64]time.Time{}
	for _, sl := range s.st.Slots() {
		id := sl.AccountID
		if v, ok := via[id]; ok {
			id = v
		}
		if sl.LastDeliver.After(lastFetch[id]) {
			lastFetch[id] = sl.LastDeliver
		}
	}
	mints := s.st.RegionMints()
	gws := s.st.Gateways()
	// 九宫格的滑窗 = 槽位冷却：和调度那把尺子同一个值（见 regionCells）。
	window := s.slotCooldown()
	now := time.Now().UTC()
	var newest time.Time
	out := make([]ownerAccountRow, 0, len(accs))
	for _, a := range accs {
		row := ownerAccountRow{
			ID: a.ID, Label: a.Label, Plan: a.PlanType,
			Enabled: a.Enabled, DisabledReason: a.DisabledReason,
			Since:       a.CreatedAt.Format(time.DateTime),
			ViewToken:   a.ViewToken,
			ConsumerKey: s.ConsumerKeyFor(a),
			Requests:    reqs[a.ID],
			NewGateways: s.foundBy(a.ID),
			Regions:     regionCells(a.ID, mints, samples, gws, window),
			Fails:       s.fails.failRows(a.ID, 20, false),
		}
		if t := lastFetch[a.ID]; !t.IsZero() {
			row.LastFetch, row.LastFetchAt = ago(now, t), t.UTC().Format(time.DateTime)
			if t.After(newest) {
				newest = t
			}
		}
		out = append(out, row)
	}
	// 稳定顺序：加入先后就是人心里的顺序，而 map 遍历不是。
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	if newest.IsZero() {
		return out, ""
	}
	return out, ago(now, newest)
}

// ago 是「12 分钟前」。不到一分钟（含时钟略有偏差、t 比 now 还晚）一律「刚刚」：
// shortDur 对非正数回「已过期」，拼上「前」就成了胡话。
func ago(now, t time.Time) string {
	if d := now.Sub(t); d >= time.Minute {
		return shortDur(d) + "前"
	}
	return "刚刚"
}

// regionCells 画一个账号的九宫格：types.Regions 那九个区域，终身铸过票的算绿。
//
// 按 (账号, 区域) 两个**字段**查台账，不去拼 store 那个 "<id>|<区域>" 键：键格式是
// 存储层的事，抄一份迟早对不上（handlePool 对槽位表也是这么做的）。
//
// 顺带兜住一件事：台账里可能有 RegionKeyOf 反查回空串的行（手改的 state.json），
// 而这里是**照 types.Regions 画格子、再回头查台账**，所以那种行天然画不出第十个格子。
// 绿/灰按 window 这个**滑窗**判（调用方传 slot_cooldown，默认 4 小时），不按终身台账：
// 这张卡的用处是「此刻这个号还能派去哪个区域铸票」，终身绿会把早就凉了的区域标成绿。
// 终身读数（台账的 Count/LastAt）退到 Note 里，悬停还看得见。
//
// 网关名只在样本里（台账只记计数），所以 samples/gws 一起传进来：gws 用来判
// 「这个网关是不是记在别人名下」。两者为 nil 时格子照画，只是列不出名字。
func regionCells(accountID int64, mints map[string]types.RegionMint,
	samples []types.Sample, gws map[string]types.Gateway, window time.Duration) regionGrid {
	byRegion := map[string]types.RegionMint{}
	for _, m := range mints {
		if m.AccountID == accountID && m.RegionKey != "" {
			byRegion[m.RegionKey] = m
		}
	}
	// 按**样本原序**走（samples.jsonl 就是追加序），于是「这个账号第一次在哪个区域
	// 撞见这个网关」天然按时间定下来，后面那几个区域才算重复。
	// 只认画得出格子的那九个区域 key。样本里的 RegionKey 反查不到区域时退回国家码
	// （"IT"，见 sched.sampleRegion），放进来的话它会**悄悄占掉**某个网关的「第一次」，
	// 于是九宫格里真正第一次撞见它的那格被标成「重」，配对的「新」在页面上根本不存在。
	//
	// 也正因为样本那一列会是国家码，**绿/灰不能按样本数**——那等于把一部分真实铸票
	// 判成没发生。绿按铸票台账（NoteRegionMint 只记成功、而且只用真区域 key）。
	known := make(map[string]bool, len(types.Regions))
	for _, r := range types.Regions {
		known[r.Key] = true
	}
	firstRegion := map[string]string{}
	inRegion := map[string]map[string]bool{}
	for _, sm := range samples {
		// Gateway 非空就是「这一发真取到了路由」——照 types.MintResult 的语义，
		// 取不到的那几发 Gateway 为空。
		if sm.AccountID != accountID || sm.Gateway == "" || !known[sm.RegionKey] {
			continue
		}
		if _, ok := firstRegion[sm.Gateway]; !ok {
			firstRegion[sm.Gateway] = sm.RegionKey
		}
		if inRegion[sm.RegionKey] == nil {
			inRegion[sm.RegionKey] = map[string]bool{}
		}
		inRegion[sm.RegionKey][sm.Gateway] = true
	}
	now := time.Now()
	grid := regionGrid{Window: shortDur(window), Cells: make([]regionCell, 0, len(types.Regions))}
	for _, r := range types.Regions {
		c := regionCell{Key: r.Key, Label: regionLabel(r.Key)}
		m, had := byRegion[r.Key]
		c.Fresh = had && m.Count > 0 && !m.LastAt.IsZero() && now.Sub(m.LastAt) < window
		if c.Fresh {
			c.Ago = shortDur(now.Sub(m.LastAt))
			c.Note = c.Ago + "前在此区域取得路由 Cookie"
		} else {
			c.Note = grid.Window + "内未在此区域取得路由 Cookie，可安排铸票"
		}
		// 终身读数退到悬停：窗口是「现在能不能去」，终身是「以前去过没有」。
		if had && m.Count > 0 {
			c.Note += fmt.Sprintf("；累计 %d 次", m.Count)
			if !m.LastAt.IsZero() {
				c.Note += "，最近一次 " + m.LastAt.UTC().Format(time.DateTime) + " UTC"
			}
		} else {
			c.Note += "；累计未取得过"
		}
		for name := range inRegion[r.Key] {
			dup := firstRegion[name] != r.Key
			if g, ok := gws[name]; ok && g.FirstSeenBy != accountID {
				dup = true
			}
			c.Gateways = append(c.Gateways, regionGateway{Name: name,
				Short: strings.TrimPrefix(name, "unified-"), Dup: dup})
		}
		// 按编号排：map 遍历每次都换顺序，刷新一下页面就跳。先比长度再比字面 = 数值序——
		// 格子里只显示编号，字面序会把 126 排在 73 前面，看着像乱的。
		sort.Slice(c.Gateways, func(i, j int) bool {
			a, b := c.Gateways[i].Short, c.Gateways[j].Short
			if len(a) != len(b) {
				return len(a) < len(b)
			}
			return a < b
		})
		grid.Cells = append(grid.Cells, c)
	}
	return grid
}

// ---------- /cookie ----------

func (s *Server) handleCookie(w http.ResponseWriter, r *http.Request) {
	req, wait, perr := parseCookieRequest(r)
	if perr != nil {
		s.badRequest(w, http.StatusBadRequest, perr.Error())
		return
	}
	c, ok := s.resolveCaller(r)
	if !ok {
		s.serveAnonymous(w, r, req, wait)
		return
	}
	acct := c.acct
	ref := strings.TrimSpace(r.URL.Query().Get("account"))
	if c.admin {
		// 管理员兜底必须显式说替谁取：槽位是 (账号 × 网关)，替谁取决定了取哪个槽位，
		// 猜一个账号就等于把 A 的槽位发给 B。
		if ref == "" {
			s.badRequestFor(w, http.StatusBadRequest, 0, "管理员调用必须带 account 参数指明替谁取")
			return
		}
		if acct, ok = s.findAccount(ref); !ok {
			s.badRequestFor(w, http.StatusNotFound, 0, "没有这个账号")
			return
		}
		ref = ""
	}
	// 一把 key 可以替**别的**上游账号取票（上传一份凭据换一把 key，拿它给自己手上好几个
	// 号用）。那种票的槽位必须按报上来的那个账号记：A 烧掉一个网关和 B 在同一个网关上
	// 是不是满血毫无关系。
	//
	// 代价说在明处：池子**验不了**那张票——state-echo 要用那个账号的凭据打一发，池子
	// 手上只有上传者的。所以走和匿名同一条路，回去的 verified_full 是 false。
	if len(ref) > 128 { // 和匿名分支同一个上限：占位行按它建，标识会落盘、上管理页
		s.badRequest(w, http.StatusBadRequest, "account 参数太长")
		return
	}
	if ref != "" && !accountRefMatches(acct, ref) {
		// 记账归上传者（他的页面该看见这一行），**调度归占位行**：轮询要按那个号的
		// 冷却判，发现扫描要排除的也是那个号——而池内行里只有上传者。
		//
		// authed=true：这条路的调用方**鉴权过了**，走 DeliverAnonymous 只是因为池子
		// 手上没有那个号的凭据（验不了、也不替它铸），不该连带吃匿名的开放门槛。
		s.serveCookie(w, r, acct.ID, s.rowFor(types.AnonPrefix+ref), req, wait,
			func(rq sched.Request) (types.Delivery, error) {
				d, err := s.pool.DeliverAnonymous(r.Context(), ref, rq, true)
				if err == nil {
					s.noteVia(ref, acct.ID)
				}
				return d, err
			})
		return
	}
	// 挑不出可交付的槽位就 503。宁可 503，也不把降智的东西发出去。
	s.serveCookie(w, r, acct.ID, acct.ID, req, wait, func(rq sched.Request) (types.Delivery, error) {
		return s.pool.Deliver(r.Context(), acct.ID, rq)
	})
}

// serveAnonymous 处理没带有效 key 的 /cookie。
//
// 票富裕时自动对外开放、回落就自动关上（见 sched.PublicState）。
// 刻意不做限流/配额：现在没需求，门槛本身就是闸门。
func (s *Server) serveAnonymous(w http.ResponseWriter, r *http.Request, req sched.Request, wait time.Duration) {
	ps := s.pool.Public()
	if !ps.Enabled {
		// 没带有效 key，而池子又不对外开放 ⇒ 这就是「你的 key 不认」。
		s.unauthorized(w, "需要本账号的 key")
		return
	}
	if !ps.Open {
		s.publicClosed(w, fmt.Sprintf("池子暂不对外开放：托管账号 %d 个，门槛 %d 个",
			ps.Accounts, ps.Threshold))
		return
	}
	// 必须报上他**自己的**上游账号标识：槽位是 (账号 × 网关)，不知道是谁就没法
	// 避开他已经烧过的网关。报错的只会害他自己，所以不防乱填。
	ref := strings.TrimSpace(r.URL.Query().Get("account"))
	if ref == "" || len(ref) > 128 {
		s.badRequest(w, http.StatusBadRequest, "匿名取用必须带 account 参数（你自己的上游 account_id）")
		return
	}
	s.serveCookie(w, r, 0, s.rowFor(types.AnonPrefix+ref), req, wait,
		func(rq sched.Request) (types.Delivery, error) {
			// authed=false：真匿名，门槛照常卡他（那是给上传者留的余量）。
			d, err := s.pool.DeliverAnonymous(r.Context(), ref, rq, false)
			if err == nil {
				s.noteVia(ref, 0) // 这一次没带 key
			}
			return d, err
		})
}

// noteVia 记下匿名占位行最近一次**取到票**是拿谁的 key（0 = 没带 key）。只记成功的：
// 失败的尝试谁都能发（门槛关着时不带 key 的扫描器），不该把记录冲掉。占位行是取票时
// 才建的，所以在取票之后记；「这是谁在用」只有 /cookie 这里知道。没变就不写盘。
func (s *Server) noteVia(ref string, via int64) {
	if id := s.rowFor(types.AnonPrefix + ref); id != 0 {
		if err := s.st.SetAccountVia(id, via); err != nil {
			s.opt.Log.Error("记匿名行的来源失败", "account", id, "err", err)
		}
	}
}

// deliveryResponse 在 types.Delivery 之外多说两句话。
// 包一层而不是改 types：那边签名是冻结的。嵌入是匿名字段，JSON 会摊平成同一层。
type deliveryResponse struct {
	types.Delivery
	// TTLIsAdvisory 恒为 true：valid_for_s 是**建议值**不是承诺。
	// 该不该换 pair 由客户端判断——只有它知道自己那一发对不对劲，
	// 判断该换了就带 force=1 再来一次。
	TTLIsAdvisory bool `json:"ttl_is_advisory"`
	// CookieVersion 是这张**具体的票**的不透明身份，用来回头指名它：
	// exclude_versions=<它> 表示「别再给我这张」，/release 用它说「这张我没用」。
	// 算法见 types.CookieVersion（确定性、不存状态、重启后仍一致）。
	CookieVersion string `json:"cookie_version"`
	// PairRemainingS 是**这张 pair 自己**还能活多久（秒）。
	//
	// 和 ValidForS 是**两件事**，一个字都不许混：ValidForS 是满血窗口的剩余
	// （150 秒量级，过了就该换票），PairRemainingS 是路由的剩余（小时量级，
	// 过了这张 cookie 谁都用不了）。消费端拿它决定要不要在业务请求里顺手续一发
	// （剩不到 12 分钟才续，见 /pair/renew）。混成一个的后果是每发都续、或者永不续。
	PairRemainingS int `json:"pair_remaining_s"`
}

func (s *Server) delivered(d types.Delivery) deliveryResponse {
	return deliveryResponse{Delivery: d, TTLIsAdvisory: true,
		CookieVersion:  types.CookieVersion(d.Cookie),
		PairRemainingS: s.pairRemainingS(d)}
}

// pairRemainingS 算交付出去这张 pair 的剩余寿命：向下取整到秒，负数钳 0。
//
// 按 CflbExpiresAt 算，因为钉路由的是 __cflb（见 types.PairLifetime），续期换到的
// 也正是它。读不出来就报 0：
//   - 零值 = 老记录（store.Open 的兜底会填上，这里只是不去赌它填过了）；
//   - cookie 对不上 = 池子手上那张已经不是交付出去的这张，那它的死期和消费端
//     手里这张无关。
//
// 两种都报 0，而消费端把 0 和字段缺失都当成「不续」——方向是安全的那边
// （少续一次只是少一张票，照着一个不属于这张 cookie 的死期去续才会写坏账本）。
func (s *Server) pairRemainingS(d types.Delivery) int {
	p, ok := s.st.Pairs()[d.Gateway]
	if !ok || p.Cookie != d.Cookie {
		return 0
	}
	return int(max(time.Until(p.CflbExpiresAt), 0) / time.Second)
}

// truthy 认几种常见的「真」写法：客户端是别人写的，别为了一个开关卡他。
func truthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// deliverFailed 统一回写交付失败。细节只进日志不进响应：上游的错误串里可能
// 裹着请求头之类的东西（红线 1）。
//
// 状态码一律 503：对客户端来说这九种都是「这一发拿不到票」，该不该再来、等多久
// 由码和 Retry-After 说，不由状态码说（而 sub2api 的客户端只认 503 → ErrNoSlot）。
func (s *Server) deliverFailed(w http.ResponseWriter, err error, acctID, consumerID int64, gateway string) {
	s.opt.Log.Info("交付失败", "account", acctID, "consumer", consumerID, "gateway", gateway, "err", err)
	code, msg, retry := s.classify(err)
	if errors.Is(err, sched.ErrNoFullSlot) {
		// 这个账号在所有已知网关上都烧完了。光回 503 的话池子就卡在这儿了——
		// 同一个网关上重铸一张新 pair 对它没用（烧掉的是「这个号接触过这个网关」，
		// 换票不解封），只有**没碰过的新网关**才有窗口。所以去找新落点。
		//
		// 派去的人要排除的是**消费者**，不是记账那一行：跨账号时消费者是占位行，
		// 而上传者在这里恰恰是最合适的铸票者（烧它的窗口不影响消费者对新落点的首次
		// 接触）。把上传者当成消费者排掉的话，单上传者的池子永远挑不出铸票者 ⇒
		// 这种失败唯一的出路被堵死。消费者是占位行时传 0（它没凭据，本来也选不上它）。
		if s.scan != nil {
			s.scan.EnqueueDiscovery(consumerID)
		}
	}
	// 页面上要看得见：客户端那头只有一个 502/503，上传者对着它猜不出是哪种。
	// 码也要落下来——同一句人话底下可能是两种完全不同的处置。
	detail := err.Error()
	if code == codeNoGateway {
		// 冷启动得人踢一脚，而这件事只在日志里说过。只有一个启用账号时它自己就是
		// 消费者 ⇒ 派谁去找落点都会被排掉（派他自己等于当场烧掉那个落点），
		// 而网关表空着时现铸也无从下手（pickExit 要一个已在表上的网关）。
		detail += "｜池子一个网关都没有：要么再上传一个账号（让它去找落点），" +
			"要么手动 POST /admin/rescan 踢一次"
	}
	s.fails.add(deliverFail{At: time.Now(), AccountID: acctID, Gateway: gateway,
		Code: code, Reason: msg, Detail: detail})
	writeFail(w, http.StatusServiceUnavailable,
		apiError{Code: code, Message: msg, RetryAfterSeconds: retrySeconds(retry)})
}

// handleAccountEnabled 让上传者自己开关这个账号。
//
// 鉴权就是 view_token 本身——和这一页同一把钥匙。没有额外的密码：拿得到这个
// 128 位链接的人本来就能看到这一页的全部内容（含那把 key），再加一道没有意义。
//
// 停用后 resolveCaller 不再认这把 key，于是请求掉进匿名通道：公开池关着就是 401，
// 开着但没到门槛就是 503。这正是「停用后没开公开池就请求不了」。
func (s *Server) handleAccountEnabled(w http.ResponseWriter, r *http.Request) {
	tok := r.PathValue("token")
	if tok == "" {
		http.Error(w, "没有这个视图链接", http.StatusNotFound)
		return
	}
	a, ok := s.st.AccountByViewToken(tok)
	if !ok {
		http.Error(w, "没有这个视图链接", http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<10))
	if err != nil {
		http.Error(w, "读请求体失败", http.StatusBadRequest)
		return
	}
	form, err := parseForm(body)
	if err != nil {
		http.Error(w, "表单解不开", http.StatusBadRequest)
		return
	}
	if err := s.st.SetAccountEnabled(a.ID, truthy(form.Get("enabled"))); err != nil {
		s.opt.Log.Error("改账号启用状态失败", "account", a.ID, "err", err)
		http.Error(w, "写失败", http.StatusInternalServerError)
		return
	}
	// 303 回本页：刷新不会重复提交，地址栏也还是那个链接。
	http.Redirect(w, r, "/a/"+url.PathEscape(tok), http.StatusSeeOther)
}

// handleAdminRescan 手动踢一发「去找新落点」的裸铸。
//
// 为什么留着这个端点（而不是跟着首扫一起删）：自动触发只有两条路——交付撞上
// ErrNoFullSlot、以及保温预铸，而两条都要**先有需求**。一个刚起的空池子
// （零网关）在没人取票之前永远不会自己去找落点，而那正是最需要踢一脚的时候。
//
// account 可选，语义是「替谁找」⇒ 它自己**不会**被派出去：找到的落点必须对他还是
// 首次接触，派他自己去等于当场烧掉（见 Scanner.EnqueueDiscovery）。
// 不给就是「随便派一个有空区域的号」。
//
// 刻意只给管理员：每踢一下都真打上游、真烧一个 (账号 × 网关)。
func (s *Server) handleAdminRescan(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	var forWhom int64
	if ref := r.URL.Query().Get("account"); ref != "" {
		a, ok := s.findAccount(ref)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "没有这个账号"})
			return
		}
		forWhom = a.ID
	}
	// 如实回话。「踢了但其实没派出去」的三种情形（挑不出人、配额打满、还在发现冷却里）
	// 从前都回 queued:true，只有服务端日志说了实话——而这个按钮正是单账号冷池子唯一的
	// 出路，看不出踢没踢动的话，运维只能一直点。
	why := s.scan.EnqueueDiscovery(forWhom)
	// 表单提交 ⇒ 303 回管理页，结论挂在查询串上（与删账号那个按钮同口径）。
	if !strings.Contains(r.Header.Get("Accept"), "json") {
		to := "/admin/?rescan=queued"
		if why != "" {
			to = "/admin/?rescan=" + url.QueryEscape(why)
		}
		http.Redirect(w, r, to, http.StatusSeeOther)
		return
	}
	if why != "" {
		writeJSON(w, http.StatusOK,
			map[string]any{"account": forWhom, "queued": false, "reason": why})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"account": forWhom, "queued": true})
}

// handleAdminDeleteAccount 删掉一个账号。管理页上的按钮走这里。
//
// 为什么管理员要有这个而上传者只有「停用」：无效凭据（贴错的、过期的、free 的）
// 是运维要清掉的垃圾，而上传者那边的语义是「我先不参与了」，两件事不一样。
//
// 表单提交 ⇒ 303 回点按钮的那一页；带 Accept: json 的（curl、脚本）给 JSON。
func (s *Server) handleAdminDeleteAccount(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	id, form, ok := s.adminAccountForm(w, r)
	if !ok {
		return
	}
	if err := s.st.DeleteAccount(id); err != nil {
		s.opt.Log.Error("删账号失败", "account", id, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.opt.Log.Info("删了账号", "account", id)
	if strings.Contains(r.Header.Get("Accept"), "json") {
		writeJSON(w, http.StatusOK, map[string]any{"account": id, "deleted": true})
		return
	}
	http.Redirect(w, r, adminBack(form), http.StatusSeeOther)
}

// handleAdminAccountEnabled 停用/启用一个账号：管理页账号页签的卡片上直接点，
// 不用再进账号详情页。和上传者自己点的是同一个方法（翻了开关才清自动停用的理由）。
func (s *Server) handleAdminAccountEnabled(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	id, form, ok := s.adminAccountForm(w, r)
	if !ok {
		return
	}
	// 只收按钮发的 0 / 1：缺了这个字段就当「停用」的话，脚本发个 JSON 体就会把号静默停掉。
	v := form.Get("enabled")
	if v != "0" && v != "1" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "enabled 只收 0 或 1"})
		return
	}
	enabled := v == "1"
	if err := s.st.SetAccountEnabled(id, enabled); err != nil {
		s.opt.Log.Error("改账号启用状态失败", "account", id, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.opt.Log.Info("管理员改了账号启用状态", "account", id, "enabled", enabled)
	if strings.Contains(r.Header.Get("Accept"), "json") {
		writeJSON(w, http.StatusOK, map[string]any{"account": id, "enabled": enabled})
		return
	}
	http.Redirect(w, r, adminBack(form), http.StatusSeeOther)
}

// adminAccountForm 读管理页账号按钮的路径 ID 和表单，账号不存在就 404。失败时已经写好了响应。
// 存在与否先查一遍：之后 store 再报错就只能是写盘失败（500），不会和「没这个号」混成一个 404。
func (s *Server) adminAccountForm(w http.ResponseWriter, r *http.Request) (int64, url.Values, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "账号 ID 不是正整数"})
		return 0, nil, false
	}
	form, ok := readAdminForm(w, r)
	if !ok {
		return 0, nil, false
	}
	for _, a := range s.st.Accounts() {
		if a.ID == id {
			return id, form, true
		}
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("账号 %d 不存在", id)})
	return 0, nil, false
}

// adminBack 是点完按钮回哪一页。只认这几个固定值：拿表单里的字符串直接当地址就是开放重定向。
func adminBack(form url.Values) string {
	switch form.Get("back") {
	case "external":
		return "/admin/#external"
	}
	return "/admin/#accounts"
}

func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	samples, err := s.samples()
	if err != nil {
		http.Error(w, "读账本失败", http.StatusInternalServerError)
		return
	}
	now := time.Now().UTC()
	v := adminView{base: base{Title: "管理", Mode: s.pool.Mode()}}
	v.Config = buildConfigView(s.opt.Live,
		r.URL.Query().Get("cfg_note"), r.URL.Query().Get("cfg_err"))
	v.Rescan = sanitizeLabel(r.URL.Query().Get("rescan"), "")
	v.ProxyNote, v.ProxyErr = r.URL.Query().Get("proxy_note"), r.URL.Query().Get("proxy_err")
	v.ProxyEditable = s.canEditProxies()
	v.Fails = s.fails.failRows(0, failLogKeep, true)

	if s.opt.ProxyStates != nil {
		for _, p := range s.opt.ProxyStates() {
			row := proxyRow{
				Name: p.Name, Kind: p.Kind, Host: p.Host, UserMask: p.UserMask,
				Enabled: p.Enabled, Sticky: p.Sticky, Fails: p.Fails, BoundSIDs: p.BoundSIDs,
			}
			if left := p.SkipUntil.Sub(now); left > 0 {
				row.SkipLeft = shortDur(left)
			}
			v.Proxies = append(v.Proxies, row)
		}
	}

	reqs := map[int64]int{}
	for _, sm := range samples {
		reqs[sm.AccountID]++
	}
	gws := s.st.Gateways()
	found := map[int64]int{}
	for _, g := range gws {
		found[g.FirstSeenBy]++
	}
	accounts := s.st.Accounts()
	byID, byUpstream := map[int64]types.Account{}, map[string]types.Account{}
	for _, a := range accounts {
		byID[a.ID] = a
		if a.Creds.AccessToken != "" {
			byUpstream[a.Creds.AccountID] = a
		}
	}
	name := func(id int64) string { return acctName(byID, id) }
	owners := s.st.Owners()
	ownerLabels := ownerNames(owners, accounts)
	ownerName := map[int64]string{}
	for _, o := range owners {
		ownerName[o.ID] = fmt.Sprintf("#%d %s", o.ID, ownerLabels[o.ID])
	}
	covered := regionsCovered(s.st.RegionMints())
	slots := s.st.Slots()
	delivered, lastDeliver := map[int64]int{}, map[int64]time.Time{}
	for _, sl := range slots {
		if sl.LastDeliver.IsZero() {
			continue
		}
		delivered[sl.AccountID]++
		if sl.LastDeliver.After(lastDeliver[sl.AccountID]) {
			lastDeliver[sl.AccountID] = sl.LastDeliver
		}
	}
	holds := s.pool.AccountHolds()
	ownerAccounts := map[int64]int{}
	for _, a := range accounts {
		ownerAccounts[a.OwnerID]++
		row := accountRow{
			ID: a.ID, Label: a.Label, Plan: a.PlanType,
			Enabled: a.Enabled, DisabledReason: a.DisabledReason,
			Requests: reqs[a.ID], Found: found[a.ID],
			ViewToken: a.ViewToken, Since: a.CreatedAt.Format(time.DateTime),
			Upstream: a.Creds.AccountID, OwnerID: a.OwnerID,
		}
		if d := holds[a.ID]; d > 0 {
			row.Held = shortDur(d)
		}
		if a.Creds.AccessToken != "" {
			row.Owner = ownerName[a.OwnerID]
			if row.Owner == "" {
				row.Owner = fmt.Sprintf("#%d", a.OwnerID)
			}
			n := covered[a.ID]
			row.Covered = fmt.Sprintf("%d/%d", n, len(types.Regions))
			row.Facet = accountFacet(a, n)
			v.Accounts = append(v.Accounts, row)
			continue
		}
		// 外部账号单独一张表：混在账号表里，它那几格请求数、发现网关、账号页全是空的。
		row.Upstream = strings.TrimPrefix(a.Creds.AccountID, types.AnonPrefix)
		row.Delivered = delivered[a.ID]
		if a.Via != 0 {
			row.Via, row.ViaToken = name(a.Via), byID[a.Via].ViewToken
		}
		if same, ok := byUpstream[row.Upstream]; ok {
			row.SameAs = fmt.Sprintf("#%d %s", same.ID, same.Label)
			if !same.Enabled {
				row.SameAs += "（已停用）"
			}
		}
		if t := lastDeliver[a.ID]; !t.IsZero() {
			row.LastDeliver = t.UTC().Format(time.DateTime)
		}
		v.External = append(v.External, row)
	}
	sort.Slice(v.Accounts, func(i, j int) bool { return v.Accounts[i].ID < v.Accounts[j].ID })
	sort.Slice(v.External, func(i, j int) bool { return v.External[i].ID < v.External[j].ID })
	v.AccountFacets = accountFacets(v.Accounts, owners, ownerName)
	// 账号页签一号一张紧凑卡：最近取票、接入 key、九区域格子和 /u/ 那张卡同一份读数。
	hosted := make([]types.Account, 0, len(v.Accounts))
	for _, row := range v.Accounts {
		hosted = append(hosted, byID[row.ID])
	}
	cards, _ := s.ownerAccountRows(hosted, samples)
	cardOf := make(map[int64]ownerAccountRow, len(cards))
	for _, c := range cards {
		cardOf[c.ID] = c
	}
	for i := range v.Accounts {
		v.Accounts[i].Card = cardOf[v.Accounts[i].ID]
	}

	// owner 的令牌必须在**这一页**看得见：迁移出来那个 owner 的令牌没人见过，
	// 不显示就等于现有那几个账号的总览页谁都进不去。而 /a/ 那一页刻意**不**印它
	// （见 handleAccount），所以这里是唯一的找回入口。
	for _, o := range owners {
		v.Owners = append(v.Owners, ownerRow{ID: o.ID, Label: ownerLabels[o.ID], Token: o.Token,
			Accounts: ownerAccounts[o.ID], Since: o.CreatedAt.Format(time.DateTime)})
	}
	sort.Slice(v.Owners, func(i, j int) bool { return v.Owners[i].ID < v.Owners[j].ID })

	v.SlotAccounts, v.SlotVerdicts = slotSummaries(slots, lastDeliver, name, s.slotCooldown(), now)
	v.SlotTotal = len(slots)

	// 网关表顺带报它手上那张 pair 的剩余时间和铸它的账号（原来单独一个页签），
	// cookie 本体一个字都不往页面上放。
	pairs := s.st.Pairs()
	places := gatewayPlaces(samples)
	for _, g := range gws {
		exits := make([]string, 0, len(g.ReachableExits))
		for _, e := range g.ReachableExits {
			exits = append(exits, exitLabel(e))
		}
		sort.Strings(exits)
		row := adminGatewayRow{Name: g.Name, FirstSeenBy: name(g.FirstSeenBy),
			FirstSeen: g.FirstSeenAt.Format(time.DateTime), Exits: len(g.ReachableExits),
			ExitLabels: strings.Join(exits, " "), PairState: "none", Places: places[g.Name]}
		if p, ok := pairs[g.Name]; ok {
			row.PairState, row.PairLeft, row.MintedBy = "expired", "已过期", name(p.MintedBy)
			if left := p.ExpiresAt.Sub(now); left > 0 {
				row.PairState, row.PairLeft = "live", shortDur(left)
			}
		}
		v.Gateways = append(v.Gateways, row)
	}
	sort.Slice(v.Gateways, func(i, j int) bool { return v.Gateways[i].Name < v.Gateways[j].Name })

	for _, sm := range samples {
		v.Samples = append(v.Samples, adminSampleRow{
			At: sm.At.Format(time.DateTime), AccountID: sm.AccountID, Region: dash(sm.RegionKey),
			Where: geoLabel(sm), ASN: dash(sm.Observed.ASN), Gateway: dash(sm.Gateway), Status: sm.Status,
		})
	}
	reverse(v.Samples)
	v.TotalSamples = len(v.Samples)
	if len(v.Samples) > 300 {
		v.Samples = v.Samples[:300]
	}
	for i := range v.Samples {
		v.Samples[i].Account = name(v.Samples[i].AccountID)
	}
	for i := range v.Fails {
		v.Fails[i].Account = name(v.Fails[i].AccountID)
	}
	v.Started = s.started.Format(time.DateTime)
	s.render(w, s.tplAdmin, v)
}

// acctName 是管理页各表里「账号」那一格：编号 + 名字。只写编号的话几十个号得来回对照。
// 外部账号写「外部」+ 上游 ID 的头 8 个字符；0 = 没带 key 的匿名，交给模板写。
func acctName(byID map[int64]types.Account, id int64) string {
	if id == 0 {
		return ""
	}
	a, ok := byID[id]
	switch {
	case !ok:
		return fmt.Sprintf("#%d（已删除）", id)
	case a.Creds.AccessToken == "":
		ref := []rune(strings.TrimPrefix(a.Creds.AccountID, types.AnonPrefix))
		if len(ref) > 8 {
			ref = append(ref[:8], '…')
		}
		return fmt.Sprintf("#%d 外部 %s", id, string(ref))
	}
	return fmt.Sprintf("#%d %s", id, a.Label)
}

// regionsCovered 数每个账号铸出过票的区域：只数 types.Regions 那九个，和九宫格同一张台账
// （NoteRegionMint 只记成功的铸票）。
func regionsCovered(mints map[string]types.RegionMint) map[int64]int {
	known := make(map[string]bool, len(types.Regions))
	for _, r := range types.Regions {
		known[r.Key] = true
	}
	out := map[int64]int{}
	for _, m := range mints {
		if m.Count > 0 && known[m.RegionKey] {
			out[m.AccountID]++
		}
	}
	return out
}

// accountFacet 是账号表一行的筛选标签，一组一个、空格分隔：状态、套餐、区域覆盖、上传者。
// 和 accountFacets 拼出来的按钮一一对应，改一边要改另一边。
func accountFacet(a types.Account, covered int) string {
	state := "on"
	if !a.Enabled {
		state = "off"
	}
	cover := "cov-part"
	switch {
	case covered == 0:
		cover = "cov-none"
	case covered >= len(types.Regions):
		cover = "cov-all"
	}
	return fmt.Sprintf("%s %s %s o%d", state, planFacet(a.PlanType), cover, a.OwnerID)
}

// planFacet 把套餐名变成筛选标签：只留小写字母和数字，套餐名来自票里的声明，不可控。
func planFacet(plan string) string {
	key := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, strings.ToLower(plan))
	if key == "" {
		key = "unknown"
	}
	return "plan-" + key
}

// defaultOwnerLabel 是上传时现建的上传者的名字（没地方让他起名）。
const defaultOwnerLabel = "上传者"

// ownerNames 是管理页上「上传者」的显示名。现建的全叫「上传者」，十几个人分不出谁是谁，
// 就换成他名下第一个托管账号的名字（不止一个时加「等 N 个」）。只改显示，不改数据。
func ownerNames(owners []types.Owner, accounts []types.Account) map[int64]string {
	first, n := map[int64]types.Account{}, map[int64]int{}
	for _, a := range accounts {
		if a.Creds.AccessToken == "" {
			continue // 外部账号不归任何人
		}
		n[a.OwnerID]++
		if f, ok := first[a.OwnerID]; !ok || a.ID < f.ID {
			first[a.OwnerID] = a
		}
	}
	out := make(map[int64]string, len(owners))
	for _, o := range owners {
		out[o.ID] = o.Label
		if f, ok := first[o.ID]; ok && (o.Label == defaultOwnerLabel || o.Label == "") {
			out[o.ID] = f.Label
			if n[o.ID] > 1 {
				out[o.ID] += fmt.Sprintf(" 等 %d 个", n[o.ID])
			}
		}
	}
	return out
}

// accountFacets 拼账号表那几组筛选（格式见 layout.html 的 grid()；「状态=」是组名，带组名的是下拉框）。
// 套餐和上传者只列表里真有的：选下去一行都没有，等于没给。
func accountFacets(rows []accountRow, owners []types.Owner, ownerName map[int64]string) string {
	plans := map[string]string{}
	inTable := map[int64]bool{}
	for _, r := range rows {
		label := r.Plan
		if label == "" {
			label = "未知"
		}
		plans[planFacet(r.Plan)] = label
		inTable[r.OwnerID] = true
	}
	planKeys := make([]string, 0, len(plans))
	for k := range plans {
		planKeys = append(planKeys, k)
	}
	sort.Strings(planKeys)
	var planGroup []string
	for _, k := range planKeys {
		planGroup = append(planGroup, k+":"+facetLabel(plans[k]))
	}
	var ownerGroup []string
	for _, o := range owners {
		if inTable[o.ID] {
			ownerGroup = append(ownerGroup, fmt.Sprintf("o%d:%s", o.ID, facetLabel(ownerName[o.ID])))
		}
	}
	return strings.Join([]string{
		"状态=on:启用:ok|off:已停用:bad",
		"套餐=" + strings.Join(planGroup, "|"),
		"区域=cov-all:九区都铸过:ok|cov-part:没覆盖全:warn|cov-none:从没铸出过票:bad",
		"上传者=" + strings.Join(ownerGroup, "|"),
	}, ";")
}

// facetLabel 去掉会把筛选串拆坏的三个分隔符，再截短。上限按「#编号 + 64 字备注 + 等 N 个」
// 留够：截到「等 10 个」中间会变成「等 1…」，读起来是另一个数。只防票里声明的超长套餐名。
func facetLabel(s string) string {
	s = strings.NewReplacer(":", "：", "|", "｜", ";", "；").Replace(s)
	if r := []rune(s); len(r) > 80 {
		s = string(r[:80]) + "…"
	}
	return s
}

// slotSummaries 把「账号×网关」按账号汇总，每个网关的明细压成一句话（点开才看）。
// 「冷却中」只算 sched.notCooling 的第一道闸：LastDeliver 在冷却期内。页脚也只这么说。
// 第二个返回值是全部槽位最近一次校验的分布：正常 / 降级 / 未知。
func slotSummaries(all map[string]types.Slot, lastDeliver map[int64]time.Time, name func(int64) string,
	cooldown time.Duration, now time.Time) ([]slotAccountRow, [3]int) {
	slots := make([]types.Slot, 0, len(all))
	for _, sl := range all {
		slots = append(slots, sl)
	}
	sort.Slice(slots, func(i, j int) bool {
		if slots[i].AccountID != slots[j].AccountID {
			return slots[i].AccountID < slots[j].AccountID
		}
		// 网关名按自然序：先比长度再比字面，unified-14 排在 unified-105 前面。
		a, b := slots[i].Gateway, slots[j].Gateway
		if len(a) != len(b) {
			return len(a) < len(b)
		}
		return a < b
	})
	var out []slotAccountRow
	var verdicts [3]int
	for i, sl := range slots {
		if i == 0 || sl.AccountID != slots[i-1].AccountID {
			row := slotAccountRow{Account: name(sl.AccountID)}
			if t := lastDeliver[sl.AccountID]; !t.IsZero() {
				row.LastDeliver = t.UTC().Format(time.DateTime)
			}
			out = append(out, row)
		}
		row := &out[len(out)-1]
		note := []string{sl.Gateway}
		if sl.LastTouch.IsZero() {
			note = append(note, "从未用过")
		} else {
			row.Touched++
			note = append(note, ago(now, sl.LastTouch)+"用过")
		}
		if !sl.LastDeliver.IsZero() && now.Sub(sl.LastDeliver) < cooldown {
			row.Cooling++
			note = append(note, "冷却中")
		}
		if now.Before(sl.BackoffUntil) {
			row.Backoff++
			note = append(note, "退避 "+shortDur(sl.BackoffUntil.Sub(now)))
		}
		switch sl.LastVerdict {
		case types.VerdictFull:
			row.Full++
			verdicts[0]++
			note = append(note, "正常")
		case types.VerdictDegraded:
			row.Degraded++
			verdicts[1]++
			note = append(note, "降级")
		case types.VerdictUnknown:
			verdicts[2]++
			note = append(note, "未知")
		}
		row.Gateways = append(row.Gateways, strings.Join(note, " · "))
	}
	return out, verdicts
}

// gatewayPlaces 汇总「从哪些国家/城市到达过这个网关」。地理用 ipinfo 观测到的那份
// （Observed），不用请求时报的出口：请求 US 实际可能从任何一个美国城市出去，城市库存
// 不够时上游甚至换国家。Gateway 非空就是这一发真落到了这个网关（同 regionCells）。
// 城市多的国家排前面。
func gatewayPlaces(samples []types.Sample) map[string][]gatewayPlace {
	const cityUnknown = "（城市不明）" // ipinfo 没给城市；列出来，但不算进「几城」
	seen := map[string]map[string]map[string]bool{}
	for _, sm := range samples {
		cc := sm.Observed.Country
		if sm.Gateway == "" || cc == "" {
			continue
		}
		if seen[sm.Gateway] == nil {
			seen[sm.Gateway] = map[string]map[string]bool{}
		}
		if seen[sm.Gateway][cc] == nil {
			seen[sm.Gateway][cc] = map[string]bool{}
		}
		city := sm.Observed.City
		if city == "" {
			city = cityUnknown
		}
		seen[sm.Gateway][cc][city] = true
	}
	out := make(map[string][]gatewayPlace, len(seen))
	for gw, byCC := range seen {
		places := make([]gatewayPlace, 0, len(byCC))
		for cc, cities := range byCC {
			p := gatewayPlace{Country: cc}
			for c := range cities {
				p.Cities = append(p.Cities, c)
				if c != cityUnknown {
					p.Known++
				}
			}
			sort.Strings(p.Cities)
			places = append(places, p)
		}
		sort.Slice(places, func(i, j int) bool {
			if places[i].Known != places[j].Known {
				return places[i].Known > places[j].Known
			}
			return places[i].Country < places[j].Country
		})
		out[gw] = places
	}
	return out
}

// ---------- 小工具 ----------

func (s *Server) render(w http.ResponseWriter, t *template.Template, data any) {
	s.renderCode(w, t, data, http.StatusOK)
}

func (s *Server) renderCode(w http.ResponseWriter, t *template.Template, data any, code int) {
	// 先渲进 buffer：模板中途出错时不会已经吐出半个页面还带着 200。
	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, "layout", data); err != nil {
		s.opt.Log.Error("渲染失败", "err", err)
		http.Error(w, "渲染失败", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write(buf.Bytes())
}

// renderGate 出门禁页。**一律 403**：扫描器照着证书透明日志找上门，
// 给 200 等于告诉它这里有东西。wrong 只控制文案，不改状态码。
func (s *Server) renderGate(w http.ResponseWriter, wrong bool) {
	s.renderCode(w, s.tplGate, gateView{
		base:  base{Title: "需要访问码", Mode: s.pool.Mode()},
		Wrong: wrong,
	}, http.StatusForbidden)
}

// samplesTTL 是账本缓存的窗口。够短，人看不出来；够长，刷新按不垮。
const samplesTTL = 5 * time.Second

func (s *Server) samples() ([]types.Sample, error) {
	s.smu.Lock()
	defer s.smu.Unlock()
	if s.sm != nil && time.Since(s.smAt) < samplesTTL {
		return s.sm, nil
	}
	sm, err := s.st.Samples()
	if err != nil {
		return nil, err
	}
	s.sm, s.smAt = sm, time.Now()
	return sm, nil
}

// writeJSON 的入参只能是本包自己的视图结构体或 map。
//
// **绝不要往里塞 types.Account**：types.Creds 封住了打印路径（String/GoString/
// LogValue），但刻意没有 MarshalJSON——state.json 要靠 json 原样存回 token，
// 打码的 MarshalJSON 会让凭据存成 "***"、重启后全部账号失效。
// 代价就是 json.Marshal 一个 Account 会把 access_token 带出去，这道口子只能在这里守。
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// validName 校验网关名这种会被当成 map key 和账本字段的外部输入。
func validName(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

// sanitizeLabel 把上传者自填的标签压成一行短文本。模板会做 HTML 转义，
// 这里只管别让控制字符和超长串进账本。
func sanitizeLabel(v, fallback string) string {
	clean := func(s string) string {
		return strings.TrimSpace(strings.Map(func(r rune) rune {
			if r < 0x20 || r == 0x7f {
				return -1
			}
			return r
		}, s))
	}
	// 兜底值也要过一遍：它可能来自票里的声明（邮箱），而票不验签。
	if v = clean(v); v == "" {
		v = clean(fallback)
	}
	// 按**字符**截，不按字节：`v[:64]` 会把一个汉字劈成半个，落盘就是坏字节。
	// 前端那个 maxlength 数的是 UTF-16 码元，和这里不是同一个口径（emoji 会被它
	// 砍到 32 个）——方向是前端更严，所以无害，别指望两边数出同一个值。
	if r := []rune(v); len(r) > labelMaxRunes {
		v = string(r[:labelMaxRunes])
	}
	return v
}

// labelMaxRunes 是备注/套餐名的长度上限，单位是**字符**（rune），服务端说了算。
const labelMaxRunes = 64

// geoLabel 按**观测到的**地理显示，不按请求的 region：城市库存不足时上游会
// 回退到别的国家，按请求显示就是假账（SPEC 红线 6）。
func geoLabel(sm types.Sample) string {
	cc, city := sm.Observed.Country, sm.Observed.City
	switch {
	case cc == "" && city == "":
		return "—"
	case city == "":
		return cc
	default:
		return cc + "/" + city
	}
}

func exitLabel(e types.Exit) string {
	if e.City == "" {
		return e.Region
	}
	return e.Region + ":" + e.City
}

func dash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

func shortDur(d time.Duration) string {
	if d <= 0 {
		return "已过期"
	}
	if d < time.Minute {
		return fmt.Sprintf("%d 秒", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%d 分钟", int(d.Minutes()))
	}
	return fmt.Sprintf("%.1f 小时", d.Hours())
}

func reverse[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
