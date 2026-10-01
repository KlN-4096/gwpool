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
	// DeliverAnonymous 给没上传凭据的人交付。upstreamAccountID 是他自己的上游账号标识，
	// 槽位照常按它记——调度单位永远是 (账号 × 网关)。
	DeliverAnonymous(ctx context.Context, upstreamAccountID string, req sched.Request) (types.Delivery, error)
	// Release 还原一次「票取了但一发没打」的交付，返回还原了几个槽位。
	Release(accountID int64, cookieVersion string) (int, error)
	// HasCandidate 是 wait 轮询的判据：此刻有没有一张**这个账号**拿得到、而且不用铸
	// 的票。判据必须和交付是同一把尺子，所以归调度（它才看得见冷却），HTTP 这层
	// 绝不自己算一份。
	HasCandidate(accountID int64, req sched.Request) bool
	// Public 是此刻的对外开放状态，现算不缓存。
	Public() sched.PublicState
	// PairTarget 是目标活 pair 数，只给 /gateways 的读数用。
	PairTarget() int
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
	// SetAccountEnabled 只翻一个 bool。给上传者自己停用/启用用。
	SetAccountEnabled(id int64, enabled bool) error

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
	// 实现方负责挑人（按 (账号 × 区域) 台账）和限流；没人可派就什么都不做。
	EnqueueDiscovery(exclude int64)
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

	// Refresh 换票。nil = 用 oauth.Refresh 直连上游。
	// 存在的唯一理由是测试：测试绝不打真实上游（SPEC 红线 8），而换票是
	// parseCreds 里唯一会出网的一步。生产永远不设它。
	Refresh func(ctx context.Context, refreshToken string) (access, newRefresh string, err error)

	// Live 非 nil 时管理页可以改配置并热重载；为空则那一节只读（测试走这条路）。
	Live *config.Live

	// Model 是铸票那一发用的模型，照 config.Model 传进来。
	// 上传者视图要向托管者如实交代用了他的凭据打什么，所以不能写死。
	Model string

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

	tplPool, tplAccount, tplAdmin, tplGate *template.Template
}

// probeEffort 必须跟 mint 包实际发出去的 payload 一致（SPEC 第 2 节）：
// 上传者视图拿它向托管者交代「我们只用你的凭据打了 ping」。
// 模型不是常量——它由 config.Model 决定，走 Options.Model 传进来，否则页面上
// 那句交代就是假的（这正好是「必须一致」的意思）。
const probeEffort = "low"

// probeModel 是页面上要向托管者交代的模型名。
func (s *Server) probeModel() string {
	if s.opt.Model != "" {
		return s.opt.Model
	}
	return "(未配置)"
}

// restedThreshold 只是公开页上「歇着的槽位」这个人眼读数的门槛。
// (账号 × 网关) 的真实冷却时长至今没测准（≥4 h 只是没验过的估算），
// 所以这个数不参与任何调度决策，纯展示。
const restedThreshold = time.Hour

func New(p Pool, st Store, sc Scanner, opt Options) (*Server, error) {
	if opt.Log == nil {
		opt.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s := &Server{pool: p, st: st, scan: sc, opt: opt}
	var err error
	// 每个页面单独和 layout 一起解析：三份 "content" 定义放一个集合里会撞名。
	if s.tplPool, err = parse("pool.html"); err != nil {
		return nil, err
	}
	if s.tplAccount, err = parse("account.html"); err != nil {
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
	mux.Handle("GET /{$}", s.gate(http.HandlerFunc(s.handlePool)))
	mux.Handle("POST /upload", s.gate(http.HandlerFunc(s.handleUpload)))
	mux.Handle("GET /a/{token}", s.gate(http.HandlerFunc(s.handleAccount)))
	mux.Handle("POST /a/{token}/enabled", s.gate(http.HandlerFunc(s.handleAccountEnabled)))
	mux.HandleFunc("GET /cookie", s.handleCookie)
	// /release 和 /cookie 同一把钥匙、同一套 account 语义（见 handleRelease）。
	mux.HandleFunc("POST /release", s.handleRelease)
	mux.HandleFunc("GET /gateways", s.handleGateways)
	mux.HandleFunc("POST /admin/rescan", s.handleAdminRescan)
	mux.HandleFunc("POST /admin/accounts/{id}/delete", s.handleAdminDeleteAccount)
	mux.HandleFunc("POST /admin/config", s.handleAdminConfig)
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
	fmt.Fprintf(m, "gwpool-consumer-key|%d|%s", a.ID, a.Creds.AccountID)
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
	// 按值建索引，不去拼 "<id>|<网关>"：那个键格式是存储层的事，抄一份迟早对不上。
	slots := map[int64]map[string]types.Slot{}
	for _, sl := range s.st.Slots() {
		if slots[sl.AccountID] == nil {
			slots[sl.AccountID] = map[string]types.Slot{}
		}
		slots[sl.AccountID][sl.Gateway] = sl
	}

	// 只数**托管**账号：匿名调用方的占位行（sched.anonAccount）没有凭据，既铸不了票
	// 也不是谁托管的。算进来的话这张对外的页面上「托管账号」会虚高一大截，
	// 「歇着的槽位」也会多出一堆永远用不上的槽。
	hosted := accounts[:0:0]
	for _, a := range accounts {
		if a.Creds.AccessToken != "" {
			hosted = append(hosted, a)
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
		for _, a := range hosted {
			if !a.Enabled {
				continue
			}
			sl, seen := slots[a.ID][name]
			if seen && (now.Before(sl.BackoffUntil) || now.Sub(sl.LastTouch) < restedThreshold) {
				continue
			}
			row.RestedSlots++
		}
		v.Gateways = append(v.Gateways, row)
	}
	sort.Slice(v.Gateways, func(i, j int) bool { return v.Gateways[i].Name < v.Gateways[j].Name })
	for _, g := range v.Gateways {
		if g.PairReady {
			v.ReadyCount++
		}
	}
	ps := s.pool.Public()
	v.PublicEnabled, v.PublicOpen = ps.Enabled, ps.Open
	v.FreeGateways, v.Threshold = ps.Free, ps.Threshold
	v.Accounts = len(hosted)
	v.RestedLabel = shortDur(restedThreshold)
	v.Model, v.Effort = s.probeModel(), probeEffort
	s.render(w, s.tplPool, v)
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
// 返回的第二个值是 token 自己写的 plan（free/plus/pro/…），空表示票里没这个声明。
func (s *Server) parseCreds(ctx context.Context, raw []byte) (types.Creds, string, error) {
	var f authUpload
	if err := json.Unmarshal(raw, &f); err != nil {
		return types.Creds{}, "", errors.New("不是合法 JSON")
	}
	access, refresh, account := f.pick()
	if access == "" && refresh == "" {
		return types.Creds{}, "", errors.New("access_token 和 refresh_token 至少要有一个")
	}
	if refresh != "" && (len(refresh) < 20 || hasBlank(refresh)) {
		return types.Creds{}, "", errors.New("refresh_token 形状不对")
	}
	// 只给了 refresh_token：当场换一张。换不出来就别建号——建了也是个废号，
	// 第一次扫描才发现，白打一轮。
	if access == "" {
		got, newRefresh, err := s.refresh(ctx, refresh)
		if err != nil {
			return types.Creds{}, "", fmt.Errorf("用 refresh_token 换 access_token 失败：%w", err)
		}
		access = got
		if newRefresh != "" {
			refresh = newRefresh
		}
	}
	if len(access) < 20 || hasBlank(access) {
		return types.Creds{}, "", errors.New("access_token 形状不对")
	}
	if len(account) > 128 || hasBlank(account) {
		return types.Creds{}, "", errors.New("account_id 形状不对")
	}

	cl, err := oauth.Parse(access)
	if err != nil {
		return types.Creds{}, "", err
	}
	switch {
	case account == "" && cl.AccountID == "":
		return types.Creds{}, "", errors.New("票里没有 chatgpt_account_id，得自己贴一个 account_id")
	case account == "":
		account = cl.AccountID
	case cl.AccountID != "" && cl.AccountID != account:
		return types.Creds{}, "", fmt.Errorf(
			"account_id 和 access_token 里写的不是同一个账号（票里是 %s）——八成是两处凭据贴串了",
			cl.AccountID)
	}
	if !cl.Expires.IsZero() && time.Now().After(cl.Expires) {
		if refresh == "" {
			return types.Creds{}, "", errors.New("access_token 已经过期了，换一张新的（或者贴 refresh_token）")
		}
		// 有 refresh 就自己续，不必让人回去重新导出。
		got, newRefresh, rerr := s.refresh(ctx, refresh)
		if rerr != nil {
			return types.Creds{}, "", fmt.Errorf("access_token 过期且换票失败：%w", rerr)
		}
		access = got
		if newRefresh != "" {
			refresh = newRefresh
		}
		if cl, err = oauth.Parse(access); err != nil {
			return types.Creds{}, "", err
		}
	}
	return types.Creds{AccessToken: access, AccountID: account, RefreshToken: refresh}, cl.Plan, nil
}

// wrapBareToken 把「光一个 token」包成 parseCreds 认的 JSON。
//
// 不是所有人都愿意把整份 auth.json 交出来（里面还有 id_token 和别的东西），
// 所以单贴一个 token 也要收。已经是 JSON 的原样放过。
//
// kind 来自上传页的单选：auto 时按形状猜——access_token 是 JWT（三段、点分隔），
// refresh_token 不是，这个区分是可靠的，不用猜第二次。
func wrapBareToken(raw []byte, kind string) []byte {
	if len(raw) == 0 || raw[0] == '{' {
		return raw
	}
	tok, err := json.Marshal(string(raw))
	if err != nil { // 只会在非法 UTF-8 时发生，交给 parseCreds 去报「不是合法 JSON」
		return raw
	}
	field := "refresh_token"
	switch kind {
	case "access":
		field = "access_token"
	case "refresh":
	default: // auto
		if len(strings.Split(string(raw), ".")) == 3 {
			field = "access_token"
		}
	}
	return []byte(`{"` + field + `":` + string(tok) + `}`)
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
		raw = wrapBareToken(raw, form.Get("kind"))
	}
	creds, tokenPlan, err := s.parseCreds(r.Context(), raw)
	if err != nil {
		http.Error(w, "凭据有问题："+err.Error(), http.StatusBadRequest)
		return
	}
	// token 自己写的 plan 压过表单里手填的那个：手填的只是个标签，票里的是事实，
	// 而「这票是 free」正是会让整次扫描全军覆没的那件事，得在页面上看得见。
	if tokenPlan != "" {
		plan = tokenPlan
	}

	// 上传即 both：产品规则是「池子的目的是共享 pair，上传了凭据的就可以请求到 pair」。
	// 没有角色之分，也就没有「谁能取 pair」这个问题。
	// view_token 留空，由存储层生成（它负责保证不重样），这边只负责交到上传者手里。
	a, err := s.st.AddAccount(types.Account{
		Label:     sanitizeLabel(label, creds.AccountID),
		PlanType:  sanitizeLabel(plan, "unknown"),
		Creds:     creds,
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		s.opt.Log.Error("建账号失败", "err", err)
		http.Error(w, "存储失败", http.StatusInternalServerError)
		return
	}
	// **不排首扫**：新账号进来时没有任何需求，那九发上游纯属白花，而且会把它在
	// 九个落点上的首次接触全烧掉。等有人要票时按需去找落点（见 Scanner 的注释）。
	s.opt.Log.Info("收到新账号", "account", a.ID, "label", a.Label)

	if wantJSON || strings.Contains(r.Header.Get("Accept"), "json") {
		writeJSON(w, http.StatusOK, map[string]string{
			"view_token":   a.ViewToken,
			"url":          "/a/" + a.ViewToken,
			"consumer_key": s.ConsumerKeyFor(a),
		})
		return
	}
	http.Redirect(w, r, "/a/"+a.ViewToken, http.StatusSeeOther)
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
	}
	for _, g := range s.st.Gateways() {
		if g.FirstSeenBy == a.ID {
			v.NewGateways++
		}
	}
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

// ---------- /cookie ----------

func (s *Server) handleCookie(w http.ResponseWriter, r *http.Request) {
	req, wait, perr := parseCookieRequest(r)
	if perr != nil {
		badRequest(w, http.StatusBadRequest, perr.Error())
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
	if ref != "" && !accountRefMatches(acct, ref) {
		// 记账归上传者（他的页面该看见这一行），**调度归占位行**：轮询要按那个号的
		// 冷却判，发现扫描要排除的也是那个号——而池内行里只有上传者。
		s.serveCookie(w, r, acct.ID, s.rowFor(types.AnonPrefix+ref), req, wait,
			func(rq sched.Request) (types.Delivery, error) {
				return s.pool.DeliverAnonymous(r.Context(), ref, rq)
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
		s.publicClosed(w, fmt.Sprintf("池子暂不对外开放：空闲 %d 个网关，门槛 %d", ps.Free, ps.Threshold))
		return
	}
	// 必须报上他**自己的**上游账号标识：槽位是 (账号 × 网关)，不知道是谁就没法
	// 避开他已经烧过的网关。报错的只会害他自己，所以不防乱填。
	ref := strings.TrimSpace(r.URL.Query().Get("account"))
	if ref == "" || len(ref) > 128 {
		badRequest(w, http.StatusBadRequest, "匿名取用必须带 account 参数（你自己的上游 account_id）")
		return
	}
	s.serveCookie(w, r, 0, s.rowFor(types.AnonPrefix+ref), req, wait,
		func(rq sched.Request) (types.Delivery, error) {
			return s.pool.DeliverAnonymous(r.Context(), ref, rq)
		})
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
}

func delivered(d types.Delivery) deliveryResponse {
	return deliveryResponse{Delivery: d, TTLIsAdvisory: true,
		CookieVersion: types.CookieVersion(d.Cookie)}
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
	s.fails.add(deliverFail{At: time.Now(), AccountID: acctID, Gateway: gateway,
		Code: code, Reason: msg, Detail: err.Error()})
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
	s.scan.EnqueueDiscovery(forWhom)
	writeJSON(w, http.StatusAccepted, map[string]any{"account": forWhom, "queued": true})
}

// handleAdminDeleteAccount 删掉一个账号。管理页上的按钮走这里。
//
// 为什么管理员要有这个而上传者只有「停用」：无效凭据（贴错的、过期的、free 的）
// 是运维要清掉的垃圾，而上传者那边的语义是「我先不参与了」，两件事不一样。
//
// 表单提交 ⇒ 303 回管理页；带 Accept: json 的（curl、脚本）给 JSON。
func (s *Server) handleAdminDeleteAccount(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "账号 ID 不是正整数"})
		return
	}
	if err := s.st.DeleteAccount(id); err != nil {
		s.opt.Log.Warn("删账号失败", "account", id, "err", err)
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	s.opt.Log.Info("删了账号", "account", id)
	if strings.Contains(r.Header.Get("Accept"), "json") {
		writeJSON(w, http.StatusOK, map[string]any{"account": id, "deleted": true})
		return
	}
	http.Redirect(w, r, "/admin/", http.StatusSeeOther)
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
	found := map[int64]int{}
	for _, g := range s.st.Gateways() {
		found[g.FirstSeenBy]++
	}
	for _, a := range s.st.Accounts() {
		v.Accounts = append(v.Accounts, accountRow{
			ID: a.ID, Label: a.Label, Plan: a.PlanType,
			Enabled: a.Enabled, DisabledReason: a.DisabledReason,
			Requests: reqs[a.ID], Found: found[a.ID],
			ViewToken: a.ViewToken, Since: a.CreatedAt.Format(time.DateTime),
			HasCreds: a.Creds.AccessToken != "", Upstream: a.Creds.AccountID,
		})
	}
	sort.Slice(v.Accounts, func(i, j int) bool { return v.Accounts[i].ID < v.Accounts[j].ID })

	// pair 只报剩余时间和铸它的账号，cookie 本体一个字都不往页面上放。
	for name, p := range s.st.Pairs() {
		v.Pairs = append(v.Pairs, pairRow{Gateway: name, Left: shortDur(p.ExpiresAt.Sub(now)), MintedBy: p.MintedBy})
	}
	sort.Slice(v.Pairs, func(i, j int) bool { return v.Pairs[i].Gateway < v.Pairs[j].Gateway })

	for _, sl := range s.st.Slots() {
		row := slotRow{AccountID: sl.AccountID, Gateway: sl.Gateway, Verdict: sl.LastVerdict,
			Rested: shortDur(now.Sub(sl.LastTouch))}
		if sl.LastTouch.IsZero() {
			row.Rested = "从未"
		}
		if now.Before(sl.BackoffUntil) {
			row.Backoff = shortDur(sl.BackoffUntil.Sub(now))
		}
		v.Slots = append(v.Slots, row)
	}
	sort.Slice(v.Slots, func(i, j int) bool {
		if v.Slots[i].AccountID != v.Slots[j].AccountID {
			return v.Slots[i].AccountID < v.Slots[j].AccountID
		}
		return v.Slots[i].Gateway < v.Slots[j].Gateway
	})

	for _, g := range s.st.Gateways() {
		exits := make([]string, 0, len(g.ReachableExits))
		for _, e := range g.ReachableExits {
			exits = append(exits, exitLabel(e))
		}
		sort.Strings(exits)
		v.Gateways = append(v.Gateways, adminGatewayRow{Name: g.Name, FirstSeenBy: g.FirstSeenBy,
			FirstSeen: g.FirstSeenAt.Format(time.DateTime), Exits: strings.Join(exits, " ")})
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
	s.render(w, s.tplAdmin, v)
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
	v = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, strings.TrimSpace(v))
	if v == "" {
		v = fallback
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
