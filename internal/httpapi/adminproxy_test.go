package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gwpool/internal/config"
	"gwpool/internal/mint"
	"gwpool/internal/types"
)

const proxyCfg = `{"mode":"pool","listen":"127.0.0.1:1","admin_key":"ak","consumer_key":"ck","models":["m1"],
	"proxies":[{"name":"nov","kind":"novproxy","host":"h:1","user":"u","pass":"p-secret","enabled":true},
	{"name":"ws","kind":"webshare","host":"w:2","user":"wu","pass":"w-secret","enabled":true}]}`

type proxyCalls struct {
	probed, added []config.Proxy
	toggled       []string
	probeErr      error
}

// proxyRig 在标准 rig 上接一份真的配置文件，和三个记账的假出口操作。
func proxyRig(t *testing.T) (http.Handler, *Server, string, *proxyCalls) {
	t.Helper()
	srv, h, _, _, _ := rig(t, types.ModePool)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(proxyCfg), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	calls := &proxyCalls{}
	srv.opt.Live = config.NewLive(path, cfg)
	srv.opt.ProbeProxy = func(_ context.Context, p config.Proxy) (types.Observed, error) {
		calls.probed = append(calls.probed, p)
		if calls.probeErr != nil {
			return types.Observed{}, calls.probeErr
		}
		return types.Observed{IP: "203.0.113.9", Country: "US", City: "Los Angeles"}, nil
	}
	srv.opt.AddProxy = func(p config.Proxy) error { calls.added = append(calls.added, p); return nil }
	srv.opt.SetProxyEnabled = func(name string, on bool) error {
		calls.toggled = append(calls.toggled, name+map[bool]string{true: "=on", false: "=off"}[on])
		return nil
	}
	srv.opt.ProxyStates = func() []mint.ProxyState {
		return []mint.ProxyState{
			{Name: "nov", Kind: "novproxy", Host: "h:1", UserMask: "***", Enabled: true, Sticky: true},
			{Name: "ws", Kind: "webshare", Host: "w:2", UserMask: "***", Enabled: false, Sticky: true},
		}
	}
	return h, srv, path, calls
}

func postForm(h http.Handler, path string, form url.Values) *httptest.ResponseRecorder {
	return do(h, authed(httptest.NewRequest("POST", path, strings.NewReader(form.Encode())), "akey"))
}

func addForm(name string) url.Values {
	return url.Values{"name": {name}, "kind": {"novproxy"}, "host": {"us.novproxy.io:1000"},
		"user": {"user-3"}, "pass": {"pass3-secret"}}
}

func fileText(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// 加代理：先试通，通了写进配置文件（原有口令不动）、热重载、加进运行中的轮转；
// 跳回去的地址里有出口 IP，没有用户名和密码。
func TestAdminAddProxy(t *testing.T) {
	h, srv, path, calls := proxyRig(t)
	w := postForm(h, "/admin/proxies", addForm("nov3"))
	loc := w.Header().Get("Location")
	if w.Code != http.StatusSeeOther || !strings.Contains(loc, "proxy_note=") || !strings.HasSuffix(loc, "#proxies") {
		t.Fatalf("没跳回代理那一节：%d %q", w.Code, loc)
	}
	if !strings.Contains(loc, "203.0.113.9") || strings.Contains(loc, "pass3") || strings.Contains(loc, "user-3") {
		t.Fatalf("跳转地址不对，或者带了凭据：%q", loc)
	}
	if len(calls.probed) != 1 || len(calls.added) != 1 || calls.added[0].Pass.Reveal() != "pass3-secret" ||
		calls.added[0].Name != "nov3" || !calls.added[0].Enabled {
		t.Fatalf("试通 / 加进轮转不对：%+v", calls)
	}
	text := fileText(t, path)
	for _, want := range []string{`"nov3"`, `"pass3-secret"`, `"p-secret"`, `"w-secret"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("配置文件里缺了 %s：\n%s", want, text)
		}
	}
	if ps := srv.opt.Live.Get().Proxies; len(ps) != 3 || ps[2].Name != "nov3" {
		t.Fatalf("没热重载：%+v", ps)
	}
}

// 试不通就什么都不存、不进轮转，原因显示出来。
func TestAdminAddProxyProbeFails(t *testing.T) {
	h, _, path, calls := proxyRig(t)
	before := fileText(t, path)
	calls.probeErr = errors.New("代理认证失败")
	loc := postForm(h, "/admin/proxies", addForm("nov3")).Header().Get("Location")
	if !strings.Contains(loc, "proxy_err=") || !strings.Contains(loc, url.QueryEscape("代理认证失败")) {
		t.Fatalf("没报原因：%q", loc)
	}
	if len(calls.added) != 0 || fileText(t, path) != before {
		t.Fatal("没试通也存了")
	}
}

func TestAdminAddStaticProxiesCanCoexist(t *testing.T) {
	h, srv, _, calls := proxyRig(t)

	f := addForm("bv-seed")
	f.Set("kind", "seed")
	f.Set("host", "127.0.0.1:35444")
	if loc := postForm(h, "/admin/proxies", f).Header().Get("Location"); !strings.Contains(loc, "proxy_note=") {
		t.Fatalf("第一条种子出口该能加，跳到了 %q", loc)
	}
	if len(calls.added) != 1 || calls.added[0].Kind != config.KindSeed {
		t.Fatalf("没把种子出口加进运行中的清单：%+v", calls.added)
	}
	// 热重载保留静态出口，运行时由独立静态轮询选择。
	if p, ok := srv.opt.Live.Get().SeedProxy(); !ok || p.Name != "bv-seed" {
		t.Fatalf("热重载后 SeedProxy 没找到它：%+v ok=%v", p, ok)
	}

	f2 := addForm("bv-seed-2")
	f2.Set("kind", "seed")
	f2.Set("host", "127.0.0.1:35445")
	if loc := postForm(h, "/admin/proxies", f2).Header().Get("Location"); !strings.Contains(loc, "proxy_note=") {
		t.Fatalf("第二条静态出口应能共存，跳到了 %q", loc)
	}
	if len(calls.added) != 2 {
		t.Fatalf("静态出口没有全部加入运行中：%+v", calls.added)
	}
}

// 表单是系统边界：不合法的连试都不试。
func TestAdminAddProxyValidates(t *testing.T) {
	h, _, path, calls := proxyRig(t)
	before := fileText(t, path)
	for name, mutate := range map[string]func(url.Values){
		"重名":        func(f url.Values) { f.Set("name", "nov") },
		"不认的类型":     func(f url.Values) { f.Set("kind", "socks5") },
		"带协议头的地址":   func(f url.Values) { f.Set("host", "socks5://h:1") },
		"带用户名的地址":   func(f url.Values) { f.Set("host", "user@h:1") },
		"没端口":       func(f url.Values) { f.Set("host", "h") },
		"空密码":       func(f url.Values) { f.Set("pass", " ") },
		"名称带控制字符":   func(f url.Values) { f.Set("name", "a\x01b") },
		"用户名太长":     func(f url.Values) { f.Set("user", strings.Repeat("u", 201)) },
		"名称超过32个字":  func(f url.Values) { f.Set("name", strings.Repeat("名", 33)) },
		"密码超过255字节": func(f url.Values) { f.Set("pass", strings.Repeat("p", 256)) },
		"端口为0":      func(f url.Values) { f.Set("host", "h:0") },
		"端口超范围":     func(f url.Values) { f.Set("host", "h:65536") },
		"用户名带控制字符":  func(f url.Values) { f.Set("user", "u\x7fx") },
		"密码带控制字符":   func(f url.Values) { f.Set("pass", "p\nx") },
		"名称不是UTF8":  func(f url.Values) { f.Set("name", "a\xffb") },
	} {
		f := addForm("nov3")
		mutate(f)
		if loc := postForm(h, "/admin/proxies", f).Header().Get("Location"); !strings.Contains(loc, "proxy_err=") {
			t.Fatalf("%s：没拒，跳到了 %q", name, loc)
		}
	}
	if len(calls.probed) != 0 || len(calls.added) != 0 || fileText(t, path) != before {
		t.Fatalf("不合法的表单也去试通或者存了：%+v", calls)
	}
}

// 写文件失败（这里是有人刚往文件里手加了同名的一条、还没热重载）就不许动运行中的轮转：
// 不然运行中多出一条文件里没有的，重启就没了。
func TestAdminAddProxyFileFailureLeavesRuntimeAlone(t *testing.T) {
	h, _, path, calls := proxyRig(t)
	if err := config.AppendProxy(path, config.Proxy{Name: "nov3", Kind: config.KindNovproxy,
		Host: "h:9", User: "x", Pass: "y", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	loc := postForm(h, "/admin/proxies", addForm("nov3")).Header().Get("Location")
	if !strings.Contains(loc, "proxy_err=") || len(calls.added) != 0 {
		t.Fatalf("文件没写成却加进了运行中：%q %+v", loc, calls.added)
	}
}

// 运行中的清单和文件对不上（有人手改了文件没重启）时，按运行中的再查一遍：
// 文件里 ws 是启用的，可运行中 ws 停着，停掉 nov 就一个出口都不剩。
func TestAdminProxyDisableChecksRuntime(t *testing.T) {
	h, _, path, calls := proxyRig(t)
	before := fileText(t, path)
	loc := postForm(h, "/admin/proxies/enabled", url.Values{"name": {"nov"}, "enabled": {"0"}}).Header().Get("Location")
	if !strings.Contains(loc, "proxy_err=") || !strings.Contains(loc, url.QueryEscape("运行中")) {
		t.Fatalf("运行中最后一条粘性代理也停掉了：%q", loc)
	}
	if fileText(t, path) != before || len(calls.toggled) != 0 {
		t.Fatalf("拒掉了还动了东西：%v", calls.toggled)
	}
}

// 停用 / 启用：先写文件再改运行中的；停掉最后一条粘性代理被配置校验拒掉时，运行中的不许动。
func TestAdminProxyEnabled(t *testing.T) {
	h, srv, path, calls := proxyRig(t)
	loc := postForm(h, "/admin/proxies/enabled", url.Values{"name": {"ws"}, "enabled": {"0"}}).Header().Get("Location")
	if !strings.Contains(loc, "proxy_note=") {
		t.Fatalf("停用没成功：%q", loc)
	}
	if ps := srv.opt.Live.Get().Proxies; ps[1].Enabled {
		t.Fatal("配置里还是启用的")
	}
	before := fileText(t, path)
	loc = postForm(h, "/admin/proxies/enabled", url.Values{"name": {"nov"}, "enabled": {"0"}}).Header().Get("Location")
	if !strings.Contains(loc, "proxy_err=") || fileText(t, path) != before {
		t.Fatalf("最后一条粘性代理也停掉了：%q", loc)
	}
	if loc = postForm(h, "/admin/proxies/enabled", url.Values{"name": {"nope"}, "enabled": {"1"}}).Header().Get("Location"); !strings.Contains(loc, "proxy_err=") {
		t.Fatalf("不存在的名字没报错：%q", loc)
	}
	if strings.Join(calls.toggled, ",") != "ws=off" {
		t.Fatalf("运行中的改动不对：%v", calls.toggled)
	}
	for _, bad := range []url.Values{{"name": {"ws"}, "enabled": {"true"}}, {"enabled": {"1"}}} {
		if w := postForm(h, "/admin/proxies/enabled", bad); w.Code != http.StatusBadRequest {
			t.Fatalf("%v 应该 400，得到 %d", bad, w.Code)
		}
	}
}

// 鉴权、同源、体积上限；没接配置文件的实例改不了，页面上也没有表单和按钮。
func TestAdminProxyGuards(t *testing.T) {
	h, _, _, calls := proxyRig(t)
	for _, path := range []string{"/admin/proxies", "/admin/proxies/enabled"} {
		if w := do(h, httptest.NewRequest("POST", path, strings.NewReader("name=x"))); w.Code != http.StatusUnauthorized {
			t.Fatalf("%s 没鉴权：%d", path, w.Code)
		}
		r := authed(httptest.NewRequest("POST", path, strings.NewReader("name=x")), "akey")
		r.Header.Set("Sec-Fetch-Site", "cross-site")
		if w := do(h, r); w.Code != http.StatusForbidden {
			t.Fatalf("%s 跨站没拒：%d", path, w.Code)
		}
		big := url.Values{"name": {strings.Repeat("x", 5000)}}
		if w := postForm(h, path, big); w.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s 没限体积：%d", path, w.Code)
		}
	}
	if len(calls.probed)+len(calls.added)+len(calls.toggled) != 0 {
		t.Fatalf("被拒的请求动了东西：%+v", calls)
	}

	// 只读实例：有代理行（接了状态），但没接配置文件和那三个操作。
	plainSrv, plain, _, _, _ := rig(t, types.ModePool)
	plainSrv.opt.ProxyStates = func() []mint.ProxyState {
		return []mint.ProxyState{{Name: "nov", Kind: "novproxy", Host: "h:1", UserMask: "***", Enabled: true, Sticky: true}}
	}
	if w := postForm(plain, "/admin/proxies", addForm("x")); w.Code != http.StatusNotImplemented {
		t.Fatalf("没接配置文件也能加：%d", w.Code)
	}
	body := do(plain, authed(httptest.NewRequest("GET", "/admin/", nil), "akey")).Body.String()
	if !strings.Contains(body, `<td class="mono text">h:1</td>`) || strings.Contains(body, `action="/admin/proxies`) {
		t.Fatal("只读实例：代理行没出来，或者出现了添加表单 / 停用按钮")
	}
}

// 页面：有添加表单、每行一个停用 / 启用按钮（隐藏值对上状态），回话显示出来，口令不上页面。
func TestAdminPageProxyControls(t *testing.T) {
	h, _, _, _ := proxyRig(t)
	body := do(h, authed(httptest.NewRequest("GET", "/admin/?proxy_note="+url.QueryEscape("已添加 nov3"), nil), "akey")).Body.String()
	for _, want := range []string{
		`动态探索与新铸出口`,
		`静态续票出口`,
		`id="t-proxies-dynamic"`,
		`<input type="hidden" name="kind" value="seed">`,
		`<option value="cliproxy">`,
		`<option value="ipipgo">`,
		`action="/admin/proxies"`,
		`name="pass" type="password" required autocomplete="new-password"`,
		`<input type="hidden" name="name" value="nov"><input type="hidden" name="enabled" value="0">`,
		`<input type="hidden" name="name" value="ws"><input type="hidden" name="enabled" value="1">`,
		`<div class="note info"><p>已添加 nov3</p></div>`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("管理页缺了 %q", want)
		}
	}
	if strings.Contains(body, "p-secret") || strings.Contains(body, "w-secret") {
		t.Fatal("代理口令上了页面")
	}
}

func TestAdminProxyPoolsStaySeparate(t *testing.T) {
	h, srv, _, _ := proxyRig(t)
	old := srv.opt.ProxyStates
	srv.opt.ProxyStates = func() []mint.ProxyState {
		return append(old(), mint.ProxyState{Name: "static-only", Kind: "seed", Host: "127.0.0.1:9",
			UserMask: "***", Enabled: true})
	}
	body := do(h, authed(httptest.NewRequest("GET", "/admin/", nil), "akey")).Body.String()
	dynamic := strings.Index(body, `id="t-proxies-dynamic"`)
	renew := strings.Index(body, `id="t-proxies-renew"`)
	staticRow := strings.Index(body, `<td>static-only</td>`)
	if dynamic < 0 || renew <= dynamic || staticRow <= renew ||
		strings.Contains(body[dynamic:renew], `<td>static-only</td>`) {
		t.Fatal("动态和静态出口没有分成独立列表")
	}
}
