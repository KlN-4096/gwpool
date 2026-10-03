package httpapi

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"gwpool/internal/config"
)

// configWriteMu 串起本进程里所有「读配置文件 → 改 → 写回」：存旋钮、加代理、停用代理。
// Patch 自己就是「读整份 → 改几个键 → 写整份」，两个并发的写者会吃掉对方的改动。
var configWriteMu sync.Mutex

// POST /admin/proxies：加一条出口代理。
//
// 先经它查一次 ipinfo（不是上游，不花额度），通了才写进配置文件、加进运行中的轮转，
// 不用重启。只往末尾加：sid 绑的是代理在清单里的下标，插到中间会让已经发出去的 sid
// 串到别的出口上。口令只进配置文件，页面、跳转地址、日志里都不出现。
func (s *Server) handleAdminAddProxy(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) || !s.proxyEditable(w) {
		return
	}
	form, ok := readAdminForm(w, r)
	if !ok {
		return
	}
	p, err := proxyFromForm(form, s.opt.Live.Get())
	if err != nil {
		s.adminProxyDone(w, r, "", err.Error())
		return
	}
	// 试通在锁外：住宅代理一发能卡二十来秒，别让存旋钮的人跟着等。
	obs, err := s.opt.ProbeProxy(r.Context(), p)
	if err != nil {
		s.opt.Log.Warn("新出口代理没连通，没加", "name", p.Name, "kind", string(p.Kind), "host", p.Host, "err", err)
		s.adminProxyDone(w, r, "", "没连通，没有保存："+err.Error())
		return
	}
	configWriteMu.Lock()
	defer configWriteMu.Unlock()
	// 先写文件、写成了才动运行中的：反过来的话写失败时运行中多出一条文件里没有的，重启就没了。
	if err := config.AppendProxy(s.opt.Live.Path(), p); err != nil {
		s.opt.Log.Warn("新出口代理没存进配置文件", "name", p.Name, "err", err)
		s.adminProxyDone(w, r, "", "没存进配置文件："+err.Error())
		return
	}
	if _, err := s.opt.Live.Reload(); err != nil {
		s.opt.Log.Warn("加完代理没能热重载配置", "err", err)
	}
	if err := s.opt.AddProxy(p); err != nil {
		s.opt.Log.Warn("出口代理写进了配置文件，运行中没加上，重启后才生效", "name", p.Name, "err", err)
		s.adminProxyDone(w, r, "", "已写进配置文件，但没能加进运行中的轮转（重启后生效）："+err.Error())
		return
	}
	s.opt.Log.Info("管理员加了出口代理", "name", p.Name, "kind", string(p.Kind), "host", p.Host,
		"exit_ip", obs.IP, "exit_country", obs.Country)
	s.adminProxyDone(w, r, fmt.Sprintf("已添加 %s，已经在轮转里了。试的那一发出口是 %s（%s）",
		p.Name, obs.IP, strings.TrimSpace(obs.Country+" "+obs.City)), "")
}

// POST /admin/proxies/enabled：停用 / 启用一条出口代理（表单 name、enabled=0|1）。
//
// 停用只是不再分给新会话：已经绑在它上面的会话照旧走它到用完，不然一次铸票和它的
// state-echo 会被拆到两个出口上。写回配置文件，重启后照样是停用的。
func (s *Server) handleAdminProxyEnabled(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) || !s.proxyEditable(w) {
		return
	}
	form, ok := readAdminForm(w, r)
	if !ok {
		return
	}
	name, v := form.Get("name"), form.Get("enabled")
	if name == "" || (v != "0" && v != "1") {
		http.Error(w, "要 name，enabled 只收 0 或 1", http.StatusBadRequest)
		return
	}
	on := v == "1"
	configWriteMu.Lock()
	defer configWriteMu.Unlock()
	// 「至少留一条启用着的粘性代理」配置校验只看文件；有人手改过文件没重启时，运行中的
	// 清单和文件对不上，这里再按运行中的查一遍，免得整池一个出口都挑不到。
	if !on && !s.keepsStickyProxy(name) {
		s.adminProxyDone(w, r, "", "不能停用 "+name+"：停掉它，运行中就一条启用着的粘性代理都不剩了")
		return
	}
	// 先改文件：停掉最后一条粘性代理会被配置校验拒掉，那时运行中的状态也不该动。
	if err := config.SetProxyEnabled(s.opt.Live.Path(), name, on); err != nil {
		s.adminProxyDone(w, r, "", err.Error())
		return
	}
	if _, err := s.opt.Live.Reload(); err != nil {
		s.opt.Log.Warn("改完代理没能热重载配置", "err", err)
	}
	if err := s.opt.SetProxyEnabled(name, on); err != nil {
		s.opt.Log.Warn("出口代理的启用状态写进了配置文件，运行中没改成，重启后才生效", "name", name, "enabled", on, "err", err)
		s.adminProxyDone(w, r, "", "已写进配置文件，但运行中没改成（重启后生效）："+err.Error())
		return
	}
	s.opt.Log.Info("管理员改了出口代理的启用状态", "name", name, "enabled", on)
	note := "已启用 " + name
	if !on {
		note = "已停用 " + name + "：不再分给新会话，已经在用它的会话用完为止"
	}
	s.adminProxyDone(w, r, note, "")
}

// canEditProxies：接了配置文件和出口代理，出口代理那一节才能加、能停用。
func (s *Server) canEditProxies() bool {
	return s.opt.Live != nil && s.opt.Live.Path() != "" && s.opt.ProxyStates != nil &&
		s.opt.AddProxy != nil && s.opt.SetProxyEnabled != nil && s.opt.ProbeProxy != nil
}

func (s *Server) proxyEditable(w http.ResponseWriter) bool {
	if !s.canEditProxies() {
		http.Error(w, "这个实例没接配置文件或出口代理，改不了", http.StatusNotImplemented)
		return false
	}
	return true
}

// keepsStickyProxy：停掉 name 以后，运行中还有没有别的启用着的粘性代理。
func (s *Server) keepsStickyProxy(name string) bool {
	for _, p := range s.opt.ProxyStates() {
		if p.Name != name && p.Enabled && p.Sticky {
			return true
		}
	}
	return false
}

func (s *Server) adminProxyDone(w http.ResponseWriter, r *http.Request, note, errMsg string) {
	q := "?"
	switch {
	case errMsg != "":
		q += "proxy_err=" + url.QueryEscape(errMsg)
	case note != "":
		q += "proxy_note=" + url.QueryEscape(note)
	}
	http.Redirect(w, r, "/admin/"+q+"#proxies", http.StatusSeeOther)
}

// proxyFromForm 把「添加出口代理」那张表单翻成一条代理。这是系统边界，字段都在这儿卡。
func proxyFromForm(form url.Values, cur config.Config) (config.Proxy, error) {
	p := config.Proxy{
		Name:    strings.TrimSpace(form.Get("name")),
		Kind:    config.ProxyKind(form.Get("kind")),
		Host:    strings.TrimSpace(form.Get("host")),
		User:    strings.TrimSpace(form.Get("user")),
		Pass:    config.Secret(strings.TrimSpace(form.Get("pass"))),
		Enabled: true,
	}
	pass := p.Pass.Reveal()
	switch {
	// 非法 UTF-8 存进 JSON 会被换成 U+FFFD：试通用的是原字节，文件里存的却是替换过的，
	// 重启后口令对不上、名字也和运行中的对不上。
	case !utf8.ValidString(p.Name) || !utf8.ValidString(p.Host) || !utf8.ValidString(p.User) || !utf8.ValidString(pass):
		return p, errors.New("表单里有不是 UTF-8 的字节")
	case p.Name == "" || utf8.RuneCountInString(p.Name) > 32 || strings.ContainsFunc(p.Name, unicode.IsControl):
		return p, errors.New("名称必填，不超过 32 个字，不能有控制字符")
	case !p.Kind.Valid():
		return p, errors.New("类型只能是 novproxy、webshare、cliproxy、ipipgo 或 seed")
	case !hostPort(p.Host):
		return p, errors.New("地址要写成「主机:端口」，例如 us.novproxy.io:1000，不带 socks5://")
	case p.User == "" || pass == "":
		return p, errors.New("用户名和密码都必填")
	// SOCKS5 的用户名、密码各限 255 字节，而用户名后面还要拼地区和会话号。
	case len(p.User) > 200 || len(pass) > 255 ||
		strings.ContainsFunc(p.User, unicode.IsControl) || strings.ContainsFunc(pass, unicode.IsControl):
		return p, errors.New("用户名或密码太长，或者含控制字符")
	}
	for _, q := range cur.Proxies {
		if q.Name == p.Name {
			return p, fmt.Errorf("已经有一条叫 %q 的出口代理", p.Name)
		}
	}
	return p, nil
}

func hostPort(s string) bool {
	h, port, err := net.SplitHostPort(s)
	if err != nil || h == "" || strings.ContainsAny(h, "/@ ") {
		return false
	}
	n, err := strconv.Atoi(port)
	return err == nil && n > 0 && n <= 65535
}

// readAdminForm 读管理页按钮提交的小表单。上限 4 KiB：这些表单只有几个短字段。
func readAdminForm(w http.ResponseWriter, r *http.Request) (url.Values, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<10))
	if err != nil {
		http.Error(w, "请求体太大", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	form, err := parseForm(body)
	if err != nil {
		http.Error(w, "表单解不开", http.StatusBadRequest)
		return nil, false
	}
	return form, true
}
