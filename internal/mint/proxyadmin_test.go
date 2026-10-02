package mint

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gwpool/internal/config"
)

// 加代理只往末尾加：已经绑上的 sid 不许挪到别的出口；重名拒收；新加的马上能被挑到。
func TestAddProxyAppendsWithoutMovingBoundSIDs(t *testing.T) {
	c := &Client{Proxies: []config.Proxy{novProxy}}
	sid, err := c.NewSID(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AddProxy(wsProxy); err != nil {
		t.Fatal(err)
	}
	if err := c.AddProxy(wsProxy); err == nil {
		t.Fatal("重名的代理也加进去了")
	}
	if got := c.proxyName(sid); got != novProxy.Name {
		t.Fatalf("加代理以后，已经绑上的 sid 挪到了 %q", got)
	}
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		s, err := c.NewSID(true)
		if err != nil {
			t.Fatal(err)
		}
		seen[c.proxyName(s)] = true
	}
	if !seen[wsProxy.Name] {
		t.Fatalf("新加的代理一次都没被挑到：%v", seen)
	}
	if st := c.ProxyStates(); len(st) != 2 || st[1].Name != wsProxy.Name {
		t.Fatalf("清单不对：%+v", st)
	}
}

// 停用：新 sid 不再挑它，已经绑上的照旧走它；改的是 Client 自己的副本，
// 不是交进来的那份（main 交的是配置快照里的切片）。
func TestSetProxyEnabled(t *testing.T) {
	given := []config.Proxy{novProxy, wsProxy}
	c := &Client{Proxies: given}
	if _, err := c.NewSID(true); err != nil { // 游标从 0 起：这个给 nov
		t.Fatal(err)
	}
	onWS, err := c.NewSID(true) // 这个给 ws
	if err != nil || c.proxyName(onWS) != wsProxy.Name {
		t.Fatalf("前提不成立：第二个 sid 绑到了 %q（%v）", c.proxyName(onWS), err)
	}
	if err := c.SetProxyEnabled(wsProxy.Name, false); err != nil {
		t.Fatal(err)
	}
	if !given[1].Enabled {
		t.Fatal("停用改到了交进来的那份切片上")
	}
	for i := 0; i < 4; i++ {
		s, err := c.NewSID(true)
		if err != nil {
			t.Fatal(err)
		}
		if c.proxyName(s) == wsProxy.Name {
			t.Fatal("停用了还分给新 sid")
		}
	}
	if got := c.proxyName(onWS); got != wsProxy.Name {
		t.Fatalf("停用把已经绑上的 sid 挪到了 %q", got)
	}
	if st := c.ProxyStates(); st[1].Enabled {
		t.Fatal("状态里还是启用")
	}
	if err := c.SetProxyEnabled("nope", false); err == nil {
		t.Fatal("不存在的名字也改成功了")
	}
	if err := c.SetProxyEnabled(wsProxy.Name, true); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		s, _ := c.NewSID(true)
		seen[c.proxyName(s)] = true
	}
	if !seen[wsProxy.Name] {
		t.Fatalf("启用回来以后挑不到它：%v", seen)
	}
}

// 试通：经**指定的那条**代理查 geo，用户名按这条代理拼；不进清单、不碰熔断计数。
func TestProbeProxyGoesThroughThatProxy(t *testing.T) {
	geo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"ip":"203.0.113.9","country":"US","city":"Los Angeles","org":"AS64500 Example"}`)
	}))
	defer geo.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	gotUser := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		user, target, err := fakeSocks5Handshake(conn)
		if err != nil {
			t.Error(err)
			return
		}
		gotUser <- user
		back, err := net.Dial("tcp", target)
		if err != nil {
			t.Error(err)
			return
		}
		defer back.Close()
		go func() { _, _ = io.Copy(back, conn) }()
		_, _ = io.Copy(conn, back)
	}()

	p := novProxy
	p.Name, p.Host, p.User = "new", ln.Addr().String(), "u9"
	c := &Client{Proxies: []config.Proxy{novProxy}, StickyMin: 5, GeoURL: geo.URL + "/json"}
	obs, err := c.ProbeProxy(context.Background(), p)
	if err != nil {
		t.Fatalf("试通失败：%v", err)
	}
	if obs.IP != "203.0.113.9" || obs.Country != "US" || obs.ASN != "AS64500" {
		t.Fatalf("读出来的出口不对：%+v", obs)
	}
	if u := <-gotUser; !strings.HasPrefix(u, "u9-region-US-sid-") {
		t.Fatalf("代理看到的用户名 %q，不是按这条代理拼的", u)
	}
	if st := c.ProxyStates(); len(st) != 1 || st[0].Fails != 0 {
		t.Fatalf("试通改动了运行中的清单：%+v", st)
	}
}

// 密码填错：认证被拒要当场报出来，错误里不许带口令。
func TestProbeProxyReportsAuthFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		head := make([]byte, 2)
		if _, err := io.ReadFull(conn, head); err != nil {
			return
		}
		_, _ = io.ReadFull(conn, make([]byte, head[1]))
		_, _ = conn.Write([]byte{0x05, 0x02})
		auth := make([]byte, 2)
		if _, err := io.ReadFull(conn, auth); err != nil {
			return
		}
		_, _ = io.ReadFull(conn, make([]byte, auth[1]))
		plen := make([]byte, 1)
		_, _ = io.ReadFull(conn, plen)
		_, _ = io.ReadFull(conn, make([]byte, plen[0]))
		_, _ = conn.Write([]byte{0x01, 0x01}) // 认证失败
	}()

	p := wsProxy
	p.Host, p.Pass = ln.Addr().String(), "hunter2-secret"
	c := &Client{GeoURL: "http://geo.invalid/json"}
	_, err = c.ProbeProxy(context.Background(), p)
	if err == nil || !strings.Contains(err.Error(), "代理认证失败") {
		t.Fatalf("认证被拒没报出来：%v", err)
	}
	if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), p.User) {
		t.Fatalf("错误里带了凭据：%v", err)
	}
}

// 「绑定会话」只数粘性时限内的：停用一条出口以后，管理员靠它降到 0 判断在用的会话用完没有。
func TestBoundSIDsOnlyCountsLiveBindings(t *testing.T) {
	c := &Client{Proxies: []config.Proxy{novProxy}, StickyMin: 5}
	fresh, err := c.NewSID(true)
	if err != nil {
		t.Fatal(err)
	}
	old, err := c.NewSID(true)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.sids[old] = sidBind{idx: 0, at: time.Now().Add(-c.bindTTL() - time.Second)}
	c.mu.Unlock()
	if got := c.ProxyStates()[0].BoundSIDs; got != 1 {
		t.Fatalf("过了粘性时限的绑定也算进去了：%d", got)
	}
	_ = fresh
}
