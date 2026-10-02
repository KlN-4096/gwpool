package main

import (
	"io"
	"log/slog"
	"testing"

	"gwpool/internal/config"
)

// 停用的出口也要交给铸票客户端：不交的话重启后管理页上就没有它，再也点不回「启用」。
func TestNewMinterKeepsDisabledProxies(t *testing.T) {
	cfg := config.Config{Proxies: []config.Proxy{
		{Name: "nov", Kind: config.KindNovproxy, Host: "h:1", User: "u", Pass: "p", Enabled: true},
		{Name: "ws", Kind: config.KindWebshare, Host: "w:2", User: "wu", Pass: "wp", Enabled: false},
	}}
	st := newMinter(cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).ProxyStates()
	if len(st) != 2 || st[1].Name != "ws" || st[1].Enabled {
		t.Fatalf("停用的出口没进清单，或者状态不对：%+v", st)
	}
}
