package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gwpool/internal/types"
)

// 一份最小可用配置：至少要有一个启用着的粘性供应商。
const okProxies = `"proxies":[{"name":"nov","kind":"novproxy","host":"us.novproxy.io:1000",
	"user":"u","pass":"p","enabled":true}]`

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gwpool.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadFillsDefaults(t *testing.T) {
	cfg, err := Load(writeCfg(t, `{`+okProxies+`,"consumer_key":"ck"}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != types.ModeSingle {
		t.Errorf("默认模式该是 single（池子模式会保温，必须显式开），得到 %q", cfg.Mode)
	}
	if cfg.Fingerprint != FingerprintSub2api {
		t.Errorf("默认指纹该是 sub2api，得到 %q", cfg.Fingerprint)
	}
	if cfg.StickyMinutes != 5 || cfg.DataDir != "data" || cfg.Listen == "" {
		t.Errorf("默认值没填上: %+v", cfg)
	}
	if cfg.Proxies[0].Pass.Reveal() != "p" {
		t.Errorf("Reveal 该拿到真值")
	}
}

func TestLoadRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"未知模式":       `{"mode":"turbo",` + okProxies + `}`,
		"未知指纹":       `{"fingerprint":"curl",` + okProxies + `}`,
		"空 data_dir": `{"data_dir":"",` + okProxies + `}`,
		"坏 JSON":     `{`,
		"粘性分钟为 0":    `{"sticky_minutes":0,` + okProxies + `}`,
		// 一个粘性供应商都没有时铸票和 state-echo 配不成对，整个交付路径是坏的
		"没有代理": `{"proxies":[]}`,
		"只有非粘性供应商": `{"proxies":[{"name":"ws","kind":"webshare","host":"p.webshare.io:1080",
			"user":"u","pass":"p","enabled":true}]}`,
		"粘性供应商被关掉": `{"proxies":[{"name":"nov","kind":"novproxy","host":"h","user":"u",
			"pass":"p","enabled":false}]}`,
		"代理缺 name": `{"proxies":[{"kind":"novproxy","host":"h","user":"u","pass":"p","enabled":true}]}`,
		"代理缺密码":    `{"proxies":[{"name":"n","kind":"novproxy","host":"h","user":"u","enabled":true}]}`,
		"未知 kind":  `{"proxies":[{"name":"n","kind":"squid","host":"h","user":"u","pass":"p","enabled":true}]}`,
		"name 重复": `{"proxies":[
			{"name":"n","kind":"novproxy","host":"h","user":"u","pass":"p","enabled":true},
			{"name":"n","kind":"webshare","host":"h","user":"u","pass":"p","enabled":true}]}`,
	}
	for name, body := range cases {
		if _, err := Load(writeCfg(t, body)); err == nil {
			t.Errorf("%s 该被拒", name)
		}
	}
	if _, err := Load(filepath.Join(t.TempDir(), "没有这个文件.json")); err == nil {
		t.Error("文件不存在该报错：配置里有凭据，编不出来")
	}
}

func TestProxySelection(t *testing.T) {
	cfg, err := Load(writeCfg(t, `{"proxies":[
		{"name":"nov","kind":"novproxy","host":"h","user":"u","pass":"p","enabled":true},
		{"name":"ws","kind":"webshare","host":"h","user":"u","pass":"p","enabled":true},
		{"name":"off","kind":"novproxy","host":"h","user":"u","pass":"p","enabled":false}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.EnabledProxies(); len(got) != 2 {
		t.Errorf("启用着的该有 2 条，得到 %d", len(got))
	}
	sticky := cfg.StickyProxies()
	if len(sticky) != 1 || sticky[0].Name != "nov" {
		t.Errorf("粘性的只该有 nov，得到 %+v", sticky)
	}
	if !cfg.Proxies[0].Sticky() || cfg.Proxies[1].Sticky() {
		t.Error("粘性只由 kind 决定：novproxy 有、webshare 没有")
	}
}

// 红线 1：密码不许进日志。这里把能把它漏出去的几条路都过一遍。
func TestSecretNeverPrinted(t *testing.T) {
	cfg, err := Load(writeCfg(t, `{"mode":"pool","admin_key":"AK-SECRET","proxies":[
		{"name":"nov","kind":"novproxy","host":"h","user":"u","pass":"PASS-SECRET","enabled":true}]}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	dumps := []string{
		fmt.Sprintf("%v", cfg),
		fmt.Sprintf("%+v", cfg),
		fmt.Sprintf("%#v", cfg),
		fmt.Sprintf("%s", cfg.Proxies[0].Pass),
		fmt.Sprintf("%q", cfg.AdminKey),
		string(b),
	}
	for i, d := range dumps {
		for _, leak := range []string{"PASS-SECRET", "AK-SECRET"} {
			if strings.Contains(d, leak) {
				t.Errorf("第 %d 种打印方式漏出了 %s: %s", i, leak, d)
			}
		}
	}
	if Secret("").String() != "" {
		t.Error("空密码不该打印成 ***，否则分不清「没配」和「配了」")
	}
}
