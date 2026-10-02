package config

import (
	"os"
	"strings"
	"testing"
)

// 两条代理，第一条带一个手写的额外键：改别的条目时它必须原样留着。
const twoProxies = `{"mode":"pool","listen":"127.0.0.1:1","admin_key":"ak","consumer_key":"ck","models":["m1"],
	"proxies":[{"name":"nov","kind":"novproxy","host":"h:1","user":"u","pass":"p-secret","enabled":true,"note":"手写的"},
	{"name":"ws","kind":"webshare","host":"w:2","user":"wu","pass":"w-secret","enabled":true}]}`

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// 追加：原有条目的口令和手写的键一个都不能丢，新条目口令写原文；不合法就回滚。
func TestAppendProxyKeepsExistingEntries(t *testing.T) {
	p := writeCfg(t, twoProxies)
	add := Proxy{Name: "nov2", Kind: KindNovproxy, Host: "h:3", User: "u3", Pass: "p3-secret", Enabled: true}
	if err := AppendProxy(p, add); err != nil {
		t.Fatal(err)
	}
	b := readFile(t, p)
	for _, want := range []string{`"p-secret"`, `"w-secret"`, `"p3-secret"`, `"note"`} {
		if !strings.Contains(b, want) {
			t.Fatalf("文件里缺了 %s：\n%s", want, b)
		}
	}
	if strings.Contains(b, "***") {
		t.Fatalf("口令被打码写回去了：\n%s", b)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Proxies) != 3 || c.Proxies[2].Name != "nov2" || c.Proxies[2].Pass.Reveal() != "p3-secret" || !c.Proxies[2].Enabled {
		t.Fatalf("新条目没加在末尾或者字段不对：%+v", c.Proxies)
	}
	// 重名：Load 的校验拒掉，文件回到加之前。
	if err := AppendProxy(p, add); err == nil {
		t.Fatal("重名的代理也存进去了")
	}
	if readFile(t, p) != b {
		t.Fatal("拒掉了却改了文件")
	}
}

// 停用 / 启用只动那一条的 enabled；停掉最后一条启用着的粘性代理要被拒、文件不变。
func TestSetProxyEnabledInFile(t *testing.T) {
	p := writeCfg(t, twoProxies)
	if err := SetProxyEnabled(p, "ws", false); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Proxies[1].Enabled || !c.Proxies[0].Enabled || c.Proxies[1].Pass.Reveal() != "w-secret" {
		t.Fatalf("改错了条目或者丢了口令：%+v", c.Proxies)
	}
	before := readFile(t, p)
	if err := SetProxyEnabled(p, "nov", false); err == nil {
		t.Fatal("最后一条启用着的粘性代理也停掉了")
	}
	if readFile(t, p) != before {
		t.Fatal("拒掉了却改了文件")
	}
	if err := SetProxyEnabled(p, "nope", true); err == nil {
		t.Fatal("不存在的名字也改成功了")
	}
	if err := SetProxyEnabled(p, "ws", true); err != nil {
		t.Fatal(err)
	}
	if c, err = Load(p); err != nil || !c.Proxies[1].Enabled {
		t.Fatalf("启用回来没存住：%+v %v", c.Proxies, err)
	}
	if !strings.Contains(readFile(t, p), `"note"`) {
		t.Fatal("改 enabled 把同一条里手写的键弄丢了")
	}
}

// 只改 proxies 的补丁不许动模型：老配置只写了单值 model 时，Patch 以前会顺手把它删掉，
// 默认模型就悄悄回落成 Defaults 里那个（审查抓到的）。
func TestProxyPatchesKeepLegacyModel(t *testing.T) {
	p := writeCfg(t, `{`+okProxies+`,"consumer_key":"ck","model":"gpt-5.6-sol"}`)
	if err := AppendProxy(p, Proxy{Name: "ws", Kind: KindWebshare, Host: "w:2", User: "wu", Pass: "wp", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := SetProxyEnabled(p, "ws", false); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.DefaultModel() != "gpt-5.6-sol" {
		t.Fatalf("改代理把默认模型改成了 %q", c.DefaultModel())
	}
}
