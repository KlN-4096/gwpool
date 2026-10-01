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

// models 是候选清单，第一个是默认；老配置的单值 model 要能读懂。
func TestModelsList(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	const base = `"mode":"pool","listen":"127.0.0.1:1","admin_key":"a","consumer_key":"c",` +
		`"proxies":[{"name":"nov","kind":"novproxy","host":"h:1","user":"u","pass":"p","enabled":true}]`

	// 老写法：单值 model 折进 Models 的最前面。
	c, err := Load(write(t, `{`+base+`,"model":"gpt-6-astra"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Models) != 1 || c.Models[0] != "gpt-6-astra" {
		t.Fatalf("老配置没折进 Models: %v", c.Models)
	}
	if c.DefaultModel() != "gpt-6-astra" || c.Model != "gpt-6-astra" {
		t.Fatalf("派生镜像没填: %q / %q", c.DefaultModel(), c.Model)
	}

	// 新写法 + 两种混写：单值排最前，重复项去掉，顺序保持。
	c, err = Load(write(t, `{`+base+`,"model":"gpt-6-astra","models":["gpt-5.6-sol","gpt-6-astra"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.Models, ","); got != "gpt-6-astra,gpt-5.6-sol" {
		t.Fatalf("合并/去重不对: %s", got)
	}
	if !c.AllowsModel("gpt-5.6-sol") || c.AllowsModel("gpt-6-luna") {
		t.Fatal("AllowsModel 判错")
	}

	// 清单里的每一项都要过模型名校验：它会原样进发给上游的 JSON 请求体。
	if _, err := Load(write(t, `{`+base+`,"models":["ok","坏 名字"]}`)); err == nil {
		t.Fatal("非法模型名没被拦住")
	}
	if _, err := Load(write(t, `{`+base+`,"models":[]}`)); err == nil {
		t.Fatal("空 models 没被拦住")
	}
}

// 按键打补丁：页面上没出现的字段（代理、密钥）必须原样留着。
// 整个 Config 序列化回去会被 Secret.MarshalJSON 打码成 "***"，一次保存就毁掉所有密钥。
func TestPatchKeepsEverything(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.json")
	if err := os.WriteFile(p, []byte(`{"mode":"pool","listen":"127.0.0.1:1",
		"admin_key":"ak","consumer_key":"ck","models":["m1"],
		"proxies":[{"name":"nov","kind":"novproxy","host":"h:1","user":"u","pass":"p","enabled":true}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p); err != nil {
		t.Fatal(err)
	}
	// 页面上只改了这一个
	if err := Patch(p, map[string]any{"models": []string{"m2", "m1"}}); err != nil {
		t.Fatal(err)
	}
	back, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(back.Models, ","); got != "m2,m1" {
		t.Fatalf("改动没存住: %s", got)
	}
	if len(back.Proxies) != 1 || back.Proxies[0].Name != "nov" {
		t.Fatalf("保存把代理抹了: %+v", back.Proxies)
	}
	if back.AdminKey.Reveal() != "ak" || back.ConsumerKey.Reveal() != "ck" {
		t.Fatal("保存把密钥抹了")
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// 派生镜像不许写回文件，否则下次 Load 会把它当成「又排一次」。
	if strings.Contains(string(b), `"model"`) {
		t.Fatalf("派生的 model 被写回文件了:\n%s", b)
	}
	// 口令必须留原文，不是 Secret.String() 的 ***。
	if !strings.Contains(string(b), `"p"`) || strings.Contains(string(b), `***`) {
		t.Fatalf("密钥/口令被打码写回去了:\n%s", b)
	}

	// 不合法的改动必须回滚，绝不在磁盘上留一份启动不起来的配置。
	if err := Patch(p, map[string]any{"models": []string{}}); err == nil {
		t.Fatal("空 models 居然存住了")
	}
	if back, err = Load(p); err != nil {
		t.Fatalf("回滚后配置读不出来了: %v", err)
	}
	if got := strings.Join(back.Models, ","); got != "m2,m1" {
		t.Fatalf("回滚没回到上一个好状态: %s", got)
	}
}
