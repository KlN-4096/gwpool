package httpapi

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

// 公开页的铸票记录：按时间倒序列最近几发，账号备注遮掉大部分，而且**绝不**带国家/城市、
// ASN 和上游原始错误正文 —— 前两样用户定过只给管理员看，错误正文是上游回的 JSON，
// 里面可能带账号信息。
//
// 做成历史而不是「此刻在铸的」：一发铸票只有几秒，而这一页是服务端渲染的 ⇒ 在飞快照
// 大概率恒为空，看不出池子到底在不在干活。
func TestPoolShowsMaskedMintHistory(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModePool)
	st.accounts[0].Label = "sample-account@example.test"
	// 落点名刻意用 rig 不会产生的：rig 自带一条 At=now 的采样（落点 unified-142），
	// 拿它那个名字做顺序断言会先匹配到 rig 那一行，断言就不是在测我们排的序。
	st.samples = append(st.samples,
		types.Sample{AccountID: 1, RegionKey: "west-europe", Gateway: "unified-901", Status: 200,
			Observed: types.Observed{Country: "IT", City: "Rome", ASN: "AS12345"},
			At:       time.Date(2026, 10, 3, 1, 0, 0, 0, time.UTC)},
		// 状态码 200 但读不出落点 ⇒ 不算成功：它补不上任何一个格子。
		types.Sample{AccountID: 1, RegionKey: "us-east", Status: 200,
			At: time.Date(2026, 10, 3, 1, 1, 0, 0, time.UTC)},
		types.Sample{AccountID: 2, RegionKey: "east-asia", Gateway: "unified-902", Status: 429,
			Err: "上游原始正文 acct-upstream",
			At:  time.Date(2026, 10, 3, 1, 2, 0, 0, time.UTC)},
	)
	body := do(h, httptest.NewRequest("GET", "/", nil)).Body.String()

	// 遮名：留前两个字符和域名，中间那段不许出现。
	if !strings.Contains(body, "sa***@example.test") {
		t.Fatal("铸票记录没按遮名规则显示账号")
	}
	for _, leaked := range []string{"sample-account@example.test", "mple-account", "朋友的号"} {
		if strings.Contains(body, leaked) {
			t.Fatalf("公开页漏出了账号备注原文: %s", leaked)
		}
	}
	// 白名单之外的字段一个都不许上这一页。
	for _, leaked := range []string{"Rome", "AS12345", "上游原始正文", "acct-upstream"} {
		if strings.Contains(body, leaked) {
			t.Fatalf("公开页漏出了只给管理员的字段: %s", leaked)
		}
	}

	// 新的在前，而且**按时间戳排、不靠账本的追加序**：unified-902 那条（01:02）
	// 要排在 unified-901（01:00）之前。
	late := strings.Index(body, `mint-gw">unified-902`)
	early := strings.Index(body, `mint-gw">unified-901`)
	if late < 0 || early < 0 || late > early {
		t.Fatalf("铸票记录不是新的在前（01:02 那条在 %d，01:00 那条在 %d）", late, early)
	}
	// 带状态码的失败要把状态码说出来，别只写「失败」。
	if !strings.Contains(body, "HTTP 429") {
		t.Fatal("带状态码的失败没显示状态码")
	}
	// 落点读不出来的那一发（us-east 那条）在落点列显示「—」。
	if !strings.Contains(body, `mint-gw">—`) {
		t.Fatal("读不出落点的那一发没显示占位符")
	}
}

// 遮名按**字符**切，别把一个汉字劈成半个坏字节贴到页面上；非邮箱备注也要遮。
func TestMaskLabel(t *testing.T) {
	for in, want := range map[string]string{
		"sample-account@example.test": "sa***@example.test",
		"ab@gmail.com":             "ab***@gmail.com",
		"a@gmail.com":              "a***@gmail.com",
		"朋友的号":                     "朋友***",
		"pro1":                     "pr***",
		"":                         "***",
		"   ":                      "***",
		// 没有域名的 @ 不该留一个裸 @ 在页面上。
		"someone@": "so***",
	} {
		if got := maskLabel(in); got != want {
			t.Fatalf("maskLabel(%q) = %q，想要 %q", in, got, want)
		}
	}
}
