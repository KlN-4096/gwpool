package types

import (
	"strings"
	"testing"
	"time"
)

// 区域清单是 SPEC 第 4 节那张表的副本，代表出口不许「优化」：
// 这九个是 2026-10-01 的 170+ 样本跑出来的，换个国家可能落到同一个网关、白打一发。
func TestRegionsMatchSpecTable(t *testing.T) {
	want := []string{"us-east:US", "us-west:US/Seattle", "south-america:BR", "west-europe:ES",
		"europe:IT", "east-asia:JP", "oceania:AU", "south-asia:IN", "middle-east:SA"}
	if len(Regions) != len(want) {
		t.Fatalf("区域数 %d，SPEC 第 4 节是 %d 个", len(Regions), len(want))
	}
	for i, r := range Regions {
		got := r.Key + ":" + r.Exit.Region
		if r.Exit.City != "" {
			got += "/" + r.Exit.City
		}
		if got != want[i] {
			t.Errorf("第 %d 项 %q，SPEC 是 %q", i, got, want[i])
		}
	}
}

// RegionKeyOf 必须**精确**匹配国家+城市：us-east 和 us-west 的国家都是 US，
// 只有城市分得开，而那两个区域实测落的是不同网关（167 / 126）。
func TestRegionKeyOf(t *testing.T) {
	cases := []struct {
		in   Exit
		want string
	}{
		{Exit{Region: "US"}, "us-east"},
		{Exit{Region: "US", City: "Seattle"}, "us-west"},
		{Exit{Region: "SA"}, "middle-east"},
		{Exit{Region: "US", City: "Dallas"}, ""}, // 城市不在表上 ⇒ 不归并到 us-east
		{Exit{Region: "DE"}, ""},                 // 手改 state.json 可能出现的野出口
		{Exit{}, ""},
	}
	for _, c := range cases {
		if got := RegionKeyOf(c.in); got != c.want {
			t.Errorf("RegionKeyOf(%+v) = %q，要 %q", c.in, got, c.want)
		}
	}
	// 清单上的每一项都必须能反查回自己，否则配额和台账会记到空键上。
	for _, r := range Regions {
		if got := RegionKeyOf(r.Exit); got != r.Key {
			t.Errorf("%s 的出口反查成了 %q", r.Key, got)
		}
	}
}

// 门槛 = 冷却 ÷ 单张票的可用时长。冷却是配置项，所以它必须跟着参数走，
// 不能再读常量——不然改了配置、页面上的门槛还是老的。
func TestGatewaysForContinuousFollowsCooldown(t *testing.T) {
	if got, want := GatewaysForContinuous(time.Hour), 24; got != want {
		t.Fatalf("1 小时冷却算出 %d 个，应是 %d", got, want)
	}
	if got, want := GatewaysForContinuous(30*time.Minute), 12; got != want {
		t.Fatalf("30 分钟冷却算出 %d 个，应是 %d", got, want)
	}
	// 0 和负数退回默认冷却，别算出 0 个门槛（那等于对外完全敞开）。
	if got := GatewaysForContinuous(0); got != GatewaysForContinuous(SlotCooldown) {
		t.Fatalf("0 没退回默认冷却: %d", got)
	}
	// 不能整除时要向上取整：差半张票也是不够连续供。
	if got, want := GatewaysForContinuous(200*time.Second), 2; got != want {
		t.Fatalf("200s ÷ 150s 算出 %d，应向上取整到 %d", got, want)
	}
}

// cookie_version 是调用方拿来回头指名一张票的身份（exclude_versions、/release）：
// 必须确定、定长、不泄露 cookie 本体，而且**不存状态** ⇒ 重启后仍是同一个值。
func TestCookieVersion(t *testing.T) {
	const cookie = "__cflb=cf-secret; __oailb=oailb-secret"
	v := CookieVersion(cookie)
	if len(v) != CookieVersionLen {
		t.Fatalf("长度 %d，约定是 %d", len(v), CookieVersionLen)
	}
	if v != CookieVersion(cookie) {
		t.Fatal("同一张票算出两个值，调用方的引用就对不上了")
	}
	if v == CookieVersion(cookie+"x") {
		t.Fatal("两张不同的票撞上了同一个版本号")
	}
	for _, c := range v {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Fatalf("不是小写十六进制: %q", v)
		}
	}
	// 哈希，不是摘要式的「前几个字」：cookie 本体一个字都不许漏（SPEC 红线 1）。
	if strings.Contains(v, "cf") || strings.Contains(v, "secret") {
		t.Fatalf("版本号里带了 cookie 本体: %q", v)
	}
	// 没有票就没有身份：别给调用方一个「空串的哈希」让它当成真实引用。
	if CookieVersion("") != "" {
		t.Fatal("空 cookie 该回空串")
	}
}

// 交付门槛必须明显高于一个交付窗口：消费者手上那张票不能在窗口里就硬过期。
func TestDeliverFloorOutlastsWindow(t *testing.T) {
	if DeliverFloor <= DeliverTTL {
		t.Fatalf("DeliverFloor=%v 不比交付窗口 %v 长，发出去的票会在客户端手里到点",
			DeliverFloor, DeliverTTL)
	}
	if DeliverFloor >= RenewLead {
		t.Fatalf("DeliverFloor=%v 不低于续期线 %v：两个数一样会让低于门槛的票"+
			"同时「不能发」和「该续了」，供给紧时交付只能原地等保温", DeliverFloor, RenewLead)
	}
}
