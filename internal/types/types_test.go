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

// 对外开放门槛按**账号数**算：⌈所需网关数 ÷ 区域数⌉ + 1 的重叠余量。
func TestAccountsForContinuousFollowsCooldown(t *testing.T) {
	// 默认 4h：⌈96/9⌉ = 11，+1 = 12。这个数是用户定的，改了要先问。
	if got, want := AccountsForContinuous(SlotCooldown), 12; got != want {
		t.Fatalf("4 小时冷却算出 %d 个账号，应是 %d", got, want)
	}
	// 冷却测准成 30 分钟就该大幅降门槛：⌈12/9⌉ = 2，+1 = 3。
	if got, want := AccountsForContinuous(30*time.Minute), 3; got != want {
		t.Fatalf("30 分钟冷却算出 %d 个账号，应是 %d", got, want)
	}
	// 0 和负数退回默认冷却，别算出 0（那等于对外完全敞开）。
	if got := AccountsForContinuous(0); got != AccountsForContinuous(SlotCooldown) {
		t.Fatalf("0 没退回默认冷却: %d", got)
	}
	// 那个 +1 是重叠余量（一个区域在不同账号上常落回同一个网关），不是凑整。
	// 刚好整除时也必须留着，否则门槛退回成偏乐观的那个值。
	exact := time.Duration(len(Regions)) * DeliverTTL // ⌈9/9⌉ = 1，+1 = 2
	if got, want := AccountsForContinuous(exact), 2; got != want {
		t.Fatalf("刚好整除时算出 %d，重叠余量被吃掉了（应是 %d）", got, want)
	}
	// 门槛必须随冷却单调不减：测准的冷却越长，要的账号越多。
	prev := 0
	for _, d := range []time.Duration{time.Minute, 30 * time.Minute, time.Hour, 4 * time.Hour, 12 * time.Hour} {
		got := AccountsForContinuous(d)
		if got < prev {
			t.Fatalf("冷却 %v 算出 %d，比更短的冷却还少（上一个 %d）", d, got, prev)
		}
		prev = got
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
	// 只查真·标记：`cf` 本身就是合法的十六进制双字符，查它对任何给定常量要么恒绿
	// 要么恒红（约 4% 的常量恒红），和想证明的事无关。
	if strings.Contains(v, "secret") {
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

// 一张票能用多久由 __cflb 定（它才钉路由），而它比 __oailb 早死。
// 按 __oailb 算寿命的话，「刚好够交付」那张票的路由其实已经死了。
func TestCflbDiesBeforeOailb(t *testing.T) {
	if PairLifetime >= OailbLifetime {
		t.Fatalf("PairLifetime=%v 不短于 OailbLifetime=%v，那 300 秒的带子没了",
			PairLifetime, OailbLifetime)
	}
}

// RouteDeadline 三级取值都要对，而且**一律取小**。
func TestRouteDeadlineTakesTheShorterCookie(t *testing.T) {
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	oailb := now.Add(OailbLifetime)
	for _, c := range []struct {
		name      string
		res       MintResult
		prevCflb  time.Time
		wantRoute time.Time
		wantCflb  time.Time
	}{{
		// 一级：这一发补发了 __cflb ⇒ 用它自己的 Max-Age，而它更早 ⇒ 取它。
		name:      "补发了 cflb",
		res:       MintResult{PairExpires: oailb, CflbExpires: now.Add(PairLifetime)},
		wantRoute: now.Add(PairLifetime), wantCflb: now.Add(PairLifetime),
	}, {
		// 二级：只补发了 __oailb ⇒ 旧 __cflb 原样留着 ⇒ 按账本上它的死期。
		// 这一格漏了就等于让新 __oailb 的 65 分钟盖掉旧 __cflb，虚报量无上限。
		name:     "只补发 oailb，继承旧 cflb",
		res:      MintResult{PairExpires: oailb},
		prevCflb: now.Add(400 * time.Second),
		// 旧 __cflb 只剩 400 秒 ⇒ 路由 400 秒后就死，和新 __oailb 的 3900 无关。
		wantRoute: now.Add(400 * time.Second), wantCflb: now.Add(400 * time.Second),
	}, {
		// 三级：Max-Age 读不出又没有上一张 ⇒ 按实测差值倒推 300 秒。
		name:      "两样都没有，按差值兜底",
		res:       MintResult{PairExpires: oailb},
		wantRoute: oailb.Add(-(OailbLifetime - PairLifetime)),
		wantCflb:  oailb.Add(-(OailbLifetime - PairLifetime)),
	}, {
		// __oailb 反而更早（继承来的 __cflb 还很长）⇒ 取 __oailb。
		name:      "oailb 更早就取它",
		res:       MintResult{PairExpires: now.Add(time.Minute)},
		prevCflb:  now.Add(time.Hour),
		wantRoute: now.Add(time.Minute), wantCflb: now.Add(time.Hour),
	}} {
		t.Run(c.name, func(t *testing.T) {
			route, cflb := c.res.RouteDeadline(c.prevCflb)
			if !route.Equal(c.wantRoute) {
				t.Fatalf("route=%v，应为 %v", route, c.wantRoute)
			}
			if !cflb.Equal(c.wantCflb) {
				t.Fatalf("cflb=%v，应为 %v", cflb, c.wantCflb)
			}
			if route.After(cflb) {
				t.Fatal("route 比 cflb 还晚：路由死了票就不能发了")
			}
		})
	}
}

// 兜底算法零值进零值出：零值得由调用方当「不知道」处理，绝不能变成「不过期」。
func TestFallbackCflbExpiryZeroStaysZero(t *testing.T) {
	if got := FallbackCflbExpiry(time.Time{}); !got.IsZero() {
		t.Fatalf("零值进去出来成了 %v", got)
	}
}
