package mint

import (
	"testing"
	"time"

	"gwpool/internal/types"
)

// 换 __cflb 的四条判据。能换是因为「有效的 __oailb 自己钉死网关」（2026-10-03 pro1 实测），
// 不能换的三格各自挡一种真实故障：
//   - 果实过期还换 ⇒ 落点退回大区规则 ⇒ 票上写着 A、实际把人送到他自己那个网关 ⇒
//     降智票当满血票发出去
//   - 没有更长的 cflb 还换 ⇒ 白改票号，而票号是 /release 和 /pair/renew 的归属判据
//   - 大区为空（老记录）还换 ⇒ 等于瞎猜它属于哪个大区
func TestRehome(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	short, long := now.Add(5*time.Minute), now.Add(50*time.Minute)
	pair := func(oailbExp time.Time, cflb string, cflbExp time.Time, region string) types.Pair {
		return types.Pair{
			Gateway:       "unified-87",
			Cookie:        cookieCflb + "=" + cflb + "; " + cookieOailb + "=" + fakeOailb("unified-87", oailbExp),
			ExpiresAt:     cflbExp,
			CflbExpiresAt: cflbExp,
			RegionKey:     region,
		}
	}
	best := map[string]CflbLease{"east-asia": {Cflb: "LONG", Exp: long}}

	cases := []struct {
		name    string
		in      types.Pair
		swapped bool
		expires time.Time
	}{
		{"种子快死、果实还长 ⇒ 换，寿命跟着新 cflb",
			pair(now.Add(60*time.Minute), "SHORT", short, "east-asia"), true, long},
		{"果实比新 cflb 先死 ⇒ 换，但寿命按果实",
			pair(now.Add(20*time.Minute), "SHORT", short, "east-asia"), true, now.Add(20 * time.Minute)},
		{"果实已经过期 ⇒ 不换",
			pair(now.Add(-time.Minute), "SHORT", short, "east-asia"), false, short},
		{"自己那张已经是最长的 ⇒ 不换",
			pair(now.Add(60*time.Minute), "LONG", long, "east-asia"), false, long},
		{"大区为空的老记录 ⇒ 不换",
			pair(now.Add(60*time.Minute), "SHORT", short, ""), false, short},
	}
	for _, c := range cases {
		got := Rehome(c.in, best, now)
		swapped := got.Cookie != c.in.Cookie
		if swapped != c.swapped {
			t.Errorf("%s：换了没=%v，应为 %v", c.name, swapped, c.swapped)
			continue
		}
		if !got.ExpiresAt.Equal(c.expires) {
			t.Errorf("%s：ExpiresAt=%v，应为 %v", c.name, got.ExpiresAt, c.expires)
		}
		if swapped {
			if CflbOf(got.Cookie) != "LONG" {
				t.Errorf("%s：__cflb 没换成 LONG：%q", c.name, CflbOf(got.Cookie))
			}
			// 果实必须原样留着——它才是钉网关的那件。
			if gw, _, err := PairInfo(got.Cookie); err != nil || gw != "unified-87" {
				t.Errorf("%s：换完读不出原来的网关：%q %v", c.name, gw, err)
			}
		}
	}
}

// BestCflb 只收还活着、且带得出 __cflb 的票，并且每个大区取最长寿那张。
func TestBestCflbTakesTheLongestLivingPerRegion(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	mk := func(cflb string, exp time.Time, region string) types.Pair {
		return types.Pair{Cookie: cookieCflb + "=" + cflb + "; " + cookieOailb + "=x",
			CflbExpiresAt: exp, RegionKey: region}
	}
	best := BestCflb(map[string]types.Pair{
		"a": mk("A", now.Add(10*time.Minute), "east-asia"),
		"b": mk("B", now.Add(40*time.Minute), "east-asia"),
		"c": mk("C", now.Add(-time.Minute), "east-asia"), // 已经过期，不许入选
		"d": mk("D", now.Add(90*time.Minute), ""),        // 没大区，不入选
		"e": {Cookie: cookieOailb + "=x", CflbExpiresAt: now.Add(90 * time.Minute),
			RegionKey: "europe"}, // 没 __cflb，不入选
		"f": mk("F", now.Add(20*time.Minute), "europe"),
	}, now)
	if got := best["east-asia"]; got.Cflb != "B" {
		t.Errorf("east-asia 该取最长寿的 B，得到 %q", got.Cflb)
	}
	if got := best["europe"]; got.Cflb != "F" {
		t.Errorf("europe 该取 F（唯一一张带 __cflb 的），得到 %q", got.Cflb)
	}
	if len(best) != 2 {
		t.Errorf("只该有两个大区入选，得到 %d 个：%v", len(best), best)
	}
}
