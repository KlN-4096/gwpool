package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gwpool/internal/types"
)

func open(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestOpenEmptyDirAndReload(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "data") // 连父目录都不存在
	s := open(t, dir)
	if len(s.Accounts()) != 0 || len(s.Gateways()) != 0 {
		t.Fatal("空目录该开出空状态")
	}

	a, err := s.AddAccount(types.Account{Label: "pro1", PlanType: "pro",
		Creds:     types.Creds{AccessToken: "tok", AccountID: "acc"},
		ViewToken: "vt1", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != 1 || a.CreatedAt.IsZero() {
		t.Fatalf("新账号该分到 ID 1 并带时间戳: %+v", a)
	}
	b, err := s.AddAccount(types.Account{Label: "plus-b"})
	if err != nil {
		t.Fatal(err)
	}
	if b.ID != 2 {
		t.Fatalf("第二个账号该是 ID 2，得到 %d", b.ID)
	}
	if _, err := s.UpsertGateway("unified-167", a.ID, types.Exit{Region: "US"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutPair(types.Pair{Gateway: "unified-167", Cookie: "__cflb=x; __oailb=y",
		ExpiresAt: time.Now().Add(types.PairLifetime).UTC(), MintedBy: a.ID}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutSlot(types.Slot{AccountID: a.ID, Gateway: "unified-167",
		LastTouch: time.Now().UTC(), LastVerdict: "full"}); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	// 原子写的结果必须是一份能读回来的完整 JSON。
	s2 := open(t, dir)
	if got := len(s2.Accounts()); got != 2 {
		t.Fatalf("重开后账号数 %d", got)
	}
	if _, ok := s2.AccountByViewToken("vt1"); !ok {
		t.Error("view_token 索引不出账号")
	}
	if _, ok := s2.Pair("unified-167"); !ok {
		t.Error("pair 没持久化")
	}
	if _, ok := s2.Slot(a.ID, "unified-167"); !ok {
		t.Error("slot 没持久化")
	}
	if _, err := os.Stat(filepath.Join(dir, stateFile+".tmp")); err == nil {
		t.Error("tmp 文件该在 Rename 后消失")
	}
}

// 原子写：state.json 任何时刻都该是一份可解析的完整文件，不能是写一半的。
func TestSaveIsAtomic(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	path := filepath.Join(dir, stateFile)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			if _, err := s.UpsertGateway("unified-1", 1, types.Exit{Region: "US", City: string(rune('a' + i%26))}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 200; i++ {
		b, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			continue // Windows 上 Rename 与读可能撞一下，重试即可
		}
		var st State
		if err := json.Unmarshal(b, &st); err != nil {
			t.Fatalf("读到了写一半的 state.json: %v", err)
		}
	}
	<-done
}

func TestConcurrentReadWrite(t *testing.T) {
	s := open(t, t.TempDir())
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				switch w % 4 {
				case 0:
					_, _ = s.UpsertGateway("unified-2", 1, types.Exit{Region: "JP"})
				case 1:
					_ = s.PutSlot(types.Slot{AccountID: 1, Gateway: "unified-2", LastTouch: time.Now()})
				case 2:
					_ = s.AppendSample(types.Sample{AccountID: 1, RegionKey: "east-asia",
						Gateway: "unified-2", Status: 200, At: time.Now().UTC()})
				default:
					_ = s.Gateways()
					_ = s.Slots()
					_, _ = s.Slot(1, "unified-2")
				}
			}
		}(w)
	}
	wg.Wait()
	rows, err := s.Samples()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 100 { // 2 个 writer × 50
		t.Fatalf("样本行数 %d，要 100（append-only 不该丢行也不该串行写坏）", len(rows))
	}
}

// 返回的 Gateway 必须是副本，否则调用方一改就改到了内存里那份。
func TestGatewayCopyIsolated(t *testing.T) {
	s := open(t, t.TempDir())
	if _, err := s.UpsertGateway("unified-9", 1, types.Exit{Region: "AU"}); err != nil {
		t.Fatal(err)
	}
	g := s.Gateways()["unified-9"]
	g.ReachableExits[0] = types.Exit{Region: "HACKED"}
	again := s.Gateways()["unified-9"]
	if again.ReachableExits[0].Region != "AU" {
		t.Error("外部改动渗进了 store")
	}
}

func TestUpsertGatewayNewAndExits(t *testing.T) {
	s := open(t, t.TempDir())
	isNew, err := s.UpsertGateway("unified-84", 2, types.Exit{Region: "BR"})
	if err != nil || !isNew {
		t.Fatalf("第一次该是新的: isNew=%v err=%v", isNew, err)
	}
	isNew, _ = s.UpsertGateway("unified-84", 3, types.Exit{Region: "BR"})
	if isNew {
		t.Error("第二次不该算新网关")
	}
	if _, err := s.UpsertGateway("unified-84", 3, types.Exit{Region: "AR"}); err != nil {
		t.Fatal(err)
	}
	g := s.Gateways()["unified-84"]
	if len(g.ReachableExits) != 2 {
		t.Errorf("可达出口该去重后累加，得到 %+v", g.ReachableExits)
	}
	if g.FirstSeenBy != 2 {
		t.Errorf("FirstSeenBy 不该被后来者改写，得到 %d", g.FirstSeenBy)
	}
}

// 只有城市没有国家的出口重放时会被静默降级成「不定向」，不该收进可达出口。
func TestUpsertGatewayRejectsCityWithoutCountry(t *testing.T) {
	s := open(t, t.TempDir())
	if _, err := s.UpsertGateway("unified-1", 1, types.Exit{City: "Milan"}); err != nil {
		t.Fatal(err)
	}
	if exits := s.Gateways()["unified-1"].ReachableExits; len(exits) != 0 {
		t.Errorf("没有国家的出口不该入库，得到 %+v", exits)
	}
}

// S8：落盘失败后内存必须退回去，**包括网关已存在、只追加了出口**这一支。
// 漏掉它的话新出口只活在内存里，重启后悄悄回退，而调用方以为写成功了。
func TestUpsertGatewayRollsBackExistingOnSaveFailure(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	if _, err := s.UpsertGateway("unified-5", 1, types.Exit{Region: "US"}); err != nil {
		t.Fatal(err)
	}
	// Close the actual journal handle: writes must fail, not a now-unused
	// snapshot destination that incremental commits no longer touch.
	breakSave(t, s)

	if _, err := s.UpsertGateway("unified-5", 1, types.Exit{Region: "JP"}); err == nil {
		t.Fatal("落盘失败该报错")
	}
	if exits := s.Gateways()["unified-5"].ReachableExits; len(exits) != 1 || exits[0].Region != "US" {
		t.Errorf("内存该退回落盘成功时的样子，得到 %+v", exits)
	}
}

func TestPutSlotAndPairRollBackOnSaveFailure(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	breakSave(t, s)
	if err := s.PutSlot(types.Slot{AccountID: 1, Gateway: "g"}); err == nil {
		t.Error("落盘失败该报错")
	}
	if _, ok := s.Slot(1, "g"); ok {
		t.Error("没落盘的槽位不该留在内存里")
	}
	if err := s.PutPair(types.Pair{Gateway: "g", Cookie: "c"}); err == nil {
		t.Error("落盘失败该报错")
	}
	if _, ok := s.Pair("g"); ok {
		t.Error("没落盘的 pair 不该留在内存里")
	}
	if err := s.UpdateSlot(1, "g", func(*types.Slot) {}); err == nil {
		t.Error("落盘失败该报错")
	}
	if _, ok := s.Slot(1, "g"); ok {
		t.Error("UpdateSlot 没落盘也不该留在内存里")
	}
}

// breakSave fails the actual durable write path.
func breakSave(t *testing.T, s *Store) {
	t.Helper()
	if err := s.journal.file.Close(); err != nil {
		t.Fatal(err)
	}
}

// S5：读-改-写必须在写锁里完成，否则并发下会丢更新 ——
// 丢掉消费者回报的 LastTouch 就等于把烧过的槽位当新鲜的发出去。
func TestUpdateSlotIsAtomic(t *testing.T) {
	s := open(t, t.TempDir())
	const n = 60
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.UpdateSlot(3, "unified-7", func(sl *types.Slot) {
				// 用计数器而不是时间戳：丢更新会直接体现成数值少加
				sl.LastVerdict = bump(sl.LastVerdict)
			}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	sl, ok := s.Slot(3, "unified-7")
	if !ok {
		t.Fatal("槽位该被建出来")
	}
	if sl.LastVerdict != strconv.Itoa(n) {
		t.Errorf("%d 次自增只记到 %q，说明读-改-写被并发切开了", n, sl.LastVerdict)
	}
	if sl.AccountID != 3 || sl.Gateway != "unified-7" {
		t.Errorf("键该由参数定，得到 %+v", sl)
	}
}

func bump(s string) string {
	n, _ := strconv.Atoi(s)
	return strconv.Itoa(n + 1)
}

// 改写函数不许把槽位搬到别的账号/网关名下，否则键和内容会对不上。
func TestUpdateSlotKeepsKey(t *testing.T) {
	s := open(t, t.TempDir())
	if err := s.UpdateSlot(1, "gw", func(sl *types.Slot) {
		sl.AccountID, sl.Gateway = 999, "别的网关"
	}); err != nil {
		t.Fatal(err)
	}
	sl, ok := s.Slot(1, "gw")
	if !ok || sl.AccountID != 1 || sl.Gateway != "gw" {
		t.Errorf("得到 %+v ok=%v", sl, ok)
	}
	if err := s.UpdateSlot(1, "gw", nil); err == nil {
		t.Error("没给改写函数该报错")
	}
}

// 红线 2：锁必须挂在 store 上，这样 scan 和 sched 争的是同一把。
func TestLockAccountSerializesSameAccount(t *testing.T) {
	s := open(t, t.TempDir())
	var inflight, peak int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := s.LockAccount(42)
			defer unlock()
			n := atomic.AddInt32(&inflight, 1)
			for {
				old := atomic.LoadInt32(&peak)
				if n <= old || atomic.CompareAndSwapInt32(&peak, old, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			atomic.AddInt32(&inflight, -1)
		}()
	}
	wg.Wait()
	if peak != 1 {
		t.Fatalf("同一账号最大并发 %d，必须是 1（已知的封号主因）", peak)
	}

	// 不同账号不该互相排队。
	unlock := s.LockAccount(1)
	done := make(chan struct{})
	go func() { s.LockAccount(2)(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("账号 2 被账号 1 的锁挡住了，锁串错了范围")
	}
	unlock()
}

// types.Creds 要同时满足两件相反的事：打印路径看不到 token，state.json 却必须原样存回。
// 打码的 MarshalJSON 会让凭据第一次保存就变成 "***"，重启后全部账号失效。
func TestCredsMaskedInPrintButPersisted(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	a, err := s.AddAccount(types.Account{Label: "pro1",
		Creds: types.Creds{AccessToken: "TOKEN-SECRET", AccountID: "acc-1"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{
		fmt.Sprintf("%v", a), fmt.Sprintf("%+v", a), fmt.Sprintf("%#v", a),
		fmt.Sprintf("%v", a.Creds), a.Creds.String(),
	} {
		if strings.Contains(d, "TOKEN-SECRET") {
			t.Errorf("打印漏出了 token: %s", d)
		}
	}
	_ = s.Close()
	s2 := open(t, dir)
	got, ok := s2.Account(a.ID)
	if !ok || got.Creds.AccessToken != "TOKEN-SECRET" {
		t.Fatalf("token 没能原样存回来: %+v", got.Creds)
	}
}

// 每加一个账号都要全量重写 state.json，没有上限就是外部可触发的 O(n²) 写放大。
func TestAddAccountCapped(t *testing.T) {
	s := open(t, t.TempDir())
	hosted := make([]types.Account, maxAccounts)
	for i := range hosted {
		hosted[i].Creds.AccessToken = "tok"
	}
	s.mu.Lock()
	s.st.Accounts = hosted
	s.mu.Unlock()
	if _, err := s.AddAccount(types.Account{Label: "再来一个", Creds: types.Creds{AccessToken: "tok"}}); err == nil {
		t.Fatal("超过上限该被拒")
	}
}

// 匿名占位行是「报个 ID 就长一行」，和托管账号共用一个上限的话，灌满就让所有上传
// 永久失败，而服务内没有回收手段。两个上限必须各自独立。
func TestAnonAccountsCannotBlockUploads(t *testing.T) {
	s := open(t, t.TempDir())
	// 灌到**托管**账号那格的上限：共用一个计数的旧写法在这里就把上传堵死了。
	full := make([]types.Account, maxAccounts) // 零值 = 无凭据 = 占位行
	s.mu.Lock()
	s.st.Accounts = full
	s.mu.Unlock()

	if _, err := s.AddAccount(types.Account{Label: "又一个匿名"}); err == nil {
		t.Fatal("匿名占位行超过自己的上限该被拒")
	}
	// 关键断言：匿名把自己那格占满了，真上传者照样进得来。
	if _, err := s.AddAccount(types.Account{
		Label: "真上传者", Creds: types.Creds{AccessToken: "tok"},
	}); err != nil {
		t.Fatalf("匿名占位行把上传堵死了: %v", err)
	}
}

// 换票写回：access 换新的，refresh 为空时**保留**原来那张。
// 写成空值就再也换不了票了，而那正是「池子九天后静默死掉」的样子。
func TestSetTokensKeepsRefreshWhenNotRotated(t *testing.T) {
	s := open(t, t.TempDir())
	a, err := s.AddAccount(types.Account{
		Label:   "x",
		Enabled: false,
		Creds:   types.Creds{AccessToken: "old-access", AccountID: "acct-1", RefreshToken: "old-refresh"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DisableAccount(a.ID, "上游回 401"); err != nil {
		t.Fatal(err)
	}

	if err := s.SetTokens(a.ID, "new-access", ""); err != nil {
		t.Fatal(err)
	}
	got := accountByID(t, s, a.ID)
	if got.Creds.AccessToken != "new-access" {
		t.Fatalf("access 没换: %q", got.Creds.AccessToken)
	}
	if got.Creds.RefreshToken != "old-refresh" {
		t.Fatalf("refresh 被空串盖掉了: %q", got.Creds.RefreshToken)
	}
	// 能换出新票 ⇒ 凭据还活着，之前那次 401 说的是上一张票的事。
	if !got.Enabled || got.DisabledReason != "" {
		t.Fatalf("续票后没启用回来: enabled=%v reason=%q", got.Enabled, got.DisabledReason)
	}

	// 上游轮换了就要存新的。
	if err := s.SetTokens(a.ID, "newer-access", "rotated-refresh"); err != nil {
		t.Fatal(err)
	}
	if got := accountByID(t, s, a.ID); got.Creds.RefreshToken != "rotated-refresh" {
		t.Fatalf("轮换后的 refresh 没存: %q", got.Creds.RefreshToken)
	}

	// 空 access 一律拒：那等于把账号废掉。
	if err := s.SetTokens(a.ID, "", "r"); err == nil {
		t.Fatal("空 access_token 居然写进去了")
	}
}

// 续票**不许**把上传者手动停用的号打开。
//
// 上传者在 /a/{token} 点「停用账号」，页面承诺「不再用它发请求，接入 key 失效」。而他交的是
// 整份 auth.json ⇒ 有 refresh_token ⇒ 续票循环（refreshDue 遍历的是全部账号）最长九天后
// 就会摸到他。原来 SetTokens 无条件启用，于是这个号被池子静默打开、重新拿去铸票，
// key 也重新能用，而页面上看不出发生过任何事。
//
// 区分两种停用的数据本来就有：DisableAccount（自动）写原因，SetAccountEnabled（手动）
// 刻意留空——「能换出新票说明凭据还活着」只对前者成立，后者说的是「我先不参与」。
func TestSetTokensNeverReopensAManuallyDisabledAccount(t *testing.T) {
	s := open(t, t.TempDir())
	a, err := s.AddAccount(types.Account{
		Label: "手动暂停的", Enabled: true,
		Creds: types.Creds{AccessToken: "t", RefreshToken: "r", AccountID: "acct-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetAccountEnabled(a.ID, false); err != nil { // 上传者自己点的
		t.Fatal(err)
	}

	if err := s.SetTokens(a.ID, "new-access", "new-refresh"); err != nil {
		t.Fatal(err)
	}
	got := accountByID(t, s, a.ID)
	if got.Enabled {
		t.Fatal("续票把上传者手动停用的号打开了")
	}
	// 票照换：凭据要保持新鲜，否则他重新启用时拿到的是一张九天前的死票。
	if got.Creds.AccessToken != "new-access" || got.Creds.RefreshToken != "new-refresh" {
		t.Fatalf("票没换: %+v", got.Creds)
	}
}

// 自动停用要留下原因，手动开关要清掉它。
func TestDisableAccountRecordsReasonAndManualTogglesClearIt(t *testing.T) {
	s := open(t, t.TempDir())
	a, err := s.AddAccount(types.Account{
		Label: "x", Enabled: true,
		Creds: types.Creds{AccessToken: "t", AccountID: "acct-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	const reason = `铸票 HTTP 400：{"detail":"model is not supported"}`
	if err := s.DisableAccount(a.ID, reason); err != nil {
		t.Fatal(err)
	}
	got := accountByID(t, s, a.ID)
	if got.Enabled || got.DisabledReason != reason {
		t.Fatalf("没记下原因: enabled=%v reason=%q", got.Enabled, got.DisabledReason)
	}
	// 人手点「重新启用」后那条原因就是上一次的事了，不能留着。
	if err := s.SetAccountEnabled(a.ID, true); err != nil {
		t.Fatal(err)
	}
	if got := accountByID(t, s, a.ID); !got.Enabled || got.DisabledReason != "" {
		t.Fatalf("手动启用没清掉原因: enabled=%v reason=%q", got.Enabled, got.DisabledReason)
	}
}

// 对一个**已经自动停用**的号再 POST 一次 enabled=false 不许把理由抹掉。
//
// 抹掉之后它在管理页上和「手动暂停」一模一样，于是运营方会反复打开一个凭据已经
// 死了的号（线上刚发生过 token_revoked 那次）。
func TestRepeatedDisableKeepsTheReason(t *testing.T) {
	s := open(t, t.TempDir())
	a, err := s.AddAccount(types.Account{
		Label: "x", Enabled: true,
		Creds: types.Creds{AccessToken: "t", AccountID: "acct-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	const reason = "铸票 HTTP 401：token_revoked"
	if err := s.DisableAccount(a.ID, reason); err != nil {
		t.Fatal(err)
	}
	// 开关没变（已经是停用）⇒ 什么都不该动。
	if err := s.SetAccountEnabled(a.ID, false); err != nil {
		t.Fatal(err)
	}
	if got := accountByID(t, s, a.ID); got.Enabled || got.DisabledReason != reason {
		t.Fatalf("重复停用把理由抹了: enabled=%v reason=%q", got.Enabled, got.DisabledReason)
	}
	// 另一侧：真的翻了开关（启用）才清理由——那条话说的是上一次的事。
	if err := s.SetAccountEnabled(a.ID, true); err != nil {
		t.Fatal(err)
	}
	if got := accountByID(t, s, a.ID); !got.Enabled || got.DisabledReason != "" {
		t.Fatalf("启用没清掉理由: enabled=%v reason=%q", got.Enabled, got.DisabledReason)
	}
}

// 老 state.json 里的 pairs 行没有 cflb_expires_at：那时候 ExpiresAt 存的是 __oailb
// 的 exp，整篇**虚报 300 秒**。加载时必须收口到钉路由那个 cookie 上——
// 零值绝不能退化成「不过期」，也不能退化成零值（那会让一池子票全被当成死票）。
func TestOpenNormalizesOldPairExpiry(t *testing.T) {
	dir := t.TempDir()
	oailbExp := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	// 手写一份老格式：没有 cflb_expires_at 这个键。
	old := `{"pairs":{"unified-7":{"Gateway":"unified-7","Cookie":"__cflb=a; __oailb=b",` +
		`"ExpiresAt":"` + oailbExp.Format(time.RFC3339) + `","MintedBy":1}}}`
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	p, ok := open(t, dir).Pair("unified-7")
	if !ok {
		t.Fatal("老记录没加载进来")
	}
	want := oailbExp.Add(-(types.OailbLifetime - types.PairLifetime))
	if !p.CflbExpiresAt.Equal(want) {
		t.Fatalf("cflb 死期 %v，应按兜底算成 %v", p.CflbExpiresAt, want)
	}
	if !p.ExpiresAt.Equal(want) {
		t.Fatalf("ExpiresAt 没收口：%v，应为 %v（虚报 300 秒就会发出路由已死的票）", p.ExpiresAt, want)
	}

	// 另一侧：带了新字段的记录一个字都不许动。
	dir2 := t.TempDir()
	cflb := oailbExp.Add(-time.Minute)
	now := `{"pairs":{"unified-8":{"Gateway":"unified-8","Cookie":"c","ExpiresAt":"` +
		cflb.Format(time.RFC3339) + `","CflbExpiresAt":"` + cflb.Format(time.RFC3339) + `"}}}`
	if err := os.WriteFile(filepath.Join(dir2, "state.json"), []byte(now), 0o600); err != nil {
		t.Fatal(err)
	}
	p2, _ := open(t, dir2).Pair("unified-8")
	if !p2.ExpiresAt.Equal(cflb) || !p2.CflbExpiresAt.Equal(cflb) {
		t.Fatalf("新格式的记录被改了: %+v", p2)
	}
}

func accountByID(t *testing.T, s *Store, id int64) types.Account {
	t.Helper()
	for _, a := range s.Accounts() {
		if a.ID == id {
			return a
		}
	}
	t.Fatalf("账号 %d 不见了", id)
	return types.Account{}
}

// 同一个上游凭据再上传不许建第二行：槽位表按池内行 ID 记，两行就是两套互不相识的
// 账本，池子会以为两边都有满血窗口，其实烧的是同一个上游账号。
func TestAddAccountDedupesByUpstreamID(t *testing.T) {
	s := open(t, t.TempDir())
	first, err := s.AddAccount(types.Account{Label: "我的号", PlanType: "plus",
		Creds:   types.Creds{AccessToken: "tok1", AccountID: "acct-x", RefreshToken: "ref1"},
		Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	// 它在某个网关上已经烧过一次。这段历史必须留在同一行上。
	if err := s.UpdateSlot(first.ID, "unified-7", func(sl *types.Slot) {
		sl.LastVerdict = types.VerdictDegraded
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.DisableAccount(first.ID, "上游否决"); err != nil {
		t.Fatal(err)
	}

	again, err := s.AddAccount(types.Account{Label: "", PlanType: "pro",
		Creds: types.Creds{AccessToken: "tok2", AccountID: "acct-x"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != first.ID {
		t.Fatalf("同一个上游凭据建了第二行: %d vs %d", again.ID, first.ID)
	}
	if n := len(s.Accounts()); n != 1 {
		t.Fatalf("账号行数应为 1，实为 %d", n)
	}
	if again.Creds.AccessToken != "tok2" {
		t.Fatal("没换上新 access_token")
	}
	// 上游不轮换 refresh_token 时回空串，别把好的那个覆盖掉。
	if again.Creds.RefreshToken != "ref1" {
		t.Fatalf("refresh_token 被空串覆盖了: %q", again.Creds.RefreshToken)
	}
	if again.PlanType != "pro" {
		t.Fatalf("plan 没更新: %q", again.PlanType)
	}
	if again.Label != "我的号" {
		t.Fatalf("空备注名把原来的盖掉了: %q", again.Label)
	}
	if !again.Enabled || again.DisabledReason != "" {
		t.Fatalf("重新上传没解开停用: enabled=%v reason=%q", again.Enabled, again.DisabledReason)
	}
	if again.ViewToken != first.ViewToken {
		t.Fatal("view_token 变了，上传者手里的旧链接会废掉")
	}
	if sl, ok := s.Slot(first.ID, "unified-7"); !ok || sl.LastVerdict != types.VerdictDegraded {
		t.Fatalf("槽位历史丢了: %+v", sl)
	}
}

// 一次性迁移：加 OwnerID 之前写下的托管账号要挂到一个自动建出来的 owner 下，
// 否则现有那几个账号的 /u/ 总览页谁都进不去。
//
// **幂等**是这条迁移唯一要紧的性质：再开一次不许又建一个 owner（那样每次重启都多
// 一页，而上一次给出去的那个令牌还活着）。而且令牌必须真的落了盘——没落盘就等于
// 每次重启换一个，管理页上给出去的链接立刻失效。
func TestOpenMigratesAccountsToOneOwnerIdempotently(t *testing.T) {
	dir := t.TempDir()
	// 手写一份老格式：两个托管行 + 一个匿名占位行，都没有 owner_id 这个键。
	old := `{"accounts":[` +
		`{"ID":1,"Label":"pro1","ViewToken":"vt1","Enabled":true,` +
		`"Creds":{"AccessToken":"tok1","AccountID":"acct-1"}},` +
		`{"ID":2,"Label":"plus2","ViewToken":"vt2","Enabled":true,` +
		`"Creds":{"AccessToken":"tok2","AccountID":"acct-2"}},` +
		`{"ID":3,"Label":"匿名 x","Enabled":true,"Creds":{"AccountID":"anon:x"}}]}`
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}

	s := open(t, dir)
	owners := s.Owners()
	if len(owners) != 1 {
		t.Fatalf("该正好建一个 owner，实为 %d: %+v", len(owners), owners)
	}
	o := owners[0]
	if len(o.Token) < 20 {
		t.Fatalf("owner 令牌太短或没生成: %q", o.Token)
	}
	if !strings.Contains(o.Label, "迁移") {
		t.Fatalf("备注名看不出是迁移来的: %q", o.Label)
	}
	for _, id := range []int64{1, 2} {
		if got := accountByID(t, s, id).OwnerID; got != o.ID {
			t.Fatalf("账号 %d 没挂上去: OwnerID=%d", id, got)
		}
	}
	// 匿名占位行不挂：它不是谁托管的，挂上去那个人的总览页会多出几行他没上传过的东西。
	if got := accountByID(t, s, 3).OwnerID; got != 0 {
		t.Fatalf("匿名占位行被挂到 owner %d 下了", got)
	}
	if n := len(s.AccountsByOwner(o.ID)); n != 2 {
		t.Fatalf("名下该有 2 个账号，实为 %d", n)
	}
	// ownerID 为 0 不许回「所有没归属的行」——0 是占位行的常态值。
	if got := s.AccountsByOwner(0); got != nil {
		t.Fatalf("AccountsByOwner(0) 回了 %d 行", len(got))
	}
	if _, ok := s.OwnerByToken(""); ok {
		t.Fatal("空令牌认出了一个 owner（等于把总览页开给任何人）")
	}

	// 再开一次：一个都不许多建，令牌一个字都不许变。
	s2 := open(t, dir)
	owners2 := s2.Owners()
	if len(owners2) != 1 || owners2[0].Token != o.Token || owners2[0].ID != o.ID {
		t.Fatalf("迁移不幂等：第二次开出 %+v，第一次是 %+v", owners2, owners)
	}
}

// PruneEmptyOwners 不许删「刚建出来、账号还没挂上去」的 owner。
//
// 这是并发上传的悬垂引用：handleUpload 先 AddOwner 落盘再 AddAccount，甲走到两步中间时，
// 乙的失败路径调 PruneEmptyOwners，它锁住整个 store 后看到甲那个空 owner 一并删掉；
// 随后甲的 AddAccount 照常成功，OwnerID 指向一个不存在的行。AddAccount 不校验 OwnerID、
// migrateOwners 只收养 OwnerID==0 ⇒ 悬垂静默入库，重启也不自愈。
func TestPruneEmptyOwnersSparesFreshlyCreatedOnes(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)

	// 甲：刚建完 owner，还没 AddAccount。
	jia, err := s.AddOwner("甲")
	if err != nil {
		t.Fatal(err)
	}
	// 乙：上传失败后清理。
	if n, err := s.PruneEmptyOwners(); err != nil || n != 0 {
		t.Fatalf("把刚建出来的空 owner 删了：removed=%d err=%v", n, err)
	}
	// 甲接着把账号挂上去，归属必须还在。
	if _, err := s.AddAccount(types.Account{
		Label: "pro1", Creds: types.Creds{AccessToken: "tok1", AccountID: "acct-1"}, OwnerID: jia.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.OwnerByToken(jia.Token); !ok {
		t.Fatal("甲的 owner 没了：/u/ 链接打不开，而账号已落盘指向它")
	}
	if n := len(s.AccountsByOwner(jia.ID)); n != 1 {
		t.Fatalf("甲名下该有 1 个账号，实为 %d", n)
	}

	// 宽限期过了的空 owner 照旧要收走，否则就只是把泄漏推后。
	// 手写一份 CreatedAt 很旧的，重开一次让 Open 里那遍 prune 跑到。
	old := `{"owners":[{"ID":9,"Token":"` + strings.Repeat("o", 32) +
		`","Label":"陈年孤儿","CreatedAt":"2020-01-01T00:00:00Z"}]}`
	dir2 := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir2, "state.json"), []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := open(t, dir2).Owners(); len(got) != 0 {
		t.Fatalf("陈年空 owner 没被收走: %+v", got)
	}
}

// 新建的托管账号必须有归属（调用方先建 owner 再传进来），而**已经属于别人的账号
// 去重命中时不许改归属**：否则一份公开流传的凭据就是一把搬家钥匙，谁贴一次就把
// 别人的账号搬到自己的总览页下，连带那把接入 key。
func TestAddAccountClaimsOrphansButNeverReassigns(t *testing.T) {
	s := open(t, t.TempDir())
	a, err := s.AddOwner("甲")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.AddOwner("乙")
	if err != nil {
		t.Fatal(err)
	}
	if a.Token == b.Token || a.ID == b.ID {
		t.Fatalf("两个 owner 撞上了: %+v %+v", a, b)
	}

	mine, err := s.AddAccount(types.Account{Label: "甲的号", OwnerID: a.ID, Enabled: true,
		Creds: types.Creds{AccessToken: "tok", AccountID: "acct-x"}})
	if err != nil {
		t.Fatal(err)
	}
	if mine.OwnerID != a.ID {
		t.Fatalf("归属没落上: %d", mine.OwnerID)
	}

	// 乙拿同一份凭据再上传一次：**一个字段都不动**，原样返回甲那一行。
	// 从前这里会更新 token/plan（只守住归属），那是审查 B2 的静默覆盖攻击——
	// oauth 不验签，乙拿一张自制的 acct-x 票就能把甲存着的 access_token 覆盖掉、
	// 改掉它的 plan、解开它自己关的停用，把甲托管的凭据弄废。归属之外，这些字段
	// 也只有甲能改。
	again, err := s.AddAccount(types.Account{PlanType: "pro", OwnerID: b.ID, Enabled: true,
		Creds: types.Creds{AccessToken: "tok2", AccountID: "acct-x"}})
	if err != nil {
		t.Fatal(err)
	}
	if again.OwnerID != a.ID {
		t.Fatalf("归属被抢走了: 现在挂在 owner %d 下，应仍是 %d", again.OwnerID, a.ID)
	}
	if again.Creds.AccessToken != "tok" || again.PlanType != "" {
		t.Fatalf("别人的凭据被伪造上传覆盖了（B2）: token=%q plan=%q", again.Creds.AccessToken, again.PlanType)
	}
	if n := len(s.AccountsByOwner(b.ID)); n != 0 {
		t.Fatalf("乙名下冒出了 %d 个账号", n)
	}

	// 孤儿行（老文件、手改的）照旧**补**得上：只补不改。
	s.mu.Lock()
	s.st.Accounts = append(s.st.Accounts, types.Account{ID: 99, Label: "孤儿", Enabled: true,
		Creds: types.Creds{AccessToken: "tok9", AccountID: "acct-orphan"}})
	s.mu.Unlock()
	claimed, err := s.AddAccount(types.Account{OwnerID: b.ID, Enabled: true,
		Creds: types.Creds{AccessToken: "tok9b", AccountID: "acct-orphan"}})
	if err != nil {
		t.Fatal(err)
	}
	if claimed.OwnerID != b.ID {
		t.Fatalf("没归属的行该补上归属，实为 %d", claimed.OwnerID)
	}

	// 落盘了：重开一次还是这个样子。
	s2 := open(t, s.dir)
	if got := accountByID(t, s2, mine.ID).OwnerID; got != a.ID {
		t.Fatalf("重启后归属变了: %d", got)
	}
	if n := len(s2.Owners()); n != 2 {
		t.Fatalf("重开之后 owner 数变成 %d（迁移又建了一个？）", n)
	}
}

// 备份：覆盖 state.json 之前先留两份。这个文件里的网关和槽位历史是几百发上游请求
// 换来的，每一发都烧掉一个窗口，丢了不是「重跑一下」。
func TestSaveKeepsBackups(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)

	// Open persists the one-time owner-key migration marker even for an empty
	// store. That first save has no predecessor to back up.
	if _, err := os.Stat(filepath.Join(dir, "state.json.bak")); err == nil {
		t.Fatal("首次保存不该有 .bak（那时正本还不存在）")
	}
	first, err := s.AddAccount(types.Account{Label: "一号",
		Creds: types.Creds{AccessToken: "tok", AccountID: "acct-1"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	// Backups are snapshot generations, not one full copy per mutation.
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	daily := filepath.Join(dir, "state.json.daily-"+time.Now().UTC().Format("20060102"))
	initial, err := os.ReadFile(filepath.Join(dir, "state.json.bak"))
	var initialState State
	if err != nil || json.Unmarshal(initial, &initialState) != nil ||
		!initialState.OwnerKeysMigrated || len(initialState.Accounts) != 0 {
		t.Fatalf("没有备份初始化状态: %v", err)
	}

	if _, err := s.AddAccount(types.Account{Label: "二号",
		Creds: types.Creds{AccessToken: "tok", AccountID: "acct-2"}, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// .bak 必须是**上一次**的内容：只有一号，没有二号。
	b, err := os.ReadFile(filepath.Join(dir, "state.json.bak"))
	if err != nil {
		t.Fatalf("没留 .bak: %v", err)
	}
	if !strings.Contains(string(b), "一号") || strings.Contains(string(b), "二号") {
		t.Fatal(".bak 不是上一次保存的内容")
	}
	// 当天那份只落第一次，不该被后续保存刷掉。
	d, err := os.ReadFile(daily)
	if err != nil {
		t.Fatalf("没留当天备份: %v", err)
	}
	if strings.Contains(string(d), "二号") {
		t.Fatal("当天备份被后面的保存刷掉了")
	}
	_ = first

	// 超过 dailyKeep 份就砍最老的：文件名里的日期字典序 = 时间序。
	for i := 1; i <= dailyKeep+3; i++ {
		name := filepath.Join(dir, fmt.Sprintf("state.json.daily-2026010%d", i%10))
		if err := os.WriteFile(name, []byte("{}"), filePerm); err != nil {
			t.Fatal(err)
		}
	}
	s.pruneDaily("state.json.daily-")
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "state.json.daily-") {
			n++
		}
	}
	if n != dailyKeep {
		t.Fatalf("修剪后应剩 %d 份，实为 %d", dailyKeep, n)
	}
}

// (账号 × 区域) 台账是**终身计数**，必须跟着 state.json 落盘：挑铸票者靠「这个号
// 哪个区域还没走过」，重启清零的话每次重启都会把九个区域从头摸一遍。
func TestRegionMintLedgerPersists(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	s := open(t, dir)
	a, err := s.AddAccount(types.Account{Label: "pro1",
		Creds: types.Creds{AccessToken: "tok", AccountID: "acc"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 3; i++ {
		if err := s.NoteRegionMint(a.ID, "us-east", "", at); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.NoteRegionMint(a.ID, "oceania", "", at); err != nil {
		t.Fatal(err)
	}
	// 反查不到区域的出口（手改 state.json 才有）不记：空 key 会和别的格子串在一起。
	if err := s.NoteRegionMint(a.ID, "", "", at); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	s2 := open(t, dir)
	mints := s2.RegionMints()
	if len(mints) != 2 {
		t.Fatalf("重开后台账 %d 格，应为 2: %+v", len(mints), mints)
	}
	m := mints[MintKey(a.ID, "us-east")]
	if m.Count != 3 || !m.LastAt.Equal(at) || m.AccountID != a.ID || m.RegionKey != "us-east" {
		t.Fatalf("us-east 那一格没持久化对: %+v", m)
	}
	// 返回的是副本：调用方改它不该污染库里的那张表。
	mints[MintKey(a.ID, "us-east")] = types.RegionMint{Count: 99}
	if again := s2.RegionMints()[MintKey(a.ID, "us-east")]; again.Count != 3 {
		t.Fatalf("RegionMints 给出去的不是副本: %+v", again)
	}
}

// 删号要连它的台账一起删：账号 ID 是 max+1 分配的，会被下一个号复用，
// 留着就是把前任的终身计数算在新人头上。
func TestDeleteAccountDropsRegionMints(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "data"))
	a, err := s.AddAccount(types.Account{Label: "a",
		Creds: types.Creds{AccessToken: "t1", AccountID: "acc-a"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.AddAccount(types.Account{Label: "b",
		Creds: types.Creds{AccessToken: "t2", AccountID: "acc-b"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, id := range []int64{a.ID, b.ID} {
		if err := s.NoteRegionMint(id, "us-east", "", now); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeleteAccount(a.ID); err != nil {
		t.Fatal(err)
	}
	mints := s.RegionMints()
	if _, ok := mints[MintKey(a.ID, "us-east")]; ok {
		t.Fatalf("删号没带走它的台账: %+v", mints)
	}
	if _, ok := mints[MintKey(b.ID, "us-east")]; !ok {
		t.Fatalf("把别人的台账一起删了: %+v", mints)
	}
}
