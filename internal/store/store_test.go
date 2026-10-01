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
					_, _ = s.LongestRestedSlot(1, []string{"unified-2", "unified-3"})
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

func TestLongestRestedSlot(t *testing.T) {
	s := open(t, t.TempDir())
	now := time.Now().UTC()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(s.PutSlot(types.Slot{AccountID: 7, Gateway: "unified-a", LastTouch: now.Add(-time.Minute)}))
	must(s.PutSlot(types.Slot{AccountID: 7, Gateway: "unified-b", LastTouch: now.Add(-time.Hour)}))
	must(s.PutSlot(types.Slot{AccountID: 7, Gateway: "unified-c", LastTouch: now.Add(-4 * time.Hour),
		BackoffUntil: now.Add(time.Hour)}))
	must(s.PutSlot(types.Slot{AccountID: 99, Gateway: "unified-z", LastTouch: time.Time{}}))

	got, ok := s.LongestRestedSlot(7, []string{"unified-a", "unified-b", "unified-c"})
	if !ok || got.Gateway != "unified-b" {
		t.Fatalf("该挑歇得最久且不在退避里的 b，得到 %+v ok=%v", got, ok)
	}

	// 从没碰过的 (账号 × 网关) 优先级最高：首次接触才给满血窗口。
	got, ok = s.LongestRestedSlot(7, []string{"unified-a", "unified-b", "unified-never"})
	if !ok || got.Gateway != "unified-never" || !got.LastTouch.IsZero() {
		t.Fatalf("该挑从没碰过的，得到 %+v", got)
	}

	// 别的账号的槽位不能串进来。
	if got, ok := s.LongestRestedSlot(7, []string{"unified-c"}); ok {
		t.Fatalf("只有退避中的候选时该返回 false，得到 %+v", got)
	}
	if _, ok := s.LongestRestedSlot(7, nil); ok {
		t.Error("没有候选时该返回 false")
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
	// 把 state.json 换成一个目录：写 .tmp 还能成，Rename 必失败。
	breakSave(t, dir)

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
	breakSave(t, dir)
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

// breakSave 让 save 的 Rename 必定失败：目标占成一个非空目录。
func breakSave(t *testing.T, dir string) {
	t.Helper()
	p := filepath.Join(dir, stateFile)
	if err := os.RemoveAll(p); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(p, "占位"), 0o700); err != nil {
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
		fmt.Sprintf("%v", a.Creds), fmt.Sprintf("%s", a.Creds),
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

// 备份：覆盖 state.json 之前先留两份。这个文件里的网关和槽位历史是几百发上游请求
// 换来的，每一发都烧掉一个窗口，丢了不是「重跑一下」。
func TestSaveKeepsBackups(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)

	first, err := s.AddAccount(types.Account{Label: "一号",
		Creds: types.Creds{AccessToken: "tok", AccountID: "acct-1"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	// 第一次保存时正本还不存在 ⇒ 没有 .bak 可留，但当天那份该在。
	daily := filepath.Join(dir, "state.json.daily-"+time.Now().UTC().Format("20060102"))
	if _, err := os.Stat(filepath.Join(dir, "state.json.bak")); err == nil {
		t.Fatal("首次保存不该有 .bak（那时正本还不存在）")
	}

	if _, err := s.AddAccount(types.Account{Label: "二号",
		Creds: types.Creds{AccessToken: "tok", AccountID: "acct-2"}, Enabled: true}); err != nil {
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
