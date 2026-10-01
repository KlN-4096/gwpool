package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"gwpool/internal/sched"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

// stubMinter 只为了把 sched.Scheduler 立起来：发现路径一发上游都不打
// （真打那一发在 scan.ScanRegion 里，这些用例只看入队决策）。
type stubMinter struct{}

func (stubMinter) Mint(context.Context, types.Creds, types.Exit, string, string) (types.MintResult, error) {
	return types.MintResult{}, nil
}

func (stubMinter) VerifyFull(context.Context, types.Creds, types.Exit, string, string, string) (*types.Verdict, error) {
	return nil, nil
}

func (stubMinter) Geo(context.Context, types.Exit, string) (types.Observed, error) {
	return types.Observed{}, nil
}

func (stubMinter) NewSID(bool) (string, error) { return "sid", nil }

func (stubMinter) Rate(string) (map[string]int, int) { return map[string]int{}, 6 }

func newTestQueue(t *testing.T, st *store.Store) *discoveryQueue {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	pool := sched.New(st, stubMinter{}, sched.Options{Mode: types.ModePool, Log: log})
	return newDiscoveryQueue(nil, pool, log)
}

// 槽位烧完时派去找新网关的那个号，必须是**别人**：要票的人自己去扫，
// 扫到的新网关会被扫描那一发当场烧掉，拿到票时已经不是首次接触。
func TestEnqueueDiscoveryPicksAnotherAccount(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	add := func(label, token string, enabled bool) int64 {
		a, err := st.AddAccount(types.Account{
			Label:   label,
			Creds:   types.Creds{AccessToken: token, AccountID: label},
			Enabled: enabled,
		})
		if err != nil {
			t.Fatal(err)
		}
		return a.ID
	}
	asker := add("asker", "tok-a", true)
	helper := add("helper", "tok-h", true)
	add("disabled", "tok-d", false) // 上游否决过的号，派它去只会再废一发
	add("anon", "", true)           // 匿名占位行，没凭据可打

	q := newTestQueue(t, st)

	q.EnqueueDiscovery(asker)
	select {
	case got := <-q.ch:
		if got.acct.ID != helper {
			t.Fatalf("派去扫的应该是 helper(%d)，实际 %d", helper, got.acct.ID)
		}
		// **一发**，不是九发：一次发现就是一个 (账号 × 区域) 组合。
		if got.r.Key == "" {
			t.Fatalf("没带区域，ScanRegion 要靠它定出口: %+v", got)
		}
	default:
		t.Fatal("没派任何账号去找新网关")
	}
	// 一次入队只放一个任务：早先那版是「上传后立刻扫九个区域」，九发全在没人要票时打出去。
	if n := len(q.ch); n != 0 {
		t.Fatalf("一次发现排了 %d 个任务，只该排一个", n+1)
	}

	// 冷却：一串 503 不能把同一个号反复派出去，否则它自己的槽位会被扫干。
	q.EnqueueDiscovery(asker)
	select {
	case got := <-q.ch:
		t.Fatalf("冷却期内又派了 %d", got.acct.ID)
	default:
	}
}

// 只有要票的那一个号时什么都不做——派它自己去扫是净亏。
func TestEnqueueDiscoveryNoOpWhenAlone(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	a, err := st.AddAccount(types.Account{
		Label:   "solo",
		Creds:   types.Creds{AccessToken: "tok", AccountID: "solo"},
		Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	q := newTestQueue(t, st)
	q.EnqueueDiscovery(a.ID)
	select {
	case got := <-q.ch:
		t.Fatalf("单账号池子不该派任何人去扫，实际派了 %d", got.acct.ID)
	default:
	}
}
