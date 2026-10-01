package main

import (
	"io"
	"log/slog"
	"testing"

	"gwpool/internal/store"
	"gwpool/internal/types"
)

// 槽位烧完时派去找新网关的那个号，必须是**别人**：要票的人自己去扫，
// 扫到的每个新网关都被扫描那一发当场烧掉，白跑九发。
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
	add("disabled", "tok-d", false) // 上游否决过的号，派它去只会再废九发
	add("anon", "", true)           // 匿名占位行，没凭据可打

	q := newScanQueue(nil, st, slog.New(slog.NewTextHandler(io.Discard, nil)))

	q.EnqueueDiscovery(asker)
	select {
	case got := <-q.ch:
		if got != helper {
			t.Fatalf("派去扫的应该是 helper(%d)，实际 %d", helper, got)
		}
	default:
		t.Fatal("没派任何账号去找新网关")
	}

	// 冷却：一串 503 不能把同一个号反复派出去，否则它自己的槽位会被扫干。
	q.EnqueueDiscovery(asker)
	select {
	case got := <-q.ch:
		t.Fatalf("冷却期内又派了 %d", got)
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

	q := newScanQueue(nil, st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	q.EnqueueDiscovery(a.ID)
	select {
	case got := <-q.ch:
		t.Fatalf("单账号池子不该派任何人去扫，实际派了 %d", got)
	default:
	}
}
