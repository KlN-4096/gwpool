package sched

import (
	"reflect"
	"testing"
	"time"

	"gwpool/internal/routes"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

type countedRouteStore struct {
	*store.Store
	reads int
}

func (s *countedRouteStore) RouteStates(now time.Time) map[string]types.RouteState {
	s.reads++
	return s.Store.RouteStates(now)
}

func TestRouteCacheExpiresRevisesAndRechecksClaimedStateNarrowly(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	now := time.Now().UTC()
	descriptor := routes.Describe(types.Creds{AccountID: "offline"}, types.Exit{Region: "US"},
		types.RouteSource{ID: routes.Digest("offline")}, routes.Bare, "")
	attempt := types.RouteAttempt{Route: descriptor, StartedAt: now, At: now,
		Failure: "transport", Expected: "unified-1"}
	if _, err := st.CommitMint(types.MintCommit{Route: &attempt, At: now}); err != nil {
		t.Fatal(err)
	}
	counted := &countedRouteStore{Store: st}
	s := &Scheduler{st: counted}
	key := routes.Key(descriptor)
	before := s.routeStates(now)
	for _, at := range []time.Time{now, now.Add(time.Second), now.Add(time.Minute)} {
		if !reflect.DeepEqual(s.routeStates(at), st.RouteStates(at)) {
			t.Fatal("cached route evidence changed")
		}
	}
	if counted.reads != 1 {
		t.Fatal("route table was copied again without a revision or expiry")
	}
	attempt.At, attempt.StartedAt = now.Add(time.Second), now.Add(time.Second)
	if _, err := st.CommitMint(types.MintCommit{Route: &attempt, At: attempt.At}); err != nil {
		t.Fatal(err)
	}
	if s.currentRouteState(key, attempt.At) == before[key] || counted.reads != 1 {
		t.Fatal("claimed route recheck was stale or copied unrelated routes")
	}
	after := s.routeStates(attempt.At)
	if after[key] == before[key] || counted.reads != 2 {
		t.Fatal("committed route did not invalidate the cached selection snapshot")
	}
	expiry := attempt.At.Add(routes.Retention)
	if len(s.routeStates(expiry)) != 0 || len(s.routeStates(now)) != 1 {
		t.Fatal("expiry or backwards-time route read changed the source semantics")
	}
}
