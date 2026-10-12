package store

import (
	"reflect"
	"testing"
	"time"

	"gwpool/internal/routing"
	"gwpool/internal/types"
)

func TestRoutingModelCacheTracksEvidenceIdentityAndExactTimeBounds(t *testing.T) {
	s := open(t, t.TempDir())
	account, err := s.AddAccount(types.Account{
		Creds: types.Creds{AccountID: "offline-donor", AccessToken: "offline"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	now := account.CreatedAt.Add(time.Second)
	for i := range 6 {
		if err := s.AppendSample(routingSample(account.ID, now.Add(time.Duration(i)*time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	check := func(at time.Time) *routing.Model {
		t.Helper()
		snapshot, err := s.RoutingModel(at)
		if err != nil {
			t.Fatal(err)
		}
		history, err := s.RoutingObservations(at)
		if err != nil || !reflect.DeepEqual(snapshot.Model, routing.New(history.Observations)) {
			t.Fatal("cached model disagrees with the exact observation set", err)
		}
		return snapshot.Model
	}
	first := check(now)
	if check(now.Add(time.Minute-time.Nanosecond)) != first {
		t.Fatal("unchanged observation interval rebuilt the model")
	}
	if check(now.Add(time.Minute)) == first {
		t.Fatal("future evidence was not admitted at its exact timestamp")
	}
	before := check(now.Add(10 * time.Minute))
	irrelevant := routingSample(account.ID, now)
	irrelevant.Purpose = types.PurposeRenew
	if err := s.AppendSample(irrelevant); err != nil {
		t.Fatal(err)
	}
	if check(now.Add(10*time.Minute)) != before {
		t.Fatal("non-training sample rebuilt an unchanged routing model")
	}
	if err := s.AppendSample(routingSample(account.ID, now.Add(9*time.Minute))); err != nil {
		t.Fatal(err)
	}
	if check(now.Add(10*time.Minute)) == before {
		t.Fatal("a subsequent inventory task ignored the previous task's new evidence")
	}
	check(now.Add(routing.HistoryRetention))
	check(now.Add(routing.HistoryRetention + time.Nanosecond))
	check(now.Add(time.Second)) // out-of-order callers must not inherit expired views
	if err := s.DeleteAccount(account.ID); err != nil {
		t.Fatal(err)
	}
	if got := check(now).ForExit(routing.DonorKey(account.Creds.AccountID), types.Exit{Region: "US"}); got.TrainingSamples != 0 {
		t.Fatal("deleted identity remained in the cached model")
	}
}
