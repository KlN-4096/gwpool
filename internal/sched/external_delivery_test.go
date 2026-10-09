package sched

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gwpool/internal/types"
)

func fixedExternalClock(at time.Time) func() time.Time { return func() time.Time { return at } }

func TestExternalAdmissionSamplesClockInsideLock(t *testing.T) {
	var consumers externalConsumers
	now := time.Now()
	called, done := make(chan struct{}), make(chan struct{})
	consumers.mu.Lock()
	go func() {
		consumers.admit("member", func() time.Time { close(called); return now })
		close(done)
	}()
	select {
	case <-called:
		consumers.mu.Unlock()
		<-done
		t.Fatal("timestamp sampled before admission lock")
	case <-time.After(20 * time.Millisecond):
	}
	consumers.mu.Unlock()
	<-done
	if !consumers.recent["member"][0].Equal(now) {
		t.Fatal("wrong admission timestamp")
	}
}

func TestExternalConcurrentAdmissionAndDemandExpiry(t *testing.T) {
	var consumers externalConsumers
	now := time.Now()
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for range 80 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if consumers.admit("member", fixedExternalClock(now)) == 0 {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != externalPerSecond {
		t.Fatalf("concurrent admission=%d", admitted.Load())
	}
	consumers.delivered("member", "g1", now)
	consumers.delivered("member", "g2", now)
	consumers.delivered("other", "g1", now)
	if consumers.busiest(now.Add(-time.Hour)) != 2 {
		t.Fatal("recent supply signal was lost")
	}
	if consumers.busiest(now) != 0 || len(consumers.demand) != 0 {
		t.Fatal("expired demand became permanent scheduling state")
	}
}

func TestExternalDemandCannotBeRewoundByLateRequest(t *testing.T) {
	var consumers externalConsumers
	now := time.Now()
	consumers.delivered("member", "g", now)
	consumers.delivered("member", "g", now.Add(-time.Hour))
	if consumers.busiest(now.Add(-time.Second)) != 1 {
		t.Fatal("older concurrent completion erased recent demand")
	}
}

func TestExternalDeliveryDoesNotCreateAccountsOrSlots(t *testing.T) {
	s, st, _, _ := anonRig(t, 6, 1, 1)
	accounts, slots := len(st.Accounts()), len(st.Slots())
	for i := range 401 {
		d, err := s.DeliverAnonymous(context.Background(), fmt.Sprintf("member-%d", i),
			Request{Gateway: "unified-1"}, true)
		if err != nil || d.Gateway != "unified-1" {
			t.Fatalf("consumer %d: gateway=%q err=%v", i, d.Gateway, err)
		}
	}
	if len(st.Accounts()) != accounts || len(st.Slots()) != slots {
		t.Fatalf("external delivery created persistent scheduling state: accounts %d -> %d, slots %d -> %d",
			accounts, len(st.Accounts()), slots, len(st.Slots()))
	}
	if len(mintedOf(s)) != 0 {
		t.Fatal("external requests minted a ticket")
	}
}

func TestExternalConsumerSlidingRateWindows(t *testing.T) {
	var consumers externalConsumers
	now := time.Now().UTC()
	for i := range externalPerSecond {
		if retry := consumers.admit("member", fixedExternalClock(now)); retry != 0 {
			t.Fatalf("request %d rejected early: %v", i+1, retry)
		}
	}
	if retry := consumers.admit("member", fixedExternalClock(now)); retry != time.Second {
		t.Fatalf("13th request retry=%v, want 1s", retry)
	}
	if retry := consumers.admit("other", fixedExternalClock(now)); retry != 0 {
		t.Fatal("different member was limited")
	}
	for second := 1; second < 37; second++ {
		for range externalPerSecond {
			if retry := consumers.admit("member", fixedExternalClock(now.Add(time.Duration(second)*time.Second))); retry != 0 {
				t.Fatalf("minute allowance rejected early: %v", retry)
			}
		}
	}
	if retry := consumers.admit("member", fixedExternalClock(now.Add(37*time.Second))); retry != 23*time.Second {
		t.Fatalf("445th request retry=%v, want 23s", retry)
	}
	if retry := consumers.admit("member", fixedExternalClock(now.Add(time.Minute))); retry != 0 {
		t.Fatalf("window boundary rejected: %v", retry)
	}
	consumers.admit("new", fixedExternalClock(now.Add(2*time.Minute)))
	if _, exists := consumers.recent["other"]; exists {
		t.Fatal("expired rate state was not reclaimed")
	}
}

func TestExternalMissingStockConsumesRateAllowance(t *testing.T) {
	s, _, _, _ := anonRig(t, 6, 1, 1)
	for range externalPerSecond {
		if _, err := s.DeliverAnonymous(context.Background(), "member", Request{Gateway: "missing"}, true); !errors.Is(err, ErrNoFullSlot) {
			t.Fatalf("expected missing stock, got %v", err)
		}
	}
	var limited *ConsumerRateLimit
	if _, err := s.DeliverAnonymous(context.Background(), "member", Request{Gateway: "unified-1"}, true); !errors.As(err, &limited) {
		t.Fatalf("missing requests did not consume allowance: %v", err)
	}
}

func TestExternalDeliveryRequiresExplicitGatewayAndIgnoresOldSlots(t *testing.T) {
	s, st, clk, _ := anonRig(t, 6, 1, 1)
	if _, err := s.DeliverAnonymous(context.Background(), "member", Request{}, true); err == nil {
		t.Fatal("pool selected a gateway instead of requiring the client's decision")
	}
	legacy, err := st.AddAccount(types.Account{Creds: types.Creds{AccountID: "anon:member"}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	st.slots[slotKey(legacy.ID, "unified-1")] = types.Slot{
		AccountID: legacy.ID, Gateway: "unified-1", LastTouch: clk.now(),
		BackoffUntil: clk.now().Add(time.Hour),
	}
	before := st.slots[slotKey(legacy.ID, "unified-1")]
	for range 2 {
		d, err := s.DeliverAnonymous(context.Background(), "member", Request{Gateway: "unified-1"}, true)
		if err != nil || d.Gateway != "unified-1" || d.VerifiedFull {
			t.Fatalf("legacy consumer state affected delivery: %+v err=%v", d, err)
		}
	}
	if st.slots[slotKey(legacy.ID, "unified-1")] != before {
		t.Fatal("external delivery mutated legacy slots")
	}
}
