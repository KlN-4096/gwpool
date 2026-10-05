package sched

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

var errDeliveryStorage = errors.New("injected delivery persistence failure")

type failingDeliveryStore struct{ Store }

func (f failingDeliveryStore) UpdateSlot(int64, string, func(*types.Slot)) error {
	return errDeliveryStorage
}

type blockingDeliveryStore struct {
	Store
	once            sync.Once
	entered, resume chan struct{}
}

func (s *blockingDeliveryStore) UpdateSlot(id int64, gateway string, mutate func(*types.Slot)) error {
	s.once.Do(func() {
		close(s.entered)
		<-s.resume
	})
	return s.Store.UpdateSlot(id, gateway, mutate)
}

func TestConcurrentConsumerCannotSelectUncommittedTicket(t *testing.T) {
	s, st, _, clk := rig(t, types.ModePool)
	seedPairs(st, clk)
	s.opt.Live = liveCfg(func(c *config.Config) { c.VerifyFullAtDeliver = false })
	blocking := &blockingDeliveryStore{Store: st, entered: make(chan struct{}), resume: make(chan struct{})}
	s.st = blocking
	t.Cleanup(func() {
		select {
		case <-blocking.resume:
		default:
			close(blocking.resume)
		}
	})
	first := make(chan types.Delivery, 1)
	go func() {
		delivery, err := s.Deliver(context.Background(), 1, Request{Force: true})
		if err != nil {
			t.Error(err)
		}
		first <- delivery
	}()
	select {
	case <-blocking.entered:
	case <-time.After(time.Second):
		t.Fatal("first delivery never reached persistence")
	}
	ctx, cancel := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() {
		_, err := s.Deliver(ctx, 1, Request{Force: true})
		second <- err
	}()
	cancel()
	select {
	case err := <-second:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("canceled waiter was served", err)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer lock ignored request cancellation")
	}
	close(blocking.resume)
	d1 := <-first
	d2, err := s.Deliver(context.Background(), 1, Request{Force: true})
	if err != nil || d1.Gateway == d2.Gateway {
		t.Fatal("force reused the preceding committed ticket", err)
	}
}

func TestDeliveryDoesNotAcknowledgeFailedPersistence(t *testing.T) {
	for _, anonymous := range []bool{false, true} {
		t.Run(map[bool]string{false: "managed", true: "anonymous"}[anonymous], func(t *testing.T) {
			s, st, _, clk := rig(t, types.ModePool)
			seedPairs(st, clk)
			s.opt.Live = liveCfg(func(c *config.Config) { c.VerifyFullAtDeliver = false })
			s.st = failingDeliveryStore{Store: st}
			var delivery types.Delivery
			var err error
			if anonymous {
				delivery, err = s.DeliverAnonymous(context.Background(), "external", Request{Force: true}, true)
			} else {
				delivery, err = s.Deliver(context.Background(), 1, Request{Force: true})
			}
			if !errors.Is(err, errDeliveryStorage) || delivery.Cookie != "" {
				t.Fatalf("failed storage returned a ticket: has_cookie=%v error=%v", delivery.Cookie != "", err)
			}
		})
	}
}
