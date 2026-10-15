package sched

import (
	"context"
	"errors"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

func TestAdminKeySkipsSecondAndMinuteLimitsWithoutChargingOrdinaryBucket(t *testing.T) {
	s, _, clk, _ := anonRig(t, 1, 1, 1)
	req := Request{Gateway: "unified-1", AdminKey: true}
	for range externalPerMinute + 1 {
		if _, err := s.DeliverAnonymous(context.Background(), "member", req, true); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.external.recent) != 0 {
		t.Fatal("AdminKey consumed ordinary member admission budget")
	}
	ordinary := Request{Gateway: req.Gateway}
	for range externalPerSecond {
		if _, err := s.DeliverAnonymous(context.Background(), "member", ordinary, true); err != nil {
			t.Fatal("AdminKey requests reduced ordinary allowance")
		}
	}
	var limited *ConsumerRateLimit
	if _, err := s.DeliverAnonymous(context.Background(), "member", ordinary, true); !errors.As(err, &limited) {
		t.Fatal("ordinary second limit bypassed")
	}
	if _, err := s.DeliverAnonymous(context.Background(), "member", req, true); err != nil {
		t.Fatal("ordinary second exhaustion affected AdminKey")
	}
	id := types.ExternalConsumerID("member")
	// Fill the minute bucket without also filling the most recent second.
	s.external.recent[id] = make([]time.Time, externalPerMinute)
	for i := range s.external.recent[id] {
		s.external.recent[id][i] = clk.now().Add(-2 * time.Second)
	}
	if _, err := s.DeliverAnonymous(context.Background(), "member", ordinary, true); !errors.As(err, &limited) {
		t.Fatal("ordinary minute limit bypassed")
	}
	if _, err := s.DeliverAnonymous(context.Background(), "member", req, true); err != nil {
		t.Fatal("minute exhaustion affected AdminKey")
	}
	if len(s.external.demand[id]) != 1 {
		t.Fatal("AdminKey delivery stopped recording demand")
	}
}

func TestAdminKeyNeverRelaxesStockInputsCancellationOrPublicAuthentication(t *testing.T) {
	s, st, clk, _ := anonRig(t, 1, 1, 1)
	req := Request{Gateway: "unified-1", AdminKey: true}
	for range externalPerSecond {
		if _, err := s.DeliverAnonymous(context.Background(), "public", req, false); err != nil {
			t.Fatal(err)
		}
	}
	var limited *ConsumerRateLimit
	if _, err := s.DeliverAnonymous(context.Background(), "public", req, false); !errors.As(err, &limited) {
		t.Fatal("unauthenticated marker bypassed frequency checks")
	}
	s.opt.Live = liveCfg(func(c *config.Config) { c.PublicWhenSurplus.Enabled = false })
	if _, err := s.DeliverAnonymous(context.Background(), "public", req, false); !errors.Is(err, ErrPublicClosed) {
		t.Fatal("unauthenticated marker bypassed public closure")
	}
	if _, err := s.DeliverAnonymous(context.Background(), "admin", req, true); err != nil {
		t.Fatal("authorized AdminKey obeyed public closure")
	}
	for _, input := range []struct {
		member string
		req    Request
	}{
		{"", req}, {"bad\nmember", req}, {"member", Request{AdminKey: true}},
	} {
		if _, err := s.DeliverAnonymous(context.Background(), input.member, input.req, true); !errors.Is(err, ErrExternalRequest) {
			t.Fatal("AdminKey accepted malformed inputs")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.DeliverAnonymous(ctx, "admin", req, true); !errors.Is(err, context.Canceled) {
		t.Fatal("AdminKey ignored cancellation")
	}
	pair := st.pairs[req.Gateway]
	pair.ExpiresAt = clk.now().Add(-time.Second)
	st.pairs[req.Gateway] = pair
	if _, err := s.DeliverAnonymous(context.Background(), "admin", req, true); !errors.Is(err, ErrNoFullSlot) {
		t.Fatal("AdminKey received expired stock")
	}
	if minter := s.mint.(*fakeMinter); len(minter.mintedGateways()) != 0 || minter.echoes != 0 {
		t.Fatal("AdminKey triggered an upstream mint/verification")
	}
}
