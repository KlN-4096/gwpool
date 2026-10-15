package sched

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"gwpool/internal/types"
)

const (
	externalPerSecond = 12
	externalPerMinute = 444
)

var ErrExternalRequest = errors.New("external delivery requires an account and explicit gateway")

// ConsumerRateLimit is distinct from the pool's upstream minting budget.
type ConsumerRateLimit struct{ RetryAfter time.Duration }

func (e *ConsumerRateLimit) Error() string { return "external consumer request frequency exceeded" }

type externalConsumers struct {
	mu      sync.Mutex
	recent  map[string][]time.Time
	demand  map[string]map[string]time.Time
	sweepAt time.Time
}

// Admission history only contains admitted requests, including missing stock.
// Rejected retries cannot extend the window indefinitely.
func (c *externalConsumers) admit(id string, clock func() time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Sampling under the lock preserves the chronological order required by
	// rolling windows even when a caller was descheduled before admission.
	now := clock()
	if c.recent == nil {
		c.recent = make(map[string][]time.Time)
	}
	if !now.Before(c.sweepAt) {
		for key, times := range c.recent {
			if len(times) == 0 || !times[len(times)-1].After(now.Add(-time.Minute)) {
				delete(c.recent, key)
			}
		}
		c.sweepAt = now.Add(time.Minute)
	}
	times := c.recent[id]
	first := 0
	for first < len(times) && !times[first].After(now.Add(-time.Minute)) {
		first++
	}
	if first > 0 {
		times = append(times[:0], times[first:]...)
	}
	var retry time.Duration
	if len(times) >= externalPerMinute {
		retry = times[len(times)-externalPerMinute].Add(time.Minute).Sub(now)
	}
	if len(times) >= externalPerSecond {
		retry = max(retry, times[len(times)-externalPerSecond].Add(time.Second).Sub(now))
	}
	if retry > 0 {
		c.recent[id] = times
		return retry
	}
	c.recent[id] = append(times, now)
	return 0
}

func (c *externalConsumers) delivered(id, gateway string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.demand == nil {
		c.demand = make(map[string]map[string]time.Time)
	}
	if c.demand[id] == nil {
		c.demand[id] = make(map[string]time.Time)
	}
	if now.After(c.demand[id][gateway]) {
		c.demand[id][gateway] = now
	}
}

// Recent supply demand is not consumer eligibility and has no permanent
// Account/Slot dependency. Restarting only resets this time window.
func (c *externalConsumers) busiest(cutoff time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	busiest := 0
	for id, gateways := range c.demand {
		for gateway, at := range gateways {
			if !at.After(cutoff) {
				delete(gateways, gateway)
			}
		}
		if len(gateways) == 0 {
			delete(c.demand, id)
		}
		busiest = max(busiest, len(gateways))
	}
	return busiest
}

func (s *Scheduler) deliverExternal(ctx context.Context, member string, req Request, authed bool) (delivery types.Delivery, err error) {
	if !authed && !s.Public().Open {
		return delivery, ErrPublicClosed
	}
	member = strings.TrimSpace(member)
	if member == "" || len(member) > 128 || req.Gateway == "" ||
		strings.ContainsFunc(member, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return delivery, ErrExternalRequest
	}
	if err := ctx.Err(); err != nil {
		return delivery, err
	}
	id, now := types.ExternalConsumerID(member), s.now()
	defer func() {
		if recorder, ok := s.st.(interface {
			RecordExternalUsage(string, string, time.Time, bool, bool) error
		}); ok {
			var limited *ConsumerRateLimit
			if saveErr := recorder.RecordExternalUsage(id, req.Gateway, now, err == nil, errors.As(err, &limited)); saveErr != nil {
				s.opt.Log.Error("external consumer statistics write failed", "error", saveErr)
			}
		}
	}()
	if !authed || !req.AdminKey {
		if retry := s.external.admit(id, s.now); retry > 0 {
			return delivery, &ConsumerRateLimit{RetryAfter: retry}
		}
	}
	s.markDemand()
	if _, exists := s.st.Gateways()[req.Gateway]; !exists {
		return delivery, ErrNoFullSlot
	}
	pair, fresh := s.freshPair(req.Gateway, req.Floor())
	if !fresh || req.Skips(req.Gateway, pair.Cookie) {
		return delivery, ErrNoFullSlot
	}
	if err := ctx.Err(); err != nil {
		return delivery, err
	}
	s.external.delivered(id, req.Gateway, now)
	s.opt.Log.Info("external stock delivered", "gateway", req.Gateway)
	return types.Delivery{
		Gateway: req.Gateway, Region: pair.RegionKey, Cookie: pair.Cookie,
		ExpiresAt: pair.ExpiresAt, ValidForS: int(pair.ExpiresAt.Sub(s.now()).Seconds()),
		VerifiedFull: false,
		MintExit:     pair.MintExit,
	}, nil
}
