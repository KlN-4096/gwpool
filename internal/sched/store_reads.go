package sched

import (
	"context"
	"time"

	"gwpool/internal/types"
)

// This is not an outbound account lock. Selecting and durably recording stock
// for one consumer must be serialized, but requests for different consumers
// may join the same durable write batch. No cross-consumer locks are held.
func (s *Scheduler) lockDelivery(ctx context.Context, accountID int64) (func(), error) {
	s.mu.Lock()
	if s.deliveryGates == nil {
		s.deliveryGates = map[int64]chan struct{}{}
	}
	gate := s.deliveryGates[accountID]
	if gate == nil {
		gate = make(chan struct{}, 1)
		s.deliveryGates[accountID] = gate
	}
	s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-gate
			return nil, err
		}
		return func() { <-gate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Scheduler) slotsForAccount(accountID int64) []types.Slot {
	if indexed, ok := s.st.(interface{ SlotsForAccount(int64) []types.Slot }); ok {
		return indexed.SlotsForAccount(accountID)
	}
	// Alternate stores (including fakes) retain immediate read semantics.
	var out []types.Slot
	for _, slot := range s.st.Slots() {
		if slot.AccountID == accountID {
			out = append(out, slot)
		}
	}
	return out
}

func (s *Scheduler) busiestDeliveries(cutoff time.Time) int {
	if indexed, ok := s.st.(interface{ BusiestDeliveries(time.Time) int }); ok {
		return indexed.BusiestDeliveries(cutoff)
	}
	used := map[int64]int{}
	for _, slot := range s.st.Slots() {
		if !slot.LastDeliver.IsZero() && slot.LastDeliver.After(cutoff) {
			used[slot.AccountID]++
		}
	}
	busiest := 0
	for _, account := range s.st.Accounts() {
		if account.Enabled {
			busiest = max(busiest, used[account.ID])
		}
	}
	return busiest
}
