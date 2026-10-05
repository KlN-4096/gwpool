package store

import (
	"sync"
	"time"

	"gwpool/internal/types"
)

type slotDemandCache struct {
	mu                      sync.Mutex
	ready                   bool
	accounts, slots         uint64
	fromCutoff, untilCutoff time.Time
	busiest                 int
}

// SlotsForAccount avoids copying every other consumer's ledger on a hot read.
// The index is rebuilt from snapshot+WAL and updated only after a durable save.
func (s *Store) SlotsForAccount(accountID int64) []types.Slot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows := s.slotIndex[accountID]
	out := make([]types.Slot, 0, len(rows))
	for _, slot := range rows {
		out = append(out, slot)
	}
	return out
}

func (s *Store) indexSlot(slot types.Slot) {
	if s.slotIndex == nil {
		s.slotIndex = make(map[int64]map[string]types.Slot)
	}
	if s.slotIndex[slot.AccountID] == nil {
		s.slotIndex[slot.AccountID] = make(map[string]types.Slot)
	}
	s.slotIndex[slot.AccountID][slot.Gateway] = slot
}

// BusiestDeliveries counts under a read lock rather than allocating a copy of
// the entire ledger. The cutoff and enabled state are evaluated on every read.
func (s *Store) BusiestDeliveries(cutoff time.Time) int {
	cache := &s.demandCache
	cache.mu.Lock()
	defer cache.mu.Unlock()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if cache.ready && cache.accounts == s.revisions.Accounts && cache.slots == s.revisions.Slots &&
		!cutoff.Before(cache.fromCutoff) && (cache.untilCutoff.IsZero() || cutoff.Before(cache.untilCutoff)) {
		return cache.busiest
	}
	busiest := 0
	var from, until time.Time
	for _, account := range s.st.Accounts {
		if !account.Enabled {
			continue
		}
		count := 0
		for _, slot := range s.slotIndex[account.ID] {
			if slot.LastDeliver.IsZero() {
				continue
			}
			if slot.LastDeliver.After(cutoff) {
				count++
				if until.IsZero() || slot.LastDeliver.Before(until) {
					until = slot.LastDeliver
				}
			} else if from.IsZero() || slot.LastDeliver.After(from) {
				from = slot.LastDeliver
			}
		}
		busiest = max(busiest, count)
	}
	cache.ready, cache.accounts, cache.slots = true, s.revisions.Accounts, s.revisions.Slots
	cache.fromCutoff, cache.untilCutoff, cache.busiest = from, until, busiest
	return busiest
}
