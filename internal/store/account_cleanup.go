package store

import (
	"slices"

	"gwpool/internal/types"
)

// DeleteUnauthorizedAccounts removes only unchanged, explicitly confirmed
// candidates. It makes one durable commit, retaining shared pairs and history.
// Account locks drain already-running refresh/mint work before the store lock
// rechecks the snapshot. Locks are always acquired in ascending ID order.
func (s *Store) DeleteUnauthorizedAccounts(candidates []types.AccountCleanupCandidate) (int, error) {
	idsToLock := make([]int64, 0, len(candidates))
	for _, candidate := range candidates {
		idsToLock = append(idsToLock, candidate.ID)
	}
	slices.Sort(idsToLock)
	idsToLock = slices.Compact(idsToLock)
	for _, id := range idsToLock {
		unlock := s.LockAccount(id)
		defer unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	want := make(map[int64]string, len(candidates))
	for _, candidate := range candidates {
		want[candidate.ID] = candidate.Fingerprint
	}
	drop := map[int64]bool{}
	kept := make([]types.Account, 0, len(s.st.Accounts))
	var ids []int64
	for _, a := range s.st.Accounts {
		if expected, ok := want[a.ID]; ok && types.UnauthorizedCredential(a) &&
			types.CleanupCandidate(a).Fingerprint == expected {
			drop[a.ID] = true
			ids = append(ids, a.ID)
		} else {
			kept = append(kept, a)
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	oldAccounts := s.st.Accounts
	droppedSlots := map[string]types.Slot{}
	droppedMints := map[string]types.RegionMint{}
	for key, slot := range s.st.Slots {
		if drop[slot.AccountID] {
			droppedSlots[key] = slot
			delete(s.st.Slots, key)
		}
	}
	for key, m := range s.st.Mints {
		if drop[m.AccountID] {
			droppedMints[key] = m
			delete(s.st.Mints, key)
		}
	}
	s.st.Accounts = kept
	if err := s.save(stateChange{DropAccounts: ids}); err != nil {
		s.st.Accounts = oldAccounts
		for key, slot := range droppedSlots {
			s.st.Slots[key] = slot
		}
		for key, m := range droppedMints {
			s.st.Mints[key] = m
		}
		return 0, err
	}
	return len(ids), nil
}
