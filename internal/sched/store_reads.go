package sched

import (
	"time"
)

func (s *Scheduler) busiestDeliveries(cutoff time.Time) int {
	external := s.external.busiest(cutoff)
	if indexed, ok := s.st.(interface{ BusiestDeliveries(time.Time) int }); ok {
		return max(external, indexed.BusiestDeliveries(cutoff))
	}
	used := map[int64]int{}
	for _, slot := range s.st.Slots() {
		if !slot.LastDeliver.IsZero() && slot.LastDeliver.After(cutoff) {
			used[slot.AccountID]++
		}
	}
	busiest := external
	for _, account := range s.st.Accounts() {
		if account.Enabled && account.Creds.AccessToken != "" {
			busiest = max(busiest, used[account.ID])
		}
	}
	return busiest
}
