package sched

import (
	"sort"
	"time"

	"gwpool/internal/ticket"
	"gwpool/internal/types"
)

type stockObservation struct {
	Gateway          string    `json:"gateway"`
	Ready            bool      `json:"ready"`
	DeliverableUntil time.Time `json:"deliverable_until,omitzero"`
}

// These are point-in-time stock observations, separate from one-sample-per-mint
// accounting. Adjacent observations and committed mint deadlines bound missing
// intervals; an unobserved/idle period must not be reported as an exact outage.
// No credentials, cookie material or inferred historical start time is emitted.
func (s *Scheduler) logInventorySnapshot(phase string) {
	now := s.now()
	pairs, donors := s.st.TicketInventory()
	best, known := ticket.Best(pairs, donors, now), s.st.Gateways()
	stock := make([]stockObservation, 0, len(known))
	for gateway := range known {
		pair := ticket.Rehome(pairs[gateway], best, now)
		row := stockObservation{Gateway: gateway, Ready: pair.Cookie != "" && pair.ExpiresAt.Sub(now) >= types.DeliverFloor}
		if !pair.ExpiresAt.IsZero() {
			row.DeliverableUntil = pair.ExpiresAt.Add(-types.DeliverFloor)
		}
		stock = append(stock, row)
	}
	sort.Slice(stock, func(i, j int) bool { return stock[i].Gateway < stock[j].Gateway })
	s.opt.Log.Info("库存可交付快照", "observed_at", now, "phase", phase, "stock", stock)
}
