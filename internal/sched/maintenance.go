package sched

import (
	"context"
	"sort"
	"sync"
	"time"

	"gwpool/internal/ticket"
	"gwpool/internal/types"
)

// A task can perform at most one normal mint; the existing bounded OAuth expiry
// recovery can retry once and still spends the shared durable request budget.
const maintenanceTasksPerRound = 12

type donorNeed struct {
	until      time.Time
	dependents int
}

// A donor task is not blind exploration. It is justified only by known tickets
// with live oailb material that outlives the region's available cflb supply.
func (s *Scheduler) donorNeeds() map[string]donorNeed {
	now, cfg := s.now(), s.cfg()
	pairs, donors := s.st.TicketInventory()
	best, known := ticket.Best(pairs, donors, now), s.st.Gateways()
	needs := map[string]donorNeed{}
	for gateway, pair := range pairs {
		if _, ok := known[gateway]; !ok || pair.RegionKey == "" {
			continue
		}
		region, exists := cfg.RegionByKey(pair.RegionKey)
		if !exists || !region.Enabled || !s.mint.SupportsDynamicExit(region.Exit()) {
			continue
		}
		_, fruitExpiry, err := ticket.Info(pair.Cookie)
		donor := best[pair.RegionKey]
		if err != nil || !fruitExpiry.After(now.Add(types.DeliverFloor)) ||
			!fruitExpiry.After(donor.Exp) || donor.Exp.Sub(now) >= seedCflbFloor {
			continue
		}
		effective := ticket.Rehome(pair, best, now)
		if effective.ExpiresAt.Sub(now) >= types.DeliverFloor && !cfg.RenewPairs {
			continue // Do not covertly re-enable proactive renewal.
		}
		need := needs[pair.RegionKey]
		need.until = donor.Exp.Add(-types.DeliverFloor)
		need.dependents++
		needs[pair.RegionKey] = need
	}
	return needs
}

type maintenanceTask struct {
	selection inventorySelection
	renewal   *renewTarget
	fallback  *renewTarget // Seed recovery only when bare selection spent no attempt.
	deadline  time.Time
	priority  int
	reach     int
}

func (s *Scheduler) maintenancePlan(allowBootstrap bool) ([]maintenanceTask, int) {
	if s.maintenanceOff() {
		return nil, 0
	}
	now := s.now()
	known := s.st.Gateways()
	if len(known) == 0 {
		var tasks []maintenanceTask
		if allowBootstrap {
			for i := 0; i < s.PairTarget(); i++ {
				tasks = append(tasks, maintenanceTask{selection: inventorySelection{bootstrap: true},
					deadline: now, priority: 3})
			}
		}
		return tasks, 0
	}
	reach := map[string]map[string]bool{}
	for _, route := range s.inventoryEvidence() {
		if reach[route.gateway] == nil {
			reach[route.gateway] = map[string]bool{}
		}
		reach[route.gateway][comboKey(route.account, route.region)] = true
	}
	renewals := map[string]renewTarget{}
	if s.cfg().RenewPairs {
		due, _ := s.pairsDueForRenew()
		for _, target := range due {
			renewals[target.gateway] = target
		}
	}
	pairs, donors := s.st.TicketInventory()
	best := ticket.Best(pairs, donors, now)
	var tasks []maintenanceTask
	pending := 0
	for name := range known {
		effective := ticket.Rehome(pairs[name], best, now)
		ready := effective.ExpiresAt.Sub(now) >= types.DeliverFloor
		deadline := now // No known ticket means no evidence for an earlier cutoff.
		if !effective.ExpiresAt.IsZero() {
			deadline = effective.ExpiresAt.Add(-types.DeliverFloor)
		}
		renewal, canRenew := renewals[name]
		switch {
		case !ready && len(reach[name]) > 0:
			task := maintenanceTask{selection: inventorySelection{gateway: name},
				deadline: deadline, priority: 1, reach: len(reach[name])}
			if canRenew {
				task.fallback = &renewal
			}
			tasks = append(tasks, task)
		case canRenew:
			priority := 2
			if !ready {
				priority = 1
			} else if renewal.reach <= 1 {
				priority = 0 // Protect scarce stock before it becomes a missing gateway.
			}
			tasks = append(tasks, maintenanceTask{renewal: &renewal, priority: priority,
				deadline: deadline, reach: renewal.reach})
		case !ready:
			pending++ // Keep the known gateway visible; do not invent a route.
		}
	}
	for region, need := range s.donorNeeds() {
		tasks = append(tasks, maintenanceTask{selection: inventorySelection{donor: region},
			deadline: need.until, priority: 0, reach: -need.dependents})
	}
	sort.Slice(tasks, func(i, j int) bool {
		a, b := tasks[i], tasks[j]
		if a.priority != b.priority {
			return a.priority < b.priority
		}
		if !a.deadline.Equal(b.deadline) {
			return a.deadline.Before(b.deadline)
		}
		if a.reach != b.reach {
			return a.reach < b.reach
		}
		return a.key() < b.key()
	})
	return tasks, pending
}

func (task maintenanceTask) key() string {
	if task.renewal != nil {
		return "renew|" + task.renewal.gateway
	}
	return task.selection.donor + "|" + task.selection.gateway
}

type maintenanceRound struct {
	mu        sync.Mutex
	tasks     []maintenanceTask
	next      int
	running   int
	attempted int
}

func (round *maintenanceRound) take() (maintenanceTask, bool) {
	round.mu.Lock()
	defer round.mu.Unlock()
	if round.next == len(round.tasks) || round.attempted+round.running >= maintenanceTasksPerRound {
		return maintenanceTask{}, false
	}
	task := round.tasks[round.next]
	round.next++
	round.running++
	return task, true
}

func (round *maintenanceRound) finish(attempted bool) {
	round.mu.Lock()
	defer round.mu.Unlock()
	round.running--
	if attempted {
		round.attempted++
	}
}

func (s *Scheduler) runMaintenance(ctx context.Context, allowBootstrap bool) {
	if s.maintenanceOff() {
		return
	}
	s.logInventorySnapshot("before")
	tasks, pending := s.maintenancePlan(allowBootstrap)
	if len(tasks) == 0 {
		if pending > 0 {
			s.opt.Log.Info("已知缺票等待恢复路线", "gateways", pending)
		}
		return
	}
	round := &maintenanceRound{tasks: tasks}
	var workers sync.WaitGroup
	for i := 0; i < min(s.cfg().MintWorkers(), len(tasks), maintenanceTasksPerRound); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ctx.Err() == nil && !s.maintenanceOff() {
				task, available := round.take()
				if !available {
					return
				}
				attempted := s.executeMaintenance(ctx, task)
				round.finish(attempted)
			}
		}()
	}
	workers.Wait()
	if round.attempted > 0 {
		s.logInventorySnapshot("after")
	}
	s.opt.Log.Info("按路线维护结束", "known", len(s.st.Gateways()), "live", s.livePairs(),
		"tasks", len(tasks), "attempted_tasks", round.attempted, "pending_without_route", pending)
}

func (s *Scheduler) executeMaintenance(ctx context.Context, task maintenanceTask) bool {
	if task.renewal != nil {
		return s.renewCandidates(ctx, []renewTarget{*task.renewal}, 0) > 0
	}
	if task.selection.gateway != "" {
		if _, ready := s.freshPair(task.selection.gateway, types.DeliverFloor); ready {
			return false
		}
	}
	if task.selection.donor != "" {
		if _, needed := s.donorNeeds()[task.selection.donor]; !needed {
			return false
		}
	}
	if s.runInventorySelection(ctx, 1, task.selection) > 0 {
		return true
	}
	// A known historical bare route can currently be held or unavailable.
	// It must not suppress independent seed evidence. Never add a second
	// attempt after a real bare request (including a failed binding/request).
	if task.fallback != nil && ctx.Err() == nil {
		return s.renewCandidates(ctx, []renewTarget{*task.fallback}, 0) > 0
	}
	return false
}
