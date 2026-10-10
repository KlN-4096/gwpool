package store

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gwpool/internal/routes"
	"gwpool/internal/ticket"
	"gwpool/internal/types"
)

// TicketInventory is one immutable snapshot, so delivery and catalog project
// tickets against the same committed donors instead of mixing two revisions.
func (s *Store) TicketInventory() (map[string]types.Pair, map[string]types.RegionDonor) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneMap(s.st.Pairs), cloneMap(s.st.Donors)
}

// CommitMint compares and publishes actual landing stock under a single lock.
// A rejected shorter pair can still contribute a useful independent donor.
func (s *Store) CommitMint(commit types.MintCommit) (types.MintCommitResult, error) {
	if err := validateMintCommit(commit); err != nil {
		return types.MintCommitResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	plan := ticket.StockPlan{Donors: s.st.Donors}
	if commit.Pair.Cookie != "" {
		plan = ticket.PlanStock(s.st.Pairs, s.st.Donors, commit)
	}
	change := stateChange{Donors: map[string]types.RegionDonor{}}
	for key, donor := range plan.Donors {
		if s.st.Donors[key] != donor {
			change.Donors[key] = donor
		}
	}
	for key := range s.st.Donors {
		if _, keep := plan.Donors[key]; !keep {
			change.DropDonors = append(change.DropDonors, key)
		}
	}
	old, had := s.st.Pairs[commit.Pair.Gateway]
	if plan.Result.PairChanged {
		change.Pairs = map[string]types.Pair{commit.Pair.Gateway: plan.Result.Pair}
		s.st.Pairs[commit.Pair.Gateway] = plan.Result.Pair
	}
	oldRoutes := s.st.Routes
	nextRoutes := s.st.Routes
	if commit.Route != nil {
		nextRoutes = cloneMap(s.st.Routes)
		if nextRoutes == nil {
			nextRoutes = map[string]types.RouteState{}
		}
		key := routes.Key(commit.Route.Route)
		previous := nextRoutes[key]
		if !commit.At.Before(previous.LastAttempt.Add(routes.Retention)) {
			previous = types.RouteState{}
		}
		updated := routes.Advance(previous, *commit.Route, plan.Result.Gain)
		plan.Result.Route = updated
		if nextRoutes[key] != updated {
			nextRoutes[key] = updated
			change.Routes = map[string]types.RouteState{key: updated}
		}
		change.DropRoutes = pruneRouteStates(nextRoutes, commit.At)
		for _, dropped := range change.DropRoutes {
			delete(change.Routes, dropped)
		}
	}
	if len(change.Pairs)+len(change.Donors)+len(change.DropDonors)+len(change.Routes)+len(change.DropRoutes) == 0 {
		return plan.Result, nil
	}
	oldDonors := s.st.Donors
	s.st.Donors = plan.Donors
	s.st.Routes = nextRoutes
	if err := s.save(change); err != nil {
		if had {
			s.st.Pairs[commit.Pair.Gateway] = old
		} else {
			delete(s.st.Pairs, commit.Pair.Gateway)
		}
		s.st.Donors = oldDonors
		s.st.Routes = oldRoutes
		return types.MintCommitResult{}, err
	}
	return plan.Result, nil
}

func validateMintCommit(commit types.MintCommit) error {
	if commit.At.IsZero() || (commit.Pair.Cookie == "" && commit.Route == nil) ||
		(commit.Pair.Cookie != "" && commit.Pair.Gateway == "") {
		return errors.New("invalid mint inventory commit")
	}
	if attempt := commit.Route; attempt != nil {
		if err := routes.Validate(attempt.Route); err != nil {
			return err
		}
		if attempt.StartedAt.IsZero() || attempt.At != commit.At || attempt.At.Before(attempt.StartedAt) ||
			len(attempt.Expected) > 128 || len(attempt.Gateway) > 128 ||
			(commit.Pair.Cookie != "" && (attempt.Status != 200 || attempt.Failure != "" || attempt.Gateway != commit.Pair.Gateway)) {
			return errors.New("invalid route observation")
		}
		switch attempt.Failure {
		case "", "binding", "geo", "transport", "upstream", "invalid_response":
		default:
			return errors.New("invalid route failure category")
		}
	}
	return nil
}

// RouteStates never returns expired evidence; pruning is committed alongside
// subsequent observations, not as a write on a scheduler read.
func (s *Store) RouteStates(now time.Time) map[string]types.RouteState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]types.RouteState)
	for key, state := range s.st.Routes {
		if now.Before(state.LastAttempt.Add(routes.Retention)) {
			out[key] = state
		}
	}
	return out
}

func pruneRouteStates(states map[string]types.RouteState, now time.Time) []string {
	var dropped []string
	for key, state := range states {
		if !now.Before(state.LastAttempt.Add(routes.Retention)) {
			dropped = append(dropped, key)
			delete(states, key)
		}
	}
	if len(states) > routes.MaxStates {
		keys := make([]string, 0, len(states))
		for key := range states {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			left, right := states[keys[i]].LastAttempt, states[keys[j]].LastAttempt
			if left.Equal(right) {
				return keys[i] < keys[j]
			}
			return left.Before(right)
		})
		for _, key := range keys[:len(keys)-routes.MaxStates] {
			dropped = append(dropped, key)
			delete(states, key)
		}
	}
	sort.Strings(dropped)
	return dropped
}

// Called after replay and before serving. Expiration is journaled explicitly;
// replay itself never depends on the wall clock of a later process.
func (s *Store) prepareRouteInventory(now time.Time) error {
	for key, state := range s.st.Routes {
		if !routes.ValidState(key, state) {
			return errors.New("invalid persisted route state")
		}
	}
	for key, donor := range s.st.Donors {
		if key == "" || key != donor.RegionKey || len(key) > 256 || donor.Cflb == "" ||
			len(donor.Cflb) > 64<<10 || strings.ContainsAny(donor.Cflb, "\r\n;") || donor.ExpiresAt.IsZero() {
			return errors.New("invalid persisted region donor")
		}
	}
	oldRoutes, oldDonors := s.st.Routes, s.st.Donors
	nextRoutes, nextDonors := cloneMap(oldRoutes), cloneMap(oldDonors)
	change := stateChange{DropRoutes: pruneRouteStates(nextRoutes, now)}
	for key, donor := range nextDonors {
		if !donor.ExpiresAt.After(now) {
			delete(nextDonors, key)
			change.DropDonors = append(change.DropDonors, key)
		}
	}
	if len(nextDonors) > ticket.MaxRegionDonors {
		return fmt.Errorf("too many persisted region donors: %d", len(nextDonors))
	}
	if len(change.DropRoutes)+len(change.DropDonors) == 0 {
		return nil
	}
	s.st.Routes, s.st.Donors = nextRoutes, nextDonors
	if err := s.save(change); err != nil {
		s.st.Routes, s.st.Donors = oldRoutes, oldDonors
		return err
	}
	return nil
}
