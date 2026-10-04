package sched

import (
	"context"
	"fmt"

	"gwpool/internal/types"
)

func (s *Scheduler) beginAttempt(ctx context.Context, account types.Account, exit types.Exit) (context.Context, error) {
	if s.accountHeldFor(account.ID) > 0 {
		return ctx, fmt.Errorf("%w: 上游账号仍在退避", types.ErrRateLimited)
	}
	reserved, err := s.mint.BeginAttempt(ctx, account.Creds, exit)
	if err == nil {
		s.mu.Lock()
		s.lastMintedAt[account.ID] = s.now()
		s.mu.Unlock()
	}
	return reserved, err
}

func (s *Scheduler) sampleAttemptFailure(account types.Account, exit types.Exit, sid, purpose string, obs types.Observed, err error) {
	provider := ""
	if sid != "" {
		provider = s.mint.Provider(sid)
	}
	s.sample(types.Sample{AccountID: account.ID, RegionKey: s.sampleRegion(exit),
		Requested: exit, Observed: obs, At: s.now(), Err: types.SampleErr("", err),
		Provider: provider, Purpose: purpose, TargetMatch: exit.MatchEvidence(obs, false)})
}

func (s *Scheduler) countryAttemptCounts(account types.Account) map[string]int64 {
	counts := map[string]int64{}
	for country, row := range s.st.CountryAttempts(account.Creds.AccountID) {
		counts[country] = row.Count
	}
	return counts
}
