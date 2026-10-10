package sched

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"gwpool/internal/oauth"
	"gwpool/internal/routes"
	"gwpool/internal/types"
)

type expiryRefreshKey struct{}

type expiryRefreshState struct {
	mu   sync.Mutex
	used map[int64]bool
}

func withExpiryRecovery(ctx context.Context) context.Context {
	if _, ok := ctx.Value(expiryRefreshKey{}).(*expiryRefreshState); ok {
		return ctx
	}
	return context.WithValue(ctx, expiryRefreshKey{}, &expiryRefreshState{used: make(map[int64]bool)})
}

func expiryRefreshed(ctx context.Context, id int64) bool {
	state, ok := ctx.Value(expiryRefreshKey{}).(*expiryRefreshState)
	if !ok {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.used[id]
}

func markExpiryRefreshed(ctx context.Context, id int64) {
	state := ctx.Value(expiryRefreshKey{}).(*expiryRefreshState)
	state.mu.Lock()
	defer state.mu.Unlock()
	state.used[id] = true
}

// All callers hold the store's account lock, including pre-bound background
// attempts. Refresh before reserving/binding so a known expired AT is not sent.
func (s *Scheduler) prepareAccount(ctx context.Context, a types.Account) (context.Context, types.Account, error) {
	current, enabled := s.account(a.ID)
	if !enabled {
		return ctx, a, ErrAccountUnusable
	}
	if s.opt.RefreshExpired == nil {
		return ctx, current, nil
	}
	ctx = withExpiryRecovery(ctx)
	if !oauth.AccessExpired(current.Creds.AccessToken, s.now()) {
		return ctx, current, nil
	}
	if expiryRefreshed(ctx, a.ID) {
		return ctx, current, s.rejectExpired(current, oauth.LocalExpiredReason)
	}
	markExpiryRefreshed(ctx, a.ID)
	refreshed, err := s.refreshExpired(ctx, current, oauth.LocalExpiredReason)
	if err != nil {
		return ctx, current, err
	}
	return ctx, refreshed, nil
}

func (s *Scheduler) refreshExpired(ctx context.Context, a types.Account, reason string) (types.Account, error) {
	if err := ctx.Err(); err != nil {
		return a, err
	}
	if a.Creds.RefreshToken == "" {
		return a, s.rejectExpired(a, reason)
	}
	updated, err := s.opt.RefreshExpired(ctx, a)
	if err != nil {
		s.opt.Log.Warn("过期凭据刷新失败", "account", a.ID, "err", err)
		if ctx.Err() != nil {
			return a, ctx.Err()
		}
		if oauth.RefreshTokenRejected(err) {
			reason = oauth.ExpiredRefreshRejectedReason
		}
		return a, errors.Join(s.rejectExpired(a, reason), err)
	}
	if !updated.Enabled {
		return updated, ErrAccountUnusable
	}
	return updated, nil
}

func (s *Scheduler) rejectExpired(a types.Account, reason string) error {
	var err error
	if st, ok := s.st.(interface {
		DisableAccountIfCurrent(types.Account, string) (bool, error)
	}); ok {
		_, err = st.DisableAccountIfCurrent(a, reason)
	} else {
		err = s.st.DisableAccount(a.ID, reason)
	}
	return errors.Join(fmt.Errorf("%w: %w", ErrUpstreamRejected, oauth.ErrAccessExpired), err)
}

// mintOnceFor permits exactly one expiry recovery. Each real request still goes
// through BeginAttempt and records its own HTTP sample/touches in mintAttempt.
func (s *Scheduler) mintOnceFor(ctx context.Context, a types.Account, e types.Exit, sid string,
	cur types.Pair, want, bias, purpose string) (minted, error) {
	var err error
	ctx, err = s.beginAttempt(ctx, a, e)
	if err != nil {
		return minted{}, err
	}
	if s.opt.RefreshExpired != nil {
		var enabled bool
		a, enabled = s.account(a.ID)
		if !enabled {
			return minted{}, ErrAccountUnusable
		}
	}
	got, err := s.mintAttempt(ctx, a, e, sid, cur, want, bias, purpose)
	if !errors.Is(err, oauth.ErrAccessExpired) || s.opt.RefreshExpired == nil {
		return got, err
	}
	if expiryRefreshed(ctx, a.ID) {
		return minted{}, s.rejectExpired(a, oauth.ExpiredAfterRefreshReason)
	}
	markExpiryRefreshed(ctx, a.ID)
	a, err = s.refreshExpired(ctx, a, oauth.UpstreamExpiredReason)
	if err != nil {
		return minted{}, err
	}
	if work, planned := ctx.Value(routeWorkKey{}).(routeWork); planned &&
		work.attempt.Route.Identity != routes.CredentialIdentity(a.Creds) {
		// A credential without stable member claims intentionally gets a new
		// identity after refresh. Replan and claim it rather than attributing
		// the new request to old evidence or rejecting the allowed recovery.
		var release func()
		ctx, a, sid, release, err = s.bindRoute(ctx, a, e, cur, work.attempt.Expected, purpose, work.attempt.Donor)
		if err != nil {
			return minted{}, err
		}
		defer release()
	} else {
		// The first reservation has been consumed by Mint. BeginAttempt must
		// spend another slot, and can reject the retry without sending it.
		ctx, err = s.beginAttempt(ctx, a, e)
		if err != nil {
			return minted{}, err
		}
		if planned {
			work.attempt.StartedAt = s.now()
			ctx = context.WithValue(ctx, routeWorkKey{}, work)
		}
	}
	got, err = s.mintAttempt(ctx, a, e, sid, cur, want, bias, purpose)
	if errors.Is(err, oauth.ErrAccessExpired) {
		return minted{}, s.rejectExpired(a, oauth.ExpiredAfterRefreshReason)
	}
	return got, err
}
