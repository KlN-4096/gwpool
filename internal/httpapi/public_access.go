package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	"gwpool/internal/config"
)

// One shared public key is one reporter, not one independent vote per caller.
// Owner reporters are -positiveID, so MinInt64 is outside their namespace.
const publicReporterID int64 = math.MinInt64

// PublicKey is a separate, stable machine credential. Publishing it must never
// reveal the root consumer secret or confer admin/owner credential ownership.
func (s *Server) PublicKey() string {
	if s.opt.ConsumerKey == "" {
		return ""
	}
	m := hmac.New(sha256.New, []byte(s.opt.ConsumerKey))
	_, _ = m.Write([]byte("gwpool-public-key-v1"))
	return "gwp_p_" + base64.RawURLEncoding.EncodeToString(m.Sum(nil)[:24])
}

func (s *Server) publicLimits() config.PublicWhenSurplus {
	if s.opt.Live != nil {
		return s.opt.Live.Get().PublicWhenSurplus
	}
	return config.Defaults().PublicWhenSurplus
}

func (s *Server) publicAllowed(w http.ResponseWriter) bool {
	state := s.pool.Public()
	if !state.Enabled {
		s.unauthorized(w, "公共接入已关闭")
		return false
	}
	if !state.Open {
		s.publicClosed(w, fmt.Sprintf("池子暂不对外开放：托管账号 %d 个，门槛 %d 个",
			state.Accounts, state.Threshold))
		return false
	}
	return true
}

// Only machine endpoints use this middleware. Ordinary keys never enter the
// public queue, and missing/invalid keys cannot fall back to anonymous access.
func (s *Server) consumerAPI(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = s.resolveCallerOnce(r)
		c, ok := s.resolveCaller(r)
		if c.key.ID != "" {
			s.serveManagedKey(w, r, c, ok, next)
			return
		}
		if !ok {
			s.unauthorized(w, "需要有效的接入 Key")
			return
		}
		if !c.public {
			next(w, r)
			return
		}
		if !s.publicAllowed(w) {
			return
		}
		release, retry, err := s.publicAdmission.acquire(r.Context(), s.publicLimits)
		if err != nil {
			if r.Context().Err() == nil {
				writeFail(w, http.StatusServiceUnavailable, apiError{
					Code: codeRateLimited, Message: "公共接入繁忙，请稍后重试",
					RetryAfterSeconds: retrySeconds(retry)})
			}
			return
		}
		defer release()
		// Queueing is not permission to outlive a disabled switch or threshold.
		if r.Context().Err() != nil || !s.publicAllowed(w) {
			return
		}
		next(w, r)
	})
}

// Nonzero size gives every waiter a distinct pointer identity.
type publicWaiter struct{ _ byte }

const publicConfigPollInterval = 100 * time.Millisecond

// Admission is process-local and shared by all uses of the public key. The
// rolling minute is a hard count; a token bucket smooths that budget over time,
// allowing an initial burst no larger than the configured concurrency.
// Only admitted requests consume budget, including their eventual failures.
type publicAdmission struct {
	mu       sync.Mutex
	changed  chan struct{}
	active   int
	waiters  []*publicWaiter
	recent   []time.Time
	tokens   float64
	refilled time.Time
	lastRate int
}

type publicBusy struct{}

func (publicBusy) Error() string { return "public API admission limit" }

func (a *publicAdmission) signalLocked() {
	if a.changed != nil {
		close(a.changed)
	}
	a.changed = make(chan struct{})
}

func (a *publicAdmission) retryLocked(now time.Time, limits config.PublicWhenSurplus) time.Duration {
	if a.refilled.IsZero() {
		a.tokens = float64(limits.MaxConcurrent)
	} else {
		a.tokens += now.Sub(a.refilled).Seconds() * float64(a.lastRate) / 60
		a.tokens = min(a.tokens, float64(limits.MaxConcurrent))
	}
	a.refilled, a.lastRate = now, limits.RequestsPerMinute
	cutoff := now.Add(-time.Minute)
	expired := 0
	for expired < len(a.recent) && !a.recent[expired].After(cutoff) {
		expired++
	}
	if expired > 0 {
		copy(a.recent, a.recent[expired:])
		a.recent = a.recent[:len(a.recent)-expired]
	}
	var retry time.Duration
	if a.tokens < 1 {
		retry = time.Duration(math.Ceil((1 - a.tokens) * float64(time.Minute) / float64(limits.RequestsPerMinute)))
	}
	if len(a.recent) >= limits.RequestsPerMinute {
		// If the limit shrinks, enough old admissions must expire first.
		i := len(a.recent) - limits.RequestsPerMinute
		retry = max(retry, a.recent[i].Add(time.Minute).Sub(now))
	}
	return retry
}

func (a *publicAdmission) release() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active--
	a.signalLocked()
}

func (a *publicAdmission) acquire(ctx context.Context, current func() config.PublicWhenSurplus) (func(), time.Duration, error) {
	started := time.Now()
	var waiter *publicWaiter
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.changed == nil {
		a.signalLocked()
	}
	defer func() {
		if waiter == nil {
			return
		}
		for i, pending := range a.waiters {
			if pending == waiter {
				a.waiters = append(a.waiters[:i], a.waiters[i+1:]...)
				a.signalLocked()
				return
			}
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		limits := current()
		now := time.Now()
		retry := a.retryLocked(now, limits)
		left := started.Add(time.Duration(limits.QueueWaitMS) * time.Millisecond).Sub(now)
		if waiter != nil && left <= 0 {
			return nil, max(retry, time.Second), publicBusy{}
		}
		first := len(a.waiters) == 0 || a.waiters[0] == waiter
		if first && a.active < limits.MaxConcurrent && retry <= 0 {
			a.tokens--
			a.recent = append(a.recent, now)
			a.active++
			var once sync.Once
			return func() { once.Do(a.release) }, 0, nil
		}
		if left <= 0 || (waiter == nil && len(a.waiters) >= limits.MaxQueued) {
			return nil, max(retry, time.Second), publicBusy{}
		}
		if waiter == nil {
			waiter = &publicWaiter{}
			a.waiters = append(a.waiters, waiter)
		}
		// Releases/cancellations wake the queue immediately. Periodic wakeups
		// also pick up hot configuration changes without a background worker.
		delay := min(left, publicConfigPollInterval)
		if first && retry > 0 {
			delay = min(delay, retry)
		}
		changed := a.changed
		timer := time.NewTimer(delay)
		a.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
		a.mu.Lock()
	}
}
