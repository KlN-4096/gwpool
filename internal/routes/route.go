// Package routes models temporary routing evidence, not an upstream promise
// that an identity and exit always map to one gateway.
package routes

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"gwpool/internal/geography"
	"gwpool/internal/types"
)

const (
	Bare      = "bare"
	Seed      = "seed"
	RetryBase = 5 * time.Minute
	RetryMax  = time.Hour
	Retention = 24 * time.Hour
	MaxStates = 16384
	maxStreak = 5
)

func Digest(parts ...string) string {
	body, _ := json.Marshal(parts)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// CredentialIdentity is stable across token refresh when a member is known.
// Missing member claims stay unknown: isolate by credential digest rather than
// inheriting another member's workspace history. That fallback deliberately
// makes no cross-refresh identity claim. Neither path authenticates the token.
func CredentialIdentity(creds types.Creds) string {
	member := ""
	parts := strings.Split(creds.AccessToken, ".")
	if len(parts) == 3 {
		if body, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "=")); err == nil {
			var claims struct {
				Subject string `json:"sub"`
				Auth    struct {
					UserID string `json:"chatgpt_user_id"`
				} `json:"https://api.openai.com/auth"`
			}
			if json.Unmarshal(body, &claims) == nil {
				member = claims.Auth.UserID
				if member == "" {
					member = claims.Subject
				}
			}
		}
	}
	if member == "" {
		return Digest("route-identity-unknown-v1", strings.TrimSpace(creds.AccountID), creds.AccessToken)
	}
	return Digest("route-identity-v1", strings.TrimSpace(creds.AccountID), member)
}

func Describe(creds types.Creds, exit types.Exit, source types.RouteSource, mode, seed string) types.RouteDescriptor {
	return DescribeIdentity(CredentialIdentity(creds), exit, source, mode, seed)
}

func DescribeIdentity(identity string, exit types.Exit, source types.RouteSource, mode, seed string) types.RouteDescriptor {
	exit.Region = geography.CountryKey(exit.Region)
	exit.State = strings.ToLower(strings.TrimSpace(exit.State))
	exit.City = strings.ToLower(strings.TrimSpace(exit.City))
	out := types.RouteDescriptor{Identity: identity,
		Exit: exit, Source: source.ID, Mode: mode}
	if mode == Seed && seed != "" {
		out.Seed = Digest("route-seed-v1", seed)
	}
	return out
}

func Key(route types.RouteDescriptor) string {
	return Digest("route-v1", route.Identity, route.Exit.Region, route.Exit.State,
		route.Exit.City, route.Mode, route.Source, route.Seed)
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func Validate(route types.RouteDescriptor) error {
	if !validDigest(route.Identity) || !validDigest(route.Source) ||
		len(route.Exit.Region) != 2 || !geography.Supported(route.Exit.Region) {
		return errors.New("invalid route identity, source or country")
	}
	for _, value := range []string{route.Exit.State, route.Exit.City} {
		if value != "" && !types.ValidLocationName(value) {
			return errors.New("invalid route location")
		}
	}
	if (route.Mode == Bare && route.Seed == "") || (route.Mode == Seed && validDigest(route.Seed)) {
		return nil
	}
	return errors.New("invalid route mode or seed fingerprint")
}

func ValidState(key string, state types.RouteState) bool {
	return Validate(state.Route) == nil && key == Key(state.Route) &&
		!state.StartedAt.IsZero() && !state.LastAttempt.Before(state.StartedAt) &&
		state.LowYieldStreak >= 0 && state.LowYieldStreak <= maxStreak &&
		!state.NextCheck.After(state.LastAttempt.Add(RetryMax)) &&
		!state.ObservedAt.After(state.LastAttempt)
}

// Advance preserves the last successful mapping on transport failures. A useful
// ticket at B does not erase evidence that this route missed the requested A.
func Advance(old types.RouteState, attempt types.RouteAttempt, gain types.StockGain) types.RouteState {
	next := old
	next.Attempts++
	if gain.Productive() {
		next.Productive++
		if gain.CoverageAdded {
			next.AddedCoverage++
		}
		next.ExtendedMS += uint64(max(0, gain.ExtendedMS))
		next.DonorGainMS += uint64(max(0, gain.DonorExtendedMS))
	}
	clockReset := old.LastAttempt.After(attempt.At.Add(RetryMax))
	if clockReset {
		next.Gateway, next.ObservedAt, next.NextCheck = "", time.Time{}, time.Time{}
		next.LowYieldStreak, next.LastMissed = 0, ""
	}
	if !clockReset && (attempt.StartedAt.Before(old.StartedAt) ||
		(attempt.StartedAt.Equal(old.StartedAt) && attempt.At.Before(old.LastAttempt))) {
		next.LastAttempt = maxTime(next.LastAttempt, attempt.At)
		return next // Account for the attempt without replacing newer routing evidence.
	}
	next.Route, next.StartedAt, next.LastAttempt = attempt.Route, attempt.StartedAt, attempt.At
	next.LastFailure = attempt.Failure
	next.LastTarget, next.LastDonor, next.LastProductive = attempt.Expected, attempt.Donor, gain.Productive()
	validLanding := attempt.Status == 200 && attempt.Gateway != "" && attempt.Failure == ""
	missed := validLanding && attempt.Expected != "" && attempt.Expected != attempt.Gateway
	if validLanding {
		if next.Gateway != attempt.Gateway {
			next.LowYieldStreak, next.LastMissed = 0, ""
		}
		next.Gateway, next.ObservedAt = attempt.Gateway, attempt.At
		next.TargetMatch = attempt.TargetMatch
		if missed {
			next.LastMissed = attempt.Expected
			next.Drifts++
		} else if attempt.Expected == next.LastMissed {
			next.LastMissed = ""
		}
	}
	if !validLanding || missed || !gain.Productive() {
		next.LowYieldStreak = min(next.LowYieldStreak+1, maxStreak)
		delay := min(RetryBase*time.Duration(1<<uint(next.LowYieldStreak-1)), RetryMax)
		next.NextCheck = attempt.At.Add(delay)
	} else {
		if next.LastMissed == "" {
			next.LowYieldStreak = 0
		}
		// Even a productive route need not be re-probed while its landing is
		// healthy. Legitimate stock/donor needs can bypass this soft interval.
		next.NextCheck = maxTime(next.NextCheck, attempt.At.Add(RetryBase))
	}
	return next
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// Allowed is only an eligibility check: expiry never schedules an upstream
// request itself. Only demonstrated useful work may bypass a soft yield hold.
func Allowed(state types.RouteState, now time.Time, usefulKnownLanding bool) bool {
	if state.LastAttempt.IsZero() || state.LastAttempt.After(now.Add(RetryMax)) || !now.Before(state.NextCheck) {
		return true
	}
	return usefulKnownLanding && state.LastFailure == "" && state.Gateway != "" &&
		!state.ObservedAt.IsZero() && !now.Before(state.ObservedAt) && now.Sub(state.ObservedAt) <= RetryMax
}
