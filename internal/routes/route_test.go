package routes

import (
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"gwpool/internal/types"
)

func testRoute() types.RouteDescriptor {
	return Describe(types.Creds{AccountID: "workspace"}, types.Exit{Region: "US"},
		types.RouteSource{ID: Digest("proxy")}, Bare, "")
}

func TestRouteIdentitySeparatesMemberSourceSeedAndFullLocation(t *testing.T) {
	token := func(member string, expiry int) string {
		body := fmt.Sprintf(`{"sub":%q,"exp":%d}`, member, expiry)
		return "e30." + base64.RawURLEncoding.EncodeToString([]byte(body)) + ".test"
	}
	creds := types.Creds{AccountID: "workspace", AccessToken: token("member-one", 1)}
	refreshed := creds
	refreshed.AccessToken = token("member-one", 2)
	other := creds
	other.AccessToken = token("member-two", 2)
	if CredentialIdentity(creds) != CredentialIdentity(refreshed) || CredentialIdentity(creds) == CredentialIdentity(other) {
		t.Fatal("identity follows token rotation or merges distinct members")
	}
	base := testRoute()
	variants := []types.RouteDescriptor{base, base, base, base, base}
	variants[0].Identity = CredentialIdentity(other)
	variants[1].Exit.State, variants[1].Exit.City = "california", "los angeles"
	variants[2].Source = Digest("another-proxy")
	variants[3].Mode, variants[3].Seed = Seed, Digest("cookie-one")
	variants[4].Mode, variants[4].Seed = Seed, Digest("cookie-two")
	seen := map[string]bool{Key(base): true}
	for _, variant := range variants {
		if seen[Key(variant)] || Validate(variant) != nil {
			t.Fatal("different route conditions merged or descriptor rejected")
		}
		seen[Key(variant)] = true
	}
}

func TestRouteGainAtDriftDoesNotEraseOriginalMiss(t *testing.T) {
	now := time.Now().UTC()
	attempt := types.RouteAttempt{Route: testRoute(), StartedAt: now, At: now,
		Status: 200, Expected: "a", Gateway: "b"}
	state := Advance(types.RouteState{}, attempt, types.StockGain{ExtendedMS: 1000})
	if state.LastMissed != "a" || state.Productive != 1 || state.Drifts != 1 ||
		Allowed(state, now, false) || !Allowed(state, now, true) {
		t.Fatal("drift gain incorrectly counts as restoring the original goal or blocks useful B work")
	}
	attempt.Expected, attempt.StartedAt, attempt.At = "b", now.Add(time.Minute), now.Add(time.Minute)
	state = Advance(state, attempt, types.StockGain{ExtendedMS: 1000})
	if state.LastMissed != "a" || state.LowYieldStreak == 0 || Allowed(state, attempt.At, false) {
		t.Fatal("useful B renewal cleared route's unresolved miss of A")
	}
	if !Allowed(state, state.NextCheck, false) {
		t.Fatal("temporary evidence became a permanent blacklist")
	}
}

func TestRouteFailuresPreserveMappingWithoutSoftBypass(t *testing.T) {
	now := time.Now().UTC()
	descriptor := testRoute()
	state := Advance(types.RouteState{}, types.RouteAttempt{Route: descriptor, StartedAt: now, At: now,
		Status: 200, Gateway: "b"}, types.StockGain{CoverageAdded: true})
	later := now.Add(time.Minute)
	state = Advance(state, types.RouteAttempt{Route: descriptor, StartedAt: later, At: later,
		Failure: "transport"}, types.StockGain{})
	if state.Gateway != "b" || state.ObservedAt != now || Allowed(state, later, true) {
		t.Fatal("transport failure fabricated a landing or useful-work bypassed transport hold")
	}
}

func TestRouteRejectsOutOfOrderMappingAndBoundsBackoff(t *testing.T) {
	now := time.Now().UTC()
	descriptor := testRoute()
	var state types.RouteState
	for i := 0; i < 12; i++ {
		state = Advance(state, types.RouteAttempt{Route: descriptor, StartedAt: now, At: now,
			Status: 200, Expected: "a", Gateway: "b"}, types.StockGain{})
		delay := state.NextCheck.Sub(now)
		if delay < RetryBase || delay > RetryMax || !ValidState(Key(descriptor), state) {
			t.Fatal("invalid or unbounded route backoff")
		}
		now = state.NextCheck
	}
	stale := Advance(state, types.RouteAttempt{Route: descriptor, StartedAt: state.StartedAt.Add(-time.Minute),
		At: now, Status: 200, Gateway: "stale"}, types.StockGain{CoverageAdded: true})
	if stale.Gateway != state.Gateway || stale.ObservedAt != state.ObservedAt {
		t.Fatal("late result replaced newer routing observation")
	}
	if stale.Attempts != state.Attempts+1 || stale.Productive != state.Productive+1 ||
		stale.AddedCoverage != state.AddedCoverage+1 || stale.NextCheck != state.NextCheck {
		t.Fatal("late productive stock was lost from accounting or reset routing backoff")
	}
}

func TestRouteClockRollbackRebuildsTemporaryEvidence(t *testing.T) {
	now := time.Now().UTC()
	attempt := types.RouteAttempt{Route: testRoute(), StartedAt: now, At: now,
		Status: 200, Expected: "a", Gateway: "b"}
	state := Advance(types.RouteState{}, attempt, types.StockGain{})
	past := now.Add(-2 * RetryMax)
	if !Allowed(state, past, false) {
		t.Fatal("large clock rollback made a route permanently unavailable")
	}
	attempt.StartedAt, attempt.At, attempt.Expected, attempt.Gateway = past, past, "", "c"
	updated := Advance(state, attempt, types.StockGain{CoverageAdded: true})
	if updated.Gateway != "c" || updated.LastMissed != "" || updated.LowYieldStreak != 0 ||
		updated.NextCheck != past.Add(RetryBase) || updated.Attempts != 2 || !ValidState(Key(attempt.Route), updated) {
		t.Fatal("clock rollback retained future mapping/hold or dropped accounting")
	}
}

func TestRouteUnknownMemberDoesNotMergeWorkspaceCredentials(t *testing.T) {
	token := func(exp int) string {
		body := fmt.Sprintf(`{"exp":%d}`, exp)
		return "e30." + base64.RawURLEncoding.EncodeToString([]byte(body)) + ".test"
	}
	a := types.Creds{AccountID: "workspace", AccessToken: token(1)}
	b := types.Creds{AccountID: "workspace", AccessToken: token(2)}
	if CredentialIdentity(a) == CredentialIdentity(b) {
		t.Fatal("unknown members inherited another credential's workspace route")
	}
	sameAccess := a
	sameAccess.RefreshToken = "rotated-refresh-token"
	if CredentialIdentity(a) != CredentialIdentity(sameAccess) {
		t.Fatal("identical unknown credentials cannot share in-flight/temporary evidence")
	}
}
