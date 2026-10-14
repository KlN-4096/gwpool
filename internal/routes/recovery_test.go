package routes

import (
	"fmt"
	"testing"
	"time"

	"gwpool/internal/types"
)

func recoverySource(gateway, identity string, at time.Time) types.RecoverySource {
	return types.RecoverySource{Gateway: gateway, LastSeen: at,
		Route: Describe(types.Creds{AccountID: identity}, types.Exit{Region: "US"},
			types.RouteSource{ID: Digest("fake-source")}, Bare, "")}
}

func TestRecoverySourcesBoundsAndExpiry(t *testing.T) {
	now := time.Now().UTC()
	sources := map[string]types.RecoverySource{}
	add := func(source types.RecoverySource) { sources[RecoveryKey(source)] = source }
	for i := 0; i < MaxRecoverySources; i++ {
		add(recoverySource(fmt.Sprint(i), "account", now))
	}
	for i := 0; i < MaxSourcesPerGateway+2; i++ {
		add(recoverySource("crowded", fmt.Sprint(i), now.Add(time.Duration(i)*time.Second)))
	}
	expired := recoverySource("expired", "account", now.Add(-RecoveryRetention))
	add(expired)
	PruneRecoverySources(sources, now)
	if len(sources) != MaxRecoverySources {
		t.Fatal("global source limit not applied")
	}
	count := 0
	for _, source := range sources {
		if source.Gateway == "crowded" {
			count++
		}
	}
	if count != MaxSourcesPerGateway {
		t.Fatal("per-gateway limit not applied")
	}
	if _, found := sources[RecoveryKey(expired)]; found {
		t.Fatal("source retained at exact expiry boundary")
	}
	if RecoveryFresh(expired, now) || RecoveryFresh(recoverySource("future", "x", now.Add(time.Second)), now) {
		t.Fatal("future/expired evidence became usable")
	}
	again := make(map[string]types.RecoverySource)
	for key, source := range sources {
		again[key] = source
	}
	PruneRecoverySources(again, now)
	if len(again) != len(sources) {
		t.Fatal("pruning not idempotent")
	}
	for key := range sources {
		if _, ok := again[key]; !ok {
			t.Fatal("deterministic eviction changed on repeated prune")
		}
	}
}

func TestRecoveryObservationRequiresSuccessfulBareLanding(t *testing.T) {
	now := time.Now().UTC()
	source := recoverySource("a", "identity", now)
	good := types.RouteAttempt{Route: source.Route, At: now, Gateway: "a", Status: 200}
	for _, mode := range []string{"ok", "seed", "429", "failure", "empty", "invalid"} {
		attempt := good
		switch mode {
		case "seed":
			attempt.Route.Mode, attempt.Route.Seed = Seed, Digest("seed")
		case "429":
			attempt.Status = 429
		case "failure":
			attempt.Failure = "invalid_response"
		case "empty":
			attempt.Gateway = ""
		case "invalid":
			attempt.Route.Source = "credential"
		}
		_, ok := RecoveryObservation(attempt)
		if ok != (mode == "ok") {
			t.Fatalf("%s unexpected observation acceptance", mode)
		}
	}
}
