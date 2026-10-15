package httpapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"gwpool/internal/mint"
	"gwpool/internal/sched"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

func TestDeliveryTimezoneWireContract(t *testing.T) {
	server := new(Server)
	for _, gateway := range []string{"unified-142", "unknown-gateway"} {
		body, err := json.Marshal(server.delivered(types.Delivery{Gateway: gateway}))
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal(body, &fields); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"datacenter_timezone", "mint_exit_timezone", "mint_exit_observed_at"} {
			if _, present := fields[name]; !present {
				t.Fatalf("missing explicit nullable field %q", name)
			}
		}
		if gateway == "unified-142" && fields["datacenter_timezone"] != "Europe/Warsaw" {
			t.Fatal("datacenter timezone does not match the historical directory")
		}
		if gateway == "unknown-gateway" && fields["datacenter_timezone"] != nil {
			t.Fatal("unknown gateway acquired a guessed datacenter timezone")
		}
		if fields["mint_exit_timezone"] != nil || fields["mint_exit_observed_at"] != nil {
			t.Fatal("legacy ticket acquired invented exit metadata")
		}
	}
}

// Any network lookup on the consumer path is a test failure, not a real probe.
type timezoneNoNetworkMinter struct{ mint.Client }

func (*timezoneNoNetworkMinter) Geo(context.Context, types.Exit, string) (types.Observed, error) {
	panic("consumer timezone metadata performed Geo")
}
func (*timezoneNoNetworkMinter) Mint(context.Context, types.Creds, types.Exit, string, string, time.Time) (types.MintResult, error) {
	panic("consumer timezone metadata minted a ticket")
}
func (*timezoneNoNetworkMinter) VerifyFull(context.Context, types.Creds, types.Exit, string, string, string) (*types.Verdict, error) {
	panic("consumer timezone metadata performed a model probe")
}

func TestCatalogAndDeliveryTimezonesFollowRehomedTicketAndInventoryRevision(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	now := time.Now().UTC().Truncate(time.Second)
	target := types.Pair{Gateway: "unified-142", RegionKey: "us-east",
		Cookie:        routeCookie("old", "unified-142", now.Add(time.Hour)),
		CflbExpiresAt: now.Add(time.Minute), ExpiresAt: now.Add(time.Minute),
		MintExit: types.ExitTimezoneObservation{Timezone: "Asia/Tokyo", ObservedAt: now.Add(-time.Minute)}}
	donor := types.Pair{Gateway: "unified-1", RegionKey: target.RegionKey,
		Cookie:        routeCookie("donor", "unified-1", now.Add(2*time.Hour)),
		CflbExpiresAt: now.Add(90 * time.Minute), ExpiresAt: now.Add(90 * time.Minute),
		MintExit: types.ExitTimezoneObservation{Timezone: "America/Chicago", ObservedAt: now}}
	legacy := types.Pair{Gateway: "outside-directory", Cookie: routeCookie("legacy", "outside-directory", now.Add(time.Hour)),
		ExpiresAt: now.Add(time.Hour), CflbExpiresAt: now.Add(time.Hour)}
	for _, pair := range []types.Pair{target, donor, legacy} {
		if _, err := st.UpsertGateway(pair.Gateway, 0, types.Exit{}); err != nil {
			t.Fatal(err)
		}
		if err := st.PutPair(pair); err != nil {
			t.Fatal(err)
		}
	}
	pool := sched.New(st, &timezoneNoNetworkMinter{}, sched.Options{Now: func() time.Time { return now }})
	server, err := New(pool, st, nil, Options{ConsumerKey: "offline-timezone"})
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	catalog := func() map[string]gatewayCatalogRow {
		t.Helper()
		recorder := do(handler, authed(httptest.NewRequest("GET", "/gateways?account=member", nil), "offline-timezone"))
		var response gatewayCatalogResponse
		if recorder.Code != 200 || json.Unmarshal(recorder.Body.Bytes(), &response) != nil {
			t.Fatal("catalog failed")
		}
		rows := map[string]gatewayCatalogRow{}
		for _, row := range response.Gateways {
			rows[row.Name] = row
		}
		return rows
	}
	deliver := func() deliveryResponse {
		t.Helper()
		recorder := do(handler, authed(httptest.NewRequest("GET", "/cookie?account=member&gateway="+target.Gateway, nil), "offline-timezone"))
		var response deliveryResponse
		if recorder.Code != 200 || json.Unmarshal(recorder.Body.Bytes(), &response) != nil {
			t.Fatal("delivery failed")
		}
		return response
	}
	check := func(fields gatewayTimezones, want types.ExitTimezoneObservation) {
		t.Helper()
		if fields.DatacenterTimezone == nil || *fields.DatacenterTimezone != "Europe/Warsaw" ||
			fields.MintExitTimezone == nil || *fields.MintExitTimezone != want.Timezone ||
			fields.MintExitObservedAt == nil || !fields.MintExitObservedAt.Equal(want.ObservedAt) {
			t.Fatal("wire metadata disagrees with exact selected ticket")
		}
	}
	rows := catalog()
	check(rows[target.Gateway].gatewayTimezones, target.MintExit)
	if row := rows[legacy.Gateway]; row.DatacenterTimezone != nil || row.MintExitTimezone != nil || row.MintExitObservedAt != nil {
		t.Fatal("old ticket or unknown datacenter acquired inferred metadata")
	}
	first := deliver()
	check(first.gatewayTimezones, target.MintExit)
	if mint.CflbOf(first.Cookie) != "donor" {
		t.Fatal("fixture did not exercise rehoming")
	}

	// Inventory can change between the catalog snapshot and actual acquisition.
	next := target
	next.Cookie = routeCookie("replacement", target.Gateway, now.Add(3*time.Hour))
	next.CflbExpiresAt, next.ExpiresAt = now.Add(2*time.Hour), now.Add(2*time.Hour)
	next.MintExit = types.ExitTimezoneObservation{Timezone: "Europe/Paris", ObservedAt: now}
	if err := st.PutPair(next); err != nil {
		t.Fatal(err)
	}
	check(deliver().gatewayTimezones, next.MintExit)
	check(catalog()[target.Gateway].gatewayTimezones, next.MintExit)

	// HTTP formatting must not reread newly replaced stock after selection.
	selected := types.Delivery{Gateway: target.Gateway, Cookie: first.Cookie, MintExit: target.MintExit}
	check(server.delivered(selected).gatewayTimezones, target.MintExit)
}

func TestIncompleteExitObservationStaysUnknownAndTimestampIsUTC(t *testing.T) {
	for _, observation := range []types.ExitTimezoneObservation{
		{Timezone: "Asia/Tokyo"}, {ObservedAt: time.Now()},
	} {
		fields := timezonesForTicket("outside-directory", observation)
		if fields.MintExitTimezone != nil || fields.MintExitObservedAt != nil {
			t.Fatal("partial provenance became a usable observation")
		}
	}
	observedAt := time.Date(2026, 10, 15, 18, 0, 0, 0, time.FixedZone("test-offset", 9*3600))
	fields := timezonesForTicket("outside-directory", types.ExitTimezoneObservation{
		Timezone: "Asia/Tokyo", ObservedAt: observedAt})
	if fields.MintExitObservedAt == nil || fields.MintExitObservedAt.Location() != time.UTC ||
		fields.MintExitObservedAt.Format(time.RFC3339) != "2026-10-15T09:00:00Z" {
		t.Fatal("observation timestamp did not preserve the instant in UTC")
	}
}
