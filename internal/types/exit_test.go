package types

import "testing"

func TestPunctuationCannotStandInForObservedLocation(t *testing.T) {
	for _, value := range []string{".", "-", "_'"} {
		if ValidLocationName(value) {
			t.Errorf("accepted %q", value)
		}
		if (Exit{Region: "US", State: value}).MatchesObserved(Observed{Country: "US"}) {
			t.Errorf("missing observed state matched %q", value)
		}
		if (Exit{Region: "US", City: value}).MatchesObserved(Observed{Country: "US"}) {
			t.Errorf("missing observed city matched %q", value)
		}
	}
}

func TestExitEvidenceAndLabel(t *testing.T) {
	e := Exit{Region: "US", State: "California", City: "Los Angeles"}
	if e.Label() != "US:California:Los Angeles" {
		t.Fatal(e.Label())
	}
	for _, tc := range []struct {
		obs  Observed
		seed bool
		want string
	}{
		{Observed{}, false, "unknown"},
		{Observed{Country: "US", State: "California"}, false, "unknown"},
		{Observed{Country: "US", State: "California", City: "Los Angeles"}, false, "matched"},
		{Observed{Country: "US", State: "Texas", City: "Dallas"}, false, "mismatch"},
		{Observed{Country: "US", State: "California", City: "Los Angeles"}, true, "seed-route"},
	} {
		if got := e.MatchEvidence(tc.obs, tc.seed); got != tc.want {
			t.Fatalf("%+v: got %s want %s", tc, got, tc.want)
		}
	}
}

func TestObservedExitMustMatchEveryRequestedComponent(t *testing.T) {
	exit := Exit{Region: "US", State: "New York", City: "New York"}
	if !exit.MatchesObserved(Observed{Country: "US", State: "NewYork", City: "New York"}) {
		t.Fatal("equivalent location spelling was rejected")
	}
	for _, observed := range []Observed{
		{},
		{Country: "US"},
		{Country: "US", State: "New York", City: "Buffalo"},
		{Country: "US", State: "California", City: "New York"},
		{Country: "CA", State: "New York", City: "New York"},
	} {
		if exit.MatchesObserved(observed) {
			t.Fatalf("unconfirmed target was accepted: %+v", observed)
		}
	}
}

func TestLocationNamesCannotInjectProxyOptions(t *testing.T) {
	for _, value := range []string{"New York", "São Paulo", "Baden-Württemberg", "'Asir Region"} {
		if !ValidLocationName(value) {
			t.Fatalf("valid directory value rejected: %q", value)
		}
	}
	for _, value := range []string{"", " New York", "New York\n", "US-sid-attacker", "X-country-DE", "x/@y", "x\x00y"} {
		if ValidLocationName(value) {
			t.Fatalf("unsafe proxy parameter accepted: %q", value)
		}
	}
}
