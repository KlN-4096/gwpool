package store

import (
	"slices"
	"testing"

	"gwpool/internal/types"
)

func TestGatewayNarrowReadsTrackKnownInventoryWithoutAliasing(t *testing.T) {
	s := open(t, t.TempDir())
	if s.GatewayCount() != 0 || len(s.GatewayNames()) != 0 || s.HasGateway("g") {
		t.Fatal("empty inventory was not empty")
	}
	for _, name := range []string{"g", "other"} {
		if _, err := s.UpsertGateway(name, 1, types.Exit{Region: "US", City: "offline"}); err != nil {
			t.Fatal(err)
		}
	}
	names := s.GatewayNames()
	if s.GatewayCount() != 2 || !s.HasGateway("g") || !slices.Contains(names, "other") {
		t.Fatal("narrow reads disagree with known gateway metadata")
	}
	names[0] = "mutated"
	if s.HasGateway("mutated") || slices.Contains(s.GatewayNames(), "mutated") {
		t.Fatal("gateway names alias writable store state")
	}
}
