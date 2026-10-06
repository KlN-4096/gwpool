package httpapi

import (
	"gwpool/internal/config"
	"gwpool/internal/coverage"
	"gwpool/internal/types"
	"testing"
	"time"
)

func TestDirectoryProgressSeparatesUnknownAndExpired(t *testing.T) {
	now := time.Now()
	gws := map[string]types.Gateway{"unified-12": {Name: "unified-12"}, "unified-outside": {Name: "unified-outside"}}
	pairs := map[string]types.Pair{"unified-12": {Cookie: "fake", ExpiresAt: now.Add(time.Hour)}}
	v := buildDirectory(coverage.Build(config.Defaults(), gws, pairs, now))
	if v.Total != 201 || v.Found != 1 || v.Unknown != 1 || v.Ready != 1 || v.Missing != 200 {
		t.Fatalf("wrong directory %+v", v)
	}
	denominators := []int{89, 14, 44, 20, 10, 10, 0, 0, 14}
	for i, row := range v.Areas {
		if row.Total != denominators[i] {
			t.Fatalf("wrong area denominator: %+v", row)
		}
		if row.Total == 0 && row.Percent != 0 {
			t.Fatal("empty directory reported complete")
		}
	}
	expired := buildDirectory(coverage.Build(config.Defaults(), gws, pairs, now.Add(2*time.Hour)))
	if expired.Found != v.Found || expired.Ready != 0 {
		t.Fatal("expiration changed discovery progress")
	}
}
