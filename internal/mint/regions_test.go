package mint

import (
	"errors"
	"testing"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

func TestRegionQuotaForCustomExitUsesStableIndependentBucket(t *testing.T) {
	cfg := config.Defaults()
	cfg.UpstreamPerRegionHour = 1
	cfg.Regions = append(cfg.RegionDefinitions(), config.ExplorationRegion{
		Key: "africa-south", Name: "South Africa", Country: "ZA", Enabled: true,
	})
	c := &Client{Live: config.NewLive("", cfg)}
	custom := types.Exit{Region: "ZA"}
	if err := c.spend("test-account", custom); err != nil {
		t.Fatal(err)
	}
	if err := c.spend("test-account", custom); !errors.Is(err, types.ErrRateLimited) {
		t.Fatal("custom region should share its existing hourly cap")
	}
	if err := c.spend("test-account", types.Exit{Region: "JP"}); err != nil {
		t.Fatal("one spent region must not consume another region's budget")
	}
	used, _ := c.Rate("test-account")
	if used["africa-south"] != 1 || used["east-asia"] != 1 || used[""] != 0 {
		t.Fatalf("custom exit fell into an anonymous bucket: %+v", used)
	}
	c.refund("test-account", custom)
	used, _ = c.Rate("test-account")
	if used["africa-south"] != 0 || used["east-asia"] != 1 {
		t.Fatal("refund crossed region boundary")
	}
}
