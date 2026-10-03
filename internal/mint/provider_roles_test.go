package mint

import (
	"errors"
	"strings"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

func TestSeedRotationHonorsBackoff(t *testing.T) {
	first, second := seedProxy, seedProxy
	first.Name, second.Name = "static-a", "static-b"
	c := &Client{Proxies: []config.Proxy{first, novProxy, second}}
	for i, want := range []string{"static-a", "static-b", "static-a"} {
		sid, err := c.NewSeedSID()
		if err != nil || c.proxyName(sid) != want {
			t.Fatalf("selection %d: name=%q err=%v, want %q", i, c.proxyName(sid), err, want)
		}
	}
	c.skipUntil = map[string]time.Time{first.Name: time.Now().Add(time.Hour)}
	sid, err := c.NewSeedSID()
	if err != nil || c.proxyName(sid) != second.Name {
		t.Fatalf("static backoff ignored: name=%q err=%v", c.proxyName(sid), err)
	}
}

func TestDynamicSelectionNeverUsesStaticExit(t *testing.T) {
	c := &Client{Proxies: []config.Proxy{seedProxy, novProxy}}
	for range 3 {
		sid, err := c.NewSID(false)
		if err != nil || c.proxyName(sid) != novProxy.Name {
			t.Fatalf("dynamic selection used static exit: name=%q err=%v", c.proxyName(sid), err)
		}
		if c.Provider(sid) != string(config.KindNovproxy) {
			t.Fatal("provider metadata must report only the bound kind")
		}
	}
	before := len(c.sids)
	if c.Provider("unknown") != "" || len(c.sids) != before {
		t.Fatal("reading provider bound an unknown SID")
	}
}

func TestCapabilityUsesTheActualSIDLengthAtUsernameLimit(t *testing.T) {
	p := novProxy
	p.User = "base"
	exit := types.Exit{Region: "US"}
	user, err := proxyUser(p, exit, "00000000", 5)
	if err != nil {
		t.Fatal(err)
	}
	p.User = strings.Repeat("x", 255-(len(user)-len(p.User)))
	c := &Client{Proxies: []config.Proxy{p}}
	if _, err := c.NewSIDFor(exit, true); err != nil {
		t.Fatal("real session unexpectedly rejected", err)
	}
	if !c.SupportsDynamicExit(exit) {
		t.Fatal("capability probe rejected a valid real session")
	}
}

func TestProviderRejectsUnsupportedCity(t *testing.T) {
	if user, err := proxyUser(wsProxy, types.Exit{Region: "US", City: "Seattle"}, "session1", 5); err == nil {
		t.Fatalf("country-only provider silently accepted a city: %q", user)
	}
}

func TestNewDynamicProviderUsernames(t *testing.T) {
	tests := []struct {
		kind, user, want string
	}{
		{"cliproxy", "clip-test", "clip-test-region-US-sid-session1-t-5"},
		{"ipipgo", "customer-test", "customer-test-country-US-session-session1-time-5"},
	}
	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			p := config.Proxy{Kind: config.ProxyKind(tt.kind), User: tt.user}
			got, err := proxyUser(p, types.Exit{Region: "US"}, "session1", 5)
			if err != nil || got != tt.want {
				t.Fatalf("username=%q err=%v, want %q", got, err, tt.want)
			}
		})
	}
}

func TestTargetedSelectionSkipsUnsupportedProviders(t *testing.T) {
	c := &Client{Proxies: []config.Proxy{wsProxy, seedProxy, novProxy}}
	for range 3 {
		sid, err := c.NewSIDFor(types.Exit{Region: "US", City: "Seattle"}, true)
		if err != nil || c.proxyName(sid) != novProxy.Name {
			t.Fatalf("city target selected an unsupported provider: name=%q err=%v", c.proxyName(sid), err)
		}
	}
	c = &Client{Proxies: []config.Proxy{wsProxy, seedProxy}}
	if _, err := c.NewSIDFor(types.Exit{Region: "US", City: "Seattle"}, true); err == nil {
		t.Fatal("city target silently fell back to a country-only or static exit")
	}
}

func TestAddedRegionsDoNotExpandAccountBudget(t *testing.T) {
	cfg := config.Defaults()
	cfg.UpstreamPerRegionHour = 1
	cfg.Regions = append(cfg.RegionDefinitions(), config.ExplorationRegion{
		Key: "canada-test", Name: "Canada", Country: "CA", Enabled: true,
	})
	c := &Client{Live: config.NewLive("", cfg)}
	for _, region := range types.Regions {
		if err := c.spend("account", region.Exit); err != nil {
			t.Fatalf("baseline budget failed early: %v", err)
		}
	}
	if err := c.spend("account", types.Exit{Region: "CA"}); !errors.Is(err, types.ErrRateLimited) {
		t.Fatalf("adding an exit expanded the account budget: %v", err)
	}
	c.refund("account", types.Regions[0].Exit)
	if err := c.spend("account", types.Exit{Region: "CA"}); err != nil {
		t.Fatalf("refunding a definitely-unsent request did not restore total budget: %v", err)
	}
}

func TestProviderStateAndCityParameters(t *testing.T) {
	clip := config.Proxy{Kind: config.KindCliproxy, User: "clip-test", Enabled: true}
	ipip := config.Proxy{Kind: config.KindIpipgo, User: "customer-test", Enabled: true}
	exit := types.Exit{Region: "US", State: "Indiana"}
	got, err := proxyUser(clip, exit, "session1", 5)
	if err != nil || got != "clip-test-region-US-st-Indiana-sid-session1-t-5" {
		t.Fatalf("Cliproxy state: %q %v", got, err)
	}
	exit.City = "Hammond"
	got, err = proxyUser(ipip, exit, "session1", 5)
	if err != nil || got != "customer-test-country-US-region-Indiana-city-Hammond-session-session1-time-5" {
		t.Fatalf("Ipipgo state/city: %q %v", got, err)
	}
	for _, unsupported := range []config.Proxy{novProxy, wsProxy, clip} {
		if _, err := proxyUser(unsupported, exit, "session1", 5); err == nil {
			t.Fatalf("%s silently ignored the state/city target", unsupported.Kind)
		}
	}
	clip.User = strings.Repeat("u", 240)
	if _, err := proxyUser(clip, types.Exit{Region: "US"}, "session1", 5); err == nil {
		t.Fatal("oversized SOCKS username was accepted")
	}
}
