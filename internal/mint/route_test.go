package mint

import (
	"strings"
	"testing"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

func TestRoutePreselectionIsReadOnlyAndBindsExactSource(t *testing.T) {
	c := &Client{Proxies: []config.Proxy{novProxy, wsProxy}}
	exit := types.Exit{Region: "US"}
	options := c.RouteSources(exit, false)
	if len(options) != 2 || c.cursor != 0 || len(c.sids) != 0 {
		t.Fatal("route preselection bound, rotated or lost a source")
	}
	first, err := c.NewSIDForRoute(exit, options[1].ID, false)
	if err != nil || c.proxyName(first) != wsProxy.Name {
		t.Fatalf("preselected source not honored: %v", err)
	}
	second, err := c.NewSIDForRoute(exit, options[1].ID, false)
	if err != nil || first == second || c.RouteSource(first) != c.RouteSource(second) {
		t.Fatal("random session became a different stable route")
	}
	if err := c.SetProxyEnabled(wsProxy.Name, false); err != nil {
		t.Fatal(err)
	}
	if _, err := c.NewSIDForRoute(exit, options[1].ID, false); err == nil {
		t.Fatal("disabled preselected source silently used or replaced")
	}
	if len(c.sent) != 0 {
		t.Fatal("local preselection/binding spent an upstream attempt")
	}
}

func TestRouteSourceChangesWithConnectionConfigNotLabelOrOrder(t *testing.T) {
	c := &Client{}
	base := novProxy
	first := c.routeSource(base)
	renamed := base
	renamed.Name = "new-label"
	if first != c.routeSource(renamed) {
		t.Fatal("a display label reset routing evidence")
	}
	for _, field := range []string{"host", "user", "pass"} {
		changed := base
		switch field {
		case "host":
			changed.Host = "proxy.example.invalid:1234"
		case "user":
			changed.User = "synthetic-other-user"
		case "pass":
			changed.Pass = config.Secret("synthetic-other-password")
		}
		source := c.routeSource(changed)
		if source == first || len(source.ID) != 64 ||
			strings.Contains(source.ID, changed.Pass.Reveal()) {
			t.Fatalf("source identity omitted configuration or exposed it: %s", field)
		}
	}
}
