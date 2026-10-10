package mint

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/routes"
	"gwpool/internal/types"
)

func (c *Client) routeSource(proxy config.Proxy) types.RouteSource {
	return types.RouteSource{ID: routes.Digest("proxy-route-v1", string(proxy.Kind),
		proxy.Host, proxy.User, proxy.Pass.Reveal(), strconv.Itoa(c.stickyMin())), Kind: string(proxy.Kind)}
}

func (c *Client) routeCapable(proxy config.Proxy, exit types.Exit, seed bool, now time.Time) bool {
	if !proxy.Enabled || now.Before(c.skipUntil[proxy.Name]) {
		return false
	}
	if seed {
		return proxy.Kind == config.KindSeed
	}
	if proxy.Kind == config.KindSeed || !proxy.Sticky() {
		return false
	}
	_, err := proxyUser(proxy, exit, strings.Repeat("0", proxySessionIDLength), c.stickyMin())
	return err == nil
}

// RouteSources is a read-only preselection. It neither binds a SID, advances a
// cursor, consumes an attempt nor contacts a proxy.
func (c *Client) RouteSources(exit types.Exit, seed bool) []types.RouteSource {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	cursor := c.cursor
	if seed {
		cursor = c.seedCursor
	}
	var out []types.RouteSource
	seen := map[string]bool{}
	for i := 0; i < len(c.Proxies); i++ {
		proxy := c.Proxies[(cursor+i)%len(c.Proxies)]
		if !c.routeCapable(proxy, exit, seed, now) {
			continue
		}
		source := c.routeSource(proxy)
		if !seen[source.ID] {
			seen[source.ID] = true
			out = append(out, source)
		}
	}
	return out
}

// NewSIDForRoute binds the exact preselected source after the caller reserves
// its attempt. A config/backoff change fails closed, rather than silently
// selecting another route whose cooldown was never checked.
func (c *Client) NewSIDForRoute(exit types.Exit, sourceID string, seed bool) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for index, proxy := range c.Proxies {
		if c.routeSource(proxy).ID != sourceID || !c.routeCapable(proxy, exit, seed, now) {
			continue
		}
		sid := randSID()
		c.bindLocked(sid, index, now)
		if seed {
			c.seedCursor = (index + 1) % len(c.Proxies)
		} else {
			c.cursor = (index + 1) % len(c.Proxies)
		}
		return sid, nil
	}
	return "", fmt.Errorf("%w: selected route is no longer available", types.ErrNoExit)
}

func (c *Client) RouteSource(sid string) types.RouteSource {
	c.mu.Lock()
	defer c.mu.Unlock()
	binding, ok := c.sids[sid]
	if !ok || binding.idx < 0 || binding.idx >= len(c.Proxies) {
		return types.RouteSource{}
	}
	return c.routeSource(c.Proxies[binding.idx])
}
