package mint

import (
	"context"
	"fmt"
	"slices"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

// 管理页在运行中改出口代理：只许往末尾**加**、改**启用标志**，不许删、不许换顺序。
// sid 绑的是 Proxies 的下标，删一条或换顺序，已经绑上的 sid 就指到别的出口，
// 铸票和随后那一发 state-echo 配不成同一个 IP。
//
// 两个方法都是写时复制：Proxies 可能和启动时那份配置共用底层数组
// （main 直接把 cfg.Proxies 交进来），原地改会改到别人手上的配置快照。

// probeExit 是试新代理时用的出口：两种供应商都认美国，novproxy 的主机本来就在美国。
var probeExit = types.Exit{Region: "US"}

// AddProxy 把一条出口加到末尾，立即参与轮转。重名拒收：名字是熔断计数、日志、
// 页面上停用按钮认的键。
func (c *Client) AddProxy(p config.Proxy) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, q := range c.Proxies {
		if q.Name == p.Name {
			return fmt.Errorf("已经有一条叫 %q 的出口代理", p.Name)
		}
	}
	c.Proxies = append(slices.Clip(c.Proxies), p)
	return nil
}

// SetProxyEnabled 停用 / 启用一条出口。
//
// 停用只是不再给**新** sid 挑它：已经绑上的 sid 照旧走它直到用完（resolve 对已绑的
// sid 不看 Enabled），不然一次铸票和它的 state-echo 会被拆到两个出口上。
// 熔断状态不动：启用一条正在熔断里的出口，它照样等到熔断期满。
func (c *Client) SetProxyEnabled(name string, on bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := slices.IndexFunc(c.Proxies, func(p config.Proxy) bool { return p.Name == name })
	if i < 0 {
		return fmt.Errorf("运行中没有叫 %q 的出口代理", name)
	}
	ps := slices.Clone(c.Proxies)
	ps[i].Enabled = on
	c.Proxies = ps
	return nil
}

// ProbeProxy 经 p 查一次出口地理（ipinfo，不是上游），看这条代理通不通。
//
// 给管理页加代理用：用户名、密码填错的话 SOCKS5 认证这一步就被拒，当场报出来，
// 而不是进了轮转再连败三次才被熔断、其间让取票失败。所以它**不进** Proxies、
// 不碰熔断计数，也不占速率配额（geo 本来就不算）。只试一次，不重试。
func (c *Client) ProbeProxy(ctx context.Context, p config.Proxy) (types.Observed, error) {
	user, err := proxyUser(p, probeExit, randSID(), c.stickyMin())
	if err != nil {
		return types.Observed{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, geoTimeout)
	defer cancel()
	req, err := c.geoRequest(ctx)
	if err != nil {
		return types.Observed{}, err
	}
	resp, err := proxyClient(p, user, c.timeout(), c.fingerprint(), nil).Do(req)
	if err != nil {
		return types.Observed{}, err
	}
	return readGeo(resp)
}
