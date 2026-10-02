package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

// 管理页往配置文件里加代理、改代理的启用标志。两个都走 Patch（校验不过就回滚）。
//
// 原有条目以 json.RawMessage 原样搬回去，一个字节都不经过 Secret——它的 MarshalJSON
// 会把口令打码成 "***"，一次保存就毁掉全部 socks 口令（见 Patch 的注释）。
//
// 读文件和 Patch 里那次读之间不加锁：本进程里的写者由调用方串起来（httpapi 的 configWriteMu）。

// AppendProxy 往 proxies 末尾追加一条。口令写原文，和手写进文件的那些一样。
func AppendProxy(path string, p Proxy) error {
	list, err := rawProxies(path)
	if err != nil {
		return err
	}
	// 外层的 Pass 盖住内嵌 Proxy 的那个（JSON 取浅的一层）：其余字段跟着 Proxy 走，
	// 将来 Proxy 加了字段也不会在这里被漏掉。
	b, err := json.Marshal(struct {
		Proxy
		Pass string `json:"pass"`
	}{p, p.Pass.Reveal()})
	if err != nil {
		return err
	}
	return Patch(path, map[string]any{"proxies": append(list, b)})
}

// SetProxyEnabled 改名叫 name 那一条的 enabled，这一条里别的键原样留着。
// 停掉最后一条启用着的粘性代理会被 Load 的校验拒掉、回滚。
func SetProxyEnabled(path, name string, on bool) error {
	list, err := rawProxies(path)
	if err != nil {
		return err
	}
	found := false
	for i, raw := range list {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			return fmt.Errorf("proxies[%d] 读不懂: %w", i, err)
		}
		var n string
		if err := json.Unmarshal(m["name"], &n); err != nil || n != name {
			continue
		}
		m["enabled"] = json.RawMessage(strconv.FormatBool(on))
		if list[i], err = json.Marshal(m); err != nil {
			return err
		}
		found = true
	}
	if !found {
		return fmt.Errorf("配置文件里没有叫 %q 的出口代理", name)
	}
	return Patch(path, map[string]any{"proxies": list})
}

func rawProxies(path string) ([]json.RawMessage, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读配置: %w", err)
	}
	var top struct {
		Proxies []json.RawMessage `json:"proxies"`
	}
	if err := json.Unmarshal(b, &top); err != nil {
		return nil, fmt.Errorf("解析配置: %w", err)
	}
	return top.Proxies, nil
}
