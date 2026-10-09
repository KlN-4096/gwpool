package mint

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math/big"
	"net/http"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

const loginSessionMinutes = 15

// LoginTransport is independent of mint scheduling, budgets, and SID maps.
// It chooses one enabled dynamic provider once, never a seed/direct fallback.
// A single sticky session must outlive the login task including email input.
func LoginTransport(cfg config.Config) (http.RoundTripper, error) {
	var eligible []config.Proxy
	for _, p := range cfg.StickyProxies() {
		if _, err := proxyUser(p, types.Exit{Region: "US"}, "0000000000000000", loginSessionMinutes); err == nil {
			eligible = append(eligible, p)
		}
	}
	if len(eligible) == 0 {
		return nil, errors.New("没有支持登录会话的已启用动态出口")
	}
	index, err := rand.Int(rand.Reader, big.NewInt(int64(len(eligible))))
	if err != nil {
		return nil, errors.New("无法选择登录出口")
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, errors.New("无法生成登录出口会话")
	}
	p := eligible[index.Int64()]
	user, err := proxyUser(p, types.Exit{Region: "US"}, hex.EncodeToString(nonce[:]), loginSessionMinutes)
	if err != nil {
		return nil, errors.New("登录出口配置无效")
	}
	client := proxyClient(p, user, 30*time.Second, config.FingerprintMint, nil)
	transport := client.Transport.(*http.Transport)
	transport.DisableKeepAlives = false
	transport.DisableCompression = false
	transport.MaxIdleConnsPerHost = 2
	transport.IdleConnTimeout = time.Minute
	transport.TLSHandshakeTimeout = 15 * time.Second
	transport.ResponseHeaderTimeout = 25 * time.Second
	return transport, nil
}
