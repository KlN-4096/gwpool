package mint

import (
	"net/http"
	"testing"

	"gwpool/internal/config"
)

func TestLoginTransportRequiresDynamicProxy(t *testing.T) {
	for _, proxies := range [][]config.Proxy{nil, {seedProxy}, {{Name: "disabled", Kind: config.KindNovproxy, Enabled: false}}} {
		if _, err := LoginTransport(config.Config{Proxies: proxies}); err == nil {
			t.Fatal("login can fall back to seed/direct/disabled proxy")
		}
	}
	transport, err := LoginTransport(config.Config{Proxies: []config.Proxy{novProxy}})
	if err != nil {
		t.Fatal(err)
	}
	tr := transport.(*http.Transport)
	if tr.DisableKeepAlives || tr.TLSClientConfig != nil {
		t.Fatal("login should retain its session without weakening TLS validation")
	}
}
