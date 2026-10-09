package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gwpool/internal/types"
)

func TestConsumerEndpointsReuseOriginalPathsWithoutCompatibility(t *testing.T) {
	_, handler, pool, _, scanner := rig(t, types.ModePool)
	for _, path := range []string{
		"/cookie?account=member&gateway=unified-142",
		"/gateways?account=member",
	} {
		w := do(handler, authed(httptest.NewRequest("GET", path, nil), "ckey"))
		if w.Code != http.StatusOK {
			t.Fatalf("original endpoint did not serve the strict contract: %s status=%d body=%s", path, w.Code, w.Body.String())
		}
	}
	calls := pool.calls
	for _, path := range []string{
		"/cookie", "/cookie?account=member", "/cookie?gateway=unified-142", "/gateways",
		"/cookie?account=member&gateway=unified-142&force=0",
		"/cookie?account=member&gateway=unified-142&wait=0",
		"/cookie?account=member&gateway=unified-142&count=1",
	} {
		w := do(handler, authed(httptest.NewRequest("GET", path, nil), "ckey"))
		if w.Code != http.StatusBadRequest || decodeFail(t, w).Error.Code != codeBadRequest ||
			!strings.Contains(w.Body.String(), "最新版") {
			t.Fatalf("old request must be rejected with upgrade guidance: %s status=%d body=%s", path, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/v2/cookie?account=member&gateway=unified-142", "/v2/gateways?account=member"} {
		w := do(handler, authed(httptest.NewRequest("GET", path, nil), "ckey"))
		if w.Code != http.StatusNotFound {
			t.Fatalf("unrequested versioned endpoint remains: %s status=%d", path, w.Code)
		}
	}
	if pool.calls != calls || len(scanner.discovery) != 0 {
		t.Fatal("invalid/removed endpoints performed consumer work")
	}
}
