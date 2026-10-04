package httpapi

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestGatewayPriorityFreshListAndDeliveryCountry(t *testing.T) {
	srv, handler, _, st, _ := rig(t, types.ModePool)
	key := srv.ConsumerKeyFor(st.accounts[0])
	gateway := "unified-142"
	st.gws[gateway] = types.Gateway{Name: gateway}
	st.pairs[gateway] = types.Pair{Gateway: gateway, ExpiresAt: time.Now().Add(time.Hour)}
	for i := range 5 {
		st.contacts = append(st.contacts, types.ContactReport{ReporterID: int64(i + 1),
			ContactObservation: types.ContactObservation{Gateway: gateway, Model: "luna", Criterion: "state-echo-v1",
				Source: "foreground", Outcome: "full", At: time.Now()}})
	}
	read := func() int {
		w := do(handler, authed(httptest.NewRequest("GET", "/gateways?model=luna", nil), key))
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var out struct {
			Gateways []struct {
				Name     string                 `json:"name"`
				Priority *types.GatewayPriority `json:"priority"`
				Country  string                 `json:"datacenter_country"`
			} `json:"gateways"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		for _, row := range out.Gateways {
			if row.Name == gateway {
				if row.Priority == nil || row.Country == "" {
					t.Fatal("missing priority or geography", row)
				}
				return row.Priority.Full
			}
		}
		t.Fatal("missing gateway")
		return 0
	}
	if read() != 5 {
		t.Fatal("missing global observations")
	}
	st.contacts[0].Outcome = "refreshed"
	if read() != 4 {
		t.Fatal("stale priority")
	}
	if srv.delivered(types.Delivery{Gateway: gateway}).DatacenterCountry == "" {
		t.Fatal("single and batch delivery must carry country too")
	}
	if srv.delivered(types.Delivery{Gateway: "unknown-gateway"}).DatacenterCountry != "" {
		t.Fatal("unknown country was guessed")
	}
}
