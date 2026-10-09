package httpapi

import (
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestGatewayCatalogEncodingPreservesWireContract(t *testing.T) {
	contacts, err := json.Marshal([]types.ContactStats{{Gateway: "g", Model: "quote\" & <script>\n", Full: 5}})
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range [][]gatewayCatalogRow{nil, {}, {{
		Name: "quoted\"gateway", PairReady: true, ValidForS: 300,
		Contacts: contacts, DatacenterCountry: "US",
		Cooldown: &types.CooldownRecommendation{Seconds: 3600, ReportIntervalSeconds: 21600,
			ReportPolicyExpiresAt: time.Now().UTC()},
		Priority: &types.GatewayPriority{Model: "m", Full: 5, Samples: 6},
	}, {Name: "without-contacts"}}} {
		response := gatewayCatalogResponse{gatewayCatalogHeader: gatewayCatalogHeader{
			Account: "quote\" & <script>", Live: len(rows), Target: 106, SnapshotAt: time.Now().UTC(), DeliverFloorS: 300},
			Gateways: rows}
		expected, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		recorder := httptest.NewRecorder()
		if err := writeGatewayCatalog(recorder, response); err != nil {
			t.Fatal(err)
		}
		var want, got any
		if err := json.Unmarshal(expected, &want); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("optimized encoding changed the JSON response")
		}
		if recorder.Header().Get("Content-Length") != strconv.Itoa(recorder.Body.Len()) ||
			strings.Contains(recorder.Body.String(), "<script>") {
			t.Fatal("incorrect framing or unsafe string encoding")
		}
	}
}
