package mint

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"gwpool/internal/types"
)

func TestGeoPreservesIANATimezone(t *testing.T) {
	calls := 0
	client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_, _ = fmt.Fprint(w, `{"ip":"192.0.2.1","country":"JP","timezone":"Asia/Tokyo"}`)
	})
	observed, err := client.Geo(context.Background(), types.Exit{Region: "JP"}, "sid1")
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(observed)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["Timezone"] != "Asia/Tokyo" {
		t.Fatal("the existing Geo response lost its IANA timezone")
	}
	if calls != 1 {
		t.Fatal("collecting timezone added a network request")
	}
}

func TestGeoOptionalTimezoneCannotBreakValidLocation(t *testing.T) {
	for _, raw := range []string{``, `,"timezone":null`, `,"timezone":""`, `,"timezone":"Local"`,
		`,"timezone":"+08:00"`, `,"timezone":"Unknown/Zone"`, `,"timezone":17`,
		`,"timezone":{}`, `,"timezone":["Asia/Tokyo"]`} {
		response := &http.Response{StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{"ip":"192.0.2.1","country":"JP"` + raw + `}`))}
		observed, err := readGeo(response)
		if err != nil || observed.Country != "JP" || observed.Timezone != "" {
			t.Fatalf("optional timezone altered Geo success: %s, err=%v", raw, err)
		}
	}
}
