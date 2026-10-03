package httpapi

import (
	"gwpool/internal/types"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSampleMetadataAppearsOnAdminAndAccountPages(t *testing.T) {
	_, handler, _, st, _ := rig(t, types.ModePool)
	st.samples[0].Provider = "cliproxy"
	st.samples[0].Purpose = types.PurposeExplore
	st.samples[0].TargetMatch = "matched"
	st.samples[0].DurationMS = 1250
	st.samples[0].Requested = types.Exit{Region: "US", State: "California", City: "Los Angeles"}
	st.samples[0].Observed = types.Observed{Country: "US", State: "California", City: "Los Angeles"}
	for _, path := range []string{"/admin/", "/a/vt-1"} {
		resp := do(handler, authed(httptest.NewRequest("GET", path, nil), "akey"))
		for _, want := range []string{"cliproxy", "专门探索", "已匹配", "US:California:Los Angeles", "US/California/Los Angeles", "1250ms"} {
			if !strings.Contains(resp.Body.String(), want) {
				t.Fatalf("%s missing %s", path, want)
			}
		}
	}
}
