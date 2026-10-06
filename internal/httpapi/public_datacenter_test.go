package httpapi

import (
	"net/http/httptest"
	"strings"
	"testing"

	"gwpool/internal/types"
)

func TestPublicDatacenterRegionsAndPrivacy(t *testing.T) {
	_, h, _, st, _ := rig(t, types.ModePool)
	// Directory: 142=Poland, 73=New Zealand. Deliberately conflicting
	// proxy exits must not change the public grouping.
	st.gws["unified-142"] = types.Gateway{Name: "unified-142", ReachableExits: []types.Exit{{Region: "US"}, {Region: "JP"}}}
	st.gws["unified-73"] = types.Gateway{Name: "unified-73", ReachableExits: []types.Exit{{Region: "US"}}}
	st.gws["unified-outside"] = types.Gateway{Name: "unified-outside", ReachableExits: []types.Exit{{Region: "US"}}}
	body := do(h, httptest.NewRequest("GET", "/", nil)).Body.String()
	for _, forbidden := range []string{"201 历史目录", "目录暂无条目", "尚缺", "unified-1\"", "unified-2\""} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("public page disclosed %q", forbidden)
		}
	}
	eu := do(h, httptest.NewRequest("GET", "/?region=europe", nil)).Body.String()
	if !strings.Contains(eu, `<td class="mono">unified-142</td>`) || strings.Contains(eu, `<td class="mono">unified-73</td>`) {
		t.Fatal("public filter uses exit region rather than datacenter region")
	}
	oceania := do(h, httptest.NewRequest("GET", "/?region=oceania", nil)).Body.String()
	if !strings.Contains(oceania, `<td class="mono">unified-73</td>`) {
		t.Fatal("missing discovered NZ gateway")
	}
	unknown := do(h, httptest.NewRequest("GET", "/?region=other", nil)).Body.String()
	if !strings.Contains(unknown, `<td class="mono">unified-outside</td>`) {
		t.Fatal("unknown datacenter inferred from proxy")
	}
	if !strings.Contains(body, "暂无已发现网关") || !strings.Contains(body, "机房区域") {
		t.Fatal("missing explicit public labels")
	}
}
