package httpapi

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestManagedKeyLastRequestUTCMetadataAndFallback(t *testing.T) {
	s, h, st, _, _ := managedKeyRig(t)
	last := time.Date(2026, 10, 8, 5, 30, 45, 0, time.FixedZone("fixture", 9*60*60))
	if err := st.SaveAccessKeyUsage(map[string]types.KeyUsage{
		"upload:1": {LastAt: last, Totals: types.KeyCounters{Requests: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	s.initKeyMetrics()
	for name, path := range map[string]string{"list.html": "/admin/keys", "detail.html": "/admin/keys?key=upload%3A1"} {
		w := do(h, authed(httptest.NewRequest("GET", path, nil), "offline-admin"))
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
		body := w.Body.String()
		for _, want := range []string{
			`data-key-last-time="2026-10-07T20:30:45Z" datetime="2026-10-07T20:30:45Z"`,
			`data-key-clock>20:30:45</span>`,
			`data-key-date>2026-10-07</small>`,
			`data-key-last-time=""><span data-key-clock>—</span>`,
			`<small data-key-timezone>UTC</small>`,
		} {
			if !strings.Contains(body, want) {
				t.Fatalf("%s missing %s", name, want)
			}
		}
		if name == "detail.html" && !strings.Contains(body, `datetime="2026-10-07T20:30:45Z">2026-10-07 20:30:45</time> <span data-key-timezone>UTC</span>`) {
			t.Fatal("direct detail lost UTC fallback")
		}
		if dir := os.Getenv("GWPOOL_KEYS_TIMEZONE_DIR"); dir != "" {
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, name), w.Body.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := st.AccessKeyUsage()["upload:1"].LastAt; !got.Equal(last) {
		t.Fatal("display changed stored instant")
	}
}
