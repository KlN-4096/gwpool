package httpapi

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"gwpool/internal/store"
	"gwpool/internal/types"
)

// Opt-in local browser integration server. It has a temporary real Store, fake
// pool/scanner, synthetic admin key, and no mint/OAuth transport.
func TestAdminAuthOfflineBrowser(t *testing.T) {
	ready := os.Getenv("GWPOOL_ADMIN_AUTH_BROWSER_READY")
	if ready == "" {
		t.Skip("set GWPOOL_ADMIN_AUTH_BROWSER_READY for the bounded local browser check")
	}
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	server := httptest.NewUnstartedServer(nil)
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	origin := "http://localhost:" + port
	s, err := New(&fakePool{mode: types.ModePool}, st, &fakeScanner{}, Options{
		AdminKey: "offline-only-admin-key", PublicBaseURL: origin,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var once sync.Once
	handler := s.Handler()
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/__offline/finish" {
			w.WriteHeader(http.StatusNoContent)
			once.Do(func() { close(done) })
			return
		}
		handler.ServeHTTP(w, r)
	})
	server.Start()
	defer server.Close()
	data, err := json.Marshal(map[string]string{
		"origin": origin, "control_url": server.URL, "admin_key": "offline-only-admin-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ready, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("offline admin browser fixture ready")
	select {
	case <-done:
	case <-time.After(55 * time.Second):
		t.Fatal("offline browser check did not complete in its bounded window")
	}
}
