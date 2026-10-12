package httpapi

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestAdminBrowserLoginPersistsAcrossBrowserSessions(t *testing.T) {
	_, h, _, _, _ := rig(t, "pool")
	r := httptest.NewRequest(http.MethodGet, "/admin/", nil)
	r.Header.Set("Accept", "text/html")
	r.Header.Set("Sec-Fetch-Mode", "navigate")
	w := do(h, r)
	if w.Code != http.StatusSeeOther || !strings.HasPrefix(w.Header().Get("Location"), "/admin/login") {
		t.Fatalf("browser should use a login page, got %d %q", w.Code, w.Header().Get("Location"))
	}
	r = httptest.NewRequest(http.MethodPost, "/admin/login",
		strings.NewReader(url.Values{"password": {"akey"}}.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	r.Header.Set("X-Forwarded-Proto", "https")
	w = do(h, r)
	if w.Code != http.StatusSeeOther {
		t.Fatalf("password login: %d %s", w.Code, w.Body.String())
	}
	var session *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == "gwp_admin" {
			session = c
		}
	}
	if session == nil || session.MaxAge != 30*24*3600 || session.Expires.IsZero() ||
		!session.HttpOnly || !session.Secure || session.Path != "/admin/" || session.Value == "akey" {
		t.Fatal("missing protected persistent 30-day admin cookie")
	}
	// A fresh browser request has no cached HTTP Basic credentials.
	r = httptest.NewRequest(http.MethodGet, "/admin/", nil)
	r.Header.Set("Accept", "text/html")
	r.AddCookie(session)
	if w = do(h, r); w.Code != http.StatusOK {
		t.Fatalf("cookie-only return visit: %d", w.Code)
	}
}

func TestAdminSessionsExpireAndConfigurationChangesRevoke(t *testing.T) {
	for _, tc := range []string{"expiry", "admin-key", "origin"} {
		t.Run(tc, func(t *testing.T) {
			s, _, b := adminRealRig(t)
			adminPasswordLogin(t, b)
			now := time.Now()
			s.admin.now = func() time.Time { return now.Add(29 * 24 * time.Hour) }
			if w := b.request(http.MethodGet, "/admin/security", "", nil); w.Code != http.StatusOK {
				t.Fatal("remembered login expired before 30 days")
			}
			switch tc {
			case "expiry":
				s.admin.now = func() time.Time { return now.Add(30*24*time.Hour + time.Second) }
			case "admin-key":
				s.opt.AdminKey = "different-administrator-key"
			case "origin":
				s.opt.PublicBaseURL = "https://different.test"
			}
			if w := b.request(http.MethodGet, "/admin/security", "", nil); w.Code != http.StatusSeeOther {
				t.Fatalf("revoked session accepted: %d", w.Code)
			}
		})
	}
}

func TestAdminLoginRejectsCSRFAndNeverReflectsPassword(t *testing.T) {
	s, _, b := adminRealRig(t)
	for _, origin := range []string{"https://evil.test", "http://pool.test"} {
		r := httptest.NewRequest(http.MethodPost, adminTestOrigin+"/admin/login", strings.NewReader("password=akey"))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("Origin", origin)
		if w := do(b.h, r); w.Code != http.StatusForbidden || len(w.Result().Cookies()) != 0 {
			t.Fatalf("cross-origin login accepted for %s", origin)
		}
	}
	for _, next := range []string{"https://evil.test", "//evil.test", "/admin/login", "/admin/\\evil"} {
		if safeAdminNext(next) != "/admin/" {
			t.Fatal("unsafe login redirect accepted")
		}
	}
	w := b.postForm("/admin/login", url.Values{"password": {"do-not-reflect-this-secret"}})
	if w.Code != http.StatusUnauthorized || strings.Contains(w.Body.String(), "do-not-reflect-this-secret") ||
		w.Header().Get("WWW-Authenticate") != "" || b.cookies[adminCookie] != nil {
		t.Fatal("failed login leaked credentials or reopened Basic dialog")
	}
	adminPasswordLogin(t, b)
	// A browser's old Basic cache must not bypass explicit logout.
	b.postForm("/admin/logout", nil)
	r := httptest.NewRequest(http.MethodGet, "/admin/", nil)
	r.Header.Set("Accept", "text/html")
	r.SetBasicAuth("admin", "akey")
	if w = do(s.Handler(), r); w.Code != http.StatusSeeOther {
		t.Fatal("cached Basic credential silently undid browser logout")
	}
	// Command-line clients retain the existing Basic and Bearer interfaces.
	for _, basic := range []bool{true, false} {
		r = httptest.NewRequest(http.MethodGet, "/admin/security", nil)
		if basic {
			r.SetBasicAuth("script", "akey")
		} else {
			r.Header.Set("Authorization", "Bearer akey")
		}
		if w = do(s.Handler(), r); w.Code != http.StatusOK {
			t.Fatalf("script authentication broken: basic=%v status=%d", basic, w.Code)
		}
	}
}

func TestAdminLoginRateLimitIsBoundedAndRecovers(t *testing.T) {
	s, _, b := adminRealRig(t)
	for range adminLoginRateLimit {
		if w := b.postForm("/admin/login", url.Values{"password": {"wrong"}}); w.Code != http.StatusUnauthorized {
			t.Fatalf("premature throttle: %d", w.Code)
		}
	}
	if w := b.postForm("/admin/login", url.Values{"password": {"akey"}}); w.Code != http.StatusTooManyRequests {
		t.Fatal("password guessing was not limited")
	}
	now := time.Now()
	s.admin.now = func() time.Time { return now.Add(adminLoginRateWindow + time.Second) }
	adminPasswordLogin(t, b)
}

func TestAdminProxyClientsDoNotSharePasswordThrottle(t *testing.T) {
	s, _, _ := adminRealRig(t)
	s.opt.AdminTrustLoopbackProxy = true
	request := func(client, password string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, adminTestOrigin+"/admin/login",
			strings.NewReader(url.Values{"password": {password}}.Encode()))
		r.RemoteAddr = "127.0.0.1:23456"
		r.Header.Set("X-Real-IP", client)
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("Origin", adminTestOrigin)
		return do(s.Handler(), r)
	}
	for range adminLoginRateLimit {
		request("198.51.100.10", "wrong")
	}
	if w := request("198.51.100.20", "akey"); w.Code != http.StatusSeeOther {
		t.Fatalf("unrelated proxy client was blocked: %d", w.Code)
	}
}

func TestAdminClientIPTrustIsExplicitAndLoopbackOnly(t *testing.T) {
	for _, tc := range []struct {
		name, peer string
		trusted    bool
	}{
		{"external-peer", "198.51.100.5:4000", true},
		{"untrusted-loopback", "127.0.0.1:4000", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := adminRealRig(t)
			s.opt.AdminTrustLoopbackProxy = tc.trusted
			for i := 0; i <= adminLoginRateLimit; i++ {
				r := httptest.NewRequest(http.MethodPost, adminTestOrigin+"/admin/login", strings.NewReader("password=wrong"))
				r.RemoteAddr = tc.peer
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				r.Header.Set("X-Real-IP", "198.51.100."+strconv.Itoa(10+i))
				w := do(s.Handler(), r)
				if i == adminLoginRateLimit && w.Code != http.StatusTooManyRequests {
					t.Fatal("spoofed forwarding header bypassed authentication throttle")
				}
			}
		})
	}
}

func TestAdminFormReferrerPolicyKeepsOriginWithoutExposingOwnerURLs(t *testing.T) {
	_, h, _, _, _ := rig(t, "pool")
	for _, tc := range []struct{ path, policy string }{
		{"/admin/login", "same-origin"}, {"/admin/security", "same-origin"},
		{"/u/" + ownerTok1, "no-referrer"}, {"/", "no-referrer"},
	} {
		w := do(h, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if w.Header().Get("Referrer-Policy") != tc.policy {
			t.Fatalf("%s has incorrect referrer policy", tc.path)
		}
	}
}
