package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestManagedKeysCategoryListsAndStatisticsAreDisjoint(t *testing.T) {
	s, h, st, _, _ := managedKeyRig(t)
	key := createManagedKeys(t, st, 1, 120, 2)[0]
	now := time.Now()
	values := map[string]types.KeyUsage{
		"upload:1":    {Totals: types.KeyCounters{Requests: 11, Tickets: 7}},
		"legacy:1":    {Totals: types.KeyCounters{Requests: 23, Tickets: 13}},
		key.Record.ID: {Totals: types.KeyCounters{Requests: 37, Tickets: 19}},
	}
	if err := st.SaveAccessKeyUsage(values); err != nil {
		t.Fatal(err)
	}
	s.initKeyMetrics()
	for i, id := range []string{"upload:1", "legacy:1", key.Record.ID} {
		s.keyMetrics.entries[id].recent[now.Unix()%keyRecentSeconds] = keySecond{at: now.Unix(), count: uint64(i + 2)}
	}
	selection := regexp.MustCompile(`name="ids" value="([^"]+)"`)
	for _, tc := range []struct {
		query, source          string
		total                  int
		requests, tickets, rpm uint64
	}{
		{"", "upload", 2, 11, 7, 2},
		{"?source=upload", "upload", 2, 11, 7, 2},
		{"?source=legacy", "legacy", 2, 23, 13, 3},
		{"?source=admin", "admin", 1, 37, 19, 4},
	} {
		w := do(h, authed(httptest.NewRequest("GET", "/admin/keys"+tc.query, nil), "offline-admin"))
		body := w.Body.String()
		if w.Code != 200 || !strings.Contains(body, `data-source="`+tc.source+`"`) {
			t.Fatal("wrong selected category", tc.source, w.Code)
		}
		matches := selection.FindAllStringSubmatch(body, -1)
		if len(matches) != tc.total {
			t.Fatal("cross-category list or missing rows", tc.source, len(matches))
		}
		for _, match := range matches {
			if !strings.HasPrefix(match[1], tc.source+":") {
				t.Fatal("cross-category selection", match[1])
			}
		}
		summary := regexp.MustCompile(`(?s)<div class="keys-summary".*?<section class="keys-list"`).FindString(body)
		wantStats := []string{
			fmt.Sprintf("<strong>%d<small>把</small>", tc.total),
			fmt.Sprintf("<strong>%d<small>次</small>", tc.rpm),
			fmt.Sprintf("<strong>%d</strong>", tc.requests),
			fmt.Sprintf("<strong>%d</strong>", tc.tickets),
		}
		for _, want := range wantStats {
			if !strings.Contains(summary, want) {
				t.Fatal("category totals missing", tc.source, want)
			}
		}
		if strings.Contains(body, `id="key-create"`) != (tc.source == "admin") ||
			strings.Contains(body, `name="creation_token"`) != (tc.source == "admin") ||
			strings.Contains(body, "<th>限额</th>") != (tc.source == "admin") {
			t.Fatal("admin-only controls leaked", tc.source)
		}
		if strings.Contains(body, "历史上传") || strings.Contains(body, `<select name="source">`) {
			t.Fatal("ambiguous mixed-source UI remains")
		}
	}
	empty := do(h, authed(httptest.NewRequest("GET", "/admin/keys?source=legacy&q=团队", nil), "offline-admin"))
	if strings.Contains(empty.Body.String(), `name="ids" value=`) || !strings.Contains(empty.Body.String(), `href="/admin/keys?source=legacy">清除筛选`) {
		t.Fatal("search crossed categories or reset lost category")
	}
}

func TestManagedKeysCategoryDetailCompatibilityAndInvalidSources(t *testing.T) {
	_, h, st, _, _ := managedKeyRig(t)
	key := createManagedKeys(t, st, 1, 0, 0)[0]
	for _, id := range []string{key.Record.ID, "upload:1", "legacy:1"} {
		w := do(h, authed(httptest.NewRequest("GET", "/admin/keys?key="+url.QueryEscape(id), nil), "offline-admin"))
		source, _, _ := types.ParseAccessKeyID(id)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `data-source="`+source+`"`) || !strings.Contains(w.Body.String(), `id="key-detail"`) {
			t.Fatal("old detail link lost category", id, w.Code)
		}
	}
	for _, tc := range []struct {
		path   string
		status int
	}{
		{"/admin/keys?source=all", 400},
		{"/admin/keys?source=unknown", 400},
		{"/admin/keys?source=upload&key=admin%3A1", 404},
		{"/admin/keys?source=admin&key=legacy%3A1", 404},
	} {
		if w := do(h, authed(httptest.NewRequest("GET", tc.path, nil), "offline-admin")); w.Code != tc.status {
			t.Fatal(tc.path, w.Code)
		}
	}
}

func TestManagedKeysCategoryMutationRejectsMixedSelectionAndKeepsRedirect(t *testing.T) {
	_, h, st, _, _ := managedKeyRig(t)
	key := createManagedKeys(t, st, 1, 0, 0)[0]
	for _, form := range []url.Values{
		{"action": {"disable"}, "source": {"upload"}, "ids": {"upload:1", key.Record.ID}},
		{"action": {"disable"}, "ids": {"upload:1", "legacy:1"}},
		{"action": {"edit"}, "source": {"legacy"}, "ids": {key.Record.ID}, "label": {"wrong"}},
		{"action": {"create"}, "source": {"upload"}, "count": {"1"}},
	} {
		if w := keyAdminForm(h, form); w.Code != 400 {
			t.Fatal("mixed/forged category accepted", w.Code)
		}
	}
	for _, id := range []string{"upload:1", "legacy:1", key.Record.ID} {
		if k, _ := st.AccessKey(id); !k.Usable() {
			t.Fatal("mixed batch partially mutated", id)
		}
	}
	for _, action := range []string{"disable", "enable", "edit"} {
		form := url.Values{"action": {action}, "source": {"admin"}, "ids": {key.Record.ID}, "label": {"更新名称"}}
		r := authed(httptest.NewRequest("POST", "http://pool.test/admin/keys?source=admin&q=team&status=active&sort=rpm&page=2", strings.NewReader(form.Encode())), "offline-admin")
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("Origin", "http://pool.test")
		w := do(h, r)
		if w.Code != http.StatusSeeOther {
			t.Fatal("category mutation failed", action, w.Code)
		}
		back, err := url.Parse(w.Header().Get("Location"))
		if err != nil {
			t.Fatal(err)
		}
		if back.Query().Get("source") != "admin" || back.Query().Get("q") != "team" || back.Query().Get("page") != "2" {
			t.Fatal("redirect lost filters", back)
		}
		if (back.Query().Get("key") == key.Record.ID) != (action == "edit") {
			t.Fatal("wrong detail redirect", back)
		}
	}
	// Existing callers with no source remain valid, but still cannot mix classes.
	if w := keyAdminForm(h, url.Values{"action": {"disable"}, "ids": {"legacy:1"}}); w.Code != 303 || w.Header().Get("Location") != "/admin/keys?source=legacy" {
		t.Fatal("legacy form lost source inference", w.Code, w.Header())
	}
}
