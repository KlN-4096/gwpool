package httpapi

import (
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestManagedKeysUploadedAccountCountsFollowCurrentOwnership(t *testing.T) {
	s, _, st, _, _ := managedKeyRig(t)
	empty, err := st.AddOwner("尚未上传")
	if err != nil {
		t.Fatal(err)
	}
	disabled, err := st.AddAccount(types.Account{OwnerID: 1, Label: "停用账号",
		Creds: types.Creds{AccountID: "disabled-upload", AccessToken: "offline-disabled"}})
	if err != nil {
		t.Fatal(err)
	}
	expired, err := st.AddAccount(types.Account{OwnerID: 1, Label: "过期账号", Enabled: true,
		Creds: types.Creds{AccountID: "expired-upload", AccessToken: fakeToken("expired-upload", "free", time.Now().Add(-time.Hour))}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.AddAccount(types.Account{Via: 1, Creds: types.Creds{AccountID: "anon:consumer"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveAccessKeyUsage(map[string]types.KeyUsage{
		"upload:1":   {AccountHashes: []string{"request-only"}},
		"upload:999": {Totals: types.KeyCounters{Requests: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	s.initKeyMetrics()
	assertCounts := func(want int) {
		t.Helper()
		rows, _ := s.managedKeyRows(st, time.Now())
		found := map[string]managedKeyRow{}
		for _, row := range rows {
			found[row.Key.ID] = row
			if row.Key.Source != types.AccessKeyUpload && (row.UploadedAccounts != 0 || row.AccountsURL != "") {
				t.Fatal("account association leaked to another key category", row.Key.ID)
			}
		}
		for id, count := range map[string]int{
			"upload:1": want, "upload:2": 1, types.AccessKeyID(types.AccessKeyUpload, empty.ID): 0, "upload:999": 0,
		} {
			row, ok := found[id]
			if !ok || row.UploadedAccounts != count {
				t.Fatalf("%s: uploaded accounts = %d, want %d (exists %v)", id, row.UploadedAccounts, count, ok)
			}
			if (row.AccountsURL != "") != (id != "upload:999") {
				t.Fatal("missing owner must not get a dead account link; empty owner must remain reachable", id)
			}
		}
		if detail := keyDetail(found["upload:1"], time.Now()); detail.Accounts != "1" || detail.UploadedAccounts != want {
			t.Fatal("request account hashes were confused with uploaded accounts")
		}
	}
	assertCounts(3) // Active, disabled and expired; not another owner or anonymous consumers.
	reuploaded, err := st.AddAccount(expired)
	if err != nil || reuploaded.ID != expired.ID {
		t.Fatal("re-upload did not update the existing account", err)
	}
	assertCounts(3)
	if err := st.DeleteAccount(disabled.ID); err != nil {
		t.Fatal(err)
	}
	assertCounts(2)
}

func TestManagedKeysUploadedAccountLinksAndCategoryIsolation(t *testing.T) {
	s, h, st, _, _ := managedKeyRig(t)
	key := createManagedKeys(t, st, 1, 0, 0)[0]
	owner := st.Owners()[0]
	ownerURL := "/u/" + url.PathEscape(owner.Token)
	get := func(path string) string {
		t.Helper()
		w := do(h, authed(httptest.NewRequest("GET", path, nil), "offline-admin"))
		if w.Code != 200 {
			t.Fatalf("GET %s: %d", path, w.Code)
		}
		return w.Body.String()
	}
	detailPath := "/admin/keys?key=" + url.QueryEscape(types.AccessKeyID(types.AccessKeyUpload, owner.ID))
	list, detail := get("/admin/keys"), get(detailPath)
	link := regexp.MustCompile(`<a href="` + regexp.QuoteMeta(ownerURL) + `"[^>]*>1 个</a>`)
	for _, body := range []string{list, detail} {
		matches := link.FindAllString(body, -1)
		if len(matches) == 0 {
			t.Fatal("missing uploaded-account count link")
		}
		for _, match := range matches {
			if strings.Contains(match, "data-key-detail") {
				t.Fatal("account navigation must not be intercepted by the key drawer")
			}
		}
	}
	panel := strings.SplitN(detail, `id="key-detail"`, 2)
	if len(panel) != 2 || !strings.Contains(panel[1], "<dt>上传账号数</dt>") || !link.MatchString(panel[1]) {
		t.Fatal("key detail omitted the account count link")
	}
	for _, id := range []string{"legacy:1", key.Record.ID} {
		body := get("/admin/keys?key=" + url.QueryEscape(id))
		if strings.Contains(body, "上传账号数") || strings.Contains(body, ownerURL) {
			t.Fatal("upload-only count or link leaked into another category", id)
		}
	}
	for source, columns := range map[string]int{"upload": 8, "legacy": 7, "admin": 8} {
		body := get("/admin/keys?source=" + source + "&q=no-matching-account-key")
		if !strings.Contains(body, fmt.Sprintf(`colspan="%d"`, columns)) {
			t.Fatal("empty table has incorrect column span", source)
		}
	}
	ownerPage := get(ownerURL)
	account := st.AccountsByOwner(owner.ID)[0]
	accountURL := "/a/" + url.PathEscape(account.ViewToken)
	if !strings.Contains(ownerPage, `href="`+accountURL+`"`) {
		t.Fatal("owner account list has no individual account information link")
	}
	for _, other := range st.Accounts() {
		if other.OwnerID != owner.ID && strings.Contains(ownerPage, `href="/a/`+other.ViewToken+`"`) {
			t.Fatal("account navigation crossed owner boundary")
		}
	}
	accountPage := get(accountURL)
	for _, secret := range []string{"", s.OwnerKeyFor(owner)} {
		w := do(h, authed(httptest.NewRequest("GET", detailPath, nil), secret))
		if w.Code != 401 || strings.Contains(w.Body.String(), ownerURL) {
			t.Fatal("account links must remain administrator-only")
		}
	}
	if dir := os.Getenv("GWPOOL_KEYS_PREVIEW_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		for name, body := range map[string]string{
			"account-keys.html": list, "account-key-detail.html": detail,
			"linked-owner.html": ownerPage, "linked-account.html": accountPage,
		} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}
