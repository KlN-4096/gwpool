package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"gwpool/internal/types"
)

func TestOwnerKeyMigrationGrandfathersOnlyPreexistingCredentialKeys(t *testing.T) {
	dir := t.TempDir()
	old := State{Accounts: []types.Account{
		{ID: 1, Creds: types.Creds{AccessToken: "old-token", AccountID: "old"}},
		{ID: 2, Creds: types.Creds{AccountID: "anonymous"}},
	}}
	raw, _ := json.Marshal(old)
	if err := os.WriteFile(filepath.Join(dir, stateFile), raw, filePerm); err != nil {
		t.Fatal(err)
	}
	s := open(t, dir)
	if !s.st.OwnerKeysMigrated || !s.Accounts()[0].LegacyConsumerKey || s.Accounts()[1].LegacyConsumerKey {
		t.Fatal("incorrect grandfathered set")
	}
	fresh, err := s.AddAccount(types.Account{Enabled: true, OwnerID: s.Accounts()[0].OwnerID,
		Creds: types.Creds{AccessToken: "new-token", AccountID: "new"}, LegacyConsumerKey: true})
	if err != nil || fresh.LegacyConsumerKey {
		t.Fatal("new credential was grandfathered")
	}
	reopened := open(t, dir)
	for _, account := range reopened.Accounts() {
		if account.LegacyConsumerKey != (account.ID == 1) {
			t.Fatal("restart migrated new credential keys")
		}
	}
}

func TestOwnerKeyMigrationMarksEmptyStoreBeforeFirstUpload(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	_, err := s.AddAccount(types.Account{Creds: types.Creds{AccessToken: "token", AccountID: "new"}})
	if err != nil {
		t.Fatal(err)
	}
	if open(t, dir).Accounts()[0].LegacyConsumerKey {
		t.Fatal("first upload became a legacy key on restart")
	}
}
