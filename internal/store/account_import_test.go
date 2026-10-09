package store

import (
	"context"
	"errors"
	"testing"

	"gwpool/internal/types"
)

func TestImportAccountChecksVersionAtomically(t *testing.T) {
	for _, change := range []string{"none", "credentials", "pause", "delete", "recreate"} {
		t.Run(change, func(t *testing.T) {
			s := open(t, t.TempDir())
			a, err := s.AddAccount(types.Account{OwnerID: 1, Label: "original", Enabled: true,
				Creds: types.Creds{AccountID: "fixture", AccessToken: "old", RefreshToken: "old-rt"}})
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "credentials":
				err = s.SetTokens(a.ID, "other", "other-rt")
			case "pause":
				err = s.SetAccountEnabled(a.ID, false)
			case "delete", "recreate":
				err = s.DeleteAccount(a.ID)
				if change == "recreate" && err == nil {
					_, err = s.AddAccount(types.Account{OwnerID: 1, Creds: types.Creds{AccountID: "fixture", AccessToken: "new-owner-upload"}})
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.AddAccountIfVersion(context.Background(), types.Account{OwnerID: 1, Creds: types.Creds{
				AccountID: "fixture", AccessToken: "reauth", RefreshToken: "reauth-rt"}}, a.ID, a.CredentialRevision)
			if change != "none" {
				if !errors.Is(err, ErrCredentialsChanged) {
					t.Fatal("late import overwrote concurrent change", err)
				}
			} else if err != nil || got.ID != a.ID || got.ViewToken != a.ViewToken ||
				got.Label != "original" || got.Creds.RefreshToken != "reauth-rt" {
				t.Fatal("import did not retain account history", err)
			}
		})
	}
}

func TestImportAccountAbsentVersionCannotOverwriteConcurrentCreation(t *testing.T) {
	s := open(t, t.TempDir())
	first, err := s.AddAccountIfVersion(context.Background(), types.Account{OwnerID: 1, Creds: types.Creds{AccountID: "new", AccessToken: "one"}}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddAccountIfVersion(context.Background(), types.Account{OwnerID: 1, Creds: types.Creds{AccountID: "new", AccessToken: "two"}}, 0, 0); !errors.Is(err, ErrCredentialsChanged) {
		t.Fatal("second absent-version write accepted", err)
	}
	current, _ := s.Account(first.ID)
	if current.Creds.AccessToken != "one" {
		t.Fatal("concurrent creation overwritten")
	}
}
