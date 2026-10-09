package store

import (
	"errors"
	"testing"

	"gwpool/internal/types"
)

func TestCommitRefreshedTokensPreservesConcurrentChanges(t *testing.T) {
	for _, mode := range []string{"manual pause", "pause then enable", "new credentials", "delete", "normal recovery"} {
		t.Run(mode, func(t *testing.T) {
			s := open(t, t.TempDir())
			a, err := s.AddAccount(types.Account{Enabled: true,
				Creds: types.Creds{AccountID: "fake-account", AccessToken: "old-access", RefreshToken: "old-refresh"}})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "normal recovery" {
				if err := s.DisableAccount(a.ID, "expiry"); err != nil {
					t.Fatal(err)
				}
				a, _ = s.Account(a.ID)
			}
			switch mode {
			case "manual pause":
				err = s.SetAccountEnabled(a.ID, false)
			case "pause then enable":
				if err = s.SetAccountEnabled(a.ID, false); err == nil {
					err = s.SetAccountEnabled(a.ID, true)
				}
			case "new credentials":
				err = s.SetTokens(a.ID, "uploaded-access", "uploaded-refresh")
			case "delete":
				err = s.DeleteAccount(a.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.CommitRefreshedTokens(a, "fresh-access", "rotated-refresh", true)
			if mode == "new credentials" || mode == "delete" {
				if !errors.Is(err, ErrCredentialsChanged) {
					t.Fatalf("stale refresh accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Enabled != (mode != "manual pause") || got.DisabledReason != "" {
				t.Fatalf("concurrent status lost: enabled=%v reason=%q", got.Enabled, got.DisabledReason)
			}
			if got.Creds.RefreshToken != "rotated-refresh" {
				t.Fatal("status-only edit discarded rotated RT")
			}
			changed, err := s.DisableAccountIfCurrent(a, "late expiry")
			if err != nil || changed {
				t.Fatalf("late expiry overwrote new credentials/status: changed=%v err=%v", changed, err)
			}
		})
	}
}

func TestRefreshCommitFailureDoesNotAcknowledgeTokens(t *testing.T) {
	s := open(t, t.TempDir())
	a, err := s.AddAccount(types.Account{Enabled: true,
		Creds: types.Creds{AccountID: "fake-account", AccessToken: "old", RefreshToken: "old-rt"}})
	if err != nil {
		t.Fatal(err)
	}
	s.journal.file = &faultyJournal{journalIO: s.journal.file, syncFail: true}
	if _, err := s.CommitRefreshedTokens(a, "new", "rotated", false); err == nil {
		t.Fatal("failed token commit acknowledged")
	}
	got, _ := s.Account(a.ID)
	if got != a {
		t.Fatal("failed token commit changed memory")
	}
	updated, err := s.CommitRefreshedTokens(a, "new", "", false)
	if err != nil || updated.Creds.RefreshToken != "old-rt" {
		t.Fatalf("non-rotating retry lost RT: %v", err)
	}
}
