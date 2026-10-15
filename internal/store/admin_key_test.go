package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gwpool/internal/types"
)

func TestAdminKeyPrivilegeIsExplicitDurableAndImmutable(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	ordinary, err := s.CreateAccessKeys(1, "", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	requestID := strings.Repeat("b", 64)
	created, err := s.CreateAdminKeysOnce(requestID, 1, "", "offline")
	if err != nil {
		t.Fatal(err)
	}
	key := created[0]
	if !key.Record.IsAdminKey() || key.Record.Label != "AdminKey" ||
		key.Record.RPM != 0 || key.Record.Concurrency != 0 || ordinary[0].Record.IsAdminKey() {
		t.Fatal("AdminKey was inferred from unlimited ordinary policy or has consumer limits")
	}
	one := 1
	if err := s.UpdateAccessKeys([]string{key.Record.ID}, types.AccessKeyPatch{Action: "edit", RPM: &one}); err == nil {
		t.Fatal("AdminKey accepted a conflicting limit")
	}
	// Both types share the durable replay namespace.
	if _, err := s.CreateAccessKeysOnce(requestID, 1, "", "", 0, 0); err == nil {
		t.Fatal("changing requested kind bypassed creation replay protection")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir)
	if restored, ok := s.ResolveIssuedAccessKey(key.Key); !ok || !restored.IsAdminKey() {
		t.Fatal("WAL replay lost explicit AdminKey privilege")
	}
	if err := s.UpdateAccessKeys([]string{key.Record.ID}, types.AccessKeyPatch{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	if disabled, _ := s.AccessKey(key.Record.ID); disabled.Usable() || !disabled.IsAdminKey() {
		t.Fatal("disable failed or silently changed kind")
	}
	if err := s.UpdateAccessKeys([]string{key.Record.ID}, types.AccessKeyPatch{Action: "enable"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateAccessKeys([]string{key.Record.ID}, types.AccessKeyPatch{Action: "revoke"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir)
	if revoked, _ := s.ResolveIssuedAccessKey(key.Key); revoked.Usable() || !revoked.IsAdminKey() {
		t.Fatal("checkpoint restart lost revocation or kind")
	}
	if err := s.UpdateAccessKeys([]string{key.Record.ID}, types.AccessKeyPatch{Action: "enable"}); err == nil {
		t.Fatal("revoked AdminKey was reenabled")
	}
	if plain, _ := s.ResolveIssuedAccessKey(ordinary[0].Key); plain.IsAdminKey() {
		t.Fatal("restart promoted ordinary key")
	}
	for _, name := range []string{stateFile, journalFile} {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), key.Key) {
			t.Fatal("plaintext AdminKey persisted")
		}
	}
}

func TestAdminKeyFailedCreationNeverPublishesPrivilege(t *testing.T) {
	s := open(t, t.TempDir())
	seq := s.st.JournalSequence
	s.journal.file = &faultyJournal{journalIO: s.journal.file, syncFail: true}
	created, err := s.CreateAdminKeysOnce(strings.Repeat("c", 64), 2, "", "")
	if err == nil || len(created) != 0 || len(s.AccessKeys()) != 0 || s.st.JournalSequence != seq {
		t.Fatal("failed commit acknowledged or exposed privileged keys")
	}
	if _, err := s.CreateAdminKeysOnce("invalid-request", 1, "", ""); err == nil {
		t.Fatal("invalid creation ID accepted")
	}
	for _, count := range []int{0, types.AccessKeyBatchLimit + 1} {
		if _, err := s.CreateAdminKeysOnce(strings.Repeat("d", 64), count, "", ""); err == nil {
			t.Fatal("AdminKey bypassed storage batch bounds")
		}
	}
}
