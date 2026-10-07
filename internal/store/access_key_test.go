package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestAccessKeysIndependentSecretsRevokeAndRestart(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	keys, err := s.CreateAccessKeys(3, "测试", "note", 120, 2)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, made := range keys {
		if seen[made.Key] {
			t.Fatal("duplicate secret")
		}
		seen[made.Key] = true
		key, ok := s.ResolveIssuedAccessKey(made.Key)
		if !ok || !key.Usable() || key.RPM != 120 || len(s.Accounts()) != 0 || len(s.Owners()) != 0 {
			t.Fatal("independent key unexpectedly depends on uploaded credentials")
		}
		if _, ok := s.ResolveIssuedAccessKey(made.Key + "x"); ok {
			t.Fatal("bad secret accepted")
		}
	}
	ids := []string{keys[0].Record.ID, keys[1].Record.ID}
	if err := s.UpdateAccessKeys(ids, types.AccessKeyPatch{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	if key, _ := s.AccessKey(ids[0]); key.Usable() {
		t.Fatal("disable lost")
	}
	if err := s.UpdateAccessKeys(ids, types.AccessKeyPatch{Action: "enable"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateAccessKeys(ids[:1], types.AccessKeyPatch{Action: "revoke"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateAccessKeys(ids, types.AccessKeyPatch{Action: "enable"}); err == nil {
		t.Fatal("revocation reversed")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{stateFile, journalFile} {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if strings.Contains(string(raw), key.Key) {
				t.Fatal("plaintext key persisted")
			}
		}
	}
	reopened := open(t, dir)
	if key, ok := reopened.ResolveIssuedAccessKey(keys[0].Key); !ok || key.Usable() {
		t.Fatal("revocation lost after restart")
	}
	if key, ok := reopened.ResolveIssuedAccessKey(keys[1].Key); !ok || !key.Usable() {
		t.Fatal("key lost after restart")
	}
}

func TestAccessKeysUploadExemptionAtomicSelectionAndWALFailure(t *testing.T) {
	s := open(t, t.TempDir())
	owner, err := s.AddOwner("uploader")
	if err != nil {
		t.Fatal(err)
	}
	id := types.AccessKeyID(types.AccessKeyUpload, owner.ID)
	limit := 1
	if err := s.UpdateAccessKeys([]string{id}, types.AccessKeyPatch{Action: "edit", RPM: &limit}); err == nil {
		t.Fatal("upload key accepted a rate limit")
	}
	if err := s.UpdateAccessKeys([]string{id, "admin:999"}, types.AccessKeyPatch{Action: "disable"}); err == nil {
		t.Fatal("partially valid selection accepted")
	}
	if key, _ := s.AccessKey(id); !key.Usable() {
		t.Fatal("partial update escaped")
	}
	s.journal.file = &faultyJournal{journalIO: s.journal.file, syncFail: true}
	if err := s.UpdateAccessKeys([]string{id}, types.AccessKeyPatch{Action: "disable"}); err == nil {
		t.Fatal("expected persistence error")
	}
	if key, _ := s.AccessKey(id); !key.Usable() {
		t.Fatal("failed commit changed authorization")
	}
	s.journal.file = &faultyJournal{journalIO: s.journal.file, syncFail: true}
	if keys, err := s.CreateAccessKeys(2, "failed", "", 0, 0); err == nil || len(keys) != 0 || len(s.AccessKeys()) != 0 {
		t.Fatal("failed creation leaked committed keys")
	}
}

func TestAccessKeyUsageClonesSnapshotsAndSurvivesJournal(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	now := time.Now().UTC()
	usage := types.KeyUsage{LastAt: now, Totals: types.KeyCounters{
		Requests: 2, Endpoints: map[string]uint64{"/gateways": 2}},
		RecentIPs: []string{"127.0.0.1"}, Hours: []types.KeyHour{{
			At: now.Truncate(time.Hour), Counters: types.KeyCounters{Endpoints: map[string]uint64{"/gateways": 2}},
		}}}
	if err := s.SaveAccessKeyUsage(map[string]types.KeyUsage{"upload:1": usage}); err != nil {
		t.Fatal(err)
	}
	usage.Totals.Endpoints["/gateways"] = 99
	snapshot := cloneState(s.st)
	snapshot.KeyUsage["upload:1"].Hours[0].Counters.Endpoints["/gateways"] = 99
	got := s.AccessKeyUsage()["upload:1"]
	if got.Totals.Endpoints["/gateways"] != 2 || got.Hours[0].Counters.Endpoints["/gateways"] != 2 {
		t.Fatal("snapshot alias")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	raw, _ := json.Marshal(reopened.AccessKeyUsage())
	if !strings.Contains(string(raw), `"requests":2`) {
		t.Fatal("usage lost")
	}
	got.Totals.Requests = 1
	if err := reopened.SaveAccessKeyUsage(map[string]types.KeyUsage{"upload:1": got}); err == nil {
		t.Fatal("usage went backwards")
	}
}

func TestAccessKeysCreationReplaySurvivesRestartAndTotalCapacity(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	requestID := strings.Repeat("a", 64)
	if _, err := s.CreateAccessKeysOnce(requestID, 2, "once", "", 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, dir)
	if _, err := reopened.CreateAccessKeysOnce(requestID, 2, "once", "", 0, 0); err == nil || len(reopened.AccessKeys()) != 2 {
		t.Fatal("creation request replayed after restart")
	}
	reopened.mu.Lock()
	for i := 1; len(reopened.st.AccessKeys) < maxAccessKeys; i++ {
		id := fmt.Sprintf("upload:%d", i)
		reopened.st.AccessKeys[id] = types.AccessKey{ID: id, Source: types.AccessKeyUpload, SubjectID: int64(i)}
	}
	reopened.mu.Unlock()
	if _, err := reopened.CreateAccessKeys(1, "over capacity", "", 0, 0); err == nil {
		t.Fatal("creation exceeded total metadata capacity")
	}
}

func TestAccessKeyTextValidationBoundaries(t *testing.T) {
	for _, tc := range []struct {
		text            string
		max             int
		multiline, want bool
	}{
		{"中文", 2, false, true},
		{"中文", 1, false, false},
		{"a\nb\tc", 8, true, true},
		{"a\nb", 8, false, false},
		{"a\x00b", 8, true, false},
		{string([]byte{0xff}), 8, true, false},
	} {
		if got := validKeyText(tc.text, tc.max, tc.multiline); got != tc.want {
			t.Errorf("validKeyText(%q, %d, %v)=%v", tc.text, tc.max, tc.multiline, got)
		}
	}
}
