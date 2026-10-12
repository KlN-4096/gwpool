package store

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gwpool/internal/types"
)

func adminAuthFixture() types.AdminAuthState {
	now := time.Now().UTC()
	id := base64.RawURLEncoding.EncodeToString([]byte("public-credential-id"))
	realm := strings.Repeat("a", 64)
	return types.AdminAuthState{
		Passkeys: map[string]types.AdminPasskey{id: {
			Realm: realm, Name: "笔记本", Credential: []byte(`{"publicKey":"public-material"}`), CreatedAt: now,
		}},
		Sessions: map[string]types.AdminSession{strings.Repeat("b", 64): {
			Realm: realm, PasskeyID: id, CreatedAt: now, ExpiresAt: now.Add(types.AdminSessionLifetime),
		}},
	}
}

func TestAdminAuthDurabilityIsolationAndCAS(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	next := adminAuthFixture()
	if err := s.CommitAdminAuthState(0, next); err != nil {
		t.Fatal(err)
	}
	expected := next.Clone()
	expected.Revision = 1
	for id, key := range next.Passkeys {
		key.Credential[0] = 'X'
		delete(next.Passkeys, id)
	}
	got := s.AdminAuthState()
	if !reflect.DeepEqual(got, expected) {
		t.Fatal("caller mutated committed authentication state")
	}
	if err := s.CommitAdminAuthState(0, expected); err == nil {
		t.Fatal("stale authentication state overwrote a newer revision")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir)
	if !reflect.DeepEqual(s.AdminAuthState(), expected) {
		t.Fatal("authentication state did not survive restart")
	}
	for _, name := range []string{stateFile, journalFile} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("browser-secret")) {
			t.Fatal("raw browser token persisted")
		}
	}
}

func TestAdminAuthFailedWALDoesNotPublish(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "short-write"}[short], func(t *testing.T) {
			s := open(t, t.TempDir())
			before := s.AdminAuthState()
			s.journal.file = &faultyJournal{journalIO: s.journal.file, shortWrite: short, syncFail: !short}
			if err := s.CommitAdminAuthState(0, adminAuthFixture()); err == nil {
				t.Fatal("failed WAL was acknowledged")
			}
			if !reflect.DeepEqual(before, s.AdminAuthState()) {
				t.Fatal("failed authentication mutation became visible")
			}
		})
	}
}

func TestAdminAuthRejectsUnboundedAndDanglingSessions(t *testing.T) {
	s := open(t, t.TempDir())
	state := adminAuthFixture()
	state.Passkeys = nil
	if err := s.CommitAdminAuthState(0, state); err == nil {
		t.Fatal("session survived removal of its passkey")
	}
	state = adminAuthFixture()
	for id, session := range state.Sessions {
		session.ExpiresAt = session.ExpiresAt.Add(time.Second)
		state.Sessions[id] = session
	}
	if err := s.CommitAdminAuthState(0, state); err == nil {
		t.Fatal("session exceeded 30-day limit")
	}
}
