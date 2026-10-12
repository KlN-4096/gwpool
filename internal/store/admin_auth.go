package store

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"gwpool/internal/types"
)

func (s *Store) AdminAuthState() types.AdminAuthState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.st.AdminAuth == nil {
		return types.AdminAuthState{}
	}
	return s.st.AdminAuth.Clone()
}

// CommitAdminAuthState durably commits a small, bounded authentication snapshot.
// The revision prevents a late login from resurrecting a revoked credential or
// session. Callers must not send a session cookie until this returns success.
func (s *Store) CommitAdminAuthState(expected uint64, next types.AdminAuthState) error {
	if err := validateAdminAuthState(next); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var current uint64
	if s.st.AdminAuth != nil {
		current = s.st.AdminAuth.Revision
	}
	if current != expected || current == ^uint64(0) {
		return errors.New("admin authentication state changed; retry")
	}
	next = next.Clone()
	next.Revision = current + 1
	before := s.st.AdminAuth
	s.st.AdminAuth = &next
	if err := s.save(stateChange{AdminAuth: &next}); err != nil {
		s.st.AdminAuth = before
		return err
	}
	return nil
}

func validAdminDigest(value string) bool {
	b, err := hex.DecodeString(value)
	return err == nil && len(b) == 32
}

func validateAdminAuthState(state types.AdminAuthState) error {
	if len(state.Passkeys) > types.MaxAdminPasskeys || len(state.Sessions) > types.MaxAdminSessions {
		return errors.New("admin authentication limit reached")
	}
	for id, key := range state.Passkeys {
		raw, err := base64.RawURLEncoding.DecodeString(id)
		if err != nil || len(raw) == 0 || len(raw) > 1023 ||
			!validAdminDigest(key.Realm) || key.CreatedAt.IsZero() ||
			len(key.Name) == 0 || len(key.Name) > 128 || !utf8.ValidString(key.Name) ||
			strings.IndexFunc(key.Name, func(r rune) bool { return r < 32 || r == 127 }) >= 0 ||
			len(key.Credential) == 0 || len(key.Credential) > 64<<10 || !json.Valid(key.Credential) {
			return errors.New("invalid admin passkey")
		}
	}
	for id, session := range state.Sessions {
		if !validAdminDigest(id) || !validAdminDigest(session.Realm) || session.CreatedAt.IsZero() ||
			!session.ExpiresAt.After(session.CreatedAt) ||
			session.ExpiresAt.Sub(session.CreatedAt) > types.AdminSessionLifetime ||
			(session.CeremonyHash != "" && !validAdminDigest(session.CeremonyHash)) {
			return errors.New("invalid admin session")
		}
		if session.PasskeyID != "" {
			key, ok := state.Passkeys[session.PasskeyID]
			if !ok || key.Realm != session.Realm {
				return errors.New("admin session references an unavailable passkey")
			}
		}
	}
	return nil
}
