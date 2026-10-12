package types

import (
	"encoding/json"
	"maps"
	"slices"
	"time"
)

const (
	AdminSessionLifetime = 30 * 24 * time.Hour
	MaxAdminPasskeys     = 32
	MaxAdminSessions     = 128
)

// AdminPasskey contains the public WebAuthn credential, never a private key.
// Realm binds it to the configured admin key and relying-party origin.
type AdminPasskey struct {
	Realm      string
	Name       string
	Credential json.RawMessage
	CreatedAt  time.Time
	LastUsedAt time.Time
}

// AdminSession is indexed by SHA256 of a random browser token. The bearer
// token itself and the admin key are never written to state or the WAL.
type AdminSession struct {
	Realm        string
	PasskeyID    string
	CeremonyHash string // Allows logout to revoke a completed login whose Cookie is still in flight.
	CreatedAt    time.Time
	ExpiresAt    time.Time
}

type AdminAuthState struct {
	Revision uint64
	Passkeys map[string]AdminPasskey
	Sessions map[string]AdminSession
}

func (a AdminAuthState) Clone() AdminAuthState {
	a.Passkeys = maps.Clone(a.Passkeys)
	for id, key := range a.Passkeys {
		key.Credential = slices.Clone(key.Credential)
		a.Passkeys[id] = key
	}
	a.Sessions = maps.Clone(a.Sessions)
	return a
}
