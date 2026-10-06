package types

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// UnauthorizedCredential deliberately recognizes only reasons written by the
// mint/scan paths. A historical sample or arbitrary text containing 401 is not
// proof that the current credentials should be removed.
func UnauthorizedCredential(a Account) bool {
	return !a.Enabled && a.Creds.AccessToken != "" &&
		(strings.HasPrefix(a.DisabledReason, "铸票 HTTP 401：") ||
			strings.HasPrefix(a.DisabledReason, "上游否决了这个账号（HTTP 401："))
}

// AccountCleanupCandidate binds a confirmation to one credential revision.
// Fingerprint is never a replacement for authentication or server signatures.
type AccountCleanupCandidate struct {
	ID          int64  `json:"id"`
	Fingerprint string `json:"fingerprint"`
}

func CleanupCandidate(a Account) AccountCleanupCandidate {
	// Include ownership, original creation, tokens and reason, not changing
	// usage counters. Re-upload, refresh or a different disable cause invalidates
	// the old preview even if the row still happens to have the same numeric ID.
	value := struct {
		ID, OwnerID int64
		ViewToken   string
		CreatedAt   string
		Creds       Creds
		Reason      string
		Revision    uint64
	}{a.ID, a.OwnerID, a.ViewToken, a.CreatedAt.String(), a.Creds, a.DisabledReason, a.CredentialRevision}
	data, _ := json.Marshal(value) // This value contains only JSON-safe primitives.
	sum := sha256.Sum256(data)
	return AccountCleanupCandidate{ID: a.ID, Fingerprint: hex.EncodeToString(sum[:])}
}
