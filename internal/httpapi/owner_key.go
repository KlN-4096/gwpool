package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"

	"gwpool/internal/oauth"
	"gwpool/internal/types"
)

// OwnerKeyFor is stable across credential replacement and distinct from both
// browser tokens and legacy credential keys. The configured root secret is never
// emitted. Availability is rechecked from current credentials on every request.
func (s *Server) OwnerKeyFor(owner types.Owner) string {
	if s.opt.ConsumerKey == "" || owner.ID <= 0 || owner.Token == "" {
		return ""
	}
	m := hmac.New(sha256.New, []byte(s.opt.ConsumerKey))
	_, _ = fmt.Fprintf(m, "gwpool-owner-key-v1|%d|%s", owner.ID, owner.Token)
	return "gwp_a_" + base64.RawURLEncoding.EncodeToString(m.Sum(nil)[:24])
}

func ownerCredentialActive(account types.Account, now time.Time) bool {
	if !account.Enabled || account.Creds.AccessToken == "" {
		return false
	}
	// No live validation request. If a locally readable expiry exists, honor it;
	// opaque tokens retain the existing enabled-state contract.
	claims, err := oauth.Parse(account.Creds.AccessToken)
	return err != nil || claims.Expires.IsZero() || claims.Expires.After(now)
}

func (s *Server) ownerKeyAccount(ownerID int64, reference string) (types.Account, bool) {
	var selected types.Account
	now := time.Now()
	for _, account := range s.st.AccountsByOwner(ownerID) {
		if !ownerCredentialActive(account, now) {
			continue
		}
		if reference != "" && accountRefMatches(account, reference) {
			return account, true
		}
		if selected.ID == 0 || account.ID < selected.ID {
			selected = account
		}
	}
	return selected, selected.ID > 0
}

func (c caller) reporterID() int64 {
	if c.public {
		return publicReporterID
	}
	if c.ownerID > 0 {
		// Disjoint namespace from positive legacy credential IDs; credential
		// rotation must not split a single owner's statistical identity.
		return -c.ownerID
	}
	return c.acct.ID
}
