package store

import (
	"errors"

	"gwpool/internal/types"
)

var ErrCredentialsChanged = errors.New("账号凭据已变更或删除")

// CommitRefreshedTokens compares credentials under the store lock. A concurrent
// upload must not be overwritten. A status-only edit keeps its latest state
// while still saving a rotated RT, so a manual pause neither loses the RT nor
// gets undone by the refresh completion.
func (s *Store) CommitRefreshedTokens(expected types.Account, access, refresh string, resume bool) (types.Account, error) {
	if access == "" {
		return types.Account{}, errors.New("access_token 不能写成空的")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, current := range s.st.Accounts {
		if current.ID != expected.ID {
			continue
		}
		if current.Creds != expected.Creds {
			return types.Account{}, ErrCredentialsChanged
		}
		next := current
		next.Creds.AccessToken = access
		if refresh != "" {
			next.Creds.RefreshToken = refresh
		}
		if resume && current.CredentialRevision == expected.CredentialRevision &&
			current.DisabledReason != "" {
			next.Enabled, next.DisabledReason = true, ""
		}
		next.CredentialRevision = s.st.JournalSequence + 1
		s.st.Accounts[i] = next
		if err := s.save(stateChange{Accounts: []types.Account{next}}); err != nil {
			s.st.Accounts[i] = current
			return types.Account{}, err
		}
		return next, nil
	}
	return types.Account{}, ErrCredentialsChanged
}

// DisableAccountIfCurrent is for expiry recovery only. A late failed request
// must not relabel a manual pause as an automatic one, or disable newer tokens.
func (s *Store) DisableAccountIfCurrent(expected types.Account, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, current := range s.st.Accounts {
		if current.ID != expected.ID {
			continue
		}
		if current.CredentialRevision != expected.CredentialRevision || current.Creds != expected.Creds {
			return false, nil
		}
		next := current
		next.Enabled, next.DisabledReason = false, reason
		next.CredentialRevision = s.st.JournalSequence + 1
		s.st.Accounts[i] = next
		if err := s.save(stateChange{Accounts: []types.Account{next}}); err != nil {
			s.st.Accounts[i] = current
			return false, err
		}
		return true, nil
	}
	return false, nil
}
