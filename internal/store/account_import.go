package store

import (
	"context"

	"gwpool/internal/types"
)

// AddAccountIfVersion is the commit point for long-running interactive imports.
// id=0 means the account did not exist when the task began. A concurrent import,
// deletion, refresh, or manual status edit must never be undone by a late task.
func (s *Store) AddAccountIfVersion(ctx context.Context, a types.Account, id int64, revision uint64) (types.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// The task may have expired while waiting for this mutex. Once admitted
	// here, the atomic durable write is no longer cancelable.
	if err := ctx.Err(); err != nil {
		return types.Account{}, err
	}
	found := false
	for _, current := range s.st.Accounts {
		if current.Creds.AccountID != a.Creds.AccountID {
			continue
		}
		found = true
		if current.ID != id || current.CredentialRevision != revision {
			return types.Account{}, ErrCredentialsChanged
		}
		break
	}
	if !found && id != 0 {
		return types.Account{}, ErrCredentialsChanged
	}
	return s.addAccountLocked(a)
}
