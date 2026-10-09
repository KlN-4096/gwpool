// Package authrefresh shares validated, durable OAuth renewal between periodic
// maintenance and expiry-only request recovery.
package authrefresh

import (
	"context"
	"errors"
	"time"

	"gwpool/internal/oauth"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

type RefreshFunc func(context.Context, types.Account) (types.Account, error)

// RefreshLocked requires the caller to hold Store.LockAccount. Never acquire it
// here: request paths already own this non-reentrant lock.
func RefreshLocked(ctx context.Context, st *store.Store, expected types.Account, resume bool) (types.Account, error) {
	if err := ctx.Err(); err != nil {
		return types.Account{}, err
	}
	current, exists := st.Account(expected.ID)
	if !exists || current.Creds != expected.Creds || current.CredentialRevision != expected.CredentialRevision {
		return types.Account{}, store.ErrCredentialsChanged
	}
	if current.Creds.RefreshToken == "" {
		return types.Account{}, errors.New("过期凭据没有 refresh_token，需重新上传")
	}
	tokens, err := oauth.Refresh(ctx, nil, current.Creds.RefreshToken)
	if err != nil {
		return types.Account{}, err
	}
	claims, err := oauth.ClaimsFor(tokens, current.Creds.AccountID)
	if err != nil {
		return types.Account{}, err
	}
	if !claims.Expires.IsZero() && !claims.Expires.After(time.Now()) {
		return types.Account{}, errors.New("刷新返回的 access_token 已过期")
	}
	// Do not discard a received rotated RT just because cancellation raced
	// with the response. Callers check cancellation before sending again.
	return st.CommitRefreshedTokens(current, tokens.AccessToken, tokens.RefreshToken, resume)
}

func ForRequests(st *store.Store) RefreshFunc {
	return func(ctx context.Context, account types.Account) (types.Account, error) {
		return RefreshLocked(ctx, st, account, false)
	}
}
