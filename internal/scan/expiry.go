package scan

import (
	"context"
	"errors"
	"fmt"

	"gwpool/internal/oauth"
	"gwpool/internal/types"
)

// ScanRegion holds the shared account lock for this operation.
func (s *Scanner) refreshExpired(ctx context.Context, a types.Account, reason string) (types.Account, error) {
	if err := ctx.Err(); err != nil {
		return a, err
	}
	if a.Creds.RefreshToken == "" {
		return a, s.rejectExpired(a, reason)
	}
	updated, err := s.RefreshExpired(ctx, a)
	if err != nil {
		if s.Log != nil {
			s.Log.Warn("过期凭据刷新失败", "account", a.ID, "err", err)
		}
		if ctx.Err() != nil {
			return a, ctx.Err()
		}
		if oauth.RefreshTokenRejected(err) {
			reason = oauth.ExpiredRefreshRejectedReason
		}
		return a, errors.Join(s.rejectExpired(a, reason), err)
	}
	if !updated.Enabled {
		return updated, errors.New("账号已停用，取消刷新后的扫描")
	}
	return updated, nil
}

func (s *Scanner) rejectExpired(a types.Account, reason string) error {
	_, err := s.Store.DisableAccountIfCurrent(a, reason)
	return errors.Join(fmt.Errorf("%w: %w", ErrAccountRejected, oauth.ErrAccessExpired), err)
}
