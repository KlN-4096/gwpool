package mint

import (
	"context"
	"sync/atomic"
	"time"

	"gwpool/internal/geography"
	"gwpool/internal/types"
)

type attemptContextKey struct{}
type attemptReservation struct {
	account, country string
	at               time.Time
	consumed         atomic.Bool
}

func (r *attemptReservation) matches(cr types.Creds, e types.Exit) bool {
	return r != nil && r.account == cr.AccountID && r.country == geography.CountryKey(e.Region) &&
		time.Since(r.at) < time.Hour
}

func (c *Client) BeginAttempt(ctx context.Context, cr types.Creds, e types.Exit) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return ctx, err
	}
	if reserved, _ := ctx.Value(attemptContextKey{}).(*attemptReservation); reserved.matches(cr, e) && !reserved.consumed.Load() {
		return ctx, nil
	}
	if err := c.spend(cr.AccountID, e); err != nil {
		return ctx, err
	}
	reserved := &attemptReservation{account: cr.AccountID, country: geography.CountryKey(e.Region), at: time.Now()}
	return context.WithValue(ctx, attemptContextKey{}, reserved), nil
}

func (c *Client) spendForContext(ctx context.Context, cr types.Creds, e types.Exit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reserved, _ := ctx.Value(attemptContextKey{}).(*attemptReservation); reserved.matches(cr, e) &&
		reserved.consumed.CompareAndSwap(false, true) {
		return nil
	}
	return c.spend(cr.AccountID, e)
}
