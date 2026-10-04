package types

import (
	"errors"
	"fmt"
	"time"
)

var ErrUnsupportedCountry = errors.New("上游不支持此国家/地区")

// CountryAttempt keeps failed attempts separate from successful RegionMints.
// Recent is bounded by the independent account-hour cap; Count drives fair
// country traversal and is never interpreted as successful gateway discovery.
type CountryAttempt struct {
	Count  int64
	LastAt time.Time
	Recent []time.Time `json:",omitempty"`
}

type AttemptLimit struct {
	Region string // ISO country code, or "all" for the independent account cap
	Limit  int
	Wait   time.Duration
}

func (e *AttemptLimit) Error() string {
	return fmt.Sprintf("%s（国家 %s 每小时上限 %d，%s 后腾出一次）",
		ErrRateLimited, e.Region, e.Limit, e.Wait.Round(time.Second))
}

func (e *AttemptLimit) Unwrap() error { return ErrRateLimited }

type AttemptLedger interface {
	ReserveAttempt(account, country string, perCountry, totalLimit int, at time.Time) error
	AttemptUsage(account string, at time.Time) map[string]int
}
