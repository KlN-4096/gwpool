package types

import "time"

// RegionDonor is private ticket material, stored with the same protection as
// Pair.Cookie. Diagnostics must only use its expiry and region, never Cflb.
type RegionDonor struct {
	RegionKey  string
	Cflb       string
	ExpiresAt  time.Time
	ObservedAt time.Time
}

// StockGain describes committed supply, independently of a requested landing.
// DonorExtendedMS is the donor's own lifetime gain, not summed gateway lifetime.
type StockGain struct {
	CoverageAdded   bool  `json:"coverage_added,omitempty"`
	ExtendedMS      int64 `json:"extended_ms,omitempty"`
	DonorExtendedMS int64 `json:"donor_extended_ms,omitempty"`
}

func (g StockGain) Productive() bool {
	return g.CoverageAdded || g.ExtendedMS > 0 || g.DonorExtendedMS > 0
}

type MintCommit struct {
	Pair  Pair
	At    time.Time
	Route *RouteAttempt
}

type MintCommitResult struct {
	Pair            Pair
	PairChanged     bool
	Gain            StockGain
	Route           RouteState
	BeforeExpiresAt time.Time // Effective incumbent before the incoming donor.
	AfterExpiresAt  time.Time // Actual committed projection, not the candidate TTL.
}
