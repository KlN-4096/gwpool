package httpapi

import (
	"net/http"
	"strings"
	"time"

	"gwpool/internal/types"
)

type accountMintStats struct {
	attempts int
	minted   int
	lastAt   time.Time
}

// summarizeAccountMints uses mint evidence, never consumer Slot/ExternalUsage
// records. Legacy samples without a purpose retain their original mint meaning.
func summarizeAccountMints(samples []types.Sample, now time.Time) map[int64]accountMintStats {
	out := make(map[int64]accountMintStats)
	for _, sample := range samples {
		switch sample.Purpose {
		case "", types.PurposeScan, types.PurposeDelivery, types.PurposeRenew,
			types.PurposeInventory, types.PurposeExplore, types.PurposeMintAll:
		default:
			continue // Verification and unknown purposes are not mint evidence.
		}
		if sample.At.After(now) {
			continue
		}
		stats := out[sample.AccountID]
		stats.attempts++
		if successfulMintSample(sample) {
			stats.minted++
			if sample.At.After(stats.lastAt) {
				stats.lastAt = sample.At
			}
		}
		out[sample.AccountID] = stats
	}
	return out
}

func successfulMintSample(sample types.Sample) bool {
	if sample.Status != http.StatusOK || strings.TrimSpace(sample.Gateway) == "" {
		return false
	}
	if sample.Route != nil {
		// A committed drift can carry an Err explaining the target miss. It
		// still obtained a ticket; no_gain likewise need not add new coverage.
		return sample.Route.Result == "productive" || sample.Route.Result == "no_gain"
	}
	// Historical samples lack a structured result. Do not promote an ambiguous
	// error to success just because it also contains HTTP 200 and a gateway.
	return sample.Err == ""
}
