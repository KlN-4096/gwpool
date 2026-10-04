package httpapi

import (
	"net/http"
	"time"

	"gwpool/internal/oauth"
	"gwpool/internal/types"
)

// Display evidence, not a new scheduling or anonymous-access gate. Temporary
// holds do not invalidate credentials; historical success is not a live probe.
type accountHealth struct {
	Hosted, Effective, Pending, Disabled, Invalid, Held int
}

func accountEvidence(mints map[string]types.RegionMint) map[int64]bool {
	proven := make(map[int64]bool)
	for _, m := range mints {
		if m.Count > 0 {
			proven[m.AccountID] = true
		}
	}
	return proven
}

func accountHealthState(a types.Account, proven bool, now time.Time) string {
	if !a.Enabled {
		return "disabled"
	}
	cl, err := oauth.ClaimsFor(oauth.Tokens{AccessToken: a.Creds.AccessToken}, a.Creds.AccountID)
	if err != nil || cl.AccountID == "" || (!cl.Expires.IsZero() && !cl.Expires.After(now)) {
		return "invalid"
	}
	if !proven {
		return "pending"
	}
	return "effective"
}

func summarizeAccounts(accounts []types.Account, proven map[int64]bool, holds map[int64]time.Duration, now time.Time) accountHealth {
	var out accountHealth
	for _, a := range accounts {
		if a.Creds.AccessToken == "" {
			continue
		}
		out.Hosted++
		switch accountHealthState(a, proven[a.ID], now) {
		case "effective":
			out.Effective++
			if holds[a.ID] > 0 {
				out.Held++
			}
		case "pending":
			out.Pending++
		case "disabled":
			out.Disabled++
		default:
			out.Invalid++
		}
	}
	return out
}

// Current inventory and recorded attempts have different denominators. Donor
// cookie expiry cannot reveal mint time, and success is not net inventory growth.
func supplySnapshot(gateways map[string]types.Gateway, pairs map[string]types.Pair, samples []types.Sample, now time.Time, target int) supplyLedger {
	out := supplyLedger{WindowMin: int(supplyWindow / time.Minute),
		FloorMin: int(types.DeliverFloor / time.Minute), Target: target}
	for name := range gateways {
		pair, ok := pairs[name]
		if !ok {
			continue
		}
		left := pair.ExpiresAt.Sub(now)
		if left >= types.DeliverFloor {
			out.Ready++
			if left-types.DeliverFloor < supplyWindow {
				out.Expiring++
			}
		}
	}
	for _, sample := range samples {
		if sample.At.Before(now.Add(-supplyWindow)) || sample.At.After(now) {
			continue
		}
		out.Attempts++
		if sample.Status == http.StatusOK && sample.Gateway != "" && sample.Err == "" {
			out.Landed++
		}
	}
	out.Gap = max(0, target-out.Ready)
	return out
}

func attachGatewayStats(rows []adminGatewayRow, contacts []types.ContactStats, cooldowns []types.CooldownStats) {
	byName := make(map[string]int, len(rows))
	for i := range rows {
		byName[rows[i].Name] = i
	}
	for _, c := range contacts {
		if i, ok := byName[c.Gateway]; ok {
			row := &rows[i]
			row.Contacts = append(row.Contacts, c)
			row.Full += c.Full
			row.Refreshed += c.Refreshed
			row.Unknown += c.Unknown
		}
	}
	for _, c := range cooldowns {
		if i, ok := byName[c.Gateway]; ok {
			rows[i].Cooldown = c
			if c.Recommendation.Seconds > 0 {
				rows[i].CooldownLabel = shortDur(time.Duration(c.Recommendation.Seconds) * time.Second)
			}
		}
	}
}
