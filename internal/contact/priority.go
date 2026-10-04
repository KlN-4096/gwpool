package contact

import (
	"time"

	"gwpool/internal/types"
)

const PriorityMinSamples = 5

// Pool-wide, model-specific empirical rate. This is not a capability score or
// evidence that changing a cooldown causes recovery. Unknowns and business
// confirmations never become initial-quality samples.
func Priorities(reports []types.ContactReport, model string, now time.Time) map[string]types.GatewayPriority {
	out := map[string]types.GatewayPriority{}
	if model == "" {
		return out
	}
	for _, report := range reports {
		if report.Model != model || report.Criterion != "state-echo-v1" || report.Source != "foreground" ||
			report.At.Before(now.Add(-Retention)) || report.At.After(now.Add(5*time.Minute)) {
			continue
		}
		if report.Outcome != "full" && report.Outcome != "refreshed" {
			continue
		}
		p := out[report.Gateway]
		p.Model = model
		p.Samples++
		if report.Outcome == "full" {
			p.Full++
		}
		out[report.Gateway] = p
	}
	for gateway, p := range out {
		if p.Samples < PriorityMinSamples {
			delete(out, gateway)
		}
	}
	return out
}
