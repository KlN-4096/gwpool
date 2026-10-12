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

// AllPriorities aggregates directly by model and gateway. Do not group full
// report structs into per-model copies merely to immediately scan them again.
func AllPriorities(reports []types.ContactReport, now time.Time) map[string]map[string]types.GatewayPriority {
	out := map[string]map[string]types.GatewayPriority{}
	oldest, newest := now.Add(-Retention), now.Add(5*time.Minute)
	for _, report := range reports {
		if report.Model == "" {
			continue
		}
		rows := out[report.Model]
		if rows == nil {
			rows = map[string]types.GatewayPriority{}
			out[report.Model] = rows
		}
		if report.Criterion != "state-echo-v1" || report.Source != "foreground" ||
			report.At.Before(oldest) || report.At.After(newest) ||
			(report.Outcome != "full" && report.Outcome != "refreshed") {
			continue
		}
		p := rows[report.Gateway]
		p.Model = report.Model
		p.Samples++
		if report.Outcome == "full" {
			p.Full++
		}
		rows[report.Gateway] = p
	}
	for _, rows := range out {
		for gateway, p := range rows {
			if p.Samples < PriorityMinSamples {
				delete(rows, gateway)
			}
		}
	}
	return out
}
