package httpapi

import (
	"time"

	"gwpool/internal/coverage"
	"gwpool/internal/types"
)

// Both endpoints emit all fields, using JSON null for unknown provenance.
// Exit metadata has already been validated on Geo intake; no I/O is performed
// here, and neither a live Store reread nor a gateway's latest sample is used.
type gatewayTimezones struct {
	DatacenterTimezone *string    `json:"datacenter_timezone"`
	MintExitTimezone   *string    `json:"mint_exit_timezone"`
	MintExitObservedAt *time.Time `json:"mint_exit_observed_at"`
}

func timezonesForTicket(gateway string, exit types.ExitTimezoneObservation) gatewayTimezones {
	var fields gatewayTimezones
	if zone := coverage.DatacenterTimezone(gateway); zone != "" {
		fields.DatacenterTimezone = &zone
	}
	if exit.Timezone != "" && !exit.ObservedAt.IsZero() {
		at := exit.ObservedAt.UTC()
		fields.MintExitTimezone, fields.MintExitObservedAt = &exit.Timezone, &at
	}
	return fields
}
