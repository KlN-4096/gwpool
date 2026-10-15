package geography

import (
	"strings"
	"time"
	_ "time/tzdata" // Keep IANA validation independent of the deployment host.
)

const maxTimezoneBytes = 128

// IANATimezone accepts a database name, never the server-local zone or a
// numeric UTC offset. Missing/malformed optional Geo metadata stays unknown.
func IANATimezone(name string) string {
	name = strings.TrimSpace(name)
	if name == "" || name == "Local" || len(name) > maxTimezoneBytes {
		return ""
	}
	if _, err := time.LoadLocation(name); err != nil {
		return ""
	}
	return name
}
