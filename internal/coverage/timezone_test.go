package coverage

import (
	"testing"

	"gwpool/internal/geography"
)

func TestDatacenterTimezonesUseCitiesAndLeaveAmbiguitiesUnknown(t *testing.T) {
	unknown := map[string]bool{
		"US\x00Des Moines": true, "US\x00Quincy": true, "US\x00Richmond": true,
	}
	seen := map[string]bool{}
	for name, row := range datacenterDirectory {
		key := row.Country + "\x00" + row.City
		seen[key] = true
		zone := DatacenterTimezone(name)
		if unknown[key] {
			if zone != "" {
				t.Fatalf("ambiguous city acquired a guessed timezone: %s", name)
			}
		} else if zone == "" || geography.IANATimezone(zone) != zone {
			t.Fatalf("known city is missing a valid IANA zone: %s", name)
		}
	}
	for key := range datacenterTimezones {
		if !seen[key] {
			t.Fatal("timezone mapping has no historical datacenter")
		}
	}
	for name, want := range map[string]string{
		"unified-1": "America/Chicago", "unified-4": "America/New_York",
		"unified-6": "America/Los_Angeles", "unified-73": "Pacific/Auckland",
		"unified-142": "Europe/Warsaw", "unified-7": "", "unified-outside": "",
	} {
		if got := DatacenterTimezone(name); got != want {
			t.Fatalf("%s timezone = %q, want %q", name, got, want)
		}
	}
}
