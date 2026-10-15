package geography

import (
	"strings"
	"testing"
	"time"
)

func TestIANATimezoneRejectsUnknownLocalAndOffsets(t *testing.T) {
	for _, name := range []string{"", " ", "Local", "+08:00", "UTC+8", "Unknown/Nowhere",
		"../Asia/Tokyo", "/Asia/Tokyo", "Asia/Tokyo\x00", strings.Repeat("x", maxTimezoneBytes+1)} {
		if got := IANATimezone(name); got != "" {
			t.Fatalf("invalid timezone accepted: %q", name)
		}
	}
	for _, name := range []string{"UTC", "Asia/Tokyo", "Asia/Kolkata", "America/Phoenix",
		"America/New_York", "Europe/Warsaw", "Australia/Sydney", "Pacific/Auckland"} {
		if got := IANATimezone(" " + name + " "); got != name {
			t.Fatalf("valid IANA zone rejected: %q", name)
		}
	}
	zone, err := time.LoadLocation(IANATimezone("America/New_York"))
	if err != nil {
		t.Fatal(err)
	}
	_, winter := time.Date(2026, 1, 1, 12, 0, 0, 0, zone).Zone()
	_, summer := time.Date(2026, 7, 1, 12, 0, 0, 0, zone).Zone()
	if winter == summer {
		t.Fatal("IANA zone was flattened into a fixed UTC offset")
	}
}
