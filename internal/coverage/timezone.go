package coverage

// Historical city -> IANA zone, not a live gateway/IP observation. Names follow
// https://data.iana.org/time-zones/tzdb/zone.tab (including its valid aliases).
// The source directory has no state/coordinates: US Des Moines, Quincy and
// Richmond remain unknown rather than guessing which same-named city it meant.
var datacenterTimezones = map[string]string{
	"AU\x00Canberra":          "Australia/Sydney",
	"AU\x00Melbourne":         "Australia/Melbourne",
	"AU\x00The Rocks":         "Australia/Sydney",
	"BE\x00Brussels":          "Europe/Brussels",
	"BR\x00São Paulo":         "America/Sao_Paulo",
	"CA\x00Toronto":           "America/Toronto",
	"CH\x00Geneva":            "Europe/Zurich",
	"CH\x00Zurich":            "Europe/Zurich",
	"DE\x00Bremen":            "Europe/Berlin",
	"DE\x00Frankfurt am Main": "Europe/Berlin",
	"DK\x00Copenhagen":        "Europe/Copenhagen",
	"ES\x00Madrid":            "Europe/Madrid",
	"GB\x00Cardiff":           "Europe/London",
	"GB\x00City of London":    "Europe/London",
	"ID\x00Jakarta":           "Asia/Jakarta",
	"IE\x00Dublin":            "Europe/Dublin",
	"IN\x00Chennai":           "Asia/Kolkata",
	"IN\x00Medchal":           "Asia/Kolkata",
	"IN\x00Pune":              "Asia/Kolkata",
	"IT\x00Milan":             "Europe/Rome",
	"JP\x00Osaka":             "Asia/Tokyo",
	"JP\x00Tokyo":             "Asia/Tokyo",
	"KR\x00Seoul":             "Asia/Seoul",
	"MY\x00Kuala Lumpur":      "Asia/Kuala_Lumpur",
	"NL\x00Amsterdam":         "Europe/Amsterdam",
	"NZ\x00Auckland":          "Pacific/Auckland",
	"PL\x00Warsaw":            "Europe/Warsaw",
	"SE\x00Gävle":             "Europe/Stockholm",
	"SG\x00Singapore":         "Asia/Singapore",
	"US\x00Atlanta":           "America/New_York",
	"US\x00Boydton":           "America/New_York",
	"US\x00Cheyenne":          "America/Denver",
	"US\x00Chicago":           "America/Chicago",
	"US\x00Phoenix":           "America/Phoenix",
	"US\x00San Antonio":       "America/Chicago",
	"US\x00San Francisco":     "America/Los_Angeles",
}

func DatacenterTimezone(gateway string) string {
	location, known := datacenterDirectory[gateway]
	if !known {
		return ""
	}
	return datacenterTimezones[location.Country+"\x00"+location.City]
}
