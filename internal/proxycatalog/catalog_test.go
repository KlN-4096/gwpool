package proxycatalog

import "testing"

func TestCliproxyDirectoryIsBoundedAndSeparateFromEnabledRegions(t *testing.T) {
	countries := CliproxyCountries()
	if len(countries) != 203 {
		t.Fatalf("country count=%d, want 203 distinct codes (excluding Rand)", len(countries))
	}
	seen := map[string]bool{}
	for _, c := range countries {
		if seen[c.Code] {
			t.Fatalf("duplicate country code %s", c.Code)
		}
		seen[c.Code] = true
	}
	if !seen["MD"] || !seen["MG"] {
		t.Fatal("Moldova or Madagascar disappeared during deduplication")
	}
	if !CliproxySupports("US", "Indiana") || CliproxySupports("US", "DefinitelyNotAState") ||
		CliproxySupports("Rand", "") || CliproxySupports("??", "") {
		t.Fatal("directory targeting check is incorrect")
	}
	countries[0].Code = "broken"
	if CliproxyCountries()[0].Code == "broken" {
		t.Fatal("caller mutated the embedded directory")
	}
}

func TestOperatorCountrySelectorsStayProviderSpecific(t *testing.T) {
	if len(novproxyCountries) != 207 || len(webshareCountries) != 249 || len(GlobalCountries()) != 249 {
		t.Fatal("operator-supplied selector counts changed")
	}
	if NovproxySupports("CN") || !WebshareSupports("CN") || !NovproxySupports("SG") ||
		WebshareSupports("ZZ") || NovproxySupports("ZZ") {
		t.Fatal("provider country capabilities were merged instead of kept separate")
	}
}
