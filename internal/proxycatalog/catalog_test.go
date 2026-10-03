package proxycatalog

import "testing"

func TestCliproxyDirectoryIsBoundedAndSeparateFromEnabledRegions(t *testing.T) {
	countries := CliproxyCountries()
	if len(countries) != 204 {
		t.Fatalf("country count=%d, want 204 (excluding Rand)", len(countries))
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
