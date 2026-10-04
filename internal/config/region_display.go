package config

import "gwpool/internal/geography"

// Geographic display buckets only: never use these to infer gateway reachability
// or to merge quota keys. Unlisted/custom countries remain explicitly ungrouped.
func CountryArea(country string) string {
	return geography.AreaOf(country).Label
}
