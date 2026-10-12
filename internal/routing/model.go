// Package routing predicts visible cookie gateway labels. It never determines
// eligibility, invents a recovery route, or claims to measure model capability.
package routing

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"gwpool/internal/geography"
	"gwpool/internal/types"
)

const (
	Version          = "inventory-country-factorized-v1"
	Pseudocount      = 10.0
	MinSupport       = 5
	MinExitAgreement = 0.8
	HistoryLimit     = 8192
	HistoryRetention = 7 * 24 * time.Hour
)

type Observation struct {
	Donor     string
	Country   string
	Gateway   string
	Requested types.Exit
	At        time.Time
}

type History struct {
	Observations []Observation
	Loaded       bool // This call initialized the cache; emit diagnostics once.
	Clipped      bool
	SkippedLines int
}

type ModelSnapshot struct {
	Model        *Model
	Loaded       bool
	Clipped      bool
	SkippedLines int
}

// DonorKey is stable across local rows and token refreshes. Neither the
// credential identifier nor token is included in forecasts or diagnostics.
func DonorKey(account string) string {
	if account == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(account))
	return hex.EncodeToString(sum[:])
}

// BareSample follows the same positive-purpose/provider whitelist as the
// research exporter. Untagged history, cookie renewal and quality probes are
// deliberately not inferred to be bare mints.
func BareSample(s types.Sample) bool {
	switch s.Purpose {
	case types.PurposeScan, types.PurposeInventory, types.PurposeExplore, types.PurposeMintAll:
	default:
		return false
	}
	switch s.Provider {
	case "novproxy", "webshare", "cliproxy", "ipipgo":
	default:
		return false
	}
	name, ok := strings.CutPrefix(s.Gateway, "unified-")
	if !ok || len(name) > 10 {
		return false
	}
	number, err := strconv.ParseUint(name, 10, 32)
	return err == nil && number > 0 && s.Status == 200 &&
		geography.CountryKey(s.Observed.Country) != "" && !s.At.IsZero()
}

func canonicalExit(exit types.Exit) types.Exit {
	return types.Exit{Region: geography.CountryKey(exit.Region),
		State: strings.TrimSpace(exit.State), City: strings.TrimSpace(exit.City)}
}

// Model is immutable after construction and can be shared by a maintenance
// round's workers. Counts are built once per round, never by reading the sample
// file in a candidate comparator.
type Model struct {
	global        map[string]int
	donors        map[string]map[string]int
	countries     map[string]map[string]int
	donorTotals   map[string]int
	countryTotals map[string]int
	exitCountries map[types.Exit]map[string]int
	total         int
	through       time.Time
}

func New(observations []Observation) *Model {
	m := &Model{
		global: map[string]int{}, donors: map[string]map[string]int{},
		countries: map[string]map[string]int{}, donorTotals: map[string]int{},
		countryTotals: map[string]int{}, exitCountries: map[types.Exit]map[string]int{},
	}
	for _, row := range observations {
		country := geography.CountryKey(row.Country)
		if row.Donor == "" || country == "" || row.Gateway == "" {
			continue
		}
		m.global[row.Gateway]++
		count(m.donors, row.Donor, row.Gateway)
		count(m.countries, country, row.Gateway)
		m.donorTotals[row.Donor]++
		m.countryTotals[country]++
		m.total++
		if row.At.After(m.through) {
			m.through = row.At
		}
		exit := canonicalExit(row.Requested)
		if exit.Region != "" {
			if m.exitCountries[exit] == nil {
				m.exitCountries[exit] = map[string]int{}
			}
			m.exitCountries[exit][country]++
		}
	}
	return m
}

func count(table map[string]map[string]int, key, value string) {
	if table[key] == nil {
		table[key] = map[string]int{}
	}
	table[key][value]++
}

// ForExit estimates the actual country from PAST observations of the requested
// exit. This is explicitly not this attempt's measured Geo. Sparse/mixed exits
// and sparse donor/country evidence abstain instead of rebranding a requested
// country as observed. Provider selection is not known here.
func (m *Model) ForExit(donor string, exit types.Exit) types.InventoryPrediction {
	p := types.InventoryPrediction{Model: Version, GeoBasis: "historical-requested-exit"}
	if m == nil {
		return p
	}
	p.TrainingSamples, p.TrainedThrough = m.total, m.through
	p.DonorSamples = m.donorTotals[donor]
	bestCount := 0
	for country, n := range m.exitCountries[canonicalExit(exit)] {
		p.ExitSamples += n
		if n > bestCount || n == bestCount && country > p.Country {
			p.Country, bestCount = country, n
		}
	}
	if p.ExitSamples < MinSupport || float64(bestCount)/float64(p.ExitSamples) < MinExitAgreement {
		p.Country = ""
		return p
	}
	p.CountrySamples = m.countryTotals[p.Country]
	if !geography.Supported(p.Country) || p.DonorSamples < MinSupport || p.CountrySamples < MinSupport {
		return p
	}
	best := -1.0
	for gateway, n := range m.global {
		p0 := float64(n) / float64(m.total)
		score := (float64(m.donors[donor][gateway]) + Pseudocount*p0) *
			(float64(m.countries[p.Country][gateway]) + Pseudocount*p0) / p0
		if score > best || score == best && gateway > p.Gateway {
			p.Gateway, best = gateway, score
		}
	}
	return p
}
