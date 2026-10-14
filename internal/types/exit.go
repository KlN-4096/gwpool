package types

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

const MaxLocationBytes = 96

const (
	PurposeScan      = "scan"
	PurposeDelivery  = "delivery"
	PurposeRenew     = "renew"
	PurposeInventory = "inventory"
	PurposeRecovery  = "recovery"
	PurposeExplore   = "explore"
	PurposeMintAll   = "mint-all"
	PurposeVerify    = "verify"
)

// MatchEvidence distinguishes missing evidence from a confirmed different exit.
func (e Exit) MatchEvidence(observed Observed, seed bool) string {
	if seed {
		return "seed-route"
	}
	if observed.Country == "" || (e.State != "" && observed.State == "") ||
		(e.City != "" && observed.City == "") {
		return "unknown"
	}
	if e.MatchesObserved(observed) {
		return "matched"
	}
	return "mismatch"
}

func (e Exit) Label() string {
	value := e.Region
	if e.State != "" {
		value += ":" + e.State
	}
	if e.City != "" {
		value += ":" + e.City
	}
	return value
}

// ValidLocationName accepts provider directory values, but never proxy options.
func ValidLocationName(value string) bool {
	if value == "" || len(value) > MaxLocationBytes || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value {
		return false
	}
	hasWord := false
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			hasWord = true
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !strings.ContainsRune(" -_.'", r) {
			return false
		}
	}
	if !hasWord {
		return false
	}
	lower := strings.ToLower(value)
	for _, option := range []string{"-region-", "-country-", "-city-", "-st-", "-sid-", "-session-", "-time-", "-t-"} {
		if strings.Contains(lower, option) {
			return false
		}
	}
	return true
}

// MatchesObserved requires evidence for every requested geographical component.
// It normalizes spelling separators, not geography: missing/different locations
// remain unconfirmed instead of silently becoming a successful target.
func (e Exit) MatchesObserved(observed Observed) bool {
	if e.Region == "" || !strings.EqualFold(e.Region, observed.Country) {
		return false
	}
	normalize := func(value string) string {
		return strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, value)
	}
	matches := func(want, got string) bool {
		return want == "" || (normalize(want) != "" && normalize(got) != "" && normalize(want) == normalize(got))
	}
	return matches(e.State, observed.State) && matches(e.City, observed.City)
}

// Country and account limits are independent. used is keyed by ISO country,
// never by display area or exit/city alias.
func MintBudgetFull(used map[string]int, perCountry, accountLimit int, country string) bool {
	total := 0
	for _, n := range used {
		total += n
	}
	return (accountLimit > 0 && total >= accountLimit) || (perCountry > 0 && country != "" && used[country] >= perCountry)
}
