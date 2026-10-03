// Package proxycatalog contains operator-supplied provider location codes.
// These are targeting capabilities, never proof of gateway coverage.
package proxycatalog

import (
	_ "embed"
	"encoding/json"
	"sort"
)

//go:embed cliproxy.json
var cliproxyJSON []byte

//go:embed novproxy.json
var novproxyJSON []byte

//go:embed webshare.json
var webshareJSON []byte

var novproxyCountries = readCountryCodes(novproxyJSON)
var webshareCountries = readCountryCodes(webshareJSON)

func readCountryCodes(raw []byte) map[string]bool {
	var directory struct {
		Countries []string `json:"countries"`
	}
	if err := json.Unmarshal(raw, &directory); err != nil {
		panic("invalid embedded provider country codes")
	}
	out := map[string]bool{}
	for _, code := range directory.Countries {
		if len(code) != 2 || code[0] < 'A' || code[0] > 'Z' || code[1] < 'A' || code[1] > 'Z' {
			panic("invalid provider country code")
		}
		out[code] = true
	}
	return out
}

type Country struct {
	Code   string   `json:"code"`
	Name   string   `json:"name"`
	Label  string   `json:"label"`
	States []string `json:"states"`
}

var cliproxyCountries = func() []Country {
	var directory struct {
		Countries []Country `json:"countries"`
	}
	if err := json.Unmarshal(cliproxyJSON, &directory); err != nil {
		panic("invalid embedded Cliproxy location directory")
	}
	// The supplied directory contains a duplicate MD row mislabeled Madagascar;
	// a separate MG row is already present. Keep the first definition by code.
	seen := map[string]bool{}
	out := make([]Country, 0, len(directory.Countries))
	for _, country := range directory.Countries {
		if !seen[country.Code] {
			out = append(out, country)
			seen[country.Code] = true
		}
	}
	return out
}()

func CliproxyCountries() []Country {
	out := make([]Country, len(cliproxyCountries))
	for i, country := range cliproxyCountries {
		out[i] = country
		out[i].States = append([]string(nil), country.States...)
	}
	return out
}

func CliproxySupports(country, state string) bool {
	if country == "" {
		return state == ""
	}
	for _, entry := range cliproxyCountries {
		if entry.Code != country {
			continue
		}
		if state == "" {
			return true
		}
		for _, candidate := range entry.States {
			if candidate == state {
				return true
			}
		}
		return false
	}
	return false
}

func NovproxySupports(country string) bool { return country == "" || novproxyCountries[country] }
func WebshareSupports(country string) bool { return webshareCountries[country] }

// GlobalCountries is the union of operator-supplied vendor selectors, not an
// assertion that each vendor serves every country or has inventory right now.
func GlobalCountries() []Country {
	entries := map[string]Country{}
	for _, country := range CliproxyCountries() {
		country.States = nil
		entries[country.Code] = country
	}
	for _, codes := range []map[string]bool{novproxyCountries, webshareCountries} {
		for code := range codes {
			if _, exists := entries[code]; !exists {
				entries[code] = Country{Code: code, Name: code, Label: code}
			}
		}
	}
	out := make([]Country, 0, len(entries))
	for _, country := range entries {
		out = append(out, country)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out
}
