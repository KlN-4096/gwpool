// Package proxycatalog contains operator-supplied provider location codes.
// These are targeting capabilities, never proof of gateway coverage.
package proxycatalog

import (
	_ "embed"
	"encoding/json"
)

//go:embed cliproxy.json
var cliproxyJSON []byte

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
	return directory.Countries
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
