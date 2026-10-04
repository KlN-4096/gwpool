package coverage

import (
	_ "embed"
	"encoding/json"
	"sort"
	"strings"
)

// Historical DNS/IP geography from gw_full_table.tsv, 2026-10-01.
// These datacenters are not proxy exits or an exhaustive upstream inventory.
//
//go:embed datacenters.json
var datacentersJSON []byte

type datacenterLocation struct {
	Name    string `json:"name"`
	Country string `json:"country"`
	City    string `json:"city"`
}

var datacenterDirectory = func() map[string]datacenterLocation {
	var rows []datacenterLocation
	if err := json.Unmarshal(datacentersJSON, &rows); err != nil {
		panic("invalid embedded gateway datacenter directory")
	}
	out := make(map[string]datacenterLocation, len(rows))
	for _, row := range rows {
		if _, duplicate := out[row.Name]; duplicate || row.Name == "" || row.Country == "" || row.City == "" {
			panic("invalid gateway datacenter directory row")
		}
		out[row.Name] = row
	}
	// Preserve the already-verified 201-name DNS inventory exactly.
	numbers := strings.Fields(dnsCandidateNumbers)
	if len(numbers) != len(out) {
		panic("gateway datacenter directory does not match DNS inventory")
	}
	for _, number := range numbers {
		if _, exists := out["unified-"+number]; !exists {
			panic("gateway datacenter directory is missing a DNS candidate")
		}
	}
	return out
}()

// DatacenterCountry is historical gateway geography, not the minting proxy's
// exit country. Missing directory entries remain unknown.
func DatacenterCountry(name string) string {
	return datacenterDirectory[name].Country
}

type DatacenterGroup struct {
	Country, City       string
	Total, Found, Ready int
	Missing             []string
	Complete            bool
}

func datacenterGroups(found, ready func(string) bool) []DatacenterGroup {
	locations := map[string]*DatacenterGroup{}
	for name, location := range datacenterDirectory {
		key := location.Country + "\x00" + location.City
		group := locations[key]
		if group == nil {
			group = &DatacenterGroup{Country: location.Country, City: location.City}
			locations[key] = group
		}
		group.Total++
		if found(name) {
			group.Found++
			if ready(name) {
				group.Ready++
			}
		} else {
			group.Missing = append(group.Missing, name)
		}
	}
	out := make([]DatacenterGroup, 0, len(locations))
	for _, group := range locations {
		sort.Strings(group.Missing)
		group.Complete = group.Found == group.Total
		out = append(out, *group)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Country != out[j].Country {
			return out[i].Country < out[j].Country
		}
		return out[i].City < out[j].City
	})
	return out
}
