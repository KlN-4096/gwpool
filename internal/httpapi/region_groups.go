package httpapi

import (
	"sort"

	"gwpool/internal/config"
)

type displayRegionGroup[T any] struct {
	Label     string
	Countries []displayRegionCountry[T]
}

type displayRegionCountry[T any] struct {
	Country string
	Cells   []T
}

// Group only the supplied visible cells. The account-list path never builds an
// empty cell for every globally enabled candidate.
func groupDisplayRegions[T any](cfg config.Config, cells []T, keyOf func(T) string) []displayRegionGroup[T] {
	byCountry := map[string][]T{}
	for _, cell := range cells {
		def, _ := cfg.RegionByKey(keyOf(cell))
		byCountry[def.Country] = append(byCountry[def.Country], cell)
	}
	countries := make([]string, 0, len(byCountry))
	for country := range byCountry {
		countries = append(countries, country)
	}
	sort.Strings(countries)
	var groups []displayRegionGroup[T]
	indices := map[string]int{}
	for _, country := range countries {
		area := config.CountryArea(country)
		index, ok := indices[area]
		if !ok {
			index = len(groups)
			indices[area] = index
			groups = append(groups, displayRegionGroup[T]{Label: area})
		}
		label := country
		if label == "" {
			label = "未归类"
		}
		groups[index].Countries = append(groups[index].Countries, displayRegionCountry[T]{
			Country: label, Cells: byCountry[country],
		})
	}
	return groups
}

func poolDisplayGroups(cfg config.Config, cells []poolRegionCell, showEmpty bool) []displayRegionGroup[poolRegionCell] {
	visible := make([]poolRegionCell, 0, len(cells))
	for _, cell := range cells {
		if !cell.All && (cell.Total > 0 || cell.Active || showEmpty) {
			visible = append(visible, cell)
		}
	}
	return groupDisplayRegions(cfg, visible, func(cell poolRegionCell) string { return cell.Key })
}
