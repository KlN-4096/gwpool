package httpapi

import (
	"gwpool/internal/coverage"
	"gwpool/internal/geography"
)

type directoryArea struct {
	Key, Label                            string
	Total, Found, Ready, Missing, Percent int
	Groups                                []coverage.DatacenterGroup
}

type directoryView struct {
	Areas                                 []directoryArea
	Total, Found, Ready, Missing, Unknown int
}

// Kept exclusively in admin view models: proxy-exit reachability on public and
// owner pages is not a denominator for the historical datacenter directory.
func buildDirectory(snapshot coverage.Snapshot) directoryView {
	v := directoryView{Unknown: snapshot.UnknownDatacenters}
	for _, area := range geography.Areas {
		row := directoryArea{Key: area.Key, Label: area.Label}
		for _, group := range snapshot.Datacenters {
			if geography.AreaOf(group.Country).Key == area.Key {
				row.Groups = append(row.Groups, group)
				row.Total += group.Total
				row.Found += group.Found
				row.Ready += group.Ready
			}
		}
		row.Missing = row.Total - row.Found
		if row.Total > 0 {
			row.Percent = row.Found * 100 / row.Total
		}
		v.Areas = append(v.Areas, row)
		v.Total += row.Total
		v.Found += row.Found
		v.Ready += row.Ready
	}
	v.Missing = v.Total - v.Found
	return v
}
