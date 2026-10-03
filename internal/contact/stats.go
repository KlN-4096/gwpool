package contact

import (
	"sort"
	"strconv"
	"time"

	"gwpool/internal/types"
)

const (
	Retention          = 7 * 24 * time.Hour
	MaxReports         = 8192
	MaxReporterReports = 1024
	MaxElapsedSeconds  = 30 * 24 * 60 * 60
	MaxWindowMS        = 10 * 60 * 60 * 1000
)

func Interval(known bool, seconds int64) string {
	if !known {
		return "unknown"
	}
	switch {
	case seconds < 3600:
		return "<1h"
	case seconds < 7200:
		return "1-2h"
	case seconds < 14400:
		return "2-4h"
	case seconds < 21600:
		return "4-6h"
	case seconds < 28800:
		return "6-8h"
	case seconds < 36000:
		return "8-10h"
	default:
		return ">=10h"
	}
}

func Summarize(reports []types.ContactReport, now time.Time) []types.ContactStats {
	type key struct {
		reporter                                                int64
		tag, gateway, model, criterion, source, first, interval string
	}
	rows := map[key]*types.ContactStats{}
	sums := map[key]int64{}
	for _, r := range reports {
		if r.At.Before(now.Add(-Retention)) || r.At.After(now.Add(5*time.Minute)) {
			continue
		}
		k := key{r.ReporterID, r.AccountTag, r.Gateway, r.Model, r.Criterion, r.Source, r.First, Interval(r.GapKnown, r.ElapsedSeconds)}
		row := rows[k]
		if row == nil {
			row = &types.ContactStats{ReporterID: r.ReporterID, AccountTag: r.AccountTag, Gateway: r.Gateway,
				Model: r.Model, Criterion: r.Criterion, Source: r.Source, First: r.First, Interval: k.interval}
			rows[k] = row
		}
		switch r.Outcome {
		case "full":
			row.Full++
		case "refreshed":
			row.Refreshed++
		default:
			row.Unknown++
		}
		if r.Outcome == "full" && r.WindowFinal && r.FullWindowMS > 0 {
			row.WindowSamples++
			sums[k] += r.FullWindowMS
		}
	}
	out := make([]types.ContactStats, 0, len(rows))
	for k, row := range rows {
		if row.WindowSamples > 0 {
			row.WindowMeanMS = sums[k] / int64(row.WindowSamples)
		}
		out = append(out, *row)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		return a.Gateway+"|"+strconv.FormatInt(a.ReporterID, 10)+"|"+a.AccountTag+"|"+a.Model+"|"+a.Criterion+"|"+a.Source+"|"+a.First+"|"+a.Interval <
			b.Gateway+"|"+strconv.FormatInt(b.ReporterID, 10)+"|"+b.AccountTag+"|"+b.Model+"|"+b.Criterion+"|"+b.Source+"|"+b.First+"|"+b.Interval
	})
	return out
}
