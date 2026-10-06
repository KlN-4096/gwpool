package httpapi

import (
	"slices"
	"time"

	"gwpool/internal/types"
)

type accountActivity struct {
	WindowMinutes, Requests, HTTP200, Timed int
	MedianMS                                int64
}

func summarizeAccountActivity(id int64, samples []types.Sample, now time.Time) accountActivity {
	const window = 15 * time.Minute
	v := accountActivity{WindowMinutes: int(window / time.Minute)}
	var durations []int64
	for _, sm := range samples {
		if sm.AccountID != id || sm.At.Before(now.Add(-window)) || sm.At.After(now) {
			continue
		}
		v.Requests++
		if sm.Status == 200 {
			v.HTTP200++
		}
		if sm.DurationMS > 0 {
			durations = append(durations, sm.DurationMS)
		}
	}
	slices.Sort(durations)
	v.Timed = len(durations)
	if v.Timed > 0 {
		v.MedianMS = durations[v.Timed/2]
		if v.Timed%2 == 0 {
			lo := durations[v.Timed/2-1]
			v.MedianMS = lo + (v.MedianMS-lo)/2
		}
	}
	return v
}
