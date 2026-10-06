package httpapi

import (
	"gwpool/internal/types"
	"testing"
	"time"
)

func TestAccountActivityScopeAndWindow(t *testing.T) {
	now := time.Now()
	samples := []types.Sample{
		{AccountID: 1, At: now, Status: 200, DurationMS: 100},
		{AccountID: 1, At: now.Add(-time.Minute), Status: 429, DurationMS: 300},
		{AccountID: 1, At: now, Status: 200},
		{AccountID: 2, At: now, Status: 200, DurationMS: 900},
		{AccountID: 1, At: now.Add(-16 * time.Minute), Status: 200, DurationMS: 900},
		{AccountID: 1, At: now.Add(time.Second), Status: 200, DurationMS: 900},
	}
	got := summarizeAccountActivity(1, samples, now)
	if got.Requests != 3 || got.HTTP200 != 2 || got.Timed != 2 || got.MedianMS != 200 {
		t.Fatalf("unexpected aggregate %+v", got)
	}
	if empty := summarizeAccountActivity(3, samples, now); empty.Requests != 0 || empty.Timed != 0 {
		t.Fatal("empty scope not empty")
	}
}
