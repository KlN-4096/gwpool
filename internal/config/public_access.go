package config

import "fmt"

// Initial sizing: five times the observed whole-site 141 requests/minute,
// rounded to 720 and shared by all public users, not multiplied by user count.
// Adjustable admission defaults, not an upstream-supply guarantee.
const (
	DefaultPublicRequestsPerMinute = 720
	DefaultPublicMaxConcurrent     = 10
	DefaultPublicMaxQueued         = 50
	DefaultPublicQueueWaitMS       = 1000

	MaxPublicRequestsPerMinute = 100000
	MaxPublicConcurrent        = 1000
	MaxPublicQueued            = 10000
	MaxPublicQueueWaitMS       = 1000 // sub2's catalog timeout is normally 2s.
)

func (p PublicWhenSurplus) validateLimits() error {
	for _, field := range []struct {
		name       string
		value, min int
		max        int
	}{
		{"requests_per_minute", p.RequestsPerMinute, 1, MaxPublicRequestsPerMinute},
		{"max_concurrent", p.MaxConcurrent, 1, MaxPublicConcurrent},
		{"max_queued", p.MaxQueued, 0, MaxPublicQueued},
		{"queue_wait_ms", p.QueueWaitMS, 0, MaxPublicQueueWaitMS},
	} {
		if field.value < field.min || field.value > field.max {
			return fmt.Errorf("public_when_surplus.%s 必须在 %d..%d 之间",
				field.name, field.min, field.max)
		}
	}
	return nil
}
