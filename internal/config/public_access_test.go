package config

import (
	"fmt"
	"testing"
)

func TestPublicAccessLimitsDefaultsAndValidation(t *testing.T) {
	for _, public := range []string{`{}`, `{"enabled":true,"min_accounts":3}`} {
		c, err := Load(writeCfg(t, `{`+okProxies+`,"public_when_surplus":`+public+`}`))
		if err != nil {
			t.Fatal(err)
		}
		p := c.PublicWhenSurplus
		if p.RequestsPerMinute != 720 || p.MaxConcurrent != 10 || p.MaxQueued != 50 || p.QueueWaitMS != 1000 {
			t.Fatalf("legacy config lost defaults: %+v", p)
		}
	}
	for field, values := range map[string][]int{
		"requests_per_minute": {0, -1, MaxPublicRequestsPerMinute + 1},
		"max_concurrent":      {0, -1, MaxPublicConcurrent + 1},
		"max_queued":          {-1, MaxPublicQueued + 1},
		"queue_wait_ms":       {-1, MaxPublicQueueWaitMS + 1},
	} {
		for _, value := range values {
			body := fmt.Sprintf(`{%s,"public_when_surplus":{%q:%d}}`, okProxies, field, value)
			if _, err := Load(writeCfg(t, body)); err == nil {
				t.Errorf("accepted %s=%d", field, value)
			}
		}
	}
	c, err := Load(writeCfg(t, `{`+okProxies+`,"public_when_surplus":{"requests_per_minute":17,"max_concurrent":2,"max_queued":0,"queue_wait_ms":0}}`))
	if err != nil {
		t.Fatal(err)
	}
	if p := c.PublicWhenSurplus; p.RequestsPerMinute != 17 || p.MaxConcurrent != 2 || p.MaxQueued != 0 || p.QueueWaitMS != 0 {
		t.Fatalf("explicit limits lost: %+v", p)
	}
}
