//go:build linux

package store

import (
	"encoding/json"
	"os"
	"testing"
)

// Independently reopen the real benchmark WAL and check every successfully
// acknowledged report against a ledger built by the separate load generator.
func TestPerformanceDatasetRecovery(t *testing.T) {
	dir, ledger := os.Getenv("GWPOOL_RECOVERY_DATA"), os.Getenv("GWPOOL_RECOVERY_LEDGER")
	if dir == "" || ledger == "" {
		t.Skip("explicit completed offline benchmark data and acknowledgement ledger required")
	}
	raw, err := os.ReadFile(ledger)
	if err != nil {
		t.Fatal(err)
	}
	var expected []struct {
		Kind        string `json:"kind"`
		ID          string `json:"id"`
		Tag         string `json:"tag"`
		WindowFinal bool   `json:"window_final"`
	}
	if err := json.Unmarshal(raw, &expected); err != nil || len(expected) == 0 {
		t.Fatal("invalid acknowledgement ledger", err)
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	contacts, cooldowns := s.ContactReports(), s.CooldownReports()
	for _, item := range expected {
		found := false
		switch item.Kind {
		case "contact":
			for _, row := range contacts {
				if row.ID == item.ID && row.AccountTag == item.Tag {
					found = row.WindowFinal == item.WindowFinal
					break
				}
			}
		case "cooldown":
			for _, row := range cooldowns {
				if row.ID == item.ID && row.AccountTag == item.Tag {
					found = true
					break
				}
			}
		default:
			t.Fatal("unknown ledger kind")
		}
		if !found {
			t.Fatalf("acknowledged %s report %s did not survive WAL replay", item.Kind, item.ID)
		}
	}
	t.Logf("replayed and verified %d acknowledged report identities", len(expected))
}
