package store

import (
	"encoding/hex"
	"errors"
	"time"

	"gwpool/internal/types"
)

// ExternalUsage is long-lived, non-authoritative telemetry. Its size never
// controls consumer admission and it is not read by stock selection.
func (s *Store) ExternalUsage() map[string]types.ExternalConsumerUsage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneMap(s.st.ExternalUsage)
}

func (s *Store) RecordExternalUsage(id, gateway string, at time.Time, delivered, limited bool) error {
	if value, err := hex.DecodeString(id); err != nil || len(value) != 32 || len(gateway) > 64 || at.IsZero() {
		return errors.New("invalid external statistics")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.ExternalUsage == nil {
		s.st.ExternalUsage = make(map[string]types.ExternalConsumerUsage)
	}
	old, exists := s.st.ExternalUsage[id]
	next := old
	if next.FirstAt.IsZero() || at.Before(next.FirstAt) {
		next.FirstAt = at
	}
	if !at.Before(next.LastAt) {
		next.LastAt, next.LastGateway = at, gateway
	}
	next.Requests++
	if delivered {
		next.Delivered++
	}
	if limited {
		next.Limited++
	}
	s.st.ExternalUsage[id] = next
	if err := s.save(stateChange{ExternalUsage: map[string]types.ExternalConsumerUsage{id: next}}); err != nil {
		if exists {
			s.st.ExternalUsage[id] = old
		} else {
			delete(s.st.ExternalUsage, id)
		}
		return err
	}
	return nil
}
