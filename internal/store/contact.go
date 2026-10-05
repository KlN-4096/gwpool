package store

import (
	"gwpool/internal/contact"
	"gwpool/internal/types"
	"time"
)

func (s *Store) ContactReports() []types.ContactReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]types.ContactReport(nil), s.st.ContactReports...)
}

func (s *Store) AddContactReport(report types.ContactReport) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	before := s.st.ContactReports
	for i, prev := range before {
		if prev.ReporterID != report.ReporterID || prev.ID != report.ID {
			continue
		}
		merged, err := prev.Merge(report)
		if err != nil {
			return false, err
		}
		if merged == prev {
			return false, nil
		}
		s.st.ContactReports = append([]types.ContactReport(nil), before...)
		s.st.ContactReports[i] = merged
		if err := s.save(stateChange{Contacts: []types.ContactReport{merged}}); err != nil {
			s.st.ContactReports = before
			return false, err
		}
		return false, nil
	}
	next := append(append([]types.ContactReport(nil), before...), report)
	kept := make([]types.ContactReport, 0, len(next))
	counts := map[int64]int{}
	cutoff := time.Now().Add(-contact.Retention)
	for i := len(next) - 1; i >= 0; i-- {
		r := next[i]
		if r.At.Before(cutoff) || counts[r.ReporterID] >= contact.MaxReporterReports {
			continue
		}
		counts[r.ReporterID]++
		kept = append(kept, r)
		if len(kept) == contact.MaxReports {
			break
		}
	}
	for i, j := 0, len(kept)-1; i < j; i, j = i+1, j-1 {
		kept[i], kept[j] = kept[j], kept[i]
	}
	s.st.ContactReports = kept
	updates, dropped := diffReports(before, kept, contactKey)
	if err := s.save(stateChange{Contacts: updates, DropContacts: dropped}); err != nil {
		s.st.ContactReports = before
		return false, err
	}
	return true, nil
}
