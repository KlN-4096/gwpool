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
		// Readers and checkpoints own copies; under mu only this row needs
		// an undo value, not a second copy of the full retained report table.
		s.st.ContactReports[i] = merged
		if err := s.save(stateChange{Contacts: []types.ContactReport{merged}}); err != nil {
			s.st.ContactReports[i] = prev
			return false, err
		}
		return false, nil
	}
	kept := make([]types.ContactReport, 0, min(len(before)+1, contact.MaxReports))
	counts := map[int64]int{}
	cutoff := time.Now().Add(-contact.Retention)
	var updates []types.ContactReport
	var dropped []reportKey
	// The newly received contact is also subject to retention. Keep the old
	// added=true return contract even when that new observation is already old.
	if !report.At.Before(cutoff) {
		kept = append(kept, report)
		counts[report.ReporterID]++
		updates = []types.ContactReport{report}
	}
	for i := len(before) - 1; i >= 0; i-- {
		r := before[i]
		if len(kept) >= contact.MaxReports || r.At.Before(cutoff) ||
			counts[r.ReporterID] >= contact.MaxReporterReports {
			dropped = append(dropped, contactKey(r))
			continue
		}
		counts[r.ReporterID]++
		kept = append(kept, r)
	}
	for i, j := 0, len(kept)-1; i < j; i, j = i+1, j-1 {
		kept[i], kept[j] = kept[j], kept[i]
	}
	s.st.ContactReports = kept
	if err := s.save(stateChange{Contacts: updates, DropContacts: dropped}); err != nil {
		s.st.ContactReports = before
		return false, err
	}
	return true, nil
}
