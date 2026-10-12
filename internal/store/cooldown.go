package store

import (
	"time"

	"gwpool/internal/cooldown"
	"gwpool/internal/types"
)

func (s *Store) CooldownReports() []types.CooldownReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]types.CooldownReport(nil), s.st.CooldownReports...)
}

// AddCooldownReport 同一报告者的事件 ID 幂等；写失败恢复旧内存，不假报持久化成功。
func (s *Store) AddCooldownReport(report types.CooldownReport) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, prev := range s.st.CooldownReports {
		if prev.ReporterID == report.ReporterID && prev.ID == report.ID {
			if !prev.SameEvent(report) {
				return false, types.ErrCooldownReportConflict
			}
			return false, nil
		}
	}
	before := s.st.CooldownReports
	kept := make([]types.CooldownReport, 0, min(len(before)+1, cooldown.MaxReports))
	cutoff := time.Now().Add(-cooldown.Retention)
	// 各 consumer 单独留存最近样本，避免一个报告者挤掉其他人的全部证据。
	// Unlike contact observations, a new cooldown report is retained even if
	// already old; only existing rows are age-pruned by this mutation.
	kept = append(kept, report)
	counts := map[int64]int{report.ReporterID: 1}
	var dropped []reportKey
	for i := len(before) - 1; i >= 0; i-- {
		prev := before[i]
		if len(kept) >= cooldown.MaxReports || prev.EventAt().Before(cutoff) ||
			counts[prev.ReporterID] >= cooldown.MaxReporterReports {
			dropped = append(dropped, cooldownKey(prev))
			continue
		}
		counts[prev.ReporterID]++
		kept = append(kept, prev)
	}
	for left, right := 0, len(kept)-1; left < right; left, right = left+1, right-1 {
		kept[left], kept[right] = kept[right], kept[left]
	}
	s.st.CooldownReports = kept
	if err := s.save(stateChange{Cooldowns: []types.CooldownReport{report}, DropCooldowns: dropped}); err != nil {
		s.st.CooldownReports = before
		return false, err
	}
	return true, nil
}
