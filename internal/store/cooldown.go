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
	next := make([]types.CooldownReport, 0, len(before)+1)
	cutoff := time.Now().Add(-cooldown.Retention)
	for _, prev := range before {
		if !prev.ReceivedAt.Before(cutoff) {
			next = append(next, prev)
		}
	}
	next = append(next, report)
	// 各 consumer 单独留存最近样本，避免一个报告者挤掉其他人的全部证据。
	kept := make([]types.CooldownReport, 0, len(next))
	counts := map[int64]int{}
	for i := len(next) - 1; i >= 0; i-- {
		prev := next[i]
		if counts[prev.ReporterID] >= cooldown.MaxReporterReports {
			continue
		}
		counts[prev.ReporterID]++
		kept = append(kept, prev)
	}
	for left, right := 0, len(kept)-1; left < right; left, right = left+1, right-1 {
		kept[left], kept[right] = kept[right], kept[left]
	}
	next = kept
	if len(next) > cooldown.MaxReports {
		next = append([]types.CooldownReport(nil), next[len(next)-cooldown.MaxReports:]...)
	}
	s.st.CooldownReports = next
	if err := s.save(); err != nil {
		s.st.CooldownReports = before
		return false, err
	}
	return true, nil
}
