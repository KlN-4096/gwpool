package store

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"gwpool/internal/cooldown"
	"gwpool/internal/types"
)

func TestCooldownReportsPersistAndDeduplicate(t *testing.T) {
	dir := t.TempDir()
	st := open(t, dir)
	report := types.CooldownReport{ID: strings.Repeat("a", 64), AccountTag: strings.Repeat("b", 64),
		Gateway: "unified-142", WindowSeconds: 3600, ElapsedSeconds: 3601, Result: "full", ReceivedAt: time.Now().UTC()}
	if added, err := st.AddCooldownReport(report); err != nil || !added {
		t.Fatalf("写入失败：%v %v", added, err)
	}
	if added, err := st.AddCooldownReport(report); err != nil || added {
		t.Fatalf("重放应幂等：%v %v", added, err)
	}
	conflict := report
	conflict.Result = "degraded"
	if added, err := st.AddCooldownReport(conflict); err == nil || added {
		t.Fatalf("同ID不同载荷必须拒绝：%v %v", added, err)
	}
	reopened := open(t, dir)
	if got := reopened.CooldownReports(); len(got) != 1 || got[0].ID != report.ID {
		t.Fatal("重新加载丢失冷却报告")
	}
	copy := reopened.CooldownReports()
	copy[0].AccountTag = "mutated"
	if reopened.CooldownReports()[0].AccountTag == "mutated" {
		t.Fatal("泄漏可变切片")
	}
}

func TestCooldownReportsBoundEachReporterWithoutEvictingOthers(t *testing.T) {
	st := open(t, t.TempDir())
	now := time.Now().UTC()
	st.st.CooldownReports = []types.CooldownReport{{ID: "other", ReporterID: 2, ReceivedAt: now}}
	for i := 0; i < cooldown.MaxReporterReports; i++ {
		st.st.CooldownReports = append(st.st.CooldownReports,
			types.CooldownReport{ID: fmt.Sprint(i), ReporterID: 1, ReceivedAt: now})
	}
	if _, err := st.AddCooldownReport(types.CooldownReport{ID: "new", ReporterID: 1, ReceivedAt: now}); err != nil {
		t.Fatal(err)
	}
	reports := st.CooldownReports()
	if len(reports) != cooldown.MaxReporterReports+1 || reports[0].ID != "other" || reports[1].ID != "1" {
		t.Fatal("必须保留其他报告者，并只淘汰该报告者最旧样本")
	}
}
