package types

import (
	"errors"
	"time"
)

var ErrCooldownReportConflict = errors.New("cooldown report ID reused with different payload")

// CooldownReport 是消费端自报的一次“静置后再次确认”的样本，不改变池子的路由票或槽位判定。
// AccountTag 是以消费端 key 为密钥派生的匿名稳定标识，不是上游账号 ID。
type CooldownReport struct {
	ID             string    `json:"id"`
	AccountTag     string    `json:"account_tag"`
	Gateway        string    `json:"gateway"`
	WindowSeconds  int       `json:"window_seconds"`
	ElapsedSeconds int       `json:"elapsed_seconds"`
	Result         string    `json:"result"`
	ObservedAt     time.Time `json:"observed_at,omitempty"`
	ReporterID     int64     `json:"reporter_id"`
	ReceivedAt     time.Time `json:"received_at"`
}

func (r CooldownReport) EventAt() time.Time {
	if !r.ObservedAt.IsZero() {
		return r.ObservedAt
	}
	return r.ReceivedAt
}

// SameEvent 不比较服务端接收时间；网络重试自然会晚到，但载荷不能改变。
func (r CooldownReport) SameEvent(other CooldownReport) bool {
	r.ReceivedAt, other.ReceivedAt = time.Time{}, time.Time{}
	return r == other
}

type CooldownRecommendation struct {
	Seconds               int       `json:"seconds"`
	Samples               int       `json:"samples"`
	Source                string    `json:"source"` // account / pool
	ReportIntervalSeconds int       `json:"report_interval_seconds,omitempty"`
	ReportPolicyExpiresAt time.Time `json:"report_policy_expires_at,omitempty"`
}

type CooldownAccountStats struct {
	ReporterID           int64
	AccountTag           string
	Samples              int
	Successes            int
	Failures             int
	FailureFloorSeconds  int
	Recommendation       CooldownRecommendation
	RecommendationReason string
}

type CooldownStats struct {
	Gateway            string
	Samples            int
	Successes          int
	Failures           int
	Accounts           []CooldownAccountStats
	Recommendation     CooldownRecommendation
	AccountDifferences bool // 有足够样本的账号建议档位不同；不是因果结论。
}
