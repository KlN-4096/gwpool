package types

import (
	"errors"
	"time"
)

var ErrContactReportConflict = errors.New("contact ID reused with different observation")

// ContactObservation is one local contact round, not every request using its ticket.
// tracked_first means first since local tracking began, never a global first-contact claim.
type ContactObservation struct {
	ID             string    `json:"id"`
	AccountTag     string    `json:"account_tag"`
	Gateway        string    `json:"gateway"`
	Model          string    `json:"model"`
	Criterion      string    `json:"criterion"`
	Source         string    `json:"source"`
	First          string    `json:"first"`
	At             time.Time `json:"at"`
	GapKnown       bool      `json:"gap_known"`
	ElapsedSeconds int64     `json:"elapsed_seconds"`
	Outcome        string    `json:"outcome"` // full / refreshed / unknown
	FullWindowMS   int64     `json:"full_window_ms"`
	WindowFinal    bool      `json:"window_final"`
}

type ContactReport struct {
	ContactObservation
	ReporterID int64     `json:"reporter_id"`
	ReceivedAt time.Time `json:"received_at"`
}

// Merge permits only a later completed-window measurement. Reordered initial
// observations cannot erase it, and never count as a second contact.
func (r ContactReport) Merge(next ContactReport) (ContactReport, error) {
	left, right := r, next
	left.ReceivedAt, right.ReceivedAt = time.Time{}, time.Time{}
	left.FullWindowMS, right.FullWindowMS = 0, 0
	left.WindowFinal, right.WindowFinal = false, false
	if left != right {
		return r, ErrContactReportConflict
	}
	if r.WindowFinal && next.WindowFinal && r.FullWindowMS != next.FullWindowMS {
		return r, ErrContactReportConflict
	}
	if next.WindowFinal {
		r.FullWindowMS, r.WindowFinal = next.FullWindowMS, true
	}
	return r, nil
}

type ContactStats struct {
	ReporterID    int64  `json:"-"`
	AccountTag    string `json:"-"`
	Gateway       string `json:"gateway"`
	Model         string `json:"model"`
	Criterion     string `json:"criterion"`
	Source        string `json:"source"`
	First         string `json:"first"`
	Interval      string `json:"interval"`
	Full          int    `json:"full"`
	Refreshed     int    `json:"refreshed"`
	Unknown       int    `json:"unknown"`
	WindowSamples int    `json:"window_samples"`
	WindowMeanMS  int64  `json:"window_mean_ms"`
}

type GatewayPriority struct {
	Model   string `json:"model"`
	Full    int    `json:"full"`
	Samples int    `json:"samples"`
}
