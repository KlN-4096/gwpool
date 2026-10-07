package types

import (
	"strconv"
	"strings"
	"time"
)

const (
	AccessKeyUpload         = "upload"
	AccessKeyLegacy         = "legacy"
	AccessKeyAdmin          = "admin"
	AccessKeyBatchLimit     = 1000
	AccessKeyMaxRPM         = 600000
	AccessKeyMaxConcurrency = 10000
	AccessKeyMaxID          = 1<<31 - 1
	KeyUsageHours           = 168
	KeyUsageIPs             = 8
	KeyUsageAccounts        = 64
)

// AccessKey is management metadata, not an uploaded upstream credential.
// Only independently issued keys have a digest; their plaintext is never stored.
// Empty policy for an existing upload key means enabled and unlimited.
type AccessKey struct {
	ID          string    `json:"id"`
	Source      string    `json:"source"`
	SubjectID   int64     `json:"subject_id"`
	Digest      string    `json:"digest,omitempty"`
	CreationID  string    `json:"creation_id,omitempty"`
	Suffix      string    `json:"suffix,omitempty"`
	Label       string    `json:"label"`
	Note        string    `json:"note,omitempty"`
	Disabled    bool      `json:"disabled,omitempty"`
	RevokedAt   time.Time `json:"revoked_at,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	RPM         int       `json:"rpm,omitempty"`
	Concurrency int       `json:"concurrency,omitempty"`
}

func AccessKeyID(source string, id int64) string {
	return source + ":" + strconv.FormatInt(id, 10)
}

func ParseAccessKeyID(id string) (source string, subject int64, ok bool) {
	source, number, found := strings.Cut(id, ":")
	if !found || (source != AccessKeyUpload && source != AccessKeyLegacy && source != AccessKeyAdmin) {
		return "", 0, false
	}
	subject, err := strconv.ParseInt(number, 10, 64)
	return source, subject, err == nil && subject > 0 && AccessKeyID(source, subject) == id
}

func (k AccessKey) Usable() bool { return !k.Disabled && k.RevokedAt.IsZero() }

// A patch cannot alter provenance, subject, secret, or creation time.
type AccessKeyPatch struct {
	Action           string
	Label, Note      *string
	RPM, Concurrency *int
}

type CreatedAccessKey struct {
	Record AccessKey
	Key    string
}

// Counters describe HTTP traffic, not inference quality or token usage.
type KeyCounters struct {
	Requests     uint64            `json:"requests"`
	OK           uint64            `json:"ok"`
	ClientErrors uint64            `json:"client_errors"`
	ServerErrors uint64            `json:"server_errors"`
	Denied       uint64            `json:"denied"`
	Tickets      uint64            `json:"tickets"`
	Bytes        uint64            `json:"bytes"`
	DurationMS   uint64            `json:"duration_ms"`
	Latency      [12]uint64        `json:"latency"`
	Endpoints    map[string]uint64 `json:"endpoints,omitempty"`
}

type KeyHour struct {
	At       time.Time   `json:"at"`
	Counters KeyCounters `json:"counters"`
}

type KeyUsage struct {
	Totals         KeyCounters `json:"totals"`
	StartedAt      time.Time   `json:"started_at"`
	LastAt         time.Time   `json:"last_at"`
	LastIP         string      `json:"last_ip,omitempty"`
	RecentIPs      []string    `json:"recent_ips,omitempty"`
	AccountHashes  []string    `json:"account_hashes,omitempty"`
	AccountsCapped bool        `json:"accounts_capped,omitempty"`
	Hours          []KeyHour   `json:"hours,omitempty"`
}

func (u KeyUsage) Clone() KeyUsage {
	u.Totals = u.Totals.Clone()
	u.RecentIPs = append([]string(nil), u.RecentIPs...)
	u.AccountHashes = append([]string(nil), u.AccountHashes...)
	u.Hours = append([]KeyHour(nil), u.Hours...)
	for i := range u.Hours {
		u.Hours[i].Counters = u.Hours[i].Counters.Clone()
	}
	return u
}

func (c KeyCounters) Clone() KeyCounters {
	copy := make(map[string]uint64, len(c.Endpoints))
	for key, value := range c.Endpoints {
		copy[key] = value
	}
	c.Endpoints = copy
	return c
}
