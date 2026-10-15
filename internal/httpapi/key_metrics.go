package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"gwpool/internal/types"
)

const keyMetricsFlushInterval = 30 * time.Second
const keyMetricsCapacity = 20000
const keyRecentSeconds = 300

var keyLatencyBounds = [...]int64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 30000}

type keySecond struct {
	at    int64
	count uint64
}
type keyMetricEntry struct {
	usage   types.KeyUsage
	recent  [keyRecentSeconds]keySecond
	active  int
	version uint64
	dirty   bool
}
type keyAdmissionEntry struct {
	recent []time.Time
	active int
}
type keyMetrics struct {
	mu               sync.Mutex
	flushMu          sync.Mutex
	entries          map[string]*keyMetricEntry
	admissions       map[string]*keyAdmissionEntry
	saveError        bool
	capacityExceeded bool
}
type keyMetricView struct {
	Usage            types.KeyUsage
	RPM, FiveMinutes uint64
	Active           int
}

func (s *Server) initKeyMetrics() {
	s.keyMetrics.entries = make(map[string]*keyMetricEntry)
	s.keyMetrics.admissions = make(map[string]*keyAdmissionEntry)
	if store, ok := s.st.(accessKeyStore); ok {
		for id, usage := range store.AccessKeyUsage() {
			s.keyMetrics.entries[id] = &keyMetricEntry{usage: usage}
		}
	}
}

func (m *keyMetrics) begin(key types.AccessKey, authorized bool, now time.Time) (bool, time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[key.ID]
	if e == nil {
		if len(m.entries) >= keyMetricsCapacity {
			m.capacityExceeded = true
		} else {
			e = &keyMetricEntry{}
			m.entries[key.ID] = e
		}
	}
	if e != nil {
		second := now.Unix()
		b := &e.recent[second%keyRecentSeconds]
		if b.at != second {
			*b = keySecond{at: second}
		}
		b.count++
	}
	if !authorized {
		return false, 0
	}
	// AdminKey, upload and grandfathered keys skip independent admission,
	// but retain telemetry and the same authorization/revocation checks.
	if key.Source == types.AccessKeyAdmin && !key.IsAdminKey() {
		// Independent admission has its own bounded namespace (at most 10,000
		// issued keys), so exhausted telemetry never denies or bypasses a limit.
		a := m.admissions[key.ID]
		if a == nil {
			a = &keyAdmissionEntry{}
			m.admissions[key.ID] = a
		}
		n := 0
		for n < len(a.recent) && !a.recent[n].After(now.Add(-time.Minute)) {
			n++
		}
		if n > 0 {
			a.recent = append(a.recent[:0], a.recent[n:]...)
		}
		if key.RPM > 0 && len(a.recent) >= key.RPM {
			return false, max(time.Second, a.recent[len(a.recent)-key.RPM].Add(time.Minute).Sub(now))
		}
		if key.Concurrency > 0 && a.active >= key.Concurrency {
			return false, time.Second
		}
		if key.RPM > 0 {
			a.recent = append(a.recent, now)
		}
		a.active++
	}
	if e != nil {
		e.active++
	}
	return true, 0
}

func addKeyCounter(c *types.KeyCounters, path string, status, size, tickets int, duration int64, denied bool) {
	c.Requests++
	switch {
	case status >= 500:
		c.ServerErrors++
	case status >= 400:
		c.ClientErrors++
	case status >= 200 && status < 300:
		c.OK++
	}
	if denied {
		c.Denied++
	}
	c.Bytes += uint64(max(size, 0))
	c.Tickets += uint64(max(tickets, 0))
	c.DurationMS += uint64(max(duration, 0))
	bucket := len(keyLatencyBounds)
	for i, bound := range keyLatencyBounds {
		if duration <= bound {
			bucket = i
			break
		}
	}
	c.Latency[bucket]++
	if c.Endpoints == nil {
		c.Endpoints = map[string]uint64{}
	}
	c.Endpoints[path]++
}

func (m *keyMetrics) finish(id string, r *http.Request, started time.Time, status, size, tickets int, admitted bool) {
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[id]
	if a := m.admissions[id]; a != nil && admitted {
		a.active--
	}
	if e == nil {
		return
	}
	if admitted {
		e.active--
	}
	u := &e.usage
	if u.StartedAt.IsZero() {
		u.StartedAt = started.UTC()
	}
	if now.After(u.LastAt) {
		u.LastAt = now
	}
	duration := now.Sub(started).Milliseconds()
	addKeyCounter(&u.Totals, r.URL.Path, status, size, tickets, duration, !admitted)
	cutoff := now.Truncate(time.Hour).Add(-time.Duration(types.KeyUsageHours-1) * time.Hour)
	u.Hours = slices.DeleteFunc(u.Hours, func(h types.KeyHour) bool { return h.At.Before(cutoff) })
	hour := now.Truncate(time.Hour)
	i := slices.IndexFunc(u.Hours, func(h types.KeyHour) bool { return h.At.Equal(hour) })
	if i < 0 {
		u.Hours = append(u.Hours, types.KeyHour{At: hour})
		i = len(u.Hours) - 1
	}
	addKeyCounter(&u.Hours[i].Counters, r.URL.Path, status, size, tickets, duration, !admitted)
	// RemoteAddr is the TCP peer, not a spoofable forwarding header.
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && net.ParseIP(host) != nil {
		u.LastIP = host
		u.RecentIPs = slices.DeleteFunc(u.RecentIPs, func(ip string) bool { return ip == host })
		u.RecentIPs = append([]string{host}, u.RecentIPs...)
		if len(u.RecentIPs) > types.KeyUsageIPs {
			u.RecentIPs = u.RecentIPs[:types.KeyUsageIPs]
		}
	}
	if ref := strings.TrimSpace(r.URL.Query().Get("account")); ref != "" && len(ref) <= 128 {
		sum := sha256.Sum256([]byte(id + "\x00" + ref))
		hash := hex.EncodeToString(sum[:])
		if !slices.Contains(u.AccountHashes, hash) {
			if len(u.AccountHashes) < types.KeyUsageAccounts {
				u.AccountHashes = append(u.AccountHashes, hash)
			} else {
				u.AccountsCapped = true
			}
		}
	}
	e.version++
	e.dirty = true
}

func (m *keyMetrics) snapshot(now time.Time) (map[string]keyMetricView, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]keyMetricView, len(m.entries))
	for id, e := range m.entries {
		v := keyMetricView{Usage: e.usage.Clone(), Active: e.active}
		for _, b := range e.recent {
			if b.at <= now.Unix() && b.at > now.Unix()-keyRecentSeconds {
				v.FiveMinutes += b.count
			}
			if b.at <= now.Unix() && b.at > now.Unix()-60 {
				v.RPM += b.count
			}
		}
		out[id] = v
	}
	for id, a := range m.admissions {
		v := out[id]
		v.Active = a.active
		out[id] = v
	}
	return out, m.saveError || m.capacityExceeded
}

// FlushKeyMetrics serializes saves; no metrics lock is held during disk I/O.
func (s *Server) FlushKeyMetrics() error {
	store, ok := s.st.(accessKeyStore)
	if !ok {
		return nil
	}
	m := &s.keyMetrics
	m.flushMu.Lock()
	defer m.flushMu.Unlock()
	values := map[string]types.KeyUsage{}
	versions := map[string]uint64{}
	m.mu.Lock()
	for id, e := range m.entries {
		if e.dirty {
			values[id], versions[id] = e.usage.Clone(), e.version
		}
	}
	m.mu.Unlock()
	if len(values) == 0 {
		return nil
	}
	err := store.SaveAccessKeyUsage(values)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saveError = err != nil
	if err == nil {
		for id, version := range versions {
			if e := m.entries[id]; e.version == version {
				e.dirty = false
			}
		}
	}
	return err
}

// RunKeyMetrics is owned by the application lifecycle; New starts no goroutine.
func (s *Server) RunKeyMetrics(ctx context.Context) {
	ticker := time.NewTicker(keyMetricsFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.FlushKeyMetrics(); err != nil {
				s.opt.Log.Error("Key 统计持久化失败", "err", err)
			}
		}
	}
}
