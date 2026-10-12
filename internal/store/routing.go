package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gwpool/internal/routing"
	"gwpool/internal/types"
)

const (
	maxRoutingHistoryBytes = 16 << 20
	maxRoutingLineBytes    = 64 << 10
	routingLoadRetryGap    = time.Minute
)

// This cache is only an optimization over the append-only sample file, not new
// durable state. Keep compact whitelisted fields; no IP, error body or tokens.
type routingHistoryEntry struct {
	account   int64
	country   string
	gateway   string
	requested types.Exit
	at        time.Time
}

type routingHistoryCache struct {
	rows         []routingHistoryEntry
	next         int
	clipped      bool
	skippedLines int
	revision     uint64
}

func (c *routingHistoryCache) append(sample types.Sample) {
	if !routing.BareSample(sample) {
		return
	}
	row := routingHistoryEntry{account: sample.AccountID, country: sample.Observed.Country,
		gateway: sample.Gateway, requested: sample.Requested, at: sample.At}
	c.appendEntry(row)
}

func (c *routingHistoryCache) appendEntry(row routingHistoryEntry) {
	c.revision++
	if len(c.rows) < routing.HistoryLimit {
		c.rows = append(c.rows, row)
		return
	}
	c.rows[c.next] = row
	c.next = (c.next + 1) % routing.HistoryLimit
}

// RoutingObservations loads a bounded tail once without holding the state lock.
// A bounded pending ring reconciles appends after the captured file size.
func (s *Store) RoutingObservations(now time.Time) (routing.History, error) {
	loaded, err := s.ensureRoutingHistory(now)
	if err != nil {
		return routing.History{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out, _, _ := s.routingObservationsLocked(now)
	return routing.History{Observations: out, Loaded: loaded, Clipped: s.routingHistory.clipped,
		SkippedLines: s.routingHistory.skippedLines}, nil
}

// Caller holds Store.mu. The boundaries capture future admission and strict
// retention expiry, including eligible rows outside the current window.
func (s *Store) routingObservationsLocked(now time.Time) ([]routing.Observation, time.Time, time.Time) {
	type donor struct {
		key     string
		created time.Time
	}
	accounts := make(map[int64]donor, len(s.st.Accounts))
	for _, account := range s.st.Accounts {
		if account.Creds.AccountID != "" {
			accounts[account.ID] = donor{routing.DonorKey(account.Creds.AccountID), account.CreatedAt}
		}
	}
	cutoff := now.Add(-routing.HistoryRetention)
	out := make([]routing.Observation, 0, len(s.routingHistory.rows))
	var from, until time.Time
	boundary := func(at time.Time) {
		if at.After(now) {
			if until.IsZero() || at.Before(until) {
				until = at
			}
		} else if from.IsZero() || at.After(from) {
			from = at
		}
	}
	for _, row := range s.routingHistory.rows {
		account, ok := accounts[row.account]
		// Local IDs must not silently join history to a different current
		// identity. Deleted accounts and samples predating creation abstain.
		if !ok || row.at.Before(account.created) {
			continue
		}
		boundary(row.at)
		boundary(row.at.Add(routing.HistoryRetention + time.Nanosecond))
		if row.at.Before(cutoff) || row.at.After(now) {
			continue
		}
		out = append(out, routing.Observation{Donor: account.key,
			Country: row.country, Gateway: row.gateway, Requested: row.requested, At: row.at})
	}
	return out, from, until
}

func (s *Store) ensureRoutingHistory(now time.Time) (bool, error) {
	s.routingLoadMu.Lock()
	defer s.routingLoadMu.Unlock()
	s.mu.RLock()
	ready := s.routingHistory != nil
	s.mu.RUnlock()
	if ready {
		return false, nil
	}
	if s.routingLoadErr != nil && now.Before(s.routingRetryAt) {
		return false, s.routingLoadErr
	}
	f, err := os.Open(filepath.Join(s.dir, samplesFile))
	if err != nil {
		return s.routingLoadFailed(now, err)
	}
	defer func() { _ = f.Close() }()
	if err := s.loadRoutingSnapshot(f); err != nil {
		return s.routingLoadFailed(now, err)
	}
	s.routingLoadErr, s.routingRetryAt = nil, time.Time{}
	return true, nil
}

type routingSnapshotFile interface {
	io.ReaderAt
	Stat() (os.FileInfo, error)
}

// Caller holds routingLoadMu, so only one snapshot can own routingPending.
func (s *Store) loadRoutingSnapshot(f routingSnapshotFile) error {
	// Capture the boundary and start buffering appends in one short critical
	// section. The expensive read/JSON decoding below never holds Store.mu.
	s.mu.Lock()
	stat, err := f.Stat()
	if err == nil {
		s.routingPending = &routingHistoryCache{}
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	cache, err := readRoutingHistory(f, stat.Size())
	s.mu.Lock()
	if err == nil {
		pending := s.routingPending
		start := 0
		if len(pending.rows) == routing.HistoryLimit {
			start = pending.next
		}
		for i := range pending.rows {
			cache.appendEntry(pending.rows[(start+i)%len(pending.rows)])
		}
		cache.skippedLines += pending.skippedLines
		s.routingHistory = cache
	}
	s.routingPending = nil
	s.mu.Unlock()
	return err
}

// Caller holds routingLoadMu. Surface I/O errors, but do not rescan a broken
// file on every request-driven maintenance wake.
func (s *Store) routingLoadFailed(now time.Time, err error) (bool, error) {
	s.routingLoadErr = fmt.Errorf("读取有界选路历史: %w", err)
	s.routingRetryAt = now.Add(routingLoadRetryGap)
	return false, s.routingLoadErr
}

func readRoutingHistory(f io.ReaderAt, size int64) (*routingHistoryCache, error) {
	start := max(int64(0), size-maxRoutingHistoryBytes)
	cache := &routingHistoryCache{clipped: start > 0}
	reader := bufio.NewReaderSize(io.NewSectionReader(f, start, size-start), maxRoutingLineBytes)
	skipFirst, oversized := start > 0, false
	for {
		line, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			oversized = true // Discard fragments without allocating a huge line.
			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		switch {
		case skipFirst:
			skipFirst = false // The tail may begin inside a record.
		case oversized:
			cache.skippedLines++
		case len(bytes.TrimSpace(line)) > 0:
			var sample types.Sample
			if json.Unmarshal(line, &sample) == nil {
				cache.append(sample)
			} else {
				cache.skippedLines++
			}
		}
		oversized = false
		if errors.Is(err, io.EOF) {
			return cache, nil
		}
	}
}
