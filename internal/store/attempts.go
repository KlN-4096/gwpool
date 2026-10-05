package store

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gwpool/internal/geography"
	"gwpool/internal/types"
)

func attemptIdentity(account string) string {
	sum := sha256.Sum256([]byte(account))
	return hex.EncodeToString(sum[:])
}

func (s *Store) CountryAttempts(account string) map[string]types.CountryAttempt {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[string]types.CountryAttempt{}
	for country, row := range s.st.CountryAttempts[attemptIdentity(account)] {
		row.Recent = append([]time.Time(nil), row.Recent...)
		out[country] = row
	}
	return out
}

func (s *Store) AttemptUsage(account string, now time.Time) map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cut := now.Add(-time.Hour)
	used := map[string]int{}
	for country, row := range s.st.CountryAttempts[attemptIdentity(account)] {
		for _, at := range row.Recent {
			if at.After(cut) {
				used[country]++
			}
		}
	}
	return used
}

// ReserveAttempt persists before any network operation. A failed save restores
// memory and returns an error; binding/Geo/transport failures never refund it.
func (s *Store) ReserveAttempt(account, country string, perCountry, totalLimit int, at time.Time) error {
	country = geography.CountryKey(country)
	if account == "" || country == "" || perCountry <= 0 || totalLimit <= 0 || at.IsZero() {
		return errors.New("国家尝试预算参数无效")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := attemptIdentity(account)
	before := s.st.CountryAttempts[key]
	next := make(map[string]types.CountryAttempt, len(before)+1)
	cut := at.Add(-time.Hour)
	total, oldest, countryOldest := 0, time.Time{}, time.Time{}
	for code, row := range before {
		recent := make([]time.Time, 0, len(row.Recent))
		for _, sent := range row.Recent {
			if !sent.After(cut) {
				continue
			}
			recent = append(recent, sent)
			total++
			if oldest.IsZero() || sent.Before(oldest) {
				oldest = sent
			}
			if code == country && (countryOldest.IsZero() || sent.Before(countryOldest)) {
				countryOldest = sent
			}
		}
		row.Recent = recent
		next[code] = row
	}
	if total >= totalLimit {
		return &types.AttemptLimit{Region: "all", Limit: totalLimit, Wait: oldest.Add(time.Hour).Sub(at)}
	}
	row := next[country]
	if len(row.Recent) >= perCountry {
		return &types.AttemptLimit{Region: country, Limit: perCountry, Wait: countryOldest.Add(time.Hour).Sub(at)}
	}
	row.Count++
	row.LastAt = at.UTC()
	row.Recent = append(row.Recent, at.UTC())
	next[country] = row
	if s.st.CountryAttempts == nil {
		s.st.CountryAttempts = map[string]map[string]types.CountryAttempt{}
	}
	s.st.CountryAttempts[key] = next
	if err := s.save(stateChange{Attempts: map[string]map[string]types.CountryAttempt{key: next}}); err != nil {
		if before == nil {
			delete(s.st.CountryAttempts, key)
		} else {
			s.st.CountryAttempts[key] = before
		}
		return fmt.Errorf("保存国家尝试预算: %w", err)
	}
	return nil
}

// MigrateAttempts is called once before starting the production minter. Existing
// samples provide a conservative recent window, so deploying this budget does
// not grant another full hourly allowance. Old unrecorded binding failures
// cannot be reconstructed; all new paths reserve before binding.
func (s *Store) MigrateAttempts() (skipped int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.AttemptsMigrated {
		return 0, nil
	}
	file, err := os.Open(filepath.Join(s.dir, samplesFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	if file != nil {
		defer func() { _ = file.Close() }()
	}
	identities := map[int64]string{}
	for _, account := range s.st.Accounts {
		if account.Creds.AccountID != "" {
			identities[account.ID] = attemptIdentity(account.Creds.AccountID)
		}
	}
	next := map[string]map[string]types.CountryAttempt{}
	now, cut := time.Now(), time.Now().Add(-time.Hour)
	if file != nil {
		reader := bufio.NewScanner(file)
		reader.Buffer(make([]byte, 4096), 1024*1024)
		for reader.Scan() {
			var sample types.Sample
			if json.Unmarshal(reader.Bytes(), &sample) != nil {
				skipped++
				continue
			}
			key, country := identities[sample.AccountID], geography.CountryKey(sample.Requested.Region)
			if key == "" || country == "" || sample.At.IsZero() || sample.At.After(now.Add(time.Minute)) {
				continue
			}
			if next[key] == nil {
				next[key] = map[string]types.CountryAttempt{}
			}
			row := next[key][country]
			row.Count++
			if sample.At.After(row.LastAt) {
				row.LastAt = sample.At
			}
			if sample.At.After(cut) {
				row.Recent = append(row.Recent, sample.At)
			}
			next[key][country] = row
		}
		if err := reader.Err(); err != nil {
			return skipped, err
		}
	}
	before := s.st.CountryAttempts
	s.st.CountryAttempts, s.st.AttemptsMigrated = next, true
	if err := s.save(stateChange{Attempts: next, ReplaceAttempts: true, AttemptsMigrated: true}); err != nil {
		s.st.CountryAttempts, s.st.AttemptsMigrated = before, false
		return skipped, err
	}
	return skipped, nil
}
