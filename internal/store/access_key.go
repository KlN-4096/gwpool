package store

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"gwpool/internal/types"
)

const (
	maxAccessKeys       = 20000 // includes retained upload/legacy policies and usage
	maxIssuedAccessKeys = 10000
	// Reserve a bounded reporter namespace separately from public/owner keys.
	maxIssuedAccessKeyID = types.AccessKeyMaxID
)

func validKeyText(value string, max int, multiline bool) bool {
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > max {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) && (!multiline || (r != '\n' && r != '\t')) {
			return false
		}
	}
	return true
}

func keyLimitsValid(rpm, concurrency int) bool {
	return rpm >= 0 && rpm <= types.AccessKeyMaxRPM &&
		concurrency >= 0 && concurrency <= types.AccessKeyMaxConcurrency
}

func (s *Store) AccessKeys() []types.AccessKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]types.AccessKey, 0, len(s.st.AccessKeys))
	for _, key := range s.st.AccessKeys {
		out = append(out, key)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *Store) AccessKey(id string) (types.AccessKey, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.accessKeyLocked(id)
}

func (s *Store) accessKeyLocked(id string) (types.AccessKey, bool) {
	if key, ok := s.st.AccessKeys[id]; ok {
		return key, true
	}
	source, subject, ok := types.ParseAccessKeyID(id)
	if !ok {
		return types.AccessKey{}, false
	}
	switch source {
	case types.AccessKeyUpload:
		for _, owner := range s.st.Owners {
			if owner.ID == subject {
				return types.AccessKey{ID: id, Source: source, SubjectID: subject,
					Label: owner.Label, CreatedAt: owner.CreatedAt}, true
			}
		}
	case types.AccessKeyLegacy:
		for _, account := range s.st.Accounts {
			if account.ID == subject && account.LegacyConsumerKey {
				return types.AccessKey{ID: id, Source: source, SubjectID: subject,
					Label: account.Label, CreatedAt: account.CreatedAt}, true
			}
		}
	}
	return types.AccessKey{}, false
}

// ResolveIssuedAccessKey returns recognized disabled/revoked keys as well:
// authentication and management policy are deliberately separate so rejected
// calls can be attributed without trusting a user-supplied ID alone.
func (s *Store) ResolveIssuedAccessKey(secret string) (types.AccessKey, bool) {
	body, ok := strings.CutPrefix(secret, "gwp_k_")
	if !ok || len(secret) > 128 {
		return types.AccessKey{}, false
	}
	number, _, ok := strings.Cut(body, "_")
	if !ok {
		return types.AccessKey{}, false
	}
	id, err := strconv.ParseInt(number, 36, 64)
	if err != nil || id <= 0 || id > maxIssuedAccessKeyID {
		return types.AccessKey{}, false
	}
	s.mu.RLock()
	key, found := s.st.AccessKeys[types.AccessKeyID(types.AccessKeyAdmin, id)]
	s.mu.RUnlock()
	sum := sha256.Sum256([]byte(secret))
	digest := hex.EncodeToString(sum[:])
	return key, found && key.Source == types.AccessKeyAdmin && key.Digest != "" &&
		subtle.ConstantTimeCompare([]byte(key.Digest), []byte(digest)) == 1
}

func (s *Store) CreateAccessKeys(count int, label, note string, rpm, concurrency int) ([]types.CreatedAccessKey, error) {
	return s.createAccessKeys("", count, label, note, rpm, concurrency, false)
}

// CreateAccessKeysOnce records only the request hash, never the form token or
// plaintext secrets. Repeated POSTs cannot mint another batch, even after restart.
func (s *Store) CreateAccessKeysOnce(requestID string, count int, label, note string, rpm, concurrency int) ([]types.CreatedAccessKey, error) {
	return s.createAccessKeysOnce(requestID, count, label, note, rpm, concurrency, false)
}

// AdminKey is a distinct, explicitly selected consumer privilege. Existing keys
// are never promoted by setting ordinary RPM/concurrency limits to zero.
func (s *Store) CreateAdminKeysOnce(requestID string, count int, label, note string) ([]types.CreatedAccessKey, error) {
	return s.createAccessKeysOnce(requestID, count, label, note, 0, 0, true)
}

func (s *Store) createAccessKeysOnce(requestID string, count int, label, note string, rpm, concurrency int, adminKey bool) ([]types.CreatedAccessKey, error) {
	decoded, err := hex.DecodeString(requestID)
	if err != nil || len(decoded) != sha256.Size {
		return nil, errors.New("invalid creation request")
	}
	return s.createAccessKeys(requestID, count, label, note, rpm, concurrency, adminKey)
}

func (s *Store) createAccessKeys(requestID string, count int, label, note string, rpm, concurrency int, adminKey bool) ([]types.CreatedAccessKey, error) {
	label, note = strings.TrimSpace(label), strings.TrimSpace(note)
	if count < 1 || count > types.AccessKeyBatchLimit || !validKeyText(label, 80, false) ||
		!validKeyText(note, 1000, true) || !keyLimitsValid(rpm, concurrency) {
		return nil, errors.New("invalid key count, label, note, or limits")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	issued := 0
	for _, key := range s.st.AccessKeys {
		if requestID != "" && key.CreationID == requestID {
			return nil, errors.New("this creation request was already completed")
		}
		if key.Source == types.AccessKeyAdmin {
			issued++
			s.st.NextAccessKeyID = max(s.st.NextAccessKeyID, key.SubjectID)
		}
	}
	if issued+count > maxIssuedAccessKeys || len(s.st.AccessKeys)+count > maxAccessKeys ||
		s.st.NextAccessKeyID > maxIssuedAccessKeyID-int64(count) {
		return nil, errors.New("access key capacity reached")
	}
	before := s.st.AccessKeys
	next := cloneMap(before)
	if next == nil {
		next = map[string]types.AccessKey{}
	}
	changes := make(map[string]types.AccessKey, count)
	created := make([]types.CreatedAccessKey, 0, count)
	now := time.Now().UTC()
	for i := 0; i < count; i++ {
		id := bumpID(&s.st.NextAccessKeyID, 0)
		nonce := make([]byte, 32)
		if _, err := rand.Read(nonce); err != nil {
			return nil, fmt.Errorf("generate access key: %w", err)
		}
		secret := "gwp_k_" + strconv.FormatInt(id, 36) + "_" + base64.RawURLEncoding.EncodeToString(nonce)
		sum := sha256.Sum256([]byte(secret))
		name := label
		if name == "" {
			name = "接入 Key"
			if adminKey {
				name = "AdminKey"
			}
		}
		if count > 1 {
			name += fmt.Sprintf(" %d", i+1)
		}
		key := types.AccessKey{ID: types.AccessKeyID(types.AccessKeyAdmin, id),
			Source: types.AccessKeyAdmin, SubjectID: id, Digest: hex.EncodeToString(sum[:]),
			CreationID: requestID,
			Suffix:     secret[len(secret)-6:], Label: name, Note: note, CreatedAt: now, UpdatedAt: now,
			RPM: rpm, Concurrency: concurrency, AdminKey: adminKey}
		next[key.ID], changes[key.ID] = key, key
		created = append(created, types.CreatedAccessKey{Record: key, Key: secret})
	}
	s.st.AccessKeys = next
	if err := s.save(stateChange{AccessKeys: changes, NextAccessKeyID: s.st.NextAccessKeyID}); err != nil {
		s.st.AccessKeys = before
		return nil, err
	}
	return created, nil
}

func (s *Store) UpdateAccessKeys(ids []string, patch types.AccessKeyPatch) error {
	if len(ids) == 0 || len(ids) > types.AccessKeyBatchLimit {
		return errors.New("invalid key selection")
	}
	if patch.Action != "enable" && patch.Action != "disable" && patch.Action != "revoke" && patch.Action != "edit" {
		return errors.New("invalid key action")
	}
	if (patch.Label != nil && !validKeyText(*patch.Label, 100, false)) ||
		(patch.Note != nil && !validKeyText(*patch.Note, 1000, true)) {
		return errors.New("invalid key label or note")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	changes := make(map[string]types.AccessKey, len(ids))
	now := time.Now().UTC()
	for _, id := range ids {
		key, ok := s.accessKeyLocked(id)
		if !ok {
			return errors.New("selected key no longer exists")
		}
		if !key.RevokedAt.IsZero() && patch.Action != "revoke" {
			return errors.New("revoked keys cannot be modified or re-enabled")
		}
		// This is a server-side invariant, not merely a disabled UI control.
		if key.Source != types.AccessKeyAdmin && (patch.RPM != nil || patch.Concurrency != nil) {
			return errors.New("upload keys do not support independent limits")
		}
		if patch.RPM != nil {
			key.RPM = *patch.RPM
		}
		if patch.Concurrency != nil {
			key.Concurrency = *patch.Concurrency
		}
		if !keyLimitsValid(key.RPM, key.Concurrency) {
			return errors.New("invalid key limits")
		}
		if key.IsAdminKey() && (key.RPM != 0 || key.Concurrency != 0) {
			return errors.New("AdminKey does not accept consumer limits")
		}
		if patch.Label != nil {
			key.Label = strings.TrimSpace(*patch.Label)
		}
		if patch.Note != nil {
			key.Note = strings.TrimSpace(*patch.Note)
		}
		switch patch.Action {
		case "enable":
			key.Disabled = false
		case "disable":
			key.Disabled = true
		case "revoke":
			if key.RevokedAt.IsZero() {
				key.RevokedAt = now
			}
			key.Disabled = true
		}
		key.UpdatedAt = now
		changes[id] = key
	}
	before := s.st.AccessKeys
	next := cloneMap(before)
	if next == nil {
		next = map[string]types.AccessKey{}
	}
	for id, key := range changes {
		next[id] = key
	}
	if len(next) > maxAccessKeys {
		return errors.New("access key capacity reached")
	}
	s.st.AccessKeys = next
	if err := s.save(stateChange{AccessKeys: changes}); err != nil {
		s.st.AccessKeys = before
		return err
	}
	return nil
}

func (s *Store) AccessKeyUsage() map[string]types.KeyUsage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]types.KeyUsage, len(s.st.KeyUsage))
	for id, usage := range s.st.KeyUsage {
		out[id] = usage.Clone()
	}
	return out
}

// SaveAccessKeyUsage accepts cumulative snapshots from the single HTTP metrics
// owner. It is not on the per-request ACK path and never changes permissions.
func (s *Store) SaveAccessKeyUsage(values map[string]types.KeyUsage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(values) > maxAccessKeys {
		return errors.New("too many key usage records")
	}
	next := cloneMap(s.st.KeyUsage)
	if next == nil {
		next = map[string]types.KeyUsage{}
	}
	changes := make(map[string]types.KeyUsage, len(values))
	for id, value := range values {
		if _, _, ok := types.ParseAccessKeyID(id); !ok {
			return errors.New("invalid usage key")
		}
		if len(value.RecentIPs) > types.KeyUsageIPs || len(value.AccountHashes) > types.KeyUsageAccounts ||
			len(value.Hours) > types.KeyUsageHours || len(value.Totals.Endpoints) > 7 {
			return errors.New("key usage bounds exceeded")
		}
		for _, hour := range value.Hours {
			if len(hour.Counters.Endpoints) > 7 {
				return errors.New("key usage endpoint bounds exceeded")
			}
		}
		if prior, ok := next[id]; ok && (value.Totals.Requests < prior.Totals.Requests || value.LastAt.Before(prior.LastAt)) {
			return errors.New("key usage snapshot moved backwards")
		}
		changes[id] = value.Clone()
		next[id] = changes[id]
	}
	if len(next) > maxAccessKeys {
		return errors.New("key usage capacity reached")
	}
	before := s.st.KeyUsage
	s.st.KeyUsage = next
	if err := s.save(stateChange{KeyUsage: changes}); err != nil {
		s.st.KeyUsage = before
		return err
	}
	return nil
}
