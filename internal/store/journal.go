package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"gwpool/internal/types"
)

const (
	journalFile         = "state.wal"
	journalVersion      = 2
	journalMagic        = "GWPWAL02"
	journalHeaderSize   = 36
	journalRecordHeader = 12
	maxJournalRecord    = 64 << 20
	checkpointBytes     = 4 << 20
	// The WAL is already durable before acknowledgement. Small mutations do
	// not need a full multi-megabyte snapshot every minute. The byte threshold
	// still preempts this timer and bounds the normal replay work.
	checkpointMaxInterval   = 5 * time.Minute
	checkpointRetryInterval = time.Minute
	checkpointQueueDepth    = 1
)

// A journal entry contains the exact mutation, never a time-dependent command.
// Replaying must not re-prune reports or re-evaluate limits using a new clock.
type stateChange struct {
	Sequence          uint64                                     `json:"seq"`
	NextAccountID     int64                                      `json:"next_account_id,omitempty"`
	NextOwnerID       int64                                      `json:"next_owner_id,omitempty"`
	NextAccessKeyID   int64                                      `json:"next_access_key_id,omitempty"`
	AccessKeys        map[string]types.AccessKey                 `json:"access_keys,omitempty"`
	KeyUsage          map[string]types.KeyUsage                  `json:"key_usage,omitempty"`
	ExternalUsage     map[string]types.ExternalConsumerUsage     `json:"external_usage,omitempty"`
	AdminAuth         *types.AdminAuthState                      `json:"admin_auth,omitempty"`
	OwnerKeysMigrated bool                                       `json:"owner_keys_migrated,omitempty"`
	AttemptsMigrated  bool                                       `json:"attempts_migrated,omitempty"`
	Accounts          []types.Account                            `json:"accounts,omitempty"`
	DropAccounts      []int64                                    `json:"drop_accounts,omitempty"`
	Owners            []types.Owner                              `json:"owners,omitempty"`
	DropOwners        []int64                                    `json:"drop_owners,omitempty"`
	Gateways          map[string]types.Gateway                   `json:"gateways,omitempty"`
	Slots             map[string]types.Slot                      `json:"slots,omitempty"`
	Mints             map[string]types.RegionMint                `json:"mints,omitempty"`
	Pairs             map[string]types.Pair                      `json:"pairs,omitempty"`
	DropPairs         []string                                   `json:"drop_pairs,omitempty"`
	Donors            map[string]types.RegionDonor               `json:"region_donors,omitempty"`
	DropDonors        []string                                   `json:"drop_region_donors,omitempty"`
	Routes            map[string]types.RouteState                `json:"route_states,omitempty"`
	DropRoutes        []string                                   `json:"drop_route_states,omitempty"`
	Recovery          map[string]types.RecoverySource            `json:"recovery_sources,omitempty"`
	DropRecovery      []string                                   `json:"drop_recovery_sources,omitempty"`
	Attempts          map[string]map[string]types.CountryAttempt `json:"attempts,omitempty"`
	ReplaceAttempts   bool                                       `json:"replace_attempts,omitempty"`
	Contacts          []types.ContactReport                      `json:"contacts,omitempty"`
	DropContacts      []reportKey                                `json:"drop_contacts,omitempty"`
	Cooldowns         []types.CooldownReport                     `json:"cooldowns,omitempty"`
	DropCooldowns     []reportKey                                `json:"drop_cooldowns,omitempty"`
}

type reportKey struct {
	Reporter int64  `json:"reporter"`
	ID       string `json:"id"`
}

// Revisions are process-local cache tokens, incremented only after durable
// commits. Consumers must still evaluate time-dependent readiness at read time.
type Revisions struct {
	Accounts, Owners, Gateways, Slots, Pairs, Mints, Contacts, Cooldowns uint64
	Routes                                                               uint64
	Recovery                                                             uint64
}

func (s *Store) Revisions() Revisions {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revisions
}

func (s *Store) noteChange(c stateChange) {
	for _, id := range c.DropAccounts {
		delete(s.slotIndex, id)
	}
	for _, slot := range c.Slots {
		s.indexSlot(slot)
	}
	if len(c.Accounts)+len(c.DropAccounts) > 0 {
		s.revisions.Accounts++
	}
	if len(c.Owners)+len(c.DropOwners) > 0 {
		s.revisions.Owners++
	}
	if len(c.Gateways) > 0 {
		s.revisions.Gateways++
	}
	if len(c.Slots)+len(c.DropAccounts) > 0 {
		s.revisions.Slots++
	}
	if len(c.Pairs)+len(c.DropPairs)+len(c.Donors)+len(c.DropDonors) > 0 {
		s.revisions.Pairs++
	}
	if len(c.Mints)+len(c.DropAccounts) > 0 {
		s.revisions.Mints++
	}
	if len(c.Routes)+len(c.DropRoutes) > 0 {
		s.revisions.Routes++
	}
	if len(c.Recovery)+len(c.DropRecovery) > 0 {
		s.revisions.Recovery++
	}
	if len(c.Contacts)+len(c.DropContacts) > 0 {
		s.revisions.Contacts++
	}
	if len(c.Cooldowns)+len(c.DropCooldowns) > 0 {
		s.revisions.Cooldowns++
	}
}

type journalIO interface {
	io.Writer
	io.WriterAt
	io.Seeker
	Sync() error
	Truncate(int64) error
	Close() error
}

type journalState struct {
	file              journalIO
	offset            int64
	failed            error
	checkpointErr     error
	checkpointSeq     uint64
	checkpointVersion int
	baseSequence      uint64
	wake              chan struct{}
	stop              chan struct{}
	done              chan struct{}
	closeOnce         sync.Once
	closeErr          error
}

// The checksum-protected commit high-water mark is synced with the append,
// before ACK. A truncated committed record must not be mistaken for an
// uncommitted crash tail. Header tears fail closed, never reset the ledger.
func journalHeader(base, committed uint64, end int64) []byte {
	b := make([]byte, journalHeaderSize)
	copy(b, journalMagic)
	binary.LittleEndian.PutUint64(b[8:], base)
	binary.LittleEndian.PutUint64(b[16:], committed)
	binary.LittleEndian.PutUint64(b[24:], uint64(end))
	binary.LittleEndian.PutUint32(b[32:], crc32.ChecksumIEEE(b[:32]))
	return b
}

func (s *Store) openJournal() error {
	path := filepath.Join(s.dir, journalFile)
	if s.st.JournalVersion != 0 && s.st.JournalVersion != journalVersion {
		return errors.New("unsupported state journal version")
	}
	file, err := os.OpenFile(path, os.O_RDWR, filePerm)
	if errors.Is(err, os.ErrNotExist) && s.st.JournalVersion == 0 {
		file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, filePerm)
	}
	if err != nil {
		return fmt.Errorf("open state journal: %w", err)
	}
	_ = file.Chmod(filePerm)
	ok := false
	defer func() {
		if !ok {
			_ = file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	// A new WAL starts after the durable snapshot. Persist the directory entry
	// before any caller can receive a successful journal-backed mutation.
	if info.Size() == 0 {
		if s.st.JournalVersion != 0 {
			return errors.New("committed state journal is empty")
		}
		if _, err := file.Write(journalHeader(s.st.JournalSequence, s.st.JournalSequence, journalHeaderSize)); err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return err
		}
		if err := syncDirectory(s.dir); err != nil {
			return err
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	header := make([]byte, journalHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil || string(header[:8]) != journalMagic ||
		crc32.ChecksumIEEE(header[:32]) != binary.LittleEndian.Uint32(header[32:]) {
		return errors.New("invalid state journal header")
	}
	previous := binary.LittleEndian.Uint64(header[8:])
	baseSequence := previous
	committed := binary.LittleEndian.Uint64(header[16:])
	commitEnd := binary.LittleEndian.Uint64(header[24:])
	info, err = file.Stat()
	if err != nil {
		return err
	}
	if committed < previous || commitEnd < journalHeaderSize || commitEnd > uint64(info.Size()) {
		return errors.New("committed state journal is truncated or invalid")
	}
	snapshotSequence := s.st.JournalSequence
	if previous > snapshotSequence {
		return errors.New("state journal starts after snapshot; missing durable state")
	}
	offset := int64(journalHeaderSize)
	for offset < int64(commitEnd) {
		recordHeader := make([]byte, journalRecordHeader)
		if _, err := io.ReadFull(file, recordHeader); err != nil {
			return fmt.Errorf("read state journal: %w", err)
		}
		size := binary.LittleEndian.Uint32(recordHeader)
		if size == 0 || size > maxJournalRecord || binary.LittleEndian.Uint32(recordHeader[4:]) != ^size {
			return fmt.Errorf("invalid state journal record size at offset %d", offset)
		}
		if offset+int64(journalRecordHeader)+int64(size) > int64(commitEnd) {
			return errors.New("state journal record crosses committed boundary")
		}
		body := make([]byte, size)
		if _, err := io.ReadFull(file, body); err != nil {
			return err
		}
		if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(recordHeader[8:]) {
			return fmt.Errorf("state journal checksum mismatch at offset %d", offset)
		}
		var change stateChange
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&change); err != nil || change.Sequence != previous+1 {
			return fmt.Errorf("invalid state journal sequence or payload at offset %d", offset)
		}
		if decoder.Decode(new(any)) != io.EOF {
			return fmt.Errorf("trailing data in state journal at offset %d", offset)
		}
		previous = change.Sequence
		if change.Sequence > snapshotSequence {
			applyChange(&s.st, change)
		}
		offset += int64(journalRecordHeader) + int64(size)
	}
	if previous != committed || offset != int64(commitEnd) {
		return errors.New("state journal is missing committed records")
	}
	if info.Size() > int64(commitEnd) {
		if err := truncateJournalTail(file, int64(commitEnd)); err != nil {
			return err
		}
	}
	// A durable snapshot may cover an older WAL (e.g. interrupted compaction).
	// If the log ends before that snapshot, restart it at the snapshot sequence
	// rather than appending a non-contiguous sequence to its obsolete tail.
	if previous < snapshotSequence {
		if err := file.Truncate(0); err != nil {
			return err
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if _, err := file.Write(journalHeader(snapshotSequence, snapshotSequence, journalHeaderSize)); err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return err
		}
		offset = journalHeaderSize
		baseSequence = snapshotSequence
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	s.journal = journalState{file: file, offset: offset, checkpointSeq: snapshotSequence,
		checkpointVersion: s.st.JournalVersion, baseSequence: baseSequence,
		wake: make(chan struct{}, checkpointQueueDepth), stop: make(chan struct{}), done: make(chan struct{})}
	ok = true
	return nil
}

func truncateJournalTail(file *os.File, offset int64) error {
	// Only bytes beyond a validated commit high-water mark are discardable.
	if err := file.Truncate(offset); err != nil {
		return err
	}
	return file.Sync()
}

// save is called with mu held after the in-memory mutation. On failure the
// caller restores its before-image; readers cannot observe uncommitted state.
func (s *Store) save(change stateChange) error {
	journal := &s.journal
	if journal.failed != nil {
		return fmt.Errorf("state journal unavailable: %w", journal.failed)
	}
	if journal.file == nil {
		return errors.New("state journal is closed")
	}
	change.Sequence = s.st.JournalSequence + 1
	change.NextAccountID, change.NextOwnerID = s.st.NextAccountID, s.st.NextOwnerID
	body, err := json.Marshal(change)
	if err != nil {
		return err
	}
	if len(body) > maxJournalRecord {
		return errors.New("state journal mutation is too large")
	}
	record := make([]byte, journalRecordHeader+len(body))
	binary.LittleEndian.PutUint32(record, uint32(len(body)))
	binary.LittleEndian.PutUint32(record[4:], ^uint32(len(body)))
	binary.LittleEndian.PutUint32(record[8:], crc32.ChecksumIEEE(body))
	copy(record[journalRecordHeader:], body)
	n, err := journal.file.Write(record)
	if err == nil && n != len(record) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = writeJournalHeader(journal.file, journalHeader(journal.baseSequence, change.Sequence, journal.offset+int64(len(record))))
	}
	if err == nil {
		err = journal.file.Sync()
	}
	if err != nil {
		// A failed Sync is ambiguous. Do not permit a later successful operation
		// to accidentally commit the failed entry; durably truncate it first.
		rollback := journal.file.Truncate(journal.offset)
		if rollback == nil {
			rollback = writeJournalHeader(journal.file, journalHeader(journal.baseSequence, s.st.JournalSequence, journal.offset))
		}
		if rollback == nil {
			_, rollback = journal.file.Seek(journal.offset, io.SeekStart)
		}
		if rollback == nil {
			rollback = journal.file.Sync()
		}
		if rollback != nil {
			journal.failed = errors.Join(err, rollback)
		}
		return fmt.Errorf("persist state journal: %w", err)
	}
	journal.offset += int64(len(record))
	s.st.JournalSequence = change.Sequence
	s.noteChange(change)
	if journal.offset >= checkpointBytes {
		select {
		case journal.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

func writeJournalHeader(file journalIO, header []byte) error {
	n, err := file.WriteAt(header, 0)
	if err == nil && n != len(header) {
		return io.ErrShortWrite
	}
	return err
}

func applyChange(state *State, change stateChange) {
	state.JournalSequence = change.Sequence
	if change.AdminAuth != nil {
		auth := change.AdminAuth.Clone()
		state.AdminAuth = &auth
	}
	state.NextAccountID = max(state.NextAccountID, change.NextAccountID)
	state.NextOwnerID = max(state.NextOwnerID, change.NextOwnerID)
	state.NextAccessKeyID = max(state.NextAccessKeyID, change.NextAccessKeyID)
	if len(change.AccessKeys) > 0 && state.AccessKeys == nil {
		state.AccessKeys = map[string]types.AccessKey{}
	}
	for id, key := range change.AccessKeys {
		state.AccessKeys[id] = key
	}
	if len(change.KeyUsage) > 0 && state.KeyUsage == nil {
		state.KeyUsage = map[string]types.KeyUsage{}
	}
	for id, usage := range change.KeyUsage {
		state.KeyUsage[id] = usage
	}
	if len(change.ExternalUsage) > 0 && state.ExternalUsage == nil {
		state.ExternalUsage = map[string]types.ExternalConsumerUsage{}
	}
	for id, usage := range change.ExternalUsage {
		state.ExternalUsage[id] = usage
	}
	state.OwnerKeysMigrated = state.OwnerKeysMigrated || change.OwnerKeysMigrated
	state.AttemptsMigrated = state.AttemptsMigrated || change.AttemptsMigrated
	for _, id := range change.DropAccounts {
		state.Accounts = slices.DeleteFunc(state.Accounts, func(row types.Account) bool { return row.ID == id })
		for key, row := range state.Slots {
			if row.AccountID == id {
				delete(state.Slots, key)
			}
		}
		for key, row := range state.Mints {
			if row.AccountID == id {
				delete(state.Mints, key)
			}
		}
	}
	for _, row := range change.Accounts {
		i := slices.IndexFunc(state.Accounts, func(old types.Account) bool { return old.ID == row.ID })
		if i >= 0 {
			state.Accounts[i] = row
		} else {
			state.Accounts = append(state.Accounts, row)
		}
	}
	for _, id := range change.DropOwners {
		state.Owners = slices.DeleteFunc(state.Owners, func(row types.Owner) bool { return row.ID == id })
	}
	for _, row := range change.Owners {
		i := slices.IndexFunc(state.Owners, func(old types.Owner) bool { return old.ID == row.ID })
		if i >= 0 {
			state.Owners[i] = row
		} else {
			state.Owners = append(state.Owners, row)
		}
	}
	for key, row := range change.Gateways {
		state.Gateways[key] = row
	}
	for key, row := range change.Slots {
		state.Slots[key] = row
	}
	for key, row := range change.Mints {
		state.Mints[key] = row
	}
	for _, key := range change.DropPairs {
		delete(state.Pairs, key)
	}
	for key, row := range change.Pairs {
		state.Pairs[key] = row
	}
	if len(change.Donors) > 0 && state.Donors == nil {
		state.Donors = map[string]types.RegionDonor{}
	}
	for _, key := range change.DropDonors {
		delete(state.Donors, key)
	}
	for key, donor := range change.Donors {
		state.Donors[key] = donor
	}
	if len(change.Routes) > 0 && state.Routes == nil {
		state.Routes = map[string]types.RouteState{}
	}
	for _, key := range change.DropRoutes {
		delete(state.Routes, key)
	}
	for key, route := range change.Routes {
		state.Routes[key] = route
	}
	if len(change.Recovery) > 0 && state.Recovery == nil {
		state.Recovery = map[string]types.RecoverySource{}
	}
	for _, key := range change.DropRecovery {
		delete(state.Recovery, key)
	}
	for key, source := range change.Recovery {
		state.Recovery[key] = source
	}
	if (state.CountryAttempts == nil && len(change.Attempts) > 0) || change.ReplaceAttempts {
		state.CountryAttempts = map[string]map[string]types.CountryAttempt{}
	}
	for key, rows := range change.Attempts {
		state.CountryAttempts[key] = rows
	}
	state.ContactReports = applyReports(state.ContactReports, change.Contacts, change.DropContacts, contactKey)
	state.CooldownReports = applyReports(state.CooldownReports, change.Cooldowns, change.DropCooldowns, cooldownKey)
}

func contactKey(report types.ContactReport) reportKey {
	return reportKey{report.ReporterID, report.ID}
}

func cooldownKey(report types.CooldownReport) reportKey {
	return reportKey{report.ReporterID, report.ID}
}

func applyReports[T any](rows, updates []T, dropped []reportKey, key func(T) reportKey) []T {
	if len(updates)+len(dropped) == 0 {
		return rows
	}
	remove := make(map[reportKey]bool, len(dropped))
	for _, id := range dropped {
		remove[id] = true
	}
	rows = slices.DeleteFunc(rows, func(row T) bool { return remove[key(row)] })
	indices := make(map[reportKey]int, len(rows))
	for i, row := range rows {
		indices[key(row)] = i
	}
	for _, row := range updates {
		if i, ok := indices[key(row)]; ok {
			rows[i] = row
		} else {
			indices[key(row)] = len(rows)
			rows = append(rows, row)
		}
	}
	return rows
}

func (s *Store) checkpointLoop() {
	defer close(s.journal.done)
	timer := time.NewTimer(checkpointMaxInterval)
	defer timer.Stop()
	var retryAt time.Time
	for {
		select {
		case <-s.journal.stop:
			return
		case <-s.journal.wake:
		case <-timer.C:
		}
		if time.Now().Before(retryAt) {
			continue
		}
		if err := s.Checkpoint(); err != nil {
			s.mu.Lock()
			s.journal.checkpointErr = err
			s.mu.Unlock()
			slog.Error("state checkpoint failed; committed journal retained", "error", err)
			retryAt = time.Now().Add(checkpointRetryInterval)
			timer.Reset(checkpointRetryInterval)
		} else {
			retryAt = time.Time{}
			timer.Reset(checkpointMaxInterval)
		}
	}
}

// Checkpoint serializes a consistent clone outside mu. Only WAL compaction
// briefly excludes writers; a newer mutation is retained in the suffix.
func (s *Store) Checkpoint() error {
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()
	s.mu.RLock()
	if s.journal.file == nil || s.journal.failed != nil {
		err := s.journal.failed
		s.mu.RUnlock()
		if err != nil {
			return err
		}
		return errors.New("state journal is closed")
	}
	if s.journal.checkpointSeq == s.st.JournalSequence && s.journal.checkpointVersion == journalVersion {
		if _, err := os.Stat(filepath.Join(s.dir, stateFile)); err == nil {
			s.mu.RUnlock()
			return nil
		}
	}
	snapshot := cloneState(s.st)
	boundary := s.journal.offset
	s.mu.RUnlock()
	body, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	target := filepath.Join(s.dir, stateFile)
	temp := target + ".tmp"
	if err := s.checkpointIO.write(temp, body); err != nil {
		return err
	}
	s.backup(target)
	if err := s.checkpointIO.replace(temp, target); err != nil {
		return err
	}
	// The snapshot is durable before deleting any covered journal entries.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.journal.file == nil || s.journal.failed != nil {
		return errors.New("state journal unavailable during checkpoint")
	}
	path := filepath.Join(s.dir, journalFile)
	journal, err := os.Open(path)
	if err != nil {
		return err
	}
	suffix, err := io.ReadAll(io.NewSectionReader(journal, boundary, s.journal.offset-boundary))
	closeErr := journal.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	next := append(journalHeader(snapshot.JournalSequence, s.st.JournalSequence, int64(journalHeaderSize+len(suffix))), suffix...)
	tempJournal := path + ".tmp"
	if err := s.checkpointIO.write(tempJournal, next); err != nil {
		return err
	}
	if err := s.journal.file.Close(); err != nil {
		s.journal.failed = err
		return err
	}
	replaceErr := s.checkpointIO.replace(tempJournal, path)
	if replaceErr != nil {
		// Rename may have succeeded before the directory durability barrier
		// failed. Its file offset is now ambiguous; only Open may resolve it.
		s.journal.file = nil
		s.journal.failed = replaceErr
		return replaceErr
	}
	file, openErr := s.checkpointIO.open(path)
	if openErr != nil {
		s.journal.file = nil
		s.journal.failed = openErr
		return errors.Join(replaceErr, openErr)
	}
	s.journal.file = file
	if replaceErr == nil {
		s.journal.offset = int64(len(next))
		s.journal.checkpointSeq = snapshot.JournalSequence
		s.journal.checkpointVersion = journalVersion
		s.journal.baseSequence = snapshot.JournalSequence
	}
	if _, err := file.Seek(s.journal.offset, io.SeekStart); err != nil {
		s.journal.failed = err
		return errors.Join(replaceErr, err)
	}
	if replaceErr == nil {
		s.journal.checkpointErr = nil
	}
	return replaceErr
}

func writeDurableFile(path string, body []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, filePerm)
	if err != nil {
		return err
	}
	if err := file.Chmod(filePerm); err != nil {
		_ = file.Close()
		return err
	}
	n, err := file.Write(body)
	if err == nil && n != len(body) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = file.Sync()
	}
	return errors.Join(err, file.Close())
}

func cloneMap[K comparable, V any](from map[K]V) map[K]V {
	if from == nil {
		return nil
	}
	out := make(map[K]V, len(from))
	for key, value := range from {
		out[key] = value
	}
	return out
}

func cloneState(state State) State {
	if state.AdminAuth != nil {
		auth := state.AdminAuth.Clone()
		state.AdminAuth = &auth
	}
	state.AccessKeys = cloneMap(state.AccessKeys)
	state.KeyUsage = cloneMap(state.KeyUsage)
	state.ExternalUsage = cloneMap(state.ExternalUsage)
	for id, usage := range state.KeyUsage {
		state.KeyUsage[id] = usage.Clone()
	}
	state.Accounts = slices.Clone(state.Accounts)
	state.Owners = slices.Clone(state.Owners)
	state.ContactReports = slices.Clone(state.ContactReports)
	state.CooldownReports = slices.Clone(state.CooldownReports)
	state.Gateways = cloneMap(state.Gateways)
	for key, gateway := range state.Gateways {
		state.Gateways[key] = cloneGateway(gateway)
	}
	state.Slots = cloneMap(state.Slots)
	state.Pairs = cloneMap(state.Pairs)
	state.Donors = cloneMap(state.Donors)
	state.Routes = cloneMap(state.Routes)
	state.Recovery = cloneMap(state.Recovery)
	state.Mints = cloneMap(state.Mints)
	state.CountryAttempts = cloneMap(state.CountryAttempts)
	for key, countries := range state.CountryAttempts {
		next := cloneMap(countries)
		for country, row := range next {
			row.Recent = slices.Clone(row.Recent)
			next[country] = row
		}
		state.CountryAttempts[key] = next
	}
	return state
}
