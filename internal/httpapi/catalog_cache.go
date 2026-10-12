package httpapi

import (
	"encoding/json"
	"sync"
	"time"

	"gwpool/internal/contact"
	"gwpool/internal/cooldown"
	"gwpool/internal/mint"
	"gwpool/internal/store"
	"gwpool/internal/ticket"
	"gwpool/internal/types"
)

const maxCooldownCacheCeilings = 8

type pairCatalogCache struct {
	mu          sync.Mutex
	ready       bool
	revision    uint64
	from, until time.Time
	pairs       map[string]types.Pair
}

// Only the immutable rehoming result is cached. Readiness and remaining seconds
// still use the current request time; any donor/fruit expiry changes the result.
func (s *Server) cachedCatalogPairs(now time.Time) map[string]types.Pair {
	versioned, ok := s.st.(interface{ Revisions() store.Revisions })
	if !ok {
		return s.effectivePairs(now)
	}
	cache := &s.catalogPairs
	cache.mu.Lock()
	defer cache.mu.Unlock()
	revision := versioned.Revisions().Pairs
	if cache.ready && cache.revision == revision && cacheValid(cache.from, cache.until, now) {
		return cache.pairs
	}
	raw, donors := s.st.TicketInventory()
	best := ticket.Best(raw, donors, now)
	pairs := make(map[string]types.Pair, len(raw))
	var from, until time.Time
	for _, donor := range donors {
		from, until = cacheBoundary(from, until, donor.ExpiresAt, now)
	}
	for key, pair := range raw {
		_, fruitExpiry, _ := mint.PairInfo(pair.Cookie)
		for _, boundary := range []time.Time{pair.CflbExpiresAt, fruitExpiry} {
			if !boundary.IsZero() {
				from, until = cacheBoundary(from, until, boundary, now)
			}
		}
		pairs[key] = mint.Rehome(pair, best, now)
	}
	cache.ready, cache.revision, cache.from, cache.until = true, revision, from, until
	cache.pairs = pairs
	return pairs // caller must only read this immutable snapshot
}

type reportConsumer struct {
	reporter int64
	tag      string
}

type policyKey struct {
	consumer reportConsumer
	gateway  string
}

type policyBounds struct{ oldest, latest time.Time }

type contactCache struct {
	mu          sync.Mutex
	ready       bool
	revision    uint64
	from, until time.Time
	data        *contactCacheData
}

// Rows/priorities are immutable after publication. Encoding is lazy per actual
// consumer and belongs to this snapshot, so replacing a revision never mutates
// maps being used by an earlier request.
type contactCacheData struct {
	byConsumer map[reportConsumer]map[string][]types.ContactStats
	priorities map[string]map[string]types.GatewayPriority
	encodedMu  sync.Mutex
	encoded    map[reportConsumer]map[string]json.RawMessage
}

type cooldownCacheEntry struct {
	from, until time.Time
	stats       []types.CooldownStats
	bounds      map[policyKey]policyBounds
	accounts    map[policyKey]types.CooldownAccountStats
	gateways    map[string]int
}

type cooldownCache struct {
	mu       sync.Mutex
	revision uint64
	entries  map[int]cooldownCacheEntry
}

func cacheValid(from, until, now time.Time) bool {
	return !now.Before(from) && (until.IsZero() || now.Before(until))
}

// A report set is invariant between adjacent evidence boundaries, even when
// requests reach this cache out of timestamp order. Using build-time as the
// lower bound would repeatedly rebuild the same data during a concurrent burst.
func cacheBoundary(from, until, event, now time.Time) (time.Time, time.Time) {
	if event.After(now) {
		if until.IsZero() || event.Before(until) {
			until = event
		}
	} else if from.IsZero() || event.After(from) {
		from = event
	}
	return from, until
}

// Report slices are immutable copies. Read the revision before taking a copy:
// a concurrent newer copy then merely causes an extra rebuild next time, never
// labels old data with a newer revision. Fake stores deliberately bypass caches.
func (s *Server) contactData(now time.Time) (map[reportConsumer]map[string][]types.ContactStats, map[string]map[string]types.GatewayPriority) {
	data := s.contactSnapshot(now)
	return data.byConsumer, data.priorities
}

func (s *Server) contactSnapshot(now time.Time) *contactCacheData {
	versioned, ok := s.st.(interface{ Revisions() store.Revisions })
	if !ok {
		contacts, priorities := buildContactData(s.st.ContactReports(), now)
		return &contactCacheData{byConsumer: contacts, priorities: priorities}
	}
	cache := &s.contacts
	cache.mu.Lock()
	defer cache.mu.Unlock()
	revision := versioned.Revisions().Contacts
	if cache.ready && cache.revision == revision && cacheValid(cache.from, cache.until, now) {
		return cache.data
	}
	reports := s.st.ContactReports()
	var from, until time.Time
	for _, report := range reports {
		from, until = cacheBoundary(from, until, report.At.Add(-5*time.Minute), now)
		from, until = cacheBoundary(from, until, report.At.Add(contact.Retention+time.Nanosecond), now)
	}
	contacts, priorities := buildContactData(reports, now)
	cache.data = &contactCacheData{byConsumer: contacts, priorities: priorities}
	cache.revision, cache.from, cache.until, cache.ready = revision, from, until, true
	return cache.data
}

func (data *contactCacheData) encodedFor(consumer reportConsumer) map[string]json.RawMessage {
	gateways, exists := data.byConsumer[consumer]
	if !exists {
		return nil // arbitrary client tags must not grow a cache of empty rows
	}
	data.encodedMu.Lock()
	defer data.encodedMu.Unlock()
	if encoded, ok := data.encoded[consumer]; ok {
		return encoded
	}
	rows := make(map[string]json.RawMessage, len(gateways))
	for gateway, stats := range gateways {
		// ContactStats contains only strings and integers, no fallible hooks.
		body, _ := json.Marshal(stats)
		rows[gateway] = body
	}
	if data.encoded == nil {
		data.encoded = make(map[reportConsumer]map[string]json.RawMessage)
	}
	data.encoded[consumer] = rows
	return rows
}

func buildContactData(reports []types.ContactReport, now time.Time) (map[reportConsumer]map[string][]types.ContactStats, map[string]map[string]types.GatewayPriority) {
	byConsumer := map[reportConsumer]map[string][]types.ContactStats{}
	for _, row := range contact.Summarize(reports, now) {
		key := reportConsumer{row.ReporterID, row.AccountTag}
		if byConsumer[key] == nil {
			byConsumer[key] = map[string][]types.ContactStats{}
		}
		byConsumer[key][row.Gateway] = append(byConsumer[key][row.Gateway], row)
	}
	return byConsumer, contact.AllPriorities(reports, now)
}

func (s *Server) contactPriorities(model string, now time.Time) map[string]types.GatewayPriority {
	if _, ok := s.st.(interface{ Revisions() store.Revisions }); !ok {
		return contact.Priorities(s.st.ContactReports(), model, now)
	}
	_, priorities := s.contactData(now)
	return priorities[model]
}

func buildCooldownData(reports []types.CooldownReport, now time.Time, ceiling int) cooldownCacheEntry {
	out := cooldownCacheEntry{stats: cooldown.SummarizeWithin(reports, now, ceiling),
		bounds:   map[policyKey]policyBounds{},
		accounts: map[policyKey]types.CooldownAccountStats{},
		gateways: map[string]int{}}
	for i, stats := range out.stats {
		out.gateways[stats.Gateway] = i
		for _, account := range stats.Accounts {
			out.accounts[policyKey{reportConsumer{account.ReporterID, account.AccountTag}, stats.Gateway}] = account
		}
	}
	for _, report := range reports {
		at := report.EventAt()
		out.from, out.until = cacheBoundary(out.from, out.until, at.Add(-time.Minute), now)
		out.from, out.until = cacheBoundary(out.from, out.until, at.Add(cooldown.Retention+time.Nanosecond), now)
		if at.Before(now.Add(-cooldown.Retention)) || at.After(now.Add(time.Minute)) {
			continue
		}
		key := policyKey{reportConsumer{report.ReporterID, report.AccountTag}, report.Gateway}
		bounds := out.bounds[key]
		if bounds.oldest.IsZero() || at.Before(bounds.oldest) {
			bounds.oldest = at
		}
		if at.After(bounds.latest) {
			bounds.latest = at
		}
		out.bounds[key] = bounds
	}
	for _, bounds := range out.bounds {
		out.from, out.until = cacheBoundary(out.from, out.until, bounds.latest.Add(cooldown.ReportPolicyTTL+time.Nanosecond), now)
	}
	return out
}

func (s *Server) cooldownData(now time.Time, ceiling int) cooldownCacheEntry {
	versioned, ok := s.st.(interface{ Revisions() store.Revisions })
	if !ok {
		return buildCooldownData(s.st.CooldownReports(), now, ceiling)
	}
	cache := &s.cooldowns
	cache.mu.Lock()
	defer cache.mu.Unlock()
	revision := versioned.Revisions().Cooldowns
	if revision != cache.revision {
		cache.entries = nil
		cache.revision = revision
	}
	if entry, ok := cache.entries[ceiling]; ok && cacheValid(entry.from, entry.until, now) {
		return entry
	}
	entry := buildCooldownData(s.st.CooldownReports(), now, ceiling)
	// The client can negotiate arbitrary seconds, not just documented steps.
	// Bound cache memory without changing negotiation or clamping evidence.
	if len(cache.entries) >= maxCooldownCacheCeilings || cache.entries == nil {
		cache.entries = map[int]cooldownCacheEntry{}
	}
	cache.entries[ceiling] = entry
	return entry
}

func (entry cooldownCacheEntry) forAccount(stats types.CooldownStats, reporterID int64, tag string, now time.Time) types.CooldownRecommendation {
	key := policyKey{reportConsumer{reporterID, tag}, stats.Gateway}
	rec := stats.Recommendation
	if account, ok := entry.accounts[key]; ok {
		if account.Recommendation.Seconds > 0 {
			rec = account.Recommendation
		} else if rec.Seconds < account.FailureFloorSeconds {
			return types.CooldownRecommendation{}
		}
	}
	if rec.ReportIntervalSeconds > 0 {
		bounds := entry.bounds[key]
		// The policy's rolling TTL changes even if no report changes. Keep its
		// evidence cap separate; freezing the original expiry changes the API.
		rec.ReportPolicyExpiresAt = now.Add(cooldown.ReportPolicyTTL)
		if cap := bounds.oldest.Add(cooldown.Retention); cap.Before(rec.ReportPolicyExpiresAt) {
			rec.ReportPolicyExpiresAt = cap
		}
	}
	return rec
}

func (entry cooldownCacheEntry) recommendation(gateway string, reporterID int64, tag string, now time.Time) *types.CooldownRecommendation {
	if index, ok := entry.gateways[gateway]; ok {
		if rec := entry.forAccount(entry.stats[index], reporterID, tag, now); rec.Seconds > 0 {
			return &rec
		}
	}
	return nil
}
