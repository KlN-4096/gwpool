package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gwpool/internal/contact"
	"gwpool/internal/cooldown"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

type countedCatalogStore struct {
	*store.Store
	contacts, cooldowns atomic.Int32
	accounts            atomic.Int32
}

func (s *countedCatalogStore) Accounts() []types.Account {
	s.accounts.Add(1)
	return s.Store.Accounts()
}

func TestCatalogAuthorizationMemoIsRequestLocal(t *testing.T) {
	s, st := cacheTestServer(t)
	counted := &countedCatalogStore{Store: st}
	s.st = counted
	r := httptest.NewRequest("GET", "/gateways", nil)
	r.Header.Set("Authorization", "Bearer initially-invalid")
	cached := s.resolveCallerOnce(r)
	for range 3 {
		if _, ok := s.resolveCaller(cached); ok {
			t.Fatal("invalid key accepted")
		}
	}
	if counted.accounts.Load() != 1 {
		t.Fatal("authorization repeated within one request")
	}
	s.opt.ConsumerKey = "initially-invalid"
	if _, ok := s.resolveCaller(s.resolveCallerOnce(r)); !ok {
		t.Fatal("previous request's invalid authority leaked to next request")
	}
	s.opt.ConsumerKey = ""
	if _, ok := s.resolveCaller(s.resolveCallerOnce(r)); ok {
		t.Fatal("revoked authority leaked to next request")
	}
}

func TestCatalogPairCacheTracksDonorFruitExpiryAndReplacement(t *testing.T) {
	s, st := cacheTestServer(t)
	now := time.Now().UTC().Truncate(time.Second)
	target := types.Pair{Gateway: "target", RegionKey: "us-east",
		Cookie:        routeCookie("old", "target", now.Add(2*time.Minute)),
		CflbExpiresAt: now.Add(time.Minute), ExpiresAt: now.Add(time.Minute)}
	donor := types.Pair{Gateway: "donor", RegionKey: "us-east",
		Cookie:        routeCookie("new", "donor", now.Add(4*time.Minute)),
		CflbExpiresAt: now.Add(3 * time.Minute), ExpiresAt: now.Add(3 * time.Minute)}
	for _, pair := range []types.Pair{target, donor} {
		if err := st.PutPair(pair); err != nil {
			t.Fatal(err)
		}
	}
	check := func(at time.Time) {
		t.Helper()
		if !reflect.DeepEqual(s.cachedCatalogPairs(at), s.effectivePairs(at)) {
			t.Fatalf("cached projection disagrees at offset %s", at.Sub(now))
		}
	}
	for _, offset := range []time.Duration{0, time.Minute, 2*time.Minute - time.Nanosecond,
		2 * time.Minute, 3 * time.Minute, 4 * time.Minute, -time.Minute} {
		check(now.Add(offset))
	}
	donor.CflbExpiresAt = now.Add(5 * time.Minute)
	if err := st.PutPair(donor); err != nil {
		t.Fatal(err)
	}
	check(now)
	if err := st.DeletePair("donor"); err != nil {
		t.Fatal(err)
	}
	check(now)
}

func TestCatalogPairCacheUsesIndependentCommittedDonor(t *testing.T) {
	s, st := cacheTestServer(t)
	now := time.Now().UTC().Truncate(time.Second)
	old := types.Pair{Gateway: "target", RegionKey: "us-east",
		Cookie:        routeCookie("old", "target", now.Add(time.Hour)),
		CflbExpiresAt: now.Add(10 * time.Minute), ExpiresAt: now.Add(10 * time.Minute)}
	if err := st.PutPair(old); err != nil {
		t.Fatal(err)
	}
	if got := s.cachedCatalogPairs(now)["target"]; got.ExpiresAt != old.ExpiresAt {
		t.Fatal("unexpected initial projection")
	}
	candidate := types.Pair{Gateway: "target", RegionKey: "us-east",
		Cookie:        routeCookie("fresh-donor", "target", now.Add(12*time.Minute)),
		CflbExpiresAt: now.Add(40 * time.Minute), ExpiresAt: now.Add(12 * time.Minute)}
	result, err := st.CommitMint(types.MintCommit{Pair: candidate, At: now})
	if err != nil || result.PairChanged {
		t.Fatalf("shorter fruit replaced incumbent: err=%v changed=%v", err, result.PairChanged)
	}
	for _, at := range []time.Time{now, now.Add(40 * time.Minute), now.Add(time.Hour), now.Add(time.Minute)} {
		if !reflect.DeepEqual(s.cachedCatalogPairs(at), s.effectivePairs(at)) {
			t.Fatal("independent donor revision/expiry did not invalidate catalog")
		}
	}
	if got := s.cachedCatalogPairs(now)["target"]; got.ExpiresAt != now.Add(40*time.Minute) {
		t.Fatal("independent donor not included in catalog")
	}
}

func (s *countedCatalogStore) ContactReports() []types.ContactReport {
	s.contacts.Add(1)
	return s.Store.ContactReports()
}

func (s *countedCatalogStore) CooldownReports() []types.CooldownReport {
	s.cooldowns.Add(1)
	return s.Store.CooldownReports()
}

func TestCatalogCacheReusesOutOfOrderConcurrentReadTimes(t *testing.T) {
	s, st := cacheTestServer(t)
	now := time.Now().UTC()
	if _, err := st.AddContactReport(types.ContactReport{ReporterID: 1,
		ContactObservation: types.ContactObservation{ID: "one", At: now}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AddCooldownReport(types.CooldownReport{ID: "one", ReporterID: 1, ReceivedAt: now}); err != nil {
		t.Fatal(err)
	}
	counted := &countedCatalogStore{Store: st}
	s.st = counted
	// A later request can reach a cache first while an earlier one is delayed
	// by another lock. No evidence boundary lies between these timestamps.
	for _, at := range []time.Time{now.Add(5 * time.Second), now, now.Add(time.Second)} {
		s.contactData(at)
		s.cooldownData(at, cooldown.MaxSeconds)
	}
	if counted.contacts.Load() != 1 || counted.cooldowns.Load() != 1 {
		t.Fatalf("unchanged report sets rebuilt: contacts=%d cooldowns=%d", counted.contacts.Load(), counted.cooldowns.Load())
	}
}

func cacheTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return &Server{st: st}, st
}

func TestCatalogContactCacheMatchesVersionsAndTimeBoundaries(t *testing.T) {
	s, st := cacheTestServer(t)
	now := time.Now().UTC()
	for i := range 6 {
		at := now
		if i == 5 {
			at = now.Add(6 * time.Minute)
		}
		report := types.ContactReport{ReporterID: -1, ContactObservation: types.ContactObservation{
			ID: fmt.Sprint(i), AccountTag: "tag", Gateway: "g", Model: "model",
			Criterion: "state-echo-v1", Source: "foreground", Outcome: "full", At: at}}
		if _, err := st.AddContactReport(report); err != nil {
			t.Fatal(err)
		}
	}
	for _, at := range []time.Time{now, now.Add(time.Second), now.Add(time.Minute), now,
		now.Add(contact.Retention), now.Add(contact.Retention + time.Nanosecond)} {
		gotContacts, gotPriority := s.contactData(at)
		wantContacts, wantPriority := buildContactData(st.ContactReports(), at)
		if !reflect.DeepEqual(gotContacts, wantContacts) || !reflect.DeepEqual(gotPriority, wantPriority) {
			t.Fatalf("cached contact result differs at offset %v", at.Sub(now))
		}
		data := s.contactSnapshot(at)
		for consumer, gateways := range wantContacts {
			encoded := data.encodedFor(consumer)
			for gateway, want := range gateways {
				var got []types.ContactStats
				if err := json.Unmarshal(encoded[gateway], &got); err != nil {
					t.Fatal(err)
				}
				// These two fields are intentionally omitted from the API.
				for i := range want {
					want[i].ReporterID, want[i].AccountTag = 0, ""
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatal("cached encoded contacts changed the wire fields")
				}
			}
		}
	}
	s.contactData(now)
	report := st.ContactReports()[0]
	report.WindowFinal, report.FullWindowMS = true, 12000
	before := st.Revisions()
	if added, err := st.AddContactReport(report); added || err != nil {
		t.Fatal("window completion must update existing report", added, err)
	}
	if st.Revisions().Contacts == before.Contacts {
		t.Fatal("merged report failed to invalidate derived data")
	}
	got, _ := s.contactData(now)
	want, _ := buildContactData(st.ContactReports(), now)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("contact completion returned stale window duration")
	}
}

func TestCatalogContactEncodingIsLazyBoundedAndRevisionLocal(t *testing.T) {
	s, st := cacheTestServer(t)
	now := time.Now().UTC()
	for i := range 12 {
		if _, err := st.AddContactReport(types.ContactReport{ReporterID: int64(i),
			ContactObservation: types.ContactObservation{
				ID: fmt.Sprint(i), AccountTag: "tag", Gateway: "g", At: now, Outcome: "full",
			}}); err != nil {
			t.Fatal(err)
		}
	}
	data := s.contactSnapshot(now)
	if len(data.encoded) != 0 {
		t.Fatal("unused consumers were encoded during a cache rebuild")
	}
	consumer := reportConsumer{1, "tag"}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { data.encodedFor(consumer) })
	}
	wg.Wait()
	if len(data.encoded) != 1 {
		t.Fatal("encoding did not coalesce concurrent reads of one consumer")
	}
	for i := range 100 {
		if data.encodedFor(reportConsumer{1, fmt.Sprint(i)}) != nil {
			t.Fatal("unknown tag exposed another consumer's history")
		}
	}
	if len(data.encoded) != 1 {
		t.Fatal("untrusted absent tags grew the encoding cache")
	}
	before := string(data.encodedFor(consumer)["g"])
	report := st.ContactReports()[1]
	report.WindowFinal, report.FullWindowMS = true, 1234
	if _, err := st.AddContactReport(report); err != nil {
		t.Fatal(err)
	}
	next := s.contactSnapshot(now)
	if next == data || string(next.encodedFor(consumer)["g"]) == before {
		t.Fatal("new revision reused stale window encoding")
	}
	if string(data.encodedFor(consumer)["g"]) != before {
		t.Fatal("publishing a newer revision mutated an in-flight snapshot")
	}
}

func TestCooldownAccountIndexMatchesPersonalAndPoolFallback(t *testing.T) {
	now := time.Now().UTC()
	var reports []types.CooldownReport
	for reporter := int64(1); reporter <= 4; reporter++ {
		for i := range 3 {
			outcome, seconds := "full", 3600
			if reporter == 4 {
				outcome, seconds = "degraded", 7200
			}
			reports = append(reports, types.CooldownReport{
				ID: fmt.Sprintf("%d-%d", reporter, i), ReporterID: reporter,
				AccountTag: "same-tag", Gateway: "g", Result: outcome,
				WindowSeconds: seconds, ElapsedSeconds: seconds,
				ObservedAt: now.Add(-time.Duration(i+1) * time.Hour), ReceivedAt: now,
			})
		}
	}
	entry := buildCooldownData(reports, now, cooldown.MaxSeconds)
	stats := entry.stats[0]
	for _, consumer := range []reportConsumer{{1, "same-tag"}, {4, "same-tag"}, {1, "other"}, {99, "same-tag"}} {
		want := cooldown.ForAccount(stats, consumer.reporter, consumer.tag)
		withoutAccounts := stats
		withoutAccounts.Accounts = nil // the cached lookup must not scan this slice
		if got := entry.forAccount(withoutAccounts, consumer.reporter, consumer.tag, now); got != want {
			t.Fatalf("personal/fallback identity separation changed for %+v", consumer)
		}
	}
}

func TestCatalogCooldownCacheMatchesRollingPolicyAndCeilings(t *testing.T) {
	s, st := cacheTestServer(t)
	now := time.Now().UTC()
	for i, hours := range []int{-48, -36, -24, -3, -2, -1} {
		report := types.CooldownReport{ID: fmt.Sprint(i), ReporterID: -1, AccountTag: "tag",
			Gateway: "g", Result: "full", WindowSeconds: 3600, ElapsedSeconds: 3600,
			ObservedAt: now.Add(time.Duration(hours) * time.Hour), ReceivedAt: now}
		if _, err := st.AddCooldownReport(report); err != nil {
			t.Fatal(err)
		}
	}
	for _, at := range []time.Time{now, now.Add(time.Hour), now.Add(23 * time.Hour),
		now.Add(23*time.Hour + time.Nanosecond), now, now.Add(5*24*time.Hour + time.Nanosecond)} {
		entry := s.cooldownData(at, cooldown.MaxSeconds)
		want := cooldown.SummarizeWithin(st.CooldownReports(), at, cooldown.MaxSeconds)
		if len(entry.stats) != len(want) {
			t.Fatal("cached cooldown retained expired reports")
		}
		for i := range want {
			if got := entry.forAccount(entry.stats[i], -1, "tag", at); got != cooldown.ForAccount(want[i], -1, "tag") {
				t.Fatalf("rolling policy differs at offset %v: got %+v want %+v", at.Sub(now), got, cooldown.ForAccount(want[i], -1, "tag"))
			}
		}
	}
	if rec := s.cooldownData(now, cooldown.MaxSeconds).stats[0].Accounts[0].Recommendation; rec.ReportIntervalSeconds == 0 {
		t.Fatal("fixture did not exercise stable policy")
	}
	// Future evidence is admitted at exactly +1 minute, and a new failed report
	// must invalidate the previous personal policy without any time advance.
	failed := types.CooldownReport{ID: "future-failure", ReporterID: -1, AccountTag: "tag",
		Gateway: "g", Result: "degraded", WindowSeconds: 7200, ElapsedSeconds: 7200,
		ObservedAt: now.Add(2 * time.Minute), ReceivedAt: now}
	if _, err := st.AddCooldownReport(failed); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{now, now.Add(time.Minute)} {
		entry := s.cooldownData(at, cooldown.MaxSeconds)
		want := cooldown.SummarizeWithin(st.CooldownReports(), at, cooldown.MaxSeconds)
		if entry.forAccount(entry.stats[0], -1, "tag", at) != cooldown.ForAccount(want[0], -1, "tag") {
			t.Fatal("future report admission or new failure did not invalidate cache")
		}
	}
	for ceiling := cooldown.LegacyMaxSeconds; ceiling <= cooldown.LegacyMaxSeconds+20; ceiling++ {
		entry := s.cooldownData(now, ceiling)
		want := cooldown.SummarizeWithin(st.CooldownReports(), now, ceiling)
		if !reflect.DeepEqual(entry.stats, want) {
			t.Fatal("arbitrary negotiated ceiling changed evidence")
		}
	}
	if len(s.cooldowns.entries) > maxCooldownCacheCeilings {
		t.Fatal("unbounded client-controlled cache")
	}
}
