package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gwpool/internal/mint"
	"gwpool/internal/sched"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

func TestExactDeliveryCatalogAndDeliveryUseSameRehomedTicket(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	now := time.Now().UTC().Truncate(time.Second)
	target := types.Pair{Gateway: "target", RegionKey: "us-east",
		Cookie:        routeCookie("old", "target", now.Add(20*time.Minute)),
		CflbExpiresAt: now.Add(time.Minute), ExpiresAt: now.Add(time.Minute)}
	donor := types.Pair{Gateway: "donor", RegionKey: "us-east",
		Cookie:        routeCookie("new", "donor", now.Add(time.Hour)),
		CflbExpiresAt: now.Add(40 * time.Minute), ExpiresAt: now.Add(40 * time.Minute)}
	for _, pair := range []types.Pair{target, donor} {
		if _, err := st.UpsertGateway(pair.Gateway, 0, types.Exit{}); err != nil {
			t.Fatal(err)
		}
		if err := st.PutPair(pair); err != nil {
			t.Fatal(err)
		}
	}
	pool := sched.New(st, &mint.Client{}, sched.Options{Now: func() time.Time { return now }})
	srv, err := New(pool, st, nil, Options{ConsumerKey: "offline"})
	if err != nil {
		t.Fatal(err)
	}
	h := srv.Handler()
	catalog := do(h, authed(httptest.NewRequest("GET", "/gateways?account=member", nil), "offline"))
	if catalog.Code != 200 || !strings.Contains(catalog.Body.String(), `"name":"target"`) {
		t.Fatalf("rehomed target absent: %d %s", catalog.Code, catalog.Body.String())
	}
	w := do(h, authed(httptest.NewRequest("GET", "/cookie?account=member&gateway=target", nil), "offline"))
	var d deliveryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || mint.CflbOf(d.Cookie) != "new" || d.CookieVersion == types.CookieVersion(target.Cookie) ||
		!d.ExpiresAt.Equal(now.Add(20*time.Minute)) || d.PairRemainingS < 1198 {
		t.Fatalf("delivery disagrees with effective catalog: status=%d gateway=%s expiry=%v remaining=%d",
			w.Code, d.Gateway, d.ExpiresAt, d.PairRemainingS)
	}
}

func TestRetiredConsumerEndpointsRequireUpgradeWithoutSideEffects(t *testing.T) {
	_, handler, pool, st, scanner := rig(t, types.ModePool)
	for _, route := range []struct{ method, path string }{
		{"POST", "/release"}, {"POST", "/pair/renew"},
	} {
		w := do(handler, httptest.NewRequest(route.method, route.path, strings.NewReader("unread legacy body")))
		if w.Code != http.StatusUpgradeRequired || decodeFail(t, w).Error.Code != codeUpgradeRequired ||
			!strings.Contains(w.Body.String(), "最新版本") {
			t.Fatalf("%s %s: status=%d body=%s", route.method, route.path, w.Code, w.Body.String())
		}
	}
	if pool.calls != 0 || len(scanner.discovery) != 0 || len(st.puts) != 0 {
		t.Fatal("retired endpoint performed legacy work")
	}
}

func TestExactDeliveryEveryConsumerKeyUsesExplicitExternalIdentity(t *testing.T) {
	srv, handler, pool, st, _ := rig(t, types.ModePool)
	pool.public = sched.PublicState{Enabled: true, Open: true, Accounts: 9, Threshold: 6}
	for _, key := range []string{srv.ConsumerKeyFor(st.accounts[0]), srv.OwnerKeyFor(st.owners[0]), "akey", "ckey", srv.PublicKey()} {
		for _, query := range []string{"", "?account=own", "?gateway=unified-142"} {
			if w := do(handler, authed(httptest.NewRequest("GET", "/cookie"+query, nil), key)); w.Code != 400 {
				t.Fatalf("missing explicit selection accepted: %d %s", w.Code, w.Body.String())
			}
		}
		for _, member := range []string{st.accounts[0].Creds.AccountID, "new/member"} {
			w := do(handler, authed(httptest.NewRequest("GET", "/cookie?account="+member+"&gateway=unified-142", nil), key))
			if w.Code != 200 {
				t.Fatalf("explicit consumer rejected: %d %s", w.Code, w.Body.String())
			}
		}
	}
	if len(pool.delivered) != 0 || len(pool.anon) != 10 {
		t.Fatalf("a key entered stored-account scheduling: %+v %+v", pool.delivered, pool.anon)
	}
	for _, key := range []string{"", "wrong", "vt-1"} {
		w := do(handler, authed(httptest.NewRequest("GET", "/cookie?account=external&gateway=unified-142", nil), key))
		if w.Code != 401 || decodeFail(t, w).Error.Code != codeConsumerRejected {
			t.Fatal("invalid key was not rejected at the authentication boundary", w.Code)
		}
	}
}

func TestExactDeliveryCookieVersionDeadlineAndInputBoundaries(t *testing.T) {
	srv, handler, pool, st, _ := rig(t, types.ModePool)
	key := srv.ConsumerKeyFor(st.accounts[0])
	pool.delivery.ExpiresAt = time.Now().UTC().Add(40 * time.Minute)
	pool.delivery.ValidForS, pool.delivery.VerifiedFull = 2400, false
	path := "/cookie?account=external&gateway=unified-142"
	w := do(handler, authed(httptest.NewRequest("GET", path, nil), key))
	var delivery deliveryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &delivery); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || delivery.CookieVersion != types.CookieVersion(liveCookie) ||
		!delivery.RouteExpiresAt.Equal(pool.delivery.ExpiresAt) || delivery.PairRemainingS < 2398 ||
		delivery.VerifiedFull || !delivery.TTLIsAdvisory {
		t.Fatalf("incorrect exact-pair metadata: %+v status=%d", delivery, w.Code)
	}
	for _, query := range []string{
		"&wait=0", "&count=1", "&force=0", "&min_remaining=no",
		"&exclude=" + strings.Repeat("g,", maxExcludeItems),
		"&exclude_versions=" + strings.Repeat("v,", maxExcludeItems),
	} {
		w = do(handler, authed(httptest.NewRequest("GET", path+query, nil), key))
		if w.Code != 400 || decodeFail(t, w).Error.Code != codeBadRequest {
			t.Fatal("bad parameter accepted", query, w.Code)
		}
	}
	for raw, want := range map[string]time.Duration{"1": 30 * time.Second, "99999": time.Hour, "9223372036854775807": time.Hour} {
		w = do(handler, authed(httptest.NewRequest("GET", path+"&min_remaining="+raw, nil), key))
		if w.Code != 200 || pool.reqs[len(pool.reqs)-1].MinRemaining != want {
			t.Fatal("lifetime not safely clamped", raw)
		}
	}
	for _, member := range []string{strings.Repeat("x", 129), "x%00y", "x%0Ay"} {
		w = do(handler, authed(httptest.NewRequest("GET", "/cookie?account="+member+"&gateway=unified-142", nil), key))
		if w.Code != 400 {
			t.Fatal("invalid member accepted")
		}
	}
	pool.err = errors.New("unavailable internal state")
	w = do(handler, authed(httptest.NewRequest("GET", path, nil), key))
	if w.Code != 500 || decodeFail(t, w).Error.Code == codeNoLivePair {
		t.Fatal("internal failure disguised as missing stock")
	}
}

func TestExactDeliveryMemberLimitIsSharedAcrossKeysAndAuthFailuresDoNotConsumeIt(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	now := time.Now().UTC()
	if _, err := st.UpsertGateway("unified-142", 0, types.Exit{}); err != nil {
		t.Fatal(err)
	}
	if err := st.PutPair(types.Pair{Gateway: "unified-142", Cookie: liveCookie, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	pool := sched.New(st, nil, sched.Options{Now: func() time.Time { return now }})
	srv, err := New(pool, st, nil, Options{AdminKey: "akey", ConsumerKey: "ckey"})
	if err != nil {
		t.Fatal(err)
	}
	handler := srv.Handler()
	path := "/cookie?account=workspace/member&gateway=unified-142"
	for range 12 {
		if w := do(handler, authed(httptest.NewRequest("GET", path, nil), "wrong")); w.Code != 401 {
			t.Fatal(w.Code)
		}
	}
	for i := range 12 {
		key := "akey"
		if i%2 == 0 {
			key = "ckey"
		}
		if w := do(handler, authed(httptest.NewRequest("GET", path, nil), key)); w.Code != 200 {
			t.Fatal(i, w.Code, w.Body.String())
		}
	}
	w := do(handler, authed(httptest.NewRequest("GET", path, nil), "ckey"))
	if w.Code != 429 || w.Header().Get("Retry-After") != "1" {
		t.Fatal(w.Code, w.Body.String())
	}
	for i := range 401 {
		w = do(handler, authed(httptest.NewRequest("GET", fmt.Sprintf("/cookie?account=other-%d&gateway=unified-142", i), nil), "akey"))
		if w.Code != 200 {
			t.Fatal("consumer count became an admission gate", i, w.Code)
		}
	}
	if len(st.Accounts()) != 0 || len(st.Slots()) != 0 || len(st.ExternalUsage()) != 402 {
		t.Fatal("consumer delivery stored scheduling rows instead of independent statistics")
	}
}

func TestExternalCookieProtocolIsExactAndDoesNotWait(t *testing.T) {
	srv, handler, pool, st, scanner := rig(t, types.ModePool)
	key := srv.ConsumerKeyFor(st.accounts[0])
	for _, query := range []string{
		"account=external", "account=external&gateway=unified-142&force=1",
		"account=external&gateway=unified-142&wait=30", "account=external&gateway=unified-142&count=1",
	} {
		w := do(handler, authed(httptest.NewRequest("GET", "/cookie?"+query, nil), key))
		if w.Code != http.StatusBadRequest || pool.calls != 0 {
			t.Fatalf("invalid external selection reached pool: query=%s status=%d calls=%d", query, w.Code, pool.calls)
		}
	}
	pool.err = sched.ErrNoFullSlot
	w := do(handler, authed(httptest.NewRequest("GET", "/cookie?account=external&gateway=unified-142", nil), key))
	if w.Code != http.StatusServiceUnavailable || pool.calls != 1 || len(scanner.discovery) != 0 {
		t.Fatalf("missing stock retried or scheduled discovery: status=%d calls=%d discovery=%v", w.Code, pool.calls, scanner.discovery)
	}
	pool.err = &sched.ConsumerRateLimit{RetryAfter: time.Second + time.Nanosecond}
	w = do(handler, authed(httptest.NewRequest("GET", "/cookie?account=external&gateway=unified-142", nil), key))
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "2" ||
		!strings.Contains(w.Body.String(), `"code":"consumer_rate_limited"`) {
		t.Fatalf("wrong consumer rate refusal: status=%d headers=%v body=%s", w.Code, w.Header(), w.Body.String())
	}
}

func TestExternalCatalogIgnoresHistoricalAccountSlots(t *testing.T) {
	srv, handler, _, st, _ := rig(t, types.ModePool)
	st.accounts = append(st.accounts, types.Account{
		ID: 50, Creds: types.Creds{AccountID: "anon:external"}, Enabled: true,
	})
	st.slots["50|unified-142"] = types.Slot{
		AccountID: 50, Gateway: "unified-142", LastTouch: time.Now(),
	}
	w := do(handler, authed(httptest.NewRequest("GET", "/gateways?account=external", nil), srv.ConsumerKeyFor(st.accounts[0])))
	if w.Code != http.StatusOK || strings.Contains(w.Body.String(), `"used_by_you":true`) ||
		strings.Contains(w.Body.String(), `"last_used_at"`) {
		t.Fatalf("historical scheduling state leaked into external catalog: status=%d body=%s", w.Code, w.Body.String())
	}
}
