package mint

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

func TestCountryAttemptsShareCityAliases(t *testing.T) {
	cfg := config.Defaults()
	cfg.UpstreamPerRegionHour = 2
	c := &Client{Live: config.NewLive("", cfg)}
	if err := c.spend("one-account", types.Exit{Region: "US"}); err != nil {
		t.Fatal(err)
	}
	if err := c.spend("one-account", types.Exit{Region: "US", City: "Seattle"}); err != nil {
		t.Fatal(err)
	}
	if err := c.spend("one-account", types.Exit{Region: "US"}); !errors.Is(err, types.ErrRateLimited) {
		t.Fatalf("country-only and city exits must share the two-attempt bucket: %v", err)
	}
}

func TestCountryAttemptsReserveBeforeBindingAndReuseExactlyOnce(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	hits := 0
	c, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusBadGateway)
	})
	c.Attempts = st
	cr, exit := testCreds, types.Exit{Region: "US"}
	ctx, err := c.BeginAttempt(context.Background(), cr, exit)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = c.BeginAttempt(ctx, cr, types.Exit{Region: "US", City: "Seattle"})
	if err != nil || st.AttemptUsage(cr.AccountID, time.Now())["US"] != 1 {
		t.Fatal("same country pre-reservation was counted twice", err)
	}
	_, _ = c.Mint(ctx, cr, exit, "fake-sid", "", time.Time{})
	if st.AttemptUsage(cr.AccountID, time.Now())["US"] != 1 {
		t.Fatal("Mint charged the pre-reserved attempt twice")
	}
	_, _ = c.VerifyFull(ctx, cr, exit, "fake-sid", "cookie", "state")
	if st.AttemptUsage(cr.AccountID, time.Now())["US"] != 2 {
		t.Fatal("the extra echo request must consume its own attempt")
	}
	if _, err := c.Mint(ctx, cr, exit, "fake-sid", "", time.Time{}); !errors.Is(err, types.ErrRateLimited) {
		t.Fatalf("reusing a consumed context bypassed quota: %v", err)
	}
	if hits != 2 {
		t.Fatalf("unexpected upstream hits: %d", hits)
	}
}

func TestCountryAttemptsUnsupportedAndCanceledNeverSend(t *testing.T) {
	hits := 0
	c, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) { hits++; w.WriteHeader(200) })
	for _, country := range []string{"HK", "AQ", "ZZ", ""} {
		if _, err := c.Mint(context.Background(), testCreds, types.Exit{Region: country}, "", "", time.Time{}); !errors.Is(err, types.ErrUnsupportedCountry) {
			t.Fatalf("unsupported country %q: %v", country, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.BeginAttempt(ctx, testCreds, types.Exit{Region: "US"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if used, _ := c.Rate(testCreds.AccountID); hits != 0 || len(used) != 0 {
		t.Fatalf("blocked requests consumed budget/network: %v hits=%d", used, hits)
	}
}

func TestCountryAttemptsHotReloadZeroUsesDefaultNotStartupLimit(t *testing.T) {
	cfg := config.Defaults()
	cfg.UpstreamPerRegionHour = 0
	c := &Client{UpstreamPerRegionHour: 6, Live: config.NewLive("", cfg)}
	for range 2 {
		if err := c.spend("one", types.Exit{Region: "US"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.spend("one", types.Exit{Region: "US"}); !errors.Is(err, types.ErrRateLimited) {
		t.Fatal("hot-reload 0 restored startup limit instead of default 2", err)
	}
}
