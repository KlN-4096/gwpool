// Package ticket contains pure cookie projection shared by minting, durable
// inventory commits and delivery. It never makes requests or changes deadlines.
package ticket

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gwpool/internal/types"
)

const (
	cookieCflb  = "__cflb"
	cookieOailb = "__oailb"
)

type CflbLease struct {
	Cflb string
	Exp  time.Time
}

func Parse(pair string) map[string]string {
	out := map[string]string{}
	for _, segment := range strings.Split(pair, ";") {
		key, value, ok := strings.Cut(strings.TrimSpace(segment), "=")
		if ok && value != "" {
			out[key] = value
		}
	}
	return out
}

func CflbOf(pair string) string { return Parse(pair)[cookieCflb] }

func Info(pair string) (string, time.Time, error) {
	cookies := Parse(pair)
	if cookies[cookieCflb] == "" {
		return "", time.Time{}, errors.New("缺 " + cookieCflb + "，这张 cookie 钉不住任何网关")
	}
	return OailbInfo(cookies[cookieOailb])
}

// OailbInfo reads claims, not a verified upstream signature. Only actual
// upstream responses establish inventory; this parser cannot authenticate one.
func OailbInfo(token string) (string, time.Time, error) {
	if token == "" {
		return "", time.Time{}, errors.New("空的 __oailb")
	}
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return "", time.Time{}, errors.New("__oailb 不是 JWT")
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("payload 不是 base64url: %w", err)
	}
	var payload struct {
		Host string `json:"host"`
		Exp  int64  `json:"exp"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", time.Time{}, fmt.Errorf("payload 不是 JSON: %w", err)
	}
	segments := strings.Split(payload.Host, ".")
	if len(segments) < 3 || segments[2] == "" {
		return "", time.Time{}, fmt.Errorf("host %q 里没有第三段", payload.Host)
	}
	var expiry time.Time
	if payload.Exp > 0 {
		expiry = time.Unix(payload.Exp, 0).UTC()
	}
	return segments[2], expiry, nil
}

func SwapCflb(pair, cflb string) string {
	cookies := Parse(pair)
	if cflb == "" || cookies[cookieOailb] == "" {
		return pair
	}
	return cookieCflb + "=" + cflb + "; " + cookieOailb + "=" + cookies[cookieOailb]
}

// Best includes separately retained donors as well as legacy Pair provenance.
func Best(pairs map[string]types.Pair, donors map[string]types.RegionDonor, now time.Time) map[string]CflbLease {
	out := map[string]CflbLease{}
	add := func(region, cookie string, expiry time.Time) {
		if region != "" && cookie != "" && expiry.After(now) && expiry.After(out[region].Exp) {
			out[region] = CflbLease{Cflb: cookie, Exp: expiry}
		}
	}
	for _, pair := range pairs {
		add(pair.RegionKey, CflbOf(pair.Cookie), pair.CflbExpiresAt)
	}
	for _, donor := range donors {
		add(donor.RegionKey, donor.Cflb, donor.ExpiresAt)
	}
	return out
}

func Rehome(pair types.Pair, best map[string]CflbLease, now time.Time) types.Pair {
	if pair.Cookie == "" || pair.RegionKey == "" {
		return pair
	}
	donor, ok := best[pair.RegionKey]
	if !ok || !donor.Exp.After(pair.CflbExpiresAt) {
		return pair
	}
	_, fruitExpiry, err := Info(pair.Cookie)
	if err != nil || !fruitExpiry.After(now) {
		return pair
	}
	swapped := SwapCflb(pair.Cookie, donor.Cflb)
	// A response can extend the lease of the same cflb value. The unchanged
	// cookie bytes do not invalidate the newer, observed donor deadline.
	pair.Cookie, pair.CflbExpiresAt = swapped, donor.Exp
	pair.ExpiresAt = donor.Exp
	if fruitExpiry.Before(pair.ExpiresAt) {
		pair.ExpiresAt = fruitExpiry
	}
	return pair
}
