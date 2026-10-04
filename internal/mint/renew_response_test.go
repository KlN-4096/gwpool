package mint

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"gwpool/internal/types"
)

func TestMintDoesNotReuseExpiredOrDeletedRouteCookies(t *testing.T) {
	for _, tc := range []struct {
		name, replacement string
		oailbLifetime     time.Duration
	}{
		{"expired oailb", "", -time.Minute},
		{"deleted cflb", "__cflb=; Max-Age=0; Path=/", time.Hour},
		{"deleted oailb", "__oailb=; Max-Age=0; Path=/", time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
				if tc.replacement != "" {
					w.Header().Add("Set-Cookie", tc.replacement)
				}
				w.WriteHeader(http.StatusOK)
			})
			sent := "__cflb=old; __oailb=" + fakeOailb("unified-7", time.Now().Add(tc.oailbLifetime))
			got, err := client.Mint(context.Background(), testCreds, types.Exit{Region: "US"}, "sid", sent, liveCflb())
			if !errors.Is(err, ErrNoLanding) || got.Gateway != "" || got.NewPair != "" {
				t.Fatalf("失效 cookie 被当作可证实路由：gateway=%q, err=%v", got.Gateway, err)
			}
		})
	}
}

func TestMintCflbOnlyRefreshDoesNotProveOldGateway(t *testing.T) {
	exp := time.Now().Add(30 * time.Minute).Truncate(time.Second)
	client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add("Set-Cookie", "__cflb=new; Max-Age=3600; Path=/")
		w.WriteHeader(http.StatusOK)
	})
	sent := "__cflb=old; __oailb=" + fakeOailb("unified-7", exp)
	got, err := client.Mint(context.Background(), testCreds, types.Exit{Region: "US"}, "sid", sent, liveCflb())
	if !errors.Is(err, ErrNoLanding) || got.Gateway != "" || got.NewPair != "" {
		t.Fatalf("仅更新亲和 cookie 就推断仍在旧网关：gateway=%q, err=%v", got.Gateway, err)
	}
}
