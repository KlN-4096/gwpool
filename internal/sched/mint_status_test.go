package sched

import (
	"context"
	"errors"
	"testing"
	"time"

	"gwpool/internal/mint"
	"gwpool/internal/types"
)

type answeredErrorMinter struct {
	*fakeMinter
	status int
}

func (m *answeredErrorMinter) Mint(context.Context, types.Creds, types.Exit, string, string, time.Time) (types.MintResult, error) {
	return types.MintResult{Status: m.status, RetryAfter: 30 * time.Minute}, mint.ErrNoLanding
}

func TestMintClassifiesHTTPFailureBeforeMissingLanding(t *testing.T) {
	for _, status := range []int{0, 200, 401, 429, 503} {
		t.Run(map[int]string{0: "transport", 200: "parse", 401: "rejected", 429: "limited", 503: "unavailable"}[status], func(t *testing.T) {
			clk := newClock()
			st := newStore(clk)
			addAccount(st, 1, "acct-1")
			mn := &answeredErrorMinter{fakeMinter: newMinter(clk), status: status}
			s := New(st, mn, Options{Mode: types.ModePool, Now: clk.now})
			_, err := s.mintOnce(context.Background(), st.accounts[0], types.Regions[0].Exit,
				"seed1", types.Pair{}, "", "")
			want := ErrMintFailed
			switch status {
			case 401:
				want = ErrUpstreamRejected
			case 429:
				want = types.ErrRateLimited
			}
			if !errors.Is(err, want) {
				t.Fatalf("状态码%d被落点解析错误盖掉：%v", status, err)
			}
			if (len(st.disabled) > 0) != (status == 401) {
				t.Fatalf("错误地处理账号停用：status=%d disabled=%d", status, len(st.disabled))
			}
			if status == 429 {
				if left := s.accountHeldFor(1); left != 30*time.Minute {
					t.Fatalf("429未遵守Retry-After：%s", left)
				}
			} else if s.accountHeldFor(1) != 0 {
				t.Fatal("非429错误不应伪造账号限流")
			}
			if len(st.pairs) != 0 {
				t.Fatal("失败响应不能写成新票")
			}
		})
	}
}
