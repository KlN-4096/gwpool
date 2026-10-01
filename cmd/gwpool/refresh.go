package main

import (
	"context"
	"log/slog"
	"time"

	"gwpool/internal/oauth"
	"gwpool/internal/store"
)

const (
	// refreshEvery 是巡检间隔。access_token 活约九天，一小时查一次绰绰有余。
	refreshEvery = time.Hour
	// refreshLead 是提前多久换。留一天的余量：换票会失败（上游抖、代理挂），
	// 失败了还有二十三次重试机会，而不是最后一刻只剩一次。
	refreshLead = 24 * time.Hour
)

// refreshLoop 定期给快到期的 access_token 续票。
//
// 为什么要有这个循环：access_token 只活约九天，没它池子就在第九天静默变成
// 一串 401 —— 账号自动停用，然后等人注意到。有 refresh_token 的号本来就不该
// 需要人管。
//
// 放在 main 而不是 sched 里：sched 管的是 pair 的生命周期，凭据的生命周期是
// 另一回事，塞进去只会让那个循环多一个失败模式。
//
// 换票**直连**上游，不经住宅出口：换票和铸票不同，它不需要出口多样性，
// 而住宅池约一成的会话会摸到死节点（见 mint.do），白添一成失败率。
func refreshLoop(ctx context.Context, st *store.Store, log *slog.Logger) {
	t := time.NewTicker(refreshEvery)
	defer t.Stop()
	refreshDue(ctx, st, log) // 启动即查一次：重启往往就是因为出了问题
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			refreshDue(ctx, st, log)
		}
	}
}

func refreshDue(ctx context.Context, st *store.Store, log *slog.Logger) {
	deadline := time.Now().Add(refreshLead)
	for _, a := range st.Accounts() {
		if a.Creds.RefreshToken == "" || a.Creds.AccessToken == "" {
			continue
		}
		cl, err := oauth.Parse(a.Creds.AccessToken)
		if err != nil {
			log.Warn("账号的 access_token 读不出声明，跳过续票", "account", a.ID, "err", err)
			continue
		}
		// 零值 = 票里没写 exp。不猜，当它还能用——猜错的代价是每小时白换一次票。
		if cl.Expires.IsZero() || cl.Expires.After(deadline) {
			continue
		}
		access, refresh, err := oauth.Refresh(ctx, nil, a.Creds.RefreshToken)
		if err != nil {
			// 不在这里停用：换票失败可能只是上游抖了一下，下个小时还会再试。
			// 真的废了的话，第一次打上游就会回 401，那条路会停用它。
			log.Warn("续票失败", "account", a.ID, "expires", cl.Expires, "err", err)
			continue
		}
		if err := st.SetTokens(a.ID, access, refresh); err != nil {
			log.Error("续票写回失败", "account", a.ID, "err", err)
			continue
		}
		log.Info("续了票", "account", a.ID, "原到期", cl.Expires, "轮换了refresh", refresh != "")
	}
}
