// Package scan 用一个账号经**一个**区域的代表出口裸打一发，看它落到哪个网关。
//
// 立项前提就在这里：换一个账号、同样的区域，可能看到不同的节点集
// （2026-10-01 实测一个 Plus 号净增 5 个可点名网关）。
//
// **刻意只有单区域入口**：早先这里是「上传凭据后立刻九个区域扫一遍」，
// 2026-10-01 删掉了——那九发在没有任何需求的时候就打出去（成本不为零、还烧掉
// 执行者在九个落点上的窗口），而真正需要新落点的时刻是「槽位烧完了」。
// 现在挑谁去哪个区域打这一发归 sched.PickMinter（按 (账号 × 区域) 台账挑）。
package scan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"gwpool/internal/store"
	"gwpool/internal/types"
)

// 默认节奏。都照 geo_sampler.py 的实测值。
const (
	defaultBackoff  = 5 * time.Second
	defaultMaxTries = 3
)

// Minter 是 scan 需要的铸票能力：types.Minter 再加上挑出口的两个方法
// （types.Minter 的签名已定稿，加不进去）。接口是为了能用 fake 跑测试：
// 测试绝不打真实上游（SPEC 红线 8）。实现是 *mint.Client。
type Minter interface {
	types.Minter

	// NewSID 挑一条出口并返回绑定到它的 sid。sticky=false 时允许用非粘性出口。
	NewSID(sticky bool) (string, error)
	// Sticky 说明这个 sid 的出口能不能钉住同一个 IP。钉不住就别记地理。
	Sticky(sid string) bool
}

// Scanner 扫描账号。一个实例可以并发服务多个账号，但**单账号并发恒为 1**。
type Scanner struct {
	Mint  Minter
	Store *store.Store

	Backoff  time.Duration // 失败退避基数，按 2^n 翻倍
	MaxTries int           // 单个区域最多试几次

	// Coarse 打开后允许用非粘性出口（webshare 这类每连接换 IP 的）。
	// 代价是**查不到可信地理**：Geo 查的不是铸票那一发的 IP，所以整段跳过、
	// 样本的 Observed 留空。换来的是另一批可达网关——webshare 的机房 IP 打到过
	// unified-121 / 71，而 novproxy 的 117 个住宅样本一次都没碰到这两个。
	Coarse bool

	// Log 记每个区域的失败原因。为空时不记。
	// 只汇总 failures=N 是查不出问题的：2026-10-01 首扫九个区域全败，
	// 汇总行里 err=<nil>，真正的上游报错被 continue 吞得一干二净。
	Log *slog.Logger
}

// ErrAccountRejected 表示上游否决了这个账号本身（400/401/403），不是这一次请求的事。
// 碰到它就别重试、别接着扫，直接停用——见 types.RejectedByUpstream。
var ErrAccountRejected = errors.New("上游否决了这个账号")

// Landing 是这一发落在哪。
type Landing struct {
	Gateway string
	IsNew   bool // 对池子是全新的网关
}

// ScanRegion 用 acct 经 r 的代表出口裸打一发。失败按 2^n 指数退避重试，
// 不快重试（SPEC 红线 7）。
//
// 每发先 Geo 再 Mint，两者共用一个 sid ⇒ 同一条粘性会话、同一个出口 IP，
// 所以样本里的 Observed 确实是铸票那一发的地理（SPEC 红线 6）。
//
// 上游否决这个账号（400/401/403）就当场停用：那不是这一发的事，再派它去
// 别的区域只是把同一句话再听八遍（2026-10-01 的二十七发就是这么浪费掉的）。
func (s *Scanner) ScanRegion(ctx context.Context, acct types.Account, r types.Region) (Landing, error) {
	if s.Mint == nil || s.Store == nil {
		return Landing{}, errors.New("Scanner 的 Mint 和 Store 都必须设置")
	}
	// 锁必须是 store 上那一把：扫描和交付是两个包，各拿一张锁表等于没锁
	// （SPEC 红线 2，单账号并发硬性 1）。
	unlock := s.Store.LockAccount(acct.ID)
	defer unlock()

	var lastErr error
	tries := 0
	for try := 0; try < s.maxTries(); try++ {
		if err := ctx.Err(); err != nil {
			return Landing{}, err
		}
		if try > 0 {
			if err := sleep(ctx, s.backoff()<<(try-1)); err != nil {
				return Landing{}, err
			}
		}
		tries++
		l, err := s.attempt(ctx, acct, r)
		if err == nil {
			return l, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return Landing{}, ctx.Err()
		}
		if errors.Is(err, ErrAccountRejected) {
			if derr := s.Store.DisableAccount(acct.ID, err.Error()); derr != nil && s.Log != nil {
				s.Log.Error("自动停用失败", "account", acct.ID, "err", derr)
			}
			if s.Log != nil {
				s.Log.Warn("账号被上游否决，已自动停用", "account", acct.ID, "err", err)
			}
			return Landing{}, err
		}
		// **只有本地/传输层的失败值得重试**：代理抽风、连接断掉重打一发有意义，
		// 而上游已经答过话（429、5xx、或者 200 但读不出落点）时重试换不来别的答案——
		// 每一发都要过速率闸、都可能烧掉一个落点，而上游正在限流时连打三发
		// 恰好是把配额往最没用的地方花。速率闸和出口熔断同理：重试三轮只是把
		// 同一句话抄三遍。
		if errors.Is(err, errAnswered) ||
			errors.Is(err, types.ErrRateLimited) || errors.Is(err, types.ErrNoExit) {
			break
		}
	}
	if s.Log != nil {
		s.Log.Warn("区域扫描失败", "account", acct.ID, "region", r.Key, "tries", tries, "err", lastErr)
	}
	return Landing{}, fmt.Errorf("区域 %s 打了 %d 发仍失败: %w", r.Key, tries, lastErr)
}

// errAnswered 标记「上游已经答过话了」，调用方据此不再重试。
//
// 和 ErrAccountRejected 分开：那个是「这个账号不行」（要停用），这个是
// 「这一发不行，但重试也不行」（限流、5xx、200 但没落点）。
var errAnswered = errors.New("上游已经答复，重试换不来别的答案")

// attempt 是一发：同一个 sid 先查地理再铸票，于是两者走的是同一个出口 IP。
func (s *Scanner) attempt(ctx context.Context, acct types.Account, r types.Region) (Landing, error) {
	sid, err := s.Mint.NewSID(!s.Coarse)
	if err != nil {
		return Landing{}, err
	}
	var obs types.Observed
	// 非粘性出口每连接换 IP，查到的地理不是铸票那一发的，宁可留空也不记一个假的。
	if s.Mint.Sticky(sid) {
		if obs, err = s.Mint.Geo(ctx, r.Exit, sid); err != nil {
			return Landing{}, fmt.Errorf("查 geo: %w", err)
		}
	}

	// 裸打：扫描要的就是让上游按出口地理重新分一个网关，带 pair 会把落点钉住。
	// 扫描恒为裸铸：没有送进去的 pair，也就没有「旧 __cflb 还活着吗」这个问题。
	res, mintErr := s.Mint.Mint(ctx, acct.Creds, r.Exit, sid, "", time.Time{})

	// 样本先落盘再说：失败的那一发（HTTP 429、读不出落点）也是数据，
	// samples.jsonl 只追加、永不重写。Requested 和 Observed 两列都留着，事后能查回退。
	now := time.Now().UTC()
	sm := types.Sample{
		AccountID: acct.ID,
		RegionKey: r.Key,
		Requested: r.Exit,
		Observed:  obs,
		Gateway:   res.Gateway,
		Status:    res.Status,
		At:        now,
		Err:       types.SampleErr(res.ErrBody, mintErr),
	}
	if err := s.Store.AppendSample(sm); err != nil {
		return Landing{}, fmt.Errorf("落样本: %w", err)
	}
	if mintErr != nil {
		// **带着状态码回来的失败要先按状态码分类**，不能原样抛出去。
		//
		// 原样抛的那一版（2026-10-02 现场）漏了两件事，因为这条 return 在下面那段状态码
		// 分类**之前**：401 的裸铸只下发 `__cflb`、没有 `__oailb`（网关身份在它里面）⇒
		// mint 报 ErrNoLanding ⇒ 这里早返回 ⇒ 下面那段 RejectedByUpstream 永远到不了。
		// 于是
		//   - 外层那个三发重试照打（它只认 errAnswered / ErrAccountRejected / 限流 / 无出口），
		//     而每一发都过速率闸、碰到过上游就**不退款**，(账号 × 区域) 每小时只有 4 发
		//     ⇒ 一个凭据作废的账号能把某个区域整小时预算吃掉 3/4；
		//   - 账号也永远不会被自动停用，于是下一轮再来一次。
		// 现场读数：当天 40 发「读不出落点」里 25 发来自同一个 401 的测试克隆。
		//
		// 分类口径和下面那段**逐字一致**（400/401/403 判账号、其余非 200 判「已答复」）：
		// 两处用同一条判据，不另立一套。
		if res.Status != 0 && res.Status != http.StatusOK {
			msg := fmt.Sprintf("HTTP %d", res.Status)
			if res.ErrBody != "" {
				msg += "：" + res.ErrBody
			}
			if types.RejectedByUpstream(res.Status) {
				return Landing{}, fmt.Errorf("%w（%s）", ErrAccountRejected, msg)
			}
			return Landing{}, fmt.Errorf("铸票: %w（%s，%w）", mintErr, msg, errAnswered)
		}
		return Landing{}, fmt.Errorf("铸票: %w", mintErr)
	}
	// 限流/鉴权失败的响应也可能带 set-cookie。那种落点不是「这个账号能到这个网关」的
	// 证据，记进池子会在限流风暴里把一批网关当成新发现。
	if res.Status != http.StatusOK {
		// 带上上游那段 JSON：没有它的话「HTTP 400」什么都不告诉人。
		msg := fmt.Sprintf("HTTP %d", res.Status)
		if res.ErrBody != "" {
			msg += "：" + res.ErrBody
		}
		if types.RejectedByUpstream(res.Status) {
			return Landing{}, fmt.Errorf("%w（%s）", ErrAccountRejected, msg)
		}
		return Landing{}, fmt.Errorf("%s，不记这个落点（%w）", msg, errAnswered)
	}
	if res.Gateway == "" {
		return Landing{}, fmt.Errorf("铸票没给出落点（%w）", errAnswered)
	}

	// 记**请求过的**出口：ReachableExits 的用途是「下次想回到这个网关该怎么请求」，
	// 观测到的地理是自由文本，回灌成代理参数会认证失败或回退到别国（见 store.UpsertGateway）。
	isNew, err := s.Store.UpsertGateway(res.Gateway, acct.ID, r.Exit)
	if err != nil {
		return Landing{}, fmt.Errorf("记网关: %w", err)
	}
	// 记进 (账号 × 区域) 台账：挑下一个铸票者时「这个号哪个区域还没走过」全靠它，
	// 而没走过的区域才可能有没见过的落点。
	if err := s.Store.NoteRegionMint(acct.ID, r.Key, res.Gateway, now); err != nil {
		return Landing{}, fmt.Errorf("记区域台账: %w", err)
	}
	// 这一发本身就是这个账号对这个网关的一次接触，满血窗口已经开始烧了，记上。
	// 用 UpdateSlot 而不是 PutSlot：别把调度器刚写上的退避和判定覆盖掉。
	if err := s.Store.UpdateSlot(acct.ID, res.Gateway, func(sl *types.Slot) {
		sl.LastTouch = now
		if sl.LastVerdict == "" {
			sl.LastVerdict = types.VerdictUnknown // 扫描不验满血：验证动作本身会烧掉被测网关
		}
	}); err != nil {
		return Landing{}, fmt.Errorf("记槽位: %w", err)
	}
	if res.NewPair != "" {
		// 寿命按**路由**算（钉路由的是 __cflb，它比 __oailb 早死 300 秒，
		// 见 types.MintResult.RouteDeadline）。扫描恒为裸铸 ⇒ 没有「上一张的 __cflb」
		// 可继承，传零值让它走响应头 / 兜底那两级。
		route, cflb := res.RouteDeadline(time.Time{})
		if err := s.Store.PutPair(types.Pair{
			Gateway:       res.Gateway,
			Cookie:        res.NewPair,
			ExpiresAt:     route,
			CflbExpiresAt: cflb,
			MintedBy:      acct.ID,
			// 和上面那条区域台账、那条样本同一个口径：这一发是从 r.Exit 出去的，
			// 它的区域就是 r.Key。漏了它的那一版（到 2026-10-03）让扫描铸出来的票
			// 整片没有大区 ⇒ 消费端收到空 RegionKey、九宫格画成「未归类」
			// （现场：63 张活票里 8 张，8/8 都来自发现铸票）。
			//
			// 用 r.Key 而不是 sched 那边的 sampleRegion(e)：那个兜底是给续期型铸票的
			// —— 它的出口取自 ReachableExits，不保证正好是 Regions 里那九个代表出口。
			// 扫描恒从 r.Exit 出去，r.Key 本来就是那九个之一，不需要兜底。
			RegionKey: r.Key,
		}); err != nil {
			return Landing{}, fmt.Errorf("记 pair: %w", err)
		}
	}
	return Landing{Gateway: res.Gateway, IsNew: isNew}, nil
}

func (s *Scanner) backoff() time.Duration {
	if s.Backoff > 0 {
		return s.Backoff
	}
	return defaultBackoff
}

func (s *Scanner) maxTries() int {
	if s.MaxTries > 0 {
		return s.MaxTries
	}
	return defaultMaxTries
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
