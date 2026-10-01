// Package scan 扫描一个账号：经九个区域的代表出口各打一发，看它能落到哪些网关。
//
// 立项前提就在这里：换一个账号、同样九个区域，可能看到不同的节点集
// （2026-10-01 实测一个 Plus 号净增 5 个可点名网关）。
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

// Region 是扫描清单上的一项。
//
// 区域划分本身随 cohort 变（账号B 把南亚和中东合并了），所以这只是**扫描清单**，
// 不是数据模型里的强约束（SPEC 第 4 节）。
type Region struct {
	Key  string
	Exit types.Exit
}

// Regions 是 SPEC 第 4 节那张表，代表出口照抄，不要换成「更有代表性」的国家：
// 这些是 2026-10-01 的 170+ 样本里跑出来的，换一个国家可能落到同一个网关、白打一发。
var Regions = []Region{
	{"us-east", types.Exit{Region: "US"}},
	{"us-west", types.Exit{Region: "US", City: "Seattle"}},
	{"south-america", types.Exit{Region: "BR"}},
	{"west-europe", types.Exit{Region: "ES"}},
	{"europe", types.Exit{Region: "IT"}},
	{"east-asia", types.Exit{Region: "JP"}},
	{"oceania", types.Exit{Region: "AU"}},
	{"south-asia", types.Exit{Region: "IN"}},
	{"middle-east", types.Exit{Region: "SA"}},
}

// 默认节奏。都照 geo_sampler.py 的实测值。
const (
	defaultGap       = 3 * time.Second
	defaultBackoff   = 5 * time.Second
	defaultMaxTries  = 3
	defaultStopAfter = 3

	// 样本里的错误原因截到这么多字符：上游那段 JSON 一两百字就说完了。
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

	Gap       time.Duration // 发与发之间的间隔
	Backoff   time.Duration // 失败退避基数，按 2^n 翻倍
	MaxTries  int           // 单个区域最多试几次
	StopAfter int           // 连续多少个区域没发现新网关就停

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

// Result 是一次扫描的汇总。
type Result struct {
	Regions      int      // 实际打出去的区域数
	Samples      int      // 落盘的样本行数
	NewGateways  []string // 本次给池子新增的网关
	StoppedEarly bool     // 落点开始全部重复，提前停了
	Failures     int      // 彻底失败（重试耗尽）的区域数
	Rejected     bool     // 被上游否决，账号已自动停用，剩下的区域没打
}

// ScanAccount 把 Regions 跑一遍。
//
// 每发先 Geo 再 Mint，两者共用一个 sid ⇒ 同一条粘性会话、同一个出口 IP，
// 所以样本里的 Observed 确实是铸票那一发的地理（SPEC 红线 6）。
func (s *Scanner) ScanAccount(ctx context.Context, acct types.Account) (Result, error) {
	if s.Mint == nil || s.Store == nil {
		return Result{}, errors.New("Scanner 的 Mint 和 Store 都必须设置")
	}
	// 锁必须是 store 上那一把：扫描和交付是两个包，各拿一张锁表等于没锁
	// （SPEC 红线 2，单账号并发硬性 1）。
	unlock := s.Store.LockAccount(acct.ID)
	defer unlock()

	var res Result
	repeats := 0
	for i, r := range Regions {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if i > 0 {
			if err := sleep(ctx, s.gap()); err != nil {
				return res, err
			}
		}
		res.Regions++

		landed, err := s.scanRegion(ctx, acct, r)
		if err != nil {
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			// 失败的区域不参与「重复」计数：它什么都没告诉我们。
			res.Failures++
			if s.Log != nil {
				s.Log.Warn("区域扫描失败", "account", acct.ID, "region", r.Key, "err", err)
			}
			// 上游否决这个账号 ⇒ 剩下的区域一样会被否决。当场停用并退出，
			// 别把二十七发全打完（2026-10-01 就是这么浪费掉的）。
			if errors.Is(err, ErrAccountRejected) {
				res.Rejected = true
				if derr := s.Store.DisableAccount(acct.ID, err.Error()); derr != nil && s.Log != nil {
					s.Log.Error("自动停用失败", "account", acct.ID, "err", derr)
				}
				if s.Log != nil {
					s.Log.Warn("账号被上游否决，已自动停用", "account", acct.ID, "err", err)
				}
				return res, nil
			}
			continue
		}
		res.Samples++
		if landed.isNew {
			res.NewGateways = append(res.NewGateways, landed.gateway)
			repeats = 0
			continue
		}
		repeats++
		if repeats >= s.stopAfter() {
			// 这个 cohort 的节点集已经收录过了，继续打只是白烧自己的槽位。
			res.StoppedEarly = true
			return res, nil
		}
	}
	return res, nil
}

// ErrAccountRejected 表示上游否决了这个账号本身（400/401/403），不是这一次请求的事。
// 碰到它就别重试、别接着扫，直接停用——见 types.RejectedByUpstream。
var ErrAccountRejected = errors.New("上游否决了这个账号")

type landing struct {
	gateway string
	isNew   bool
}

// scanRegion 打一个区域。失败按 2^n 指数退避重试，不快重试（SPEC 红线 7）。
func (s *Scanner) scanRegion(ctx context.Context, acct types.Account, r Region) (landing, error) {
	var lastErr error
	for try := 0; try < s.maxTries(); try++ {
		if try > 0 {
			if err := sleep(ctx, s.backoff()<<(try-1)); err != nil {
				return landing{}, err
			}
		}
		l, err := s.attempt(ctx, acct, r)
		if err == nil {
			return l, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return landing{}, ctx.Err()
		}
		// 上游否决这个账号的话，再试两次只是把同一句话再听两遍。
		if errors.Is(err, ErrAccountRejected) {
			return landing{}, err
		}
	}
	return landing{}, fmt.Errorf("区域 %s 重试 %d 次仍失败: %w", r.Key, s.maxTries(), lastErr)
}

// attempt 是一发：同一个 sid 先查地理再铸票，于是两者走的是同一个出口 IP。
func (s *Scanner) attempt(ctx context.Context, acct types.Account, r Region) (landing, error) {
	sid, err := s.Mint.NewSID(!s.Coarse)
	if err != nil {
		return landing{}, err
	}
	var obs types.Observed
	// 非粘性出口每连接换 IP，查到的地理不是铸票那一发的，宁可留空也不记一个假的。
	if s.Mint.Sticky(sid) {
		if obs, err = s.Mint.Geo(ctx, r.Exit, sid); err != nil {
			return landing{}, fmt.Errorf("查 geo: %w", err)
		}
	}

	// 裸打：扫描要的就是让上游按出口地理重新分一个网关，带 pair 会把落点钉住。
	res, mintErr := s.Mint.Mint(ctx, acct.Creds, r.Exit, sid, "")

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
		return landing{}, fmt.Errorf("落样本: %w", err)
	}
	if mintErr != nil {
		return landing{}, fmt.Errorf("铸票: %w", mintErr)
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
			return landing{}, fmt.Errorf("%w（%s）", ErrAccountRejected, msg)
		}
		return landing{}, fmt.Errorf("%s，不记这个落点", msg)
	}
	if res.Gateway == "" {
		return landing{}, errors.New("铸票没给出落点")
	}

	// 记**请求过的**出口：ReachableExits 的用途是「下次想回到这个网关该怎么请求」，
	// 观测到的地理是自由文本，回灌成代理参数会认证失败或回退到别国（见 store.UpsertGateway）。
	isNew, err := s.Store.UpsertGateway(res.Gateway, acct.ID, r.Exit)
	if err != nil {
		return landing{}, fmt.Errorf("记网关: %w", err)
	}
	// 这一发本身就是这个账号对这个网关的一次接触，满血窗口已经开始烧了，记上。
	// 用 UpdateSlot 而不是 PutSlot：别把调度器刚写上的退避和判定覆盖掉。
	if err := s.Store.UpdateSlot(acct.ID, res.Gateway, func(sl *types.Slot) {
		sl.LastTouch = now
		if sl.LastVerdict == "" {
			sl.LastVerdict = "unknown" // 扫描不验满血：验证动作本身会烧掉被测网关
		}
	}); err != nil {
		return landing{}, fmt.Errorf("记槽位: %w", err)
	}
	if res.NewPair != "" {
		if err := s.Store.PutPair(types.Pair{
			Gateway:   res.Gateway,
			Cookie:    res.NewPair,
			ExpiresAt: res.PairExpires,
			MintedBy:  acct.ID,
		}); err != nil {
			return landing{}, fmt.Errorf("记 pair: %w", err)
		}
	}
	return landing{gateway: res.Gateway, isNew: isNew}, nil
}

func (s *Scanner) gap() time.Duration {
	if s.Gap > 0 {
		return s.Gap
	}
	return defaultGap
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

func (s *Scanner) stopAfter() int {
	if s.StopAfter > 0 {
		return s.StopAfter
	}
	return defaultStopAfter
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
