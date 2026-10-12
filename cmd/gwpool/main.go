// Command gwpool 跑网关池：扫描托管账号能到达的网关，收它们的路由 cookie，按需交付。
//
// 全部配置从 JSON 文件读。代理凭据和 access_token 同级敏感，不走命令行参数——
// argv 会被 ps 看见（SPEC 第 5 节）。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gwpool/internal/authrefresh"
	"gwpool/internal/config"
	"gwpool/internal/httpapi"
	"gwpool/internal/mint"
	"gwpool/internal/scan"
	"gwpool/internal/sched"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

func main() {
	path := flag.String("config", "config.json", "配置文件路径")
	checkpointOnly := flag.Bool("checkpoint-only", false, "仅重放并持久化状态快照后退出；必须先停服务")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	runCommand := run
	if *checkpointOnly {
		runCommand = checkpointOnlyRun
	}
	if err := runCommand(*path, log); err != nil {
		log.Error("退出", "err", err)
		os.Exit(1)
	}
}

// Offline maintenance only: no minter, listener or background scheduler exists.
// Deployment rollback uses this before switching to a snapshot-only old binary.
func checkpointOnlyRun(path string, _ *slog.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	st, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	checkpointErr := st.Checkpoint()
	return errors.Join(checkpointErr, st.Close())
}

// newMinter 按配置搭铸票客户端。
//
// 出口代理传**全集**：不是只传粘性的——Client.NewSID(true) 自己会挑粘性出口，而只传
// 粘性的会让 webshare 那批接入点永远走不到（unified-121 / 71 只有它能摸到，那正是它
// 存在的理由）；也不是只传启用的——停用的不交进去，重启后管理页上就没有它，再也点不回
// 「启用」。挑出口时 resolve 自己会跳过停用的。
func newMinter(cfg config.Config, live *config.Live, log *slog.Logger) *mint.Client {
	return &mint.Client{
		Proxies:     cfg.Proxies,
		StickyMin:   cfg.StickyMinutes,
		Fingerprint: cfg.Fingerprint,
		Model:       cfg.Model,

		UpstreamPerRegionHour:  cfg.UpstreamPerRegionHour,
		UpstreamPerAccountHour: cfg.UpstreamPerAccountHour,
		Live:                   live,
		Log:                    log, // 只用来记出口熔断的开合
	}
}

func run(path string, log *slog.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	st, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	if skipped, err := st.MigrateAttempts(); err != nil {
		return fmt.Errorf("迁移国家尝试账本: %w", err)
	} else if skipped > 0 {
		log.Warn("国家尝试迁移跳过损坏的历史样本", "skipped", skipped)
	}

	// 配置的活引用：管理页改旋钮、或者有人直接编辑文件，都当场生效，不用重启。
	// 重启会丢掉进程里的速率窗口、铸票计数、需求时间戳——而调度正依据那些。
	live := config.NewLive(path, cfg)

	minter := newMinter(cfg, live, log)
	minter.Attempts = st
	refreshExpired := authrefresh.ForRequests(st)
	pool := sched.New(st, minter, sched.Options{
		Mode:           cfg.Mode,
		RefreshExpired: refreshExpired,
		// 验满血、续期、槽位冷却、对外开放门槛这几个旋钮由调度**现读** live，
		// 改配置不用重启（管理页上那个按钮写的就是「保存并立即生效」）。
		Live: live,
		Log:  log,
	})
	queue := newDiscoveryQueue(&scan.Scanner{Mint: minter, Store: st, Log: log, RefreshExpired: refreshExpired}, pool, log)
	mintAll := sched.NewMintAllQueue(pool)

	api, err := httpapi.New(pool, st, queue, httpapi.Options{
		AdminTrustLoopbackProxy: true,
		ConsumerKey:             cfg.ConsumerKey.Reveal(),
		AdminKey:                cfg.AdminKey.Reveal(),
		SiteKey:                 cfg.SiteKey.Reveal(),
		Model:                   cfg.Model,
		// 接了 Live 之后页面现读它，这个只是启动时那一份兜底（见 Server.publicBaseURL）。
		PublicBaseURL: cfg.PublicBaseURL,
		Live:          live,
		MintAll:       mintAll,
		ProxyStates:   minter.ProxyStates,
		// 管理页加出口、停用出口：运行中只往末尾加、只改启用标志，sid 绑的下标不会挪。
		AddProxy:        minter.AddProxy,
		SetProxyEnabled: minter.SetProxyEnabled,
		ProbeProxy:      minter.ProbeProxy,
		Log:             log,
	})
	if err != nil {
		return err
	}
	// Run on every return path, including failed/expired HTTP shutdown. Give
	// canceled login transports their own bounded drain budget before st.Close.
	defer func() {
		drain, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		if err := api.ShutdownUploads(drain); err != nil {
			log.Error("关闭上传任务失败", "err", err)
		}
	}()
	defer func() {
		if err := api.FlushKeyMetrics(); err != nil {
			log.Error("关闭时保存 Key 统计失败", "err", err)
		}
	}()
	// consumer_key 同时是每账号 key 的派生密钥：空的话上传者 key 都发不出来，
	// /cookie 会对所有人 401。宁可吵一句，也别让人对着 401 猜半天。
	if cfg.ConsumerKey == "" {
		log.Warn("配置里没有 consumer_key：发不出账号 key，/cookie 会全部 401")
	}
	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: api.Handler(),
		// 上传/管理操作可能等待上游，保留原写超时策略；取票只读库存。
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	metricsStopped := make(chan struct{})
	go func() { defer close(metricsStopped); api.RunKeyMetrics(ctx) }()
	defer func() { stop(); <-metricsStopped }()
	go pool.Run(ctx)    // ModeSingle 时立刻返回：单账号模式绝不保温
	go mintAll.Run(ctx) // 仅管理员主动入队后才铸票。
	go queue.run(ctx)
	go live.Watch(ctx, 0, log)   // 配置文件变了就热重载
	go refreshLoop(ctx, st, log) // access_token 只活约九天，有 refresh 的号自己续

	log.Info("启动", "listen", cfg.Listen, "mode", cfg.Mode, "data", cfg.DataDir,
		"model", cfg.Model,
		"accounts", len(st.Accounts()), "gateways", len(st.Gateways()),
		"public_when_surplus", cfg.PublicWhenSurplus.Enabled,
		"min_accounts", cfg.PublicWhenSurplus.MinAccounts)

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	log.Info("收到退出信号，优雅关闭")
	shut, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shut); err != nil {
		return fmt.Errorf("关闭 HTTP: %w", err)
	}
	return nil
}

// discoveryQueue 把 httpapi 的「去找个新落点」接到 scan.Scanner 上。
//
// 为什么要排队：那一发真打上游，几秒到几十秒，HTTP handler 不能等它
// （要票的人这一次已经 503 了，这一发是给下一个人备的）。
// 为什么只有一个 worker：串行跑是最省心的速率纪律（2026-10-01 的教训是
// 高频铸票会把供给打空）。
//
// 派谁去哪个区域由 sched.PickMinter 定（按 (账号 × 区域) 台账 + 本小时配额），
// 这儿不自己判——那套规则只该有一份。
type discoveryQueue struct {
	ch   chan job
	sc   *scan.Scanner
	pool *sched.Scheduler
	log  *slog.Logger

	mu       sync.Mutex
	lastSent map[int64]time.Time // 账号 → 上次被派出去的时间
}

// job 是一发裸铸：谁、经哪个区域。
type job struct {
	acct types.Account
	r    types.Region
}

// discoverCooldown 是同一个账号两次被派去找落点的最小间隔。
//
// 那一发**会烧掉执行者在它落到的那个网关上的窗口**。没有节流的话一串 503 就能把
// 同一个号反复派出去，把它自己的槽位烧干。
// 进程内存态、重启清零：它是速率纪律，不是账本。
const discoverCooldown = 30 * time.Minute

func newDiscoveryQueue(sc *scan.Scanner, pool *sched.Scheduler, log *slog.Logger) *discoveryQueue {
	return &discoveryQueue{ch: make(chan job, 64), sc: sc, pool: pool, log: log,
		lastSent: map[int64]time.Time{}}
}

// EnqueueDiscovery 见 httpapi.Scanner 的注释：派**别人**去找新网关。绝不阻塞。
// 回空串 = 排进去了；非空 = 没派出去的原因（给管理页那个按钮如实回话用）。
func (q *discoveryQueue) EnqueueDiscovery(exclude int64) string {
	a, r, ok := q.pool.PickMinter(exclude)
	if !ok {
		// 池子里只有要票的这一个号，或者剩下的号九个区域本小时都打满了。
		// 派要票的人自己去是净亏：它找到的落点被那一发当场烧掉，拿到票时已经不是首次接触。
		//
		// **Info 不是 Debug**：这正是单账号冷启动的样子（唯一的号就是消费者 ⇒ 永远
		// 挑不出铸票者），而那时候池子自己走不出来，必须有人再上传一个号或者手动
		// POST /admin/rescan。默认看不见这行的话，现象就只是「一直 503」。
		q.log.Info("没有可派去找新网关的账号（池子自己走不出来，要再上传一个号或手动 /admin/rescan）",
			"asked_by", exclude)
		return "挑不出可以派去找新网关的账号：池子里只有要票的这一个号，或者剩下的号九个区域本小时都打满了"
	}
	// 冷却从**入队**算起，不是从开跑算起：队列堵着的时候按开跑计时会让同一个号
	// 连排三次，等轮到它时三次都已经过了冷却。
	q.mu.Lock()
	cooling := time.Since(q.lastSent[a.ID]) < discoverCooldown
	if !cooling {
		q.lastSent[a.ID] = time.Now()
	}
	q.mu.Unlock()
	if cooling {
		q.log.Debug("挑中的号还在发现冷却里", "asked_by", exclude, "scanner", a.ID)
		return fmt.Sprintf("挑中的号还在 %v 的发现冷却里，等它出来再踢", discoverCooldown)
	}

	q.log.Info("槽位烧完，派别的账号去找新网关",
		"asked_by", exclude, "scanner", a.ID, "label", a.Label, "region", r.Key)
	select {
	case q.ch <- job{acct: a, r: r}:
		return ""
	default:
		q.log.Warn("发现队列满，丢弃", "account", a.ID)
		return "发现队列满了，这一发被丢弃"
	}
}

func (q *discoveryQueue) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case j := <-q.ch:
			q.discover(ctx, j)
		}
	}
}

func (q *discoveryQueue) discover(ctx context.Context, j job) {
	if !q.pool.RegionEnabled(j.r) {
		q.log.Info("发现任务跳过已停用出口", "account", j.acct.ID, "region", j.r.Key)
		return
	}
	landed, err := q.sc.ScanRegion(ctx, j.acct, j.r)
	// 日志只记账号 ID、备注和落点，凭据一个字不进（SPEC 红线 1）。
	q.log.Info("发现铸票结束", "account", j.acct.ID, "label", j.acct.Label, "region", j.r.Key,
		"gateway", landed.Gateway, "is_new", landed.IsNew, "err", err)
}

// 编译期钉住：这几个具体类型必须满足各包自己定义的窄接口。
// 对不上就在这里断，而不是在运行时。
var (
	_ sched.Store     = (*store.Store)(nil)
	_ httpapi.Store   = (*store.Store)(nil)
	_ httpapi.Scanner = (*discoveryQueue)(nil)
	_ httpapi.Pool    = (*sched.Scheduler)(nil)
	_ sched.Minter    = (*mint.Client)(nil)
	_ types.Minter    = (*mint.Client)(nil)
)
