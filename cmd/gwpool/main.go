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
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(*path, log); err != nil {
		log.Error("退出", "err", err)
		os.Exit(1)
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
	defer st.Close()

	// 传**全集**，不是只传粘性的：Client.NewSID(true) 自己会挑粘性出口，
	// 而只传粘性的会让 webshare 那批接入点永远走不到——unified-121 / 71 只有它能摸到，
	// 那正是它存在的理由。
	// 配置的活引用：管理页改旋钮、或者有人直接编辑文件，都当场生效，不用重启。
	// 重启会丢掉进程里的速率窗口、铸票计数、需求时间戳——而调度正依据那些。
	live := config.NewLive(path, cfg)

	minter := &mint.Client{
		Proxies:     cfg.EnabledProxies(),
		StickyMin:   cfg.StickyMinutes,
		Fingerprint: cfg.Fingerprint,
		Model:       cfg.Model,

		UpstreamPerHour: cfg.UpstreamPerHour,
		Live:            live,
		Log:             log, // 只用来记出口熔断的开合
	}
	pool := sched.New(st, minter, sched.Options{
		Mode:              cfg.Mode,
		PublicWhenSurplus: cfg.PublicWhenSurplus.Enabled,
		MinFreeGateways:   cfg.PublicWhenSurplus.MinFreeGateways,
		Log:               log,
	})
	queue := newScanQueue(&scan.Scanner{Mint: minter, Store: st, Log: log}, st, log)

	api, err := httpapi.New(pool, st, queue, httpapi.Options{
		ConsumerKey: cfg.ConsumerKey.Reveal(),
		AdminKey:    cfg.AdminKey.Reveal(),
		SiteKey:     cfg.SiteKey.Reveal(),
		Model:       cfg.Model,
		Live:        live,
		ProxyStates: minter.ProxyStates,
		Log:         log,
	})
	if err != nil {
		return err
	}
	// consumer_key 同时是每账号 key 的派生密钥：空的话一把 key 都发不出来，
	// /cookie 会对所有人 401。宁可吵一句，也别让人对着 401 猜半天。
	if cfg.ConsumerKey == "" {
		log.Warn("配置里没有 consumer_key：发不出账号 key，/cookie 会全部 401")
	}
	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: api.Handler(),
		// 刻意不设 WriteTimeout：一次 /cookie 要真打上游铸票 + state-echo，
		// 实测 20~90 s，设了就会把正常交付切断。只拦慢读头。
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go pool.Run(ctx) // ModeSingle 时立刻返回：单账号模式绝不保温
	go queue.run(ctx)
	go live.Watch(ctx, 0, log)   // 配置文件变了就热重载
	go refreshLoop(ctx, st, log) // access_token 只活约九天，有 refresh 的号自己续

	log.Info("启动", "listen", cfg.Listen, "mode", cfg.Mode, "data", cfg.DataDir,
		"model", cfg.Model,
		"accounts", len(st.Accounts()), "gateways", len(st.Gateways()),
		"public_when_surplus", cfg.PublicWhenSurplus.Enabled,
		"min_free_gateways", cfg.PublicWhenSurplus.MinFreeGateways)

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

// scanQueue 把 httpapi 的「排队首扫」接到 scan.Scanner 上。
//
// 为什么要排队：一次首扫是九个区域、每发间隔 3 秒，分钟级，上传请求不能等它。
// 为什么只有一个 worker：扫描是真打上游，串行跑是最省心的速率纪律
// （2026-10-01 的教训是高频铸票会把供给打空）。
type scanQueue struct {
	ch  chan int64
	sc  *scan.Scanner
	st  *store.Store
	log *slog.Logger

	mu       sync.Mutex
	lastScan map[int64]time.Time // 账号 → 上次被排进队列的时间
}

// scanCooldown 是同一个账号两次扫描之间的最小间隔。
//
// 一次扫描是九发真实上游请求，而且**会烧掉执行者在它扫到的每个新网关上的窗口**。
// 没有节流的话一串 503 就能把同一个号反复派出去，把它自己的槽位全烧干。
// 进程内存态、重启清零：它是速率纪律，不是账本。
const scanCooldown = 30 * time.Minute

func newScanQueue(sc *scan.Scanner, st *store.Store, log *slog.Logger) *scanQueue {
	return &scanQueue{ch: make(chan int64, 64), sc: sc, st: st, log: log,
		lastScan: map[int64]time.Time{}}
}

// Enqueue 绝不阻塞：HTTP handler 调它。队列满了宁可丢，记一行，让人手动补扫。
func (q *scanQueue) Enqueue(id int64) {
	// 冷却从**入队**算起，不是从开跑算起：队列堵着的时候按开跑计时会让同一个号
	// 连排三次，等轮到它时三次都已经过了冷却。
	q.mu.Lock()
	q.lastScan[id] = time.Now()
	q.mu.Unlock()

	select {
	case q.ch <- id:
	default:
		q.log.Warn("首扫队列满，丢弃", "account", id)
	}
}

// EnqueueDiscovery 见 httpapi.Scanner 的注释：派**别人**去找新网关。
func (q *scanQueue) EnqueueDiscovery(exclude int64) {
	id, ok := q.pick(exclude)
	if !ok {
		// 池子里只有要票的这一个号，或者别人都在冷却里。派它自己去扫是净亏：
		// 扫到的每个新网关都被扫描那一发当场烧掉，它拿到票时已经不是首次接触。
		q.log.Info("没有可派去找新网关的账号", "asked_by", exclude)
		return
	}
	q.log.Info("槽位烧完，派别的账号去找新网关", "asked_by", exclude, "scanner", id)
	q.Enqueue(id)
}

// pick 挑一个歇得最久的别的账号。没有合格的就回 false。
func (q *scanQueue) pick(exclude int64) (int64, bool) {
	now := time.Now()
	q.mu.Lock()
	defer q.mu.Unlock()

	var best int64
	var bestAt time.Time
	for _, a := range q.st.Accounts() {
		// 没 token 的是匿名占位行，停用的多半是上游否决过的——派它去只会再废九发。
		if a.ID == exclude || !a.Enabled || a.Creds.AccessToken == "" {
			continue
		}
		at := q.lastScan[a.ID]
		if now.Sub(at) < scanCooldown {
			continue
		}
		// 轮着来：扫描对执行者是有成本的，别总挑同一个倒霉蛋。
		if best == 0 || at.Before(bestAt) {
			best, bestAt = a.ID, at
		}
	}
	return best, best != 0
}

func (q *scanQueue) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-q.ch:
			q.scan(ctx, id)
		}
	}
}

func (q *scanQueue) scan(ctx context.Context, id int64) {
	a, ok := q.st.Account(id)
	if !ok {
		q.log.Warn("要扫的账号不见了", "account", id)
		return
	}
	res, err := q.sc.ScanAccount(ctx, a)
	// 日志只记账号 ID 和落点，凭据一个字不进（SPEC 红线 1）。
	q.log.Info("首扫结束", "account", id, "regions", res.Regions, "samples", res.Samples,
		"new_gateways", res.NewGateways, "failures", res.Failures,
		"stopped_early", res.StoppedEarly, "err", err)
}

// 编译期钉住：这几个具体类型必须满足各包自己定义的窄接口。
// 对不上就在这里断，而不是在运行时。
var (
	_ sched.Store     = (*store.Store)(nil)
	_ httpapi.Store   = (*store.Store)(nil)
	_ httpapi.Scanner = (*scanQueue)(nil)
	_ httpapi.Pool    = (*sched.Scheduler)(nil)
	_ sched.Minter    = (*mint.Client)(nil)
	_ types.Minter    = (*mint.Client)(nil)
)
