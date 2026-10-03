package config

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Live 是「当前生效的配置」，可以在运行中整份换掉。
//
// 为什么需要它：管理页改一个旋钮不该要重启——重启会丢掉进程里的速率窗口、铸票计数、
// 需求时间戳这些内存态，而那些正是调度在依据的东西。整份原子换指针则什么都不丢。
//
// 只读路径一律 Get() 取一份**快照**再用。拿着快照里的两个字段去做一个判断是安全的；
// 调两次 Get() 中间配置可能变了，那种代码会在极罕见的时刻自相矛盾。
type Live struct {
	path string
	v    atomic.Pointer[Config]
	mu   sync.Mutex
}

func NewLive(path string, c Config) *Live {
	l := &Live{path: path}
	l.v.Store(&c)
	return l
}

// Get 取当前快照。永不返回零值：构造时就有一份。
func (l *Live) Get() Config {
	if l == nil {
		return Config{}
	}
	return *l.v.Load()
}

func (l *Live) Path() string { return l.path }

// Reload 重读文件。**校验不过就不换**：磁盘上出现一份坏配置时，正在跑的那份照旧有效。
func (l *Live) Reload() (Config, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, err := Load(l.path)
	if err != nil {
		return l.Get(), err
	}
	if err := ValidateRegionTransition(l.Get(), c); err != nil {
		return l.Get(), err
	}
	l.v.Store(&c)
	return c, nil
}

// Watch 轮询配置文件，变了就重载，直到 ctx 取消。
//
// 轮 mtime+size 而不是用 fsnotify：少一个依赖，而且原子保存（写 .tmp 再改名）在
// inotify 下会放出一串 CREATE/MOVED_TO 事件，还得自己去抖。几秒一次 Stat 便宜得多。
//
// 读不到文件（正在被改名的那一瞬）不算错，下一轮再看。
func (l *Live) Watch(ctx context.Context, every time.Duration, log *slog.Logger) {
	if every <= 0 {
		every = 5 * time.Second
	}
	var lastMod time.Time
	var lastSize int64
	if fi, err := os.Stat(l.path); err == nil {
		lastMod, lastSize = fi.ModTime(), fi.Size()
	}

	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		fi, err := os.Stat(l.path)
		if err != nil {
			continue
		}
		if fi.ModTime().Equal(lastMod) && fi.Size() == lastSize {
			continue
		}
		lastMod, lastSize = fi.ModTime(), fi.Size()
		c, err := l.Reload()
		if err != nil {
			// 这里是**警告不是错误退出**：配置坏了但旧的还在跑，池子继续服务。
			log.Warn("配置变了但读不进去，继续用旧的", "path", l.path, "err", err)
			continue
		}
		// 不打全量配置：里面有密钥和 socks 口令。只报几个能说明「换没换对」的旋钮。
		log.Info("配置已热重载", "models", c.Models,
			"upstream_per_region_hour", c.UpstreamPerRegionHour,
			"verify_full_at_deliver", c.VerifyFullAtDeliver,
			"slot_cooldown", c.SlotCooldown(),
			"public", c.PublicWhenSurplus.Enabled, "min_accounts", c.PublicWhenSurplus.MinAccounts)
	}
}
