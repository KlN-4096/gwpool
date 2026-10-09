// Package store stores durable state in a checksummed incremental journal plus
// periodic state.json snapshots. samples.jsonl remains a separate observation log.
package store

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"gwpool/internal/types"
)

const (
	stateFile   = "state.json"
	samplesFile = "samples.jsonl"

	// 文件里有 access_token，目录和文件都只给自己（SPEC 第 9 节）。
	dirPerm  fs.FileMode = 0o700
	filePerm fs.FileMode = 0o600

	// dailyKeep 是带日期的备份留几份。七天够覆盖「上周末改坏的、周一才发现」，
	// 而一份 state.json 是几十 KB 量级，留七份的代价可以忽略。
	dailyKeep = 7

	// maxAccounts 封住托管账号数。每加一个账号都要全量重写 state.json，
	// 不设上限的话「谁都能上传」这条路会把写放大推成 O(n²)。
	maxAccounts = 500
	// ownerPruneGrace 是空 owner 的宽限期：比这还新的空 owner 不删。
	// 覆盖「AddOwner 已落盘、AddAccount 还没走到」那个窗口（两次写同一个文件，
	// 毫秒级），一分钟是几个数量级的余量。见 PruneEmptyOwners。
	ownerPruneGrace = time.Minute

	// maxAnonAccounts 单独封匿名占位行（无凭据，sched.anonAccount 建的）。
	// 两个数分开算，不是洁癖：占位行是匿名调用方随口报个 ID 就能长出来的，
	// 和托管账号混在一个上限里，灌满之后**所有上传**永久失败，而服务内没有
	// 任何回收手段。分开之后匿名最多只能堵死匿名自己。
	maxAnonAccounts = 400
)

// State 是 state.json 的全量内容。
type State struct {
	JournalVersion    int                                        `json:"journal_version,omitempty"`
	JournalSequence   uint64                                     `json:"journal_sequence,omitempty"`
	OwnerKeysMigrated bool                                       `json:"owner_keys_migrated"`
	CountryAttempts   map[string]map[string]types.CountryAttempt `json:"country_attempts,omitempty"`
	AttemptsMigrated  bool                                       `json:"attempts_migrated,omitempty"`
	ContactReports    []types.ContactReport                      `json:"contact_reports,omitempty"`
	CooldownReports   []types.CooldownReport                     `json:"cooldown_reports,omitempty"`
	Accounts          []types.Account                            `json:"accounts"`
	// Owners 是上传者。老文件里没这个键 ⇒ nil ⇒ Open 的一次性迁移给已有的托管行
	// 补一个（见 migrateOwners）。
	Owners   []types.Owner            `json:"owners"`
	Gateways map[string]types.Gateway `json:"gateways"` // key = "unified-142"
	Slots    map[string]types.Slot    `json:"slots"`    // key = SlotKey()
	Pairs    map[string]types.Pair    `json:"pairs"`    // key = gateway
	// Mints 是 (账号 × 区域) 的终身铸票台账，key = MintKey()。
	// 必须落盘：它是「这个号还有哪个区域没走过」的唯一依据，而那正是发现新网关的
	// 全部线索。记在内存里的话每次重启所有号的九个格子一起归零，于是重启后的
	// 头几发又全打在已经走烂了的区域上。
	Mints map[string]types.RegionMint `json:"mints"`
	// NextAccountID / NextOwnerID 是**永不回收**的 ID 计数器，见 bumpID。
	// 老文件里没这两个键 ⇒ 0 ⇒ 第一次发号时按现有最大值兜底一次，不用迁移。
	NextAccountID   int64                                  `json:"next_account_id"`
	NextOwnerID     int64                                  `json:"next_owner_id"`
	NextAccessKeyID int64                                  `json:"next_access_key_id,omitempty"`
	AccessKeys      map[string]types.AccessKey             `json:"access_keys,omitempty"`
	KeyUsage        map[string]types.KeyUsage              `json:"key_usage,omitempty"`
	ExternalUsage   map[string]types.ExternalConsumerUsage `json:"external_usage,omitempty"`
}

// bumpID 发一个**永不回收**的 ID：计数器只增不减，max 只在老文件还没有计数器时兜底一次。
//
// 老写法是「现有最大 +1」，删掉末尾那个号之后下一个新号会**接上同一个 ID**，于是所有
// 按 ID 挂的历史记录都被错挂到新主人头上。DeleteAccount 已经为此跟着删了槽位和区域台账，
// 但网关表的 FirstSeenBy 没人删——2026-10-02 清空重传之后，四个新号的「发现网关」直接
// 显示成老号的 8/8/5，21 个网关一个不差全被认领了。治本只能是不回收。
//
// 落盘失败时计数器不回退：ID 跳号无所谓，重号要命。
func bumpID(counter *int64, max int64) int64 {
	if *counter < max {
		*counter = max
	}
	*counter++
	return *counter
}

// Store 是并发安全的状态句柄。
//
// ponytail: 一把读写锁管全部状态和样本追加，写操作会串起来；
// 数据量到了「一次保存超过几毫秒」才需要拆成 per-map 锁或改增量日志。
type Store struct {
	dir            string
	mu             sync.RWMutex
	st             State
	journal        journalState
	checkpointMu   sync.Mutex
	checkpointIO   checkpointOperations
	revisions      Revisions
	slotIndex      map[int64]map[string]types.Slot
	demandCache    slotDemandCache
	slotWriter     slotWriter
	samples        *os.File
	routingHistory *routingHistoryCache
	routingPending *routingHistoryCache // appends made while history is loaded outside mu
	routingLoadMu  sync.Mutex
	routingLoadErr error // routingLoadMu protects load/retry state
	routingRetryAt time.Time

	// 账号锁独立于 mu：持账号锁的人要打上游（几十秒），不能把状态读写一起挡住。
	accMu    sync.Mutex
	accLocks map[int64]*sync.Mutex
}

// SlotKey 是 slots 表的键。slot = (账号 × 网关)，降智的作用单位。
func SlotKey(accountID int64, gateway string) string {
	return fmt.Sprintf("%d|%s", accountID, gateway)
}

// MintKey 是 mints 表的键：(账号 × 区域)。
func MintKey(accountID int64, regionKey string) string {
	return fmt.Sprintf("%d|%s", accountID, regionKey)
}

// Open 打开 dir 下的两个文件。文件不存在就建空的，不报错退出（SPEC 第 9 节）。
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("建数据目录: %w", err)
	}
	// MkdirAll 对已存在的目录不改权限，老目录也收一次口。
	_ = os.Chmod(dir, dirPerm)

	s := &Store{dir: dir, st: State{
		Gateways: map[string]types.Gateway{},
		Slots:    map[string]types.Slot{},
		Pairs:    map[string]types.Pair{},
		Mints:    map[string]types.RegionMint{},
	}}

	b, err := os.ReadFile(filepath.Join(dir, stateFile))
	switch {
	case errors.Is(err, fs.ErrNotExist): // 首次启动
	case err != nil:
		return nil, fmt.Errorf("读 %s: %w", stateFile, err)
	default:
		if err := json.Unmarshal(b, &s.st); err != nil {
			return nil, fmt.Errorf("解析 %s: %w", stateFile, err)
		}
		// 旧文件或手写文件可能缺字段，补成非 nil，后面所有写路径就不用判空。
		if s.st.Gateways == nil {
			s.st.Gateways = map[string]types.Gateway{}
		}
		if s.st.Slots == nil {
			s.st.Slots = map[string]types.Slot{}
		}
		if s.st.Pairs == nil {
			s.st.Pairs = map[string]types.Pair{}
		}
		if s.st.Mints == nil {
			s.st.Mints = map[string]types.RegionMint{}
		}
	}
	if err := s.openJournal(); err != nil {
		return nil, err
	}
	upgradeJournal := s.st.JournalVersion == 0
	s.st.JournalVersion = journalVersion
	s.slotIndex = make(map[int64]map[string]types.Slot)
	for _, slot := range s.st.Slots {
		s.indexSlot(slot)
	}
	opened := false
	defer func() {
		if !opened && s.journal.file != nil {
			_ = s.journal.file.Close()
		}
	}()
	// Replay before migrations: a journal may already contain the migration or
	// a newer pair, so neither must be silently reverted by snapshot-only reads.
	{
		// 老记录里 ExpiresAt 存的是 __oailb 的 exp，而真正钉路由的 __cflb 比它早死
		// 300 秒（见 types.PairLifetime）⇒ 那些值整篇偏乐观 300 秒，照着发就会把
		// 一张路由已经死了的 cookie 当成「刚好够交付」发出去。
		// CflbExpiresAt 的零值就是「这是老记录」的标志，在这儿一次收口——
		// 零值**绝不能**当成「不过期」，所以这一步不能省。
		for gw, p := range s.st.Pairs {
			if !p.CflbExpiresAt.IsZero() {
				continue
			}
			p.CflbExpiresAt = types.FallbackCflbExpiry(p.ExpiresAt)
			if !p.CflbExpiresAt.IsZero() {
				p.ExpiresAt = p.CflbExpiresAt
			}
			s.st.Pairs[gw] = p
		}
	}

	if err := s.migrateOwners(); err != nil {
		return nil, err
	}
	if err := s.migrateOwnerKeys(); err != nil {
		return nil, err
	}
	// 清掉历史遗留的孤儿 owner（上一次运行里 AddAccount 失败或并发竞争留下的）。
	// store 层不打日志（它只把错误往上抛），清理数量无人关心就不返了。
	if _, err := s.PruneEmptyOwners(); err != nil {
		return nil, err
	}

	samplesPath := filepath.Join(dir, samplesFile)
	f, err := os.OpenFile(samplesPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, filePerm)
	if err != nil {
		return nil, fmt.Errorf("开 %s: %w", samplesFile, err)
	}
	// OpenFile 的 perm 只在**创建**时生效：早先用 0644 建出来的样本文件不会被收口，
	// 和目录一样显式 Chmod 一次。
	_ = os.Chmod(samplesPath, filePerm)
	s.samples = f
	// Publish an initial snapshot before returning on a new directory. Existing
	// stores keep migrations durable in the WAL until the next checkpoint.
	_, snapshotErr := os.Stat(filepath.Join(dir, stateFile))
	if upgradeJournal || errors.Is(snapshotErr, fs.ErrNotExist) {
		if err := s.Checkpoint(); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	opened = true
	s.startSlotWriter()
	go s.checkpointLoop()
	return s, nil
}

// Preserve only keys that could have been distributed before this migration.
// Persist even on an empty store so a later restart cannot grandfather new rows.
func (s *Store) migrateOwnerKeys() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.OwnerKeysMigrated {
		return nil
	}
	previous := append([]types.Account(nil), s.st.Accounts...)
	for i := range s.st.Accounts {
		s.st.Accounts[i].LegacyConsumerKey = s.st.Accounts[i].Creds.AccessToken != ""
	}
	s.st.OwnerKeysMigrated = true
	if err := s.save(stateChange{Accounts: s.st.Accounts, OwnerKeysMigrated: true}); err != nil {
		s.st.Accounts, s.st.OwnerKeysMigrated = previous, false
		return fmt.Errorf("迁移账号 key: %w", err)
	}
	return nil
}

// LockAccount 取这个账号的独占锁，返回解锁函数。
//
// **单账号并发硬性 1**（SPEC 红线 2）：2026-09-29 实测 5~6 并发 20~30 分钟废掉一个通道。
// 锁必须挂在 store 上而不是各包自己一份——扫描器和调度器是两个包，各拿一个
// sync.Map 等于两套互不认识的锁，首扫还在跑时一发 /cookie 就凑出 2 并发。
// per-account 而不是全局信号量：不同账号之间互不影响。
func (s *Store) LockAccount(id int64) func() {
	s.accMu.Lock()
	if s.accLocks == nil {
		s.accLocks = map[int64]*sync.Mutex{}
	}
	lk := s.accLocks[id]
	if lk == nil {
		lk = &sync.Mutex{}
		s.accLocks[id] = lk
	}
	s.accMu.Unlock()
	lk.Lock()
	return lk.Unlock
}

func (s *Store) Close() error {
	s.journal.closeOnce.Do(func() {
		s.stopSlotWriter()
		close(s.journal.stop)
		<-s.journal.done
		s.mu.Lock()
		defer s.mu.Unlock()
		var journalErr error
		if s.journal.file != nil {
			journalErr = s.journal.file.Close()
			s.journal.file = nil
		}
		s.journal.closeErr = errors.Join(s.samples.Close(), journalErr, s.journal.failed, s.journal.checkpointErr)
	})
	return s.journal.closeErr
}

// backup 在覆盖 state.json 之前留两份副本。
//
// 为什么值得做：这个文件里的十几个网关和全部槽位历史是**几百发上游请求**换来的，
// 而每一发都烧掉一个 (账号 × 网关) 的窗口。丢了不是「重跑一下」，是重新烧一遍供给。
//
// 两份是因为要防的是两种事：
//   - `.bak` 是上一次保存的内容，防的是这一次写坏（进程被杀、磁盘满、JSON 写到一半）
//   - `.daily-YYYYMMDD` 每天只落第一次，防的是「三天前被改坏了，现在才发现」——
//     只留 `.bak` 的话它早被后面成千次保存刷掉了
//
// 失败一律忽略：备份失败不该让保存失败，那会把一次磁盘小毛病升级成池子停摆。
// 凭据在这几份文件里是明文，和 state.json 本身一样——目录权限是 0700，与正本同级。
func (s *Store) backup(cur string) {
	b, err := os.ReadFile(cur)
	if err != nil || len(b) == 0 {
		return // 首次保存，或者正本已经没了：没什么可备份的
	}
	_ = os.WriteFile(cur+".bak", b, filePerm)

	daily := cur + ".daily-" + time.Now().UTC().Format("20060102")
	if _, err := os.Stat(daily); err == nil {
		return // 今天这份已经有了，别让它被当天后面的保存刷掉
	}
	_ = os.WriteFile(daily, b, filePerm)
	s.pruneDaily(filepath.Base(cur) + ".daily-")
}

// pruneDaily 只留最近 dailyKeep 份带日期的备份。
//
// 文件名里的日期是 YYYYMMDD，字典序 = 时间序，所以排个序砍尾巴就行，不用去读 mtime
// （mtime 会被 rsync、备份工具、换机器碰掉，文件名不会）。
func (s *Store) pruneDaily(prefix string) {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	var names []string
	for _, e := range ents {
		if !e.IsDir() && strings.HasPrefix(e.Name(), prefix) {
			names = append(names, e.Name())
		}
	}
	if len(names) <= dailyKeep {
		return
	}
	sort.Strings(names)
	for _, n := range names[:len(names)-dailyKeep] {
		_ = os.Remove(filepath.Join(s.dir, n))
	}
}

// ---------- 上传者 ----------

// migrateOwners 把加 types.Account.OwnerID 之前写下的托管账号挂到一个自动建出来的
// owner 下。不做这一步的话现有那几个账号的 /u/ 总览页谁都进不去。
//
// **幂等靠触发条件和效果互斥**：触发条件是「存在没有归属的托管行」，而这一趟会把
// 所有这种行一次清完**并落盘** ⇒ 下一次 Open 找不到孤儿，一个 owner 都不会再建。
// 刻意不靠「找找有没有那个迁移出来的 owner」——那要拿 Label 当标识，而 Label 是
// 随时可改的自由文本。
//
// 必须落盘（不能只改内存）：token 是随机的，不落盘就等于每次重启换一个，
// 管理页上给出去的那个链接立刻失效。
//
// 匿名占位行（没有 access_token）**不挂**：它不是谁托管的，挂上去只会让那个人的
// 总览页上多出几行他从没上传过的东西。
func (s *Store) migrateOwners() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	orphans := 0
	for _, a := range s.st.Accounts {
		if a.OwnerID == 0 && a.Creds.AccessToken != "" {
			orphans++
		}
	}
	if orphans == 0 {
		return nil
	}
	o, err := s.newOwner("迁移：加 owner 之前就在的账号")
	if err != nil {
		return err
	}
	prev := append([]types.Account(nil), s.st.Accounts...)
	for i := range s.st.Accounts {
		if s.st.Accounts[i].OwnerID == 0 && s.st.Accounts[i].Creds.AccessToken != "" {
			s.st.Accounts[i].OwnerID = o.ID
		}
	}
	s.st.Owners = append(s.st.Owners, o)
	if err := s.save(stateChange{Accounts: s.st.Accounts, Owners: []types.Owner{o}}); err != nil {
		s.st.Accounts = prev
		s.st.Owners = s.st.Owners[:len(s.st.Owners)-1]
		return fmt.Errorf("给已有账号补 owner: %w", err)
	}
	return nil
}

// PruneEmptyOwners 丢掉名下零账号的 owner。改了就落盘，返回删了几个。
//
// 为什么需要它：handleUpload 先 AddOwner（已落盘）再 AddAccount，而 AddAccount 会在
// 撞上限（池子满 maxAccounts）或并发命中同一份新凭据时失败/不插入 —— 两种情况都留下
// 一个谁都不属于的 owner。不清的话，拿着站点码反复贴新 account_id 就能让 owner 表
// 无上限增长，每建一个还要全量重写 state.json。
//
// 所以 newOwner 上方那句「至多和托管账号一样多、已被 maxAccounts 封住」其实不成立
// （上限检查在去重分支之后），这个方法是那句话的兜底。
//
// 清理是**滞后**的，不是即时的：刚建出来的空 owner 落在 ownerPruneGrace 宽限期里，
// 连建它那一发自己的失败路径也删不掉它（见下方 cut 处的注释——即时删会踩并发上传）。
// 它由下一次 prune 收走：之后任何一发上传失败，或者重启时 Open 这一遍。
func (s *Store) PruneEmptyOwners() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	used := make(map[int64]bool, len(s.st.Accounts))
	for _, a := range s.st.Accounts {
		if a.OwnerID != 0 {
			used[a.OwnerID] = true
		}
	}
	// 刚建出来的空 owner 要留一段宽限期，**不能见空就删**。
	//
	// handleUpload 是「先 AddOwner 落盘、再 AddAccount」两步，中间那一瞬 owner 名下真的
	// 是零账号。两个并发上传时，B 的失败路径会调到这里，而它锁住整个 store 之后看到的
	// 「空 owner」里**包括 A 刚建出来、账号还没挂上去的那个** ⇒ A 的 owner 被删，随后
	// A 的 AddAccount 照常成功，OwnerID 指向一个已经不存在的行。AddAccount 不校验
	// OwnerID 是否存在，于是悬垂引用静默入库：那个账号的 /u/ 链接永远打不开，
	// 而 migrateOwners 只收养 OwnerID==0 的行、修不了它 ⇒ 重启也不自愈，只能手工改库。
	cut := time.Now().Add(-ownerPruneGrace)
	kept := s.st.Owners[:0:0]
	for _, o := range s.st.Owners {
		if used[o.ID] || o.CreatedAt.After(cut) {
			kept = append(kept, o)
		}
	}
	if len(kept) == len(s.st.Owners) {
		return 0, nil
	}
	removed := len(s.st.Owners) - len(kept)
	prev := s.st.Owners
	s.st.Owners = kept
	var dropped []int64
	for _, owner := range prev {
		if !used[owner.ID] && !owner.CreatedAt.After(cut) {
			dropped = append(dropped, owner.ID)
		}
	}
	if err := s.save(stateChange{DropOwners: dropped}); err != nil {
		s.st.Owners = prev
		return 0, err
	}
	return removed, nil
}

// newOwner 造一个 owner（**不**追加进状态，也不落盘）。调用方必须已持写锁。
//
// 分两步是为了让「追加 + 落盘失败就回退」留在调用方那一个事务里：
// 这个函数自己落盘的话，账号加失败时会在账本上留一个谁都不属于的 owner。
//
// 不在这里封 owner 数：孤儿 owner 由 pruneEmptyOwners 收口，不靠「造的时候不让造」。
func (s *Store) newOwner(label string) (types.Owner, error) {
	tok, err := newToken()
	if err != nil {
		return types.Owner{}, err
	}
	max := int64(0)
	for _, x := range s.st.Owners {
		if x.ID > max {
			max = x.ID
		}
	}
	id := bumpID(&s.st.NextOwnerID, max)
	return types.Owner{ID: id, Token: tok, Label: label, CreatedAt: time.Now().UTC()}, nil
}

// Owners 返回 owner 副本。只有管理页读它（要把 token 显示出来，否则迁移出来的那个
// owner 名下的账号没人进得去）。
func (s *Store) Owners() []types.Owner {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]types.Owner(nil), s.st.Owners...)
}

// OwnerByToken 是 /u/{token} 的全部鉴权。
//
// 空串一律不认，理由和 AccountByViewToken 一样：历史上没分配过令牌的行会被空串
// 整条匹配出来，等于把别人的总览页开给任何人。
func (s *Store) OwnerByToken(tok string) (types.Owner, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if tok == "" {
		return types.Owner{}, false
	}
	for _, o := range s.st.Owners {
		if o.Token == tok {
			return o, true
		}
	}
	return types.Owner{}, false
}

// AccountsByOwner 列一个 owner 名下的账号。
//
// **ownerID 为 0 时回 nil**，不是「所有没归属的行」：0 是匿名占位行的常态值，
// 照它匹配会把一堆匿名调用方的行摊到某个人的总览页上。
func (s *Store) AccountsByOwner(ownerID int64) []types.Account {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if ownerID == 0 {
		return nil
	}
	var out []types.Account
	for _, a := range s.st.Accounts {
		if a.OwnerID == ownerID {
			out = append(out, a)
		}
	}
	return out
}

// ---------- 账号 ----------

// Accounts 返回账号副本。
func (s *Store) Accounts() []types.Account {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]types.Account, len(s.st.Accounts))
	copy(out, s.st.Accounts)
	return out
}

func (s *Store) Account(id int64) (types.Account, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, a := range s.st.Accounts {
		if a.ID == id {
			return a, true
		}
	}
	return types.Account{}, false
}

// AccountByUpstreamID reads one account without copying the full credentials
// table; in particular catalog requests only need the anonymous ledger row ID.
func (s *Store) AccountByUpstreamID(upstreamID string) (types.Account, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, account := range s.st.Accounts {
		if account.Creds.AccountID == upstreamID {
			return account, true
		}
	}
	return types.Account{}, false
}

// AccountByViewToken 供上传者信任视图用。
func (s *Store) AccountByViewToken(tok string) (types.Account, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if tok == "" {
		return types.Account{}, false
	}
	for _, a := range s.st.Accounts {
		if a.ViewToken == tok {
			return a, true
		}
	}
	return types.Account{}, false
}

// AddAccount 落一个新账号。ID 为 0 时分配一个（当前最大 +1）。
//
// **同一个上游账号只许有一行**：降智的作用单位是 (上游账号 × 网关)，而槽位表是按
// 池内行 ID 记的。同一份凭据建两行就等于两套互不相识的账本，池子会以为两边都还有
// 满血窗口，其实烧的是同一个上游账号 —— 第二行会稳定地拿到已经废掉的槽位。
// 所以撞上已有的 Creds.AccountID 就**更新那一行**（令牌、plan、备注），不新建。
// 匿名占位行（只报过 account_id、没交凭据）也走这条路升级成托管行，它攒下来的
// 槽位历史正是这个上游账号的历史，丢掉就白烧了。
func (s *Store) AddAccount(a types.Account) (types.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addAccountLocked(a)
}

func (s *Store) addAccountLocked(a types.Account) (types.Account, error) {
	if up := a.Creds.AccountID; up != "" {
		for i, x := range s.st.Accounts {
			if x.Creds.AccountID != up {
				continue
			}
			// **已经属于别人的行：原样返回，一个字段都不碰。**
			//
			// 去重只认 Creds.AccountID，而 oauth.Parse 不验签（它自己的注释写着）：
			// 知道别人上游 account_id 的人能自制一张票打上来命中这一行。下面那段
			// 「重新上传就是我来修它」只对这一行的主人成立——对别人也执行的话，攻击者
			// 的伪造票会覆盖受害者存着的 access_token、改掉他的 Label、解开他自己关掉的
			// 停用，把别人托管的凭据静默弄废。归属（OwnerID）之外，这些字段同样只有
			// 主人能改。被拒绝不是错误，上传在 handler 那头照常返回成功、但不交出任何
			// 钥匙（见 httpapi.handleUpload 的 mine 判定）。
			//
			// a.OwnerID==0 的来路：handleUpload 发现这份凭据已属于别人时就不建新 owner，
			// 所以传进来的 OwnerID 是 0，而 x.OwnerID 非 0 ⇒ 落进这个分支。
			mine := x.OwnerID == 0 || x.OwnerID == a.OwnerID
			if !mine {
				return x, nil
			}
			prev := x
			x.Creds.AccessToken = a.Creds.AccessToken
			// 上游没轮换 refresh_token 时 oauth.Refresh 回空串，别把好的那个覆盖成空。
			if a.Creds.RefreshToken != "" {
				x.Creds.RefreshToken = a.Creds.RefreshToken
			}
			if a.PlanType != "" {
				x.PlanType = a.PlanType
			}
			if a.Label != "" {
				x.Label = a.Label
			}
			// 重新上传就是「我来修它」：解开停用、清掉原因。
			x.Enabled = true
			x.DisabledReason = ""
			x.CredentialRevision = s.st.JournalSequence + 1
			// 归属只补不改：没有 owner 的（老文件、或占位行刚升级成托管行）挂到上传者
			// 名下；属于别人的在上面已经 return 掉了，到这儿只剩「无主」或「本来就是我的」。
			if x.OwnerID == 0 {
				x.OwnerID = a.OwnerID
			}
			s.st.Accounts[i] = x
			if err := s.save(stateChange{Accounts: []types.Account{x}}); err != nil {
				s.st.Accounts[i] = prev
				return types.Account{}, err
			}
			return x, nil
		}
	}

	anon := a.Creds.AccessToken == ""
	limit, kind := maxAccounts, "托管账号"
	if anon {
		limit, kind = maxAnonAccounts, "匿名占位行"
	}
	n := 0
	for _, x := range s.st.Accounts {
		if (x.Creds.AccessToken == "") == anon {
			n++
		}
	}
	if n >= limit {
		return types.Account{}, fmt.Errorf("%s数已达上限 %d", kind, limit)
	}
	if a.ID == 0 {
		max := int64(0)
		for _, x := range s.st.Accounts {
			if x.ID > max {
				max = x.ID
			}
		}
		a.ID = bumpID(&s.st.NextAccountID, max)
	}
	if a.CreatedAt.IsZero() {
		a.CreatedAt = time.Now().UTC()
	}
	a.CredentialRevision = s.st.JournalSequence + 1
	// 兜底生成：没有令牌的账号行会让 AccountByViewToken("") 整条匹配出来，
	// 等于把上传者视图开给任何人。生成放在这里，调用方传了就用调用方的。
	if a.ViewToken == "" {
		tok, err := newToken()
		if err != nil {
			return types.Account{}, err
		}
		a.ViewToken = tok
	}
	// OwnerID 原样落（调用方负责先建好 owner，见 httpapi.handleUpload）。
	// **刻意不在这儿兜底铸一个**：那样调用方就分不清回来的 a.OwnerID 是「刚为我铸的」
	// 还是「去重命中了一个已经属于别人的账号」，而那两件事的处置完全相反
	// （后者的令牌一个字都不许查——那一页上有对方每个账号的 consumer key）。
	a.LegacyConsumerKey = false // new imports never obtain a credential-level machine key
	s.st.Accounts = append(s.st.Accounts, a)
	if err := s.save(stateChange{Accounts: []types.Account{a}}); err != nil {
		s.st.Accounts = s.st.Accounts[:len(s.st.Accounts)-1] // 没落盘就别留在内存里
		return types.Account{}, err
	}
	return a, nil
}

// AddOwner 建一个上传者。
//
// 这是 owner 唯一的入口（迁移那条路除外）：谁建的谁手上就有令牌，不存在「回头去问
// 存储层刚才铸的是哪个」这回事——那个问题答错一次就是把别人的身份交出去。
func (s *Store) AddOwner(label string) (types.Owner, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, err := s.newOwner(label)
	if err != nil {
		return types.Owner{}, err
	}
	s.st.Owners = append(s.st.Owners, o)
	if err := s.save(stateChange{Owners: []types.Owner{o}}); err != nil {
		s.st.Owners = s.st.Owners[:len(s.st.Owners)-1] // 没落盘就别留在内存里
		return types.Owner{}, err
	}
	return o, nil
}

// SetAccountVia 只改 Via 一个字段（见 types.Account.Via）。没变就不写盘：同一个调用方
// 每两三分钟就来取一次票。
func (s *Store) SetAccountVia(id, via int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.st.Accounts {
		if s.st.Accounts[i].ID != id {
			continue
		}
		old := s.st.Accounts[i].Via
		if old == via {
			return nil
		}
		s.st.Accounts[i].Via = via
		if err := s.save(stateChange{Accounts: []types.Account{s.st.Accounts[i]}}); err != nil {
			s.st.Accounts[i].Via = old // 没落盘就别留在内存里
			return err
		}
		return nil
	}
	return fmt.Errorf("账号 %d 不存在", id)
}

// SetAccountEnabled 只翻 Enabled 这一个字段。
//
// 刻意不是 PutAccount(a types.Account)：整行覆盖的入口意味着调用方手里得先有一份
// 账号副本，而那份副本但凡是旧的，就会把别人之后写进去的东西（凭据、view_token）
// 一起盖掉。停用只需要一个 bool，就只给一个 bool。
//
// 停用**不删凭据**：池子不再用它发请求、派生的 key 也不再认人，但令牌本身还在
// state.json 里。要让令牌真的失效，得上传者自己去 ChatGPT 设备列表取消登录——
// 第 10 节就是这么向他承诺的，别在这里假装能替他撤销。
func (s *Store) SetAccountEnabled(id int64, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.st.Accounts {
		if s.st.Accounts[i].ID != id {
			continue
		}
		old := s.st.Accounts[i].Enabled
		oldReason := s.st.Accounts[i].DisabledReason
		oldRevision := s.st.Accounts[i].CredentialRevision
		// 开关没变就什么都不做。**特别是不许在这里清理由**：对一个已经被自动停用的号
		// 再 POST 一次 enabled=false，清掉理由就让它在管理页上和「手动暂停」长得一样，
		// 于是运营方会反复打开一个凭据已经死了的号（线上刚发生过 token_revoked 那次）。
		if old == enabled {
			return nil
		}
		s.st.Accounts[i].Enabled = enabled
		// 真的翻了开关才清自动停用的理由：那条话说的是上一次的事。
		s.st.Accounts[i].DisabledReason = ""
		s.st.Accounts[i].CredentialRevision = s.st.JournalSequence + 1
		if err := s.save(stateChange{Accounts: []types.Account{s.st.Accounts[i]}}); err != nil {
			s.st.Accounts[i].Enabled = old // 没落盘就别留在内存里
			s.st.Accounts[i].DisabledReason = oldReason
			s.st.Accounts[i].CredentialRevision = oldRevision
			return err
		}
		return nil
	}
	return fmt.Errorf("账号 %d 不存在", id)
}

// DisableAccount 因为上游否决而自动停用，带上原因。
//
// 和 SetAccountEnabled 分开：那个是人点的（会清掉 reason），这个是程序判的。
// 已经停用的号重复调用会**更新理由**而不是直接返回——第二条原因往往比第一条准。
func (s *Store) DisableAccount(id int64, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.st.Accounts {
		if s.st.Accounts[i].ID != id {
			continue
		}
		if !s.st.Accounts[i].Enabled && s.st.Accounts[i].DisabledReason == reason {
			return nil
		}
		oldEnabled, oldReason := s.st.Accounts[i].Enabled, s.st.Accounts[i].DisabledReason
		oldRevision := s.st.Accounts[i].CredentialRevision
		s.st.Accounts[i].Enabled = false
		s.st.Accounts[i].DisabledReason = reason
		s.st.Accounts[i].CredentialRevision = s.st.JournalSequence + 1
		if err := s.save(stateChange{Accounts: []types.Account{s.st.Accounts[i]}}); err != nil {
			s.st.Accounts[i].Enabled, s.st.Accounts[i].DisabledReason = oldEnabled, oldReason
			s.st.Accounts[i].CredentialRevision = oldRevision
			return err
		}
		return nil
	}
	return fmt.Errorf("账号 %d 不存在", id)
}

// SetTokens 换票之后把新的 access_token 写回去。
//
// refresh 为空 = 上游没轮换，**保留原来那张**。写成空值就再也换不了票了，
// 而那正是「池子九天后静默死掉」的样子。
//
// 顺手清掉 DisabledReason 并启用：能换出新票就说明凭据还活着，之前那次 401
// 说的是上一张票的事。
//
// **只对自动停用的行这么做**（DisabledReason 非空）。手动停用（SetAccountEnabled，
// 它刻意把 reason 留空）说的是「我先不参与」，和票的新旧毫无关系：上传者在自己页面上
// 点了「停用账号」，页面承诺「不再用它发请求，接入 key 失效」，而续票循环遍历的是全部
// 账号——照原来那样无条件启用，他的号最长九天（access_token 到期前 24h）后就会被池子
// 静默打开、重新拿去铸票，key 也重新能用，而页面上看不出发生过任何事。
func (s *Store) SetTokens(id int64, access, refresh string) error {
	if access == "" {
		return errors.New("access_token 不能写成空的")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.st.Accounts {
		if s.st.Accounts[i].ID != id {
			continue
		}
		old := s.st.Accounts[i]
		s.st.Accounts[i].Creds.AccessToken = access
		s.st.Accounts[i].CredentialRevision = s.st.JournalSequence + 1
		if refresh != "" {
			s.st.Accounts[i].Creds.RefreshToken = refresh
		}
		if s.st.Accounts[i].DisabledReason != "" {
			s.st.Accounts[i].Enabled = true
			s.st.Accounts[i].DisabledReason = ""
		}
		if err := s.save(stateChange{Accounts: []types.Account{s.st.Accounts[i]}}); err != nil {
			s.st.Accounts[i] = old
			return err
		}
		return nil
	}
	return fmt.Errorf("账号 %d 不存在", id)
}

// DeleteAccount 删掉账号连同它的槽位。
//
// 和 SetAccountEnabled 的分工：停用是「先别用，回头可能还开」，删除是「这张票没用，
// 别留在这」——2026-10-01 上传进来一张标着 pro1 其实是 free 账号的票，留着只会在
// 每次扫描里继续白打。
//
// 槽位跟着删：它的 key 带账号 ID，账号没了就永远没人读，留着只会让 state.json 长胖。
// **pair 不删**：路由 cookie 钉的是网关，和铸它的账号没关系，那是池子的产出，
// 删账号不该连带销毁已经拿到手的票。
//
// 和停用一样**不撤销令牌**：撤销只有上传者自己能做（见 SetAccountEnabled 的注释）。
func (s *Store) DeleteAccount(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := -1
	for i := range s.st.Accounts {
		if s.st.Accounts[i].ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("账号 %d 不存在", id)
	}
	acct := s.st.Accounts[idx]
	dropped := map[string]types.Slot{}
	for k, sl := range s.st.Slots {
		if sl.AccountID == id {
			dropped[k] = sl
		}
	}
	// 台账跟着删，理由和槽位一样，而且更要紧：ID 是「现有最大 +1」算的，
	// 删掉末尾那个号之后下一个新号会**接上同一个 ID**，留着的九个格子会让它
	// 一进来就以为自己已经把那些区域走遍了，于是再也不去试。
	droppedMints := map[string]types.RegionMint{}
	for k, m := range s.st.Mints {
		if m.AccountID == id {
			droppedMints[k] = m
		}
	}

	s.st.Accounts = append(s.st.Accounts[:idx:idx], s.st.Accounts[idx+1:]...)
	for k := range dropped {
		delete(s.st.Slots, k)
	}
	for k := range droppedMints {
		delete(s.st.Mints, k)
	}
	if err := s.save(stateChange{DropAccounts: []int64{id}}); err != nil {
		// 没落盘就把内存复原，否则进程还活着的这段时间里账号凭空消失了。
		s.st.Accounts = append(s.st.Accounts, types.Account{})
		copy(s.st.Accounts[idx+1:], s.st.Accounts[idx:])
		s.st.Accounts[idx] = acct
		for k, sl := range dropped {
			s.st.Slots[k] = sl
		}
		for k, m := range droppedMints {
			s.st.Mints[k] = m
		}
		return err
	}
	return nil
}

// ---------- 网关 ----------

// Gateways 返回网关副本（ReachableExits 也复制，调用方改不到内存里那份）。
func (s *Store) Gateways() map[string]types.Gateway {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]types.Gateway, len(s.st.Gateways))
	for k, g := range s.st.Gateways {
		out[k] = cloneGateway(g)
	}
	return out
}

func cloneGateway(g types.Gateway) types.Gateway {
	g.ReachableExits = append([]types.Exit(nil), g.ReachableExits...)
	return g
}

// UpsertGateway 记一个落点，返回它对池子是不是全新的。
//
// exit 是**请求过的**那个出口，不是观测到的地理：ReachableExits 的唯一用途是
// 「下次想回到这个网关该怎么请求」，而 types.Exit 按定义就是请求参数
// （观测到的地理是自由文本，塞进来会被当成代理用户名送出去 ⇒ 认证失败或回退到别国）。
// 观测到的地理记在 samples.jsonl 里，那边 Requested / Observed 两列都有（SPEC 红线 6）。
func (s *Store) UpsertGateway(name string, seenBy int64, requested types.Exit) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, found := s.st.Gateways[name]
	old := cloneGateway(g)
	if !found {
		g = types.Gateway{Name: name, FirstSeenBy: seenBy, FirstSeenAt: time.Now().UTC()}
	}
	known := false
	for _, e := range g.ReachableExits {
		if e == requested {
			known = true
			break
		}
	}
	// 只有城市没有国家的出口重放时会被静默降级成「不定向」，不收。
	if !known && requested.Region != "" {
		g.ReachableExits = append(g.ReachableExits, requested)
	}
	s.st.Gateways[name] = g
	if err := s.save(stateChange{Gateways: map[string]types.Gateway{name: g}}); err != nil {
		// 落盘失败要把内存退回去。漏掉「网关已存在」这一支的话，新追加的 ReachableExits
		// 只活在内存里，重启后悄悄回退，而调用方以为写成功了。
		if found {
			s.st.Gateways[name] = old
		} else {
			delete(s.st.Gateways, name)
		}
		return false, err
	}
	return !found, nil
}

// UpdateGateway 就地改一个网关的字段。网关不存在时什么都不做（不是错：
// 它可能刚被别的路径删了，而调用方只是想记一次续期失败）。
func (s *Store) UpdateGateway(name string, mutate func(*types.Gateway)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.st.Gateways[name]
	if !ok {
		return nil
	}
	old := cloneGateway(g)
	g = cloneGateway(g)
	mutate(&g)
	s.st.Gateways[name] = g
	if err := s.save(stateChange{Gateways: map[string]types.Gateway{name: g}}); err != nil {
		s.st.Gateways[name] = old
		return err
	}
	return nil
}

// ---------- 槽位 ----------

func (s *Store) Slot(accountID int64, gateway string) (types.Slot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sl, ok := s.st.Slots[SlotKey(accountID, gateway)]
	return sl, ok
}

func (s *Store) Slots() map[string]types.Slot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]types.Slot, len(s.st.Slots))
	for k, v := range s.st.Slots {
		out[k] = v
	}
	return out
}

func (s *Store) PutSlot(sl types.Slot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := SlotKey(sl.AccountID, sl.Gateway)
	old, had := s.st.Slots[k]
	s.st.Slots[k] = sl
	if err := s.save(stateChange{Slots: map[string]types.Slot{k: sl}}); err != nil {
		if had {
			s.st.Slots[k] = old
		} else {
			delete(s.st.Slots, k)
		}
		return err
	}
	return nil
}

// UpdateSlot 在写锁内读-改-写一个槽位，没有的就现建一条。
//
// 必须原子：交付（写 LastTouch）、判定（写 LastVerdict）、续期循环会并发改同一个
// 槽位，各读各写会丢更新，丢掉 LastTouch 就等于把烧过的槽位当新鲜的发出去。
func (s *Store) UpdateSlot(accountID int64, gateway string, mutate func(*types.Slot)) error {
	if mutate == nil {
		return errors.New("UpdateSlot 要一个改写函数")
	}
	return s.queueSlotWrite(slotMutation{accountID, gateway, mutate})
}

// ---------- 区域铸票台账 ----------

// RegionMints 返回整张 (账号 × 区域) 台账的副本，key = MintKey。
//
// 整张给出去而不是一格一格取：挑铸票者要同时看一个号的九个格子，
// 九次加锁换不来任何东西（这张表是几十行量级）。
func (s *Store) RegionMints() map[string]types.RegionMint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]types.RegionMint, len(s.st.Mints))
	for k, v := range s.st.Mints {
		out[k] = v
	}
	return out
}

// NoteRegionMint 记一次**成功**的铸票：计数 +1，记下时刻和落点。
//
// 只记成功的，见 types.RegionMint 的注释。regionKey 为空（出口反查不到区域，
// 只可能来自手改的 state.json）时什么都不做：那一格画不到九宫格上去，
// 记了也只是让 state.json 长出一行谁都不读的垃圾。
//
// gateway 为空就**保留**上一次的落点，不清掉：读不出落点的那种成功（送进去的 pair
// 被上游接受、没回新 cookie）并不意味着这个组合的落点变了，而清掉会让预铸把这个
// 组合重新当成「没铸过」、再去白打一发。
func (s *Store) NoteRegionMint(accountID int64, regionKey, gateway string, at time.Time) error {
	if regionKey == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := MintKey(accountID, regionKey)
	old, had := s.st.Mints[k]
	m := old
	m.AccountID, m.RegionKey = accountID, regionKey
	m.Count++
	m.LastAt = at
	if gateway != "" {
		m.Gateway = gateway
	}
	s.st.Mints[k] = m
	if err := s.save(stateChange{Mints: map[string]types.RegionMint{k: m}}); err != nil {
		if had {
			s.st.Mints[k] = old
		} else {
			delete(s.st.Mints, k)
		}
		return err
	}
	return nil
}

// ---------- pair ----------

func (s *Store) Pair(gateway string) (types.Pair, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.st.Pairs[gateway]
	return p, ok
}

func (s *Store) Pairs() map[string]types.Pair {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]types.Pair, len(s.st.Pairs))
	for k, v := range s.st.Pairs {
		out[k] = v
	}
	return out
}

func (s *Store) PutPair(p types.Pair) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, had := s.st.Pairs[p.Gateway]
	s.st.Pairs[p.Gateway] = p
	if err := s.save(stateChange{Pairs: map[string]types.Pair{p.Gateway: p}}); err != nil {
		if had {
			s.st.Pairs[p.Gateway] = old
		} else {
			delete(s.st.Pairs, p.Gateway)
		}
		return err
	}
	return nil
}

// ---------- 样本 ----------

// AppendSample 追加一行样本。samples.jsonl 只追加、永不重写（SPEC 第 9 节）。
// DeletePair 丢掉一张票。连续续不上的死票留着只会让下一轮又拿它去续一次。
func (s *Store) DeletePair(gateway string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.st.Pairs[gateway]
	if !ok {
		return nil
	}
	delete(s.st.Pairs, gateway)
	if err := s.save(stateChange{DropPairs: []string{gateway}}); err != nil {
		s.st.Pairs[gateway] = old
		return err
	}
	return nil
}

func (s *Store) AppendSample(sm types.Sample) error {
	b, err := json.Marshal(sm)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.samples.Write(b)
	if err == nil {
		cache := s.routingHistory
		if cache == nil {
			cache = s.routingPending
		}
		if cache != nil {
			if len(b) <= maxRoutingLineBytes {
				cache.append(sm)
			} else {
				cache.skippedLines++
			}
		}
	}
	return err
}

// Samples 回读全部样本，供 admin 视图和 cohort 分析用。坏行跳过，不让一行脏数据毁掉整个视图。
func (s *Store) Samples() ([]types.Sample, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, err := os.Open(filepath.Join(s.dir, samplesFile))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []types.Sample
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var sm types.Sample
		if json.Unmarshal(line, &sm) == nil {
			out = append(out, sm)
		}
	}
	return out, sc.Err()
}

// newToken 生成一个进 URL 的随机令牌。128 bit，URL 安全。
//
// view_token 和 owner token 共用这一份：两者的威胁模型一模一样（进 URL、谁拿到
// 谁就是他），强度也就该一模一样。另写一个只会让两边哪天不一致。
func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
