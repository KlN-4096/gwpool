# 调度

- `internal/sched/warm.go` 的 `warmOnce` 先续票、再按 `PairTarget` 补铸，最后最多一发专门探索；无需求或不足两个可用账号时不执行这些任务。
- `renew_pairs` 默认关；开启后使用 `Pair.MintedBy`，不是另挑最空闲账号。
- 种子出口通过 `NewSeedSID` 绑定；`internal/mint/mint.go` 在 `SeedBound` 路径只发送 `__cflb`。
- 种子铸票的逻辑区域来自所用票据，不是机房实际出口，也不能随机借用网关的历史出口标签。
- 库存/续票可借种子；专门探索必须 `NewSID(true)`+空票走实际出口，否则不能验证覆盖目标。seeded 请求一律不新增 `ReachableExits`，即使机房恰好同国；RegionMint 仍保留逻辑区域。
- 上游账号每区域预算由 `mint.Client.spend` 强制执行；单账号限流不是全池无出口。
- `mintOnce` 按响应实际落点保存新票并更新触碰；定向续票漂移仍报失败，不能把意外落点称为目标续成功。
- `renewed` 还要求新票 `ExpiresAt` 严格晚于原票；同落点但 TTL 不变单列 `unchanged`。借同区长寿 cflb 的换盆与持久化保留，不承诺每次同账号同区必落原网关。
- `Mint` 同时返回 HTTP 状态与解析错误时，非 200 状态优先；否则 401/429 缺少 __oailb 会掩盖停用/退避，循环白打。回归见 `mint_status_test.go`。
- `mint.Rehome` 只在旧 `__oailb` 尚有效时借同区长寿 `__cflb`；TTL 是两个组件的较早到期时间。
- 区域与账号对应落点是采样得到的预测，不是上游保证；离线测试不能代替部署后的落点/TTL观察。
- 管理员全量队列逐个执行“启用且有凭据账号 × 标准区域”，活动批次不叠加；满额/停用/无出口显式跳过，账号短间隔等待。
- 手动队列与 `warmOnce` 共用后台预算锁，仍复用 `premintSID` / `mintOnce`，不复制落账逻辑；队列不存凭据快照，重启清空。
- `RegionEnabled` 同时检查稳定 key/出口/启用态，两种延迟队列出手前复查；无上游验证的现成票交付不要求出口继续启用。
- `premintNote` 只有全池 `ErrNoExit` 才停整轮，局部 `ErrRateLimited` 走原失手预算但不设置全局 halt。
- OAuth 刷新与铸票共用 Store 账号锁，锁后重读凭据；否则刷新后的账号可能被旧 token 的迟到 401 停用。
