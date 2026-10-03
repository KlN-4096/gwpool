# 调度

- `internal/sched/warm.go` 的 `warmOnce` 先续票、再按 `PairTarget` 补铸；无需求或不足两个可用账号时不保温。
- `renew_pairs` 默认关；开启后使用 `Pair.MintedBy`，不是另挑最空闲账号。
- 种子出口通过 `NewSeedSID` 绑定；`internal/mint/mint.go` 在 `SeedBound` 路径只发送 `__cflb`。
- 种子铸票的逻辑区域来自所用票据，不是机房实际出口，也不能随机借用网关的历史出口标签。
- 上游账号每区域预算由 `mint.Client.spend` 强制执行；单账号限流不是全池无出口。
- `mintOnce` 按响应实际落点保存新票并更新触碰；定向续票漂移仍报失败，不能把意外落点称为目标续成功。
- `mint.Rehome` 只在旧 `__oailb` 尚有效时借同区长寿 `__cflb`；TTL 是两个组件的较早到期时间。
- 区域与账号对应落点是采样得到的预测，不是上游保证；离线测试不能代替部署后的落点/TTL观察。
