# 状态与边界

- `internal/store/store.go` 保存 `state.json` 和 `samples.jsonl`，不使用数据库。
- `State` 顶层 JSON 键是小写；`types.Pair` 等嵌套结构使用 Go 字段名，如 `RegionKey`、`MintedBy`。
- 所有账号出站共用 Store 的账号锁；调度需要同时持锁时先网关、后账号。
- `Pair.RegionKey`、`MintedBy` 是后续种子和续票的来源信息，不能从网关名或随机出口反推。
- 新票按响应中的真实 `Gateway` 保存；漂移不应覆盖原目标网关的票。
- cookie、凭据、身份串、票号不进诊断输出；运维脚本只输出显式允许的计数、区域、网关和本地账号 ID。
- 不直接编辑运行中的 `state.json`；它由服务持有并原子保存。
