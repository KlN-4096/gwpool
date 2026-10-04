# 状态与边界

- `internal/store/store.go` 保存 `state.json` 和 `samples.jsonl`，不使用数据库。
- `State` 顶层 JSON 键是小写；`types.Pair` 等嵌套结构使用 Go 字段名，如 `RegionKey`、`MintedBy`。
- 所有账号出站共用 Store 的账号锁；调度需要同时持锁时先网关、后账号。
- `Pair.RegionKey`、`MintedBy` 是后续种子和续票的来源信息，不能从网关名或随机出口反推。
- 新票按响应中的真实 `Gateway` 保存；漂移不应覆盖原目标网关的票。
- cookie、凭据、身份串、票号不进诊断输出；运维脚本只输出显式允许的计数、区域、网关和本地账号 ID。
- 不直接编辑运行中的 `state.json`；它由服务持有并原子保存。
- 冷却报告独立于票/槽位：仅接收已鉴权匿名观察，保留 7 天/全局 8192/每 consumer 1024 条；同事件同载荷幂等、冲突 409，写失败恢复内存。
- 推荐阶梯1/2/4/6/8/10/12/16/20/24h，下界高于失败证据，24h仍失败不给更短建议；个人至少两次成功，全池至少三个独立consumer、每consumer一票。无能力头按旧10h上限重算证据，不截断新建议。
- 冷却报告`observed_at`保原事件时间，旧报告回退ReceivedAt；同ID事件时间也不可变。POST批量最多32条逐项ACK，部分失败不丢成功；个人>=6报告、最近3条成功同推荐档且跨度>=24h才建议6h上报，新失败/过期撤回策略。
- sub2api 文件仅在上传页展开为现有单账号上传，身份校验/去重仍由 `/upload` 和 Store 负责。批量重试保留结果、原序号与 owner，不能依赖浏览器 cookie；代理/extra 不迁移。
- Owner key由根密钥与Owner稳定身份派生，名下至少一份有效凭据才可用；旧credential key仅启动迁移时grandfather，继续鉴权但UI/上传回执不再分发。空库也保存迁移标记，后建账号不得因重启获得旧key资格。
- 接触报告的Owner ReporterID与旧credential ReporterID分域；admin/root不能作为独立消费者上报。7天/8192总/每reporter1024，unknown不计有结论分母，窗口结束补报不得改初始身份/结论。
