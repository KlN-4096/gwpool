# 探索出口与覆盖目标

- `config.Regions` 是可热重载的实际出口定义，缺省为原九区；稳定 key 与国家/城市不可改或删除，名称/启用态可改。新增出口不得与旧出口别名重复，停用不重置历史/配额。
- `RegionDefinitions` 供历史/视图，`CandidateRegions` 仅含启用项；mint 速率桶、续票、按需/预铸、全量队列和公开/账号区域展示都读配置，不热改 `types.Regions` 全局变量。并发上限不因新增出口扩大。
- `CoverageGroups` 另存显示名、实际出口关联、显式目标网关和“清单完整”确认，不因分组改名修改种子或限额。
- `internal/coverage` 内置 2026-10-01 DNS 枚举得到的 201 名（最大编号215，有跳号），只是历史可解析候选；禁止当成当前可铸全集或按 IP 地理归类。
- 分组“已找满此清单”要求：非空、确认完整、每个目标均存在与关联出口匹配的历史 `ReachableExits`。票过期不抹掉历史；未知归属单列。
- 原始候选+实际发现+手填目标合并展示，当前可交付只按有效票寿命，不代表任意账号的冷却/配额资格。
- 专门探索优先未完成区的未走组合，每自然轮最多一发；已完成区不争这份预算，但续票/补库存仍可使用。所有明确目标完成并不代表未知目录也完成。
- `/admin/coverage` 写入口需 admin+sameOrigin，64KiB 表单限制；配置只按键 Patch，不能序列化整份含 Secret 的 Config。保存不立即探测。
- 回归：`config/regions_test.go`、`coverage/coverage_test.go`、`sched/{regions,explore}_test.go`、`mint/regions_test.go`、`httpapi/coverage_test.go`、`cmd/gwpool/discovery_test.go`。
- 实际出口含country/state/city，逐供应商校验能力，不支持不得静默忽略；只有Geo有全部匹配证据才新增实际可达关系，seed续票不制造地理证据。
- 增区不放大账号总小时预算（原9区×每区限额）；目录只提供合法候选，不默认全开。静态seed只续旧票，动态供应商用于新铸/库存/探索，各自轮询且共用账号锁和预算。
