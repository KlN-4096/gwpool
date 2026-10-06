# 页面、权限与清理

- 页面门禁（site_key）、上传者/单账号令牌、管理员Basic/Bearer是独立权限；`base.Admin`只在完成管理员鉴权的handler赋值。
- 管理sidebar与201历史机房目录仅管理view持有；公共`poolRegionCells`只对已发现名称查询机房归属（同后台），不遍历候选目录，不下发分母/缺项。九区+未知互斥合计等于全池，未知不从代理出口推断；紧凑网关格保留。
- `buildDirectory`只汇总Snapshot.Datacenters，目录外另列Unknown；不使用含目录外发现的Snapshot.Found当201分子。零分母显示暂无条目，不报100%。
- 公共网关无Cookie/账号来源/消费明细；公开动态用poolMintRow白名单。单账号页面只在owner cookie匹配时给总览链接；gate不含运行模式/业务导航/原始私密URL。
- `acards`和`gateway-cards`仍保留语义表格、搜索/排序/24条分页；详细观察放dialog或原生details，无JS不丢数据。
- Owner默认折叠卡片；`gwpool.expanded.v2`显式记展开集合，旧折叠集合不推断为“全部展开”。窄屏说明不放进nowrap按钮。
- 账号近15分钟统计在截断500条展示前按ID、时间窗计算；HTTP200不是交付/质量成功率，耗时中位数仅含正耗时样本。
- 401清理：只认当前禁用、有AT、mint/scan固定401前缀。GET签名候选/10分钟TTL→同源管理员POST确认→Store账号锁升序→写锁复核→一次DropAccounts WAL提交。
- 确认绑定CredentialRevision及凭据指纹；启停/重传/刷新/自动停用写入修订号，失败回滚。原始字段相同也不能绕过“恢复后再次401”的ABA防护。
- 删除保留Pairs/Gateways/历史；清理Slots/Mints失败须恢复内存。只处理确认候选，不把后来出现的401扩入；重复提交已删项跳过。
- 删除不能只等待在途任务：扫描每次attempt在账号锁内重读存在/启用/凭据，防排队旧快照在删除后出站或复活记录。
- 回归：`types/account_cleanup_test.go`、`store/account_cleanup_test.go`、`scan/cleanup_test.go`、`httpapi/{account_cleanup,directory,account_activity}_test.go`；`TestDashboardSettingsFixture`可导出8种假数据页面。
