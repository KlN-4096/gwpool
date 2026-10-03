# 验证与部署

- Go 标准库项目；入口为 `cmd/gwpool`，模板由 `web` 嵌入。
- 测试用 fake/httptest，不能请求真实上游；非平凡 bug 先添加能失败的回归用例。
- Windows 原生 Go 构建和测试；并发限定 `-p 2`，测试使用 `-timeout 60s`，不跑 `-race`。
- 小改先跑定向用例；涉及共享铸票/调度行为时运行 `go test -p 2 -timeout 60s ./...`，再 lint。
- Linux 产物采用 `GOOS=linux GOARCH=amd64 CGO_ENABLED=0`，`-trimpath -ldflags="-s -w"`。
- bytevirt 的服务和二进制是 `gwpool`、`/opt/gwpool/gwpool`；配置在 `/opt/gwpool/data/config.json`。
- 切换前比对旧二进制 SHA256，保留回滚文件，避免覆盖其他会话的部署。
- 上线验收同时看服务健康、自然续票的真实成功/失败、预铸补货和 TTL；仅“测试绿/进程 active”不算业务验收。
