# gwpool 规格（实现者必读）

网关池服务。设计背景、实证数据、已证伪清单在
`E:\Project\GO\sub2api\docs\tasks\gateway-pool.md` 和
`E:\Project\GO\sub2api\docs\conventions\codex-full-strength-tickets.md`。
**动手前先读那两份**，本文件只写实现约束。

参考实现：`G:\temp\Claude_temp\geo_sampler.py`（已验证可用，采了 170+ 个真实样本）。
铸票、代理、cookie 解析的正确做法照它翻译，不要自己发明。

---

## 0. 一句话

托管若干 Codex 账号 → 每个账号经 9 个区域出口各打一发 ping，发现它能到达的网关 →
把这些网关的路由 cookie（pair）收进池子 → 下发给任意账号使用。

**实证基础**：网关分配按「账号 cohort × 地理」决定；pair 可跨账号使用；
借来的网关对借方是**满血**的（它从没烧过那个网关）。

---

## 1. 领域概念（名词必须统一，不要另造）

| 名词 | 含义 |
|---|---|
| **网关 gateway** | `unified-N`，上游推理节点。从 `__oailb` 这个 JWT 的 `host` 字段第三段取 |
| **pair** | 路由 cookie，`__cflb` + `__oailb` 两项。**只决定路由，不携带健康度** |
| **turn-state 票** | 响应头 `x-codex-turn-state`，Fernet 串，无内嵌过期 |
| **满血窗口** | 某账号首次接触某网关后约 **183 秒**，之后**静默**失效（仍回 HTTP 200 但答错） |
| **槽位 slot** | `(account_id, gateway)` 二元组。降智的作用单位就是它 |
| **cohort** | 账号群组。同 cohort 的账号在同一区域拿到同一个网关 |

---

## 2. 铸票语义（最容易写错的地方）

一次铸票 = 经指定代理出口，向
`https://chatgpt.com/backend-api/codex/responses` POST 一个最小 ping。

请求头照 `geo_sampler.py`：`authorization` / `chatgpt-account-id` / `content-type` /
`accept: text/event-stream` / `originator: codex-tui` / `user-agent: codex-tui/0.154.0 ...`

请求体：
```json
{"model":"gpt-6-astra","instructions":"","stream":true,"store":false,
 "input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"ping"}]}],
 "reasoning":{"effort":"low"},"tool_choice":"auto","parallel_tool_calls":false}
```

### 落点判定 —— 写死这条，不要自己推理

```
响应里有新的 set-cookie __oailb  ⇒ 落点 = 该 JWT 的 host 第三段
响应里没有新的 __oailb：
    本次请求发送了 pair ⇒ 落点 = 发送的那个 pair 的网关（上游接受了，没换）
    本次请求没发 pair   ⇒ 异常，当作失败
```

依据：relay 作者的 `index.js`
```js
const node = attempt.pairs ? attempt.gateway : (attempt.steered && pair ? pair.gateway : '');
```
以及 2026-10-01 的三路对照实验（坏 pair 会触发重新下发，好 pair 不会）。

**不要把「没下发新 cookie」当成失败。**

---

## 3. 满血判据

**state-echo**：带一张**活票**（刚拿到的 `x-codex-turn-state`）再打一发同样的 ping，
只读响应头：

```
HTTP 必须 200，否则不下结论（原版把 429/403 判成满血，纯假阳）
不下发新 state，或新 state == 原票  ⇒ 满血
下发了不同的新 state                ⇒ 降级
```

**有假阴、无假阳**：判「满血」可信，判「降级」可能是误判。
所以**不要拿它当持续闸门**，只在交付槽位时验一次。

---

## 4. 九个区域与代表出口（2026-10-01 实测 170+ 样本）

扫描一个新账号时，每区打一发即可，落点开始全部重复就停，不必全量。

| 区域 key | 代表出口（novproxy region[:city]） | pro1 落点 | 账号B 落点 |
|---|---|---|---|
| `us-east` | `US` | 167 | 167 |
| `us-west` | `US:Seattle` | 126 | 123 |
| `south-america` | `BR` | 202 | 84 |
| `west-europe` | `ES` | 183 | 184 |
| `europe` | `IT` | 165 | 142 |
| `east-asia` | `JP` | 195 | 195 |
| `oceania` | `AU` | 73 | 71 |
| `south-asia` | `IN` | 144 | 190 |
| `middle-east` | `SA` | 150 | 190 |

**区域划分本身随 cohort 变**（账号B 把 south-asia 和 middle-east 合并了），
所以区域只是扫描清单，不要当成数据模型里的强约束。

---

## 5. 出口代理

novproxy，粘性会话：

```
socks5h://<user>-region-<CC>[-city-<City>]-sid-<随机8位>-t-<分钟>:<pass>@us.novproxy.io:1000
```

- **同一个 sid 在 t 分钟内固定同一出口 IP**。铸票和 state-echo 必须用**同一个 sid**，
  否则可能换 IP、换网关
- `sg.novproxy.io` **是死的**，全部走 `us.novproxy.io:1000`，它打亚洲正常
- 城市库存不足会**回退到别的国家**（实测 `city-Dallas` 给过尼日利亚 IP）
  ⇒ **按观测到的地理记录，不要按请求的 region 记录**
- 配置从文件读，**不要进命令行参数、不要进日志**

webshare 作为备选：`socks5://<user>-<CC>-rotate:<pass>@p.webshare.io:1080`，
每连接换 IP，**做不了粘性会话**，只能用于粗粒度国家扫描。

---

## 6. pair 寿命与续期

| | 值 |
|---|---|
| `__oailb` | `exp - iat` = **3900 秒（65 分钟）**，硬过期 |
| 满血窗口 | ~183 秒，与 pair 有效期**无关** |

- **pair 自我续期**：经某网关铸一发票，上游回一张**该网关的新 65 分钟 pair**
- 续期时机：剩余 < 600 秒
- **交付只给剩余 ≥ 600 秒的 pair**，否则先续再发

---

## 7. 两种运行模式（必须显式开关，不要一套逻辑硬套）

### 单账号模式（扫描者 = 消费者）

保温那一发**恰好烧掉你想用的窗口**。所以：

```
请求来了 → 现铸一发 → 立刻交付 → 用完即弃
```

不维护池子、不续期、不预热。

### 多账号模式（扫描者 ≠ 消费者）

扫描账号保温，消费账号取用。保温成本 = 每网关每 65 分钟一发 ping。

**无需求时不扫不续**，让 pair 自然过期，成本归零。

---

## 8. 调度

**分配 = `slots` 表里该消费账号歇得最久的那个 `(account, gateway)`。**

交付流程：
```
1. 选 slot（歇得最久）
2. 确保该网关有剩余 ≥600s 的 pair，没有就现铸
3. state-echo 验一次
   满血 → 交付，有效期 150 秒（实测窗口 183s，留 18% 余量）
   降级 → 标记该 slot，退避，换下一个 slot
4. 所有 slot 都降级 → 返回 503（宁可 503 也不放降智）
```

**单账号并发硬性为 1**。这是已知的封号主因（2026-09-29 实测 5~6 并发 20~30 分钟
废掉一个通道）。用 per-account mutex，不要用全局信号量。

失败的 slot 用**指数退避**，不要快速重试（快重试会让供给更快耗尽）。

---

## 9. 存储：**不要用数据库，零第三方依赖**

数据量很小（几十个账号、十几个网关、每天几十行样本），上数据库是负债。
而且 `modernc.org/sqlite` 要 Go 1.26，本机是 1.23。**`go.mod` 必须保持零 require。**

两份文件，`DataDir` 下：

```
state.json      可变状态：accounts / gateways / slots / pairs
                全量读进内存，改完**原子写**（写 .tmp 再 os.Rename）
samples.jsonl   append-only，一次扫描一行，**永不重写**
```

- `state.json` 用一把 `sync.RWMutex` 守；进程内只有一份内存副本
- 两个文件权限 **0600**，目录 **0700**（里面有 access_token）
- 启动时读不到就建空的，不要报错退出

内存结构直接用 `types` 包里的类型：

```go
type State struct {
    Accounts []types.Account            `json:"accounts"`
    Gateways map[string]types.Gateway   `json:"gateways"`   // key = "unified-142"
    Slots    map[string]types.Slot      `json:"slots"`      // key = "<accountID>|<gateway>"
    Pairs    map[string]types.Pair      `json:"pairs"`      // key = gateway
}
```

**凭据不加密、不做撤销**（运营方 2026-10-01 决定：不放心的人自己取消设备登录）。
但**不要把 access_token 写进任何日志、任何页面、任何 API 响应**。
`types.Creds` 要实现 `String()` 返回打码值，防止被 `%v` 误打出来。

因此服务**没有在线停用/删除账号的入口**（store 刻意不提供 `PutAccount`）：抹掉一行就停服务改
`state.json`。账号表分两格封顶——托管账号 500、匿名占位行 200。分开算是必须的：占位行是匿名调用方
报个 ID 就能长出来的，和托管账号共用一个上限的话，灌满之后**所有上传永久失败**而服务内无补救。

---

## 10. HTTP 接口

| 路由 | 鉴权 | 作用 |
|---|---|---|
| `GET /` | 无 | 公开池视图：网关列表、各自有没有空闲槽位。**不下发 pair** |
| `POST /upload` | 无 | 上传凭据 JSON，建账号，排队首扫，返回 `view_token` |
| `GET /a/{view_token}` | token | 上传者信任视图 |
| `GET /cookie` | consumer key | **下发网关 cookie**，参数 `gateway`（可选，不给就按调度选） |
| `GET /admin/*` | admin key | 全量使用记录 |

### 没有消费端回报这回事

票是池子发的、满血也是池子验的——交付那一刻 `Deliver` 自己就写了 slot 的
`last_touch`，`VerifyFull` 自己就写了 `last_verdict`。消费端能告诉池子的东西，
池子早就知道了，而它**不**知道的（转发路径上没有降智判据）回报里也没有。
所以不设回报端点：多一个写路径，只会让消费端的 `unknown` 覆盖掉池子的真判定。

### 上传者视图是信任凭证，不是运维面板

明写成这种句子：

> 本服务用你的凭据发起了 **21 次请求**，全部是 `ping`（`reasoning.effort=low`），
> 累计输出 **0 token**，只用过 `gpt-6-astra`，为池子发现了 **5 个新网关**。

让托管的人一眼看出没动他的额度。

### `/cookie` 返回

```json
{"gateway":"unified-142","cookie":"__cflb=...; __oailb=...",
 "expires_at":"2026-10-01T06:40:00Z","valid_for_s":150,
 "verified_full":true}
```

**怎么用是使用者的事**，服务只负责交付 cookie。

---

## 11. 包布局

```
cmd/gwpool/main.go
internal/types/      共享类型（已由架构方写好，不要改签名）
internal/config/     配置加载（代理凭据、admin key、模式开关）
internal/store/      state.json + samples.jsonl（见第 9 节，**不是数据库**）
internal/mint/       铸票 + state-echo（照 geo_sampler.py 翻译）
internal/scan/       扫描 worker
internal/sched/      调度器
internal/httpapi/    路由 + 模板
web/templates/       HTML
```

---

## 12. 红线

1. **不要把 access_token / 代理密码写进日志、页面、API 响应**
2. **单账号并发硬性 1**
3. **不要把「没下发新 __oailb」当失败**（见第 2 节）
4. **不要拿 state-echo 当持续闸门**（有假阴）
5. **不要在单账号模式下保温**（会烧掉自己要用的窗口）
6. **不要按请求的 region 记录地理**，按观测到的记录
7. 失败退避用指数，不要快重试
8. 测试不要打真实上游。mint 包要能注入 HTTP 客户端，用 httptest 跑
