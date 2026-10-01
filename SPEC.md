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

### 上游换不换 cookie，判据是**送进去的那张 pair 还好不好**

本节和第 6 节「经某网关铸票回一张新 65 分钟 pair」**两条都对，各管一边**：

| 送进去的 pair | 上游 | 对应条款 |
|---|---|---|
| 没送（裸打） | 下发一张新的 65 分钟 pair | 第 6 节 |
| 烧过的 / 已过期的 | 无视它，换一张新的 | 第 6 节 |
| 还有效的 | 接受它，**不下发新 cookie** | 本节 |

推论：**「还有效但快到点」的 pair 续不动**。所以交付路径上「所有票都低于交付门槛」
时的回落必须**裸铸**，不能带着那张短票去续——带着打等于花一发请求、烧一个铸票者
槽位，换回手里那张原封不动的短票，然后交付照样拒发（见 `sched.pairFor`）。

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
所以**不要拿它当持续闸门**，最多在交付槽位时验一次。

而 `verify_full_at_deliver` **默认关**，于是默认路径上一发验证都不打：验证要用
**消费者自己的**凭据先铸一张活票再 echo，那两发恰好烧掉它马上要用的那个窗口，
验完交出去的已经是个二手落点。关掉只少一个读数（判定如实写 `unknown`），
不会把降智的东西放出去——保证来自调度的那两条闸（第 8 节）。
**所以任何「反正交付前会验一次」的兜底推理都不成立。**

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

- **pair 自我续期**：经某网关**裸铸**（或带一张已经烧过/过期的 pair）一发票，
  上游回一张**该网关的新 65 分钟 pair**。带一张**还有效**的 pair 进去则不会换
  （判据见第 2 节那张表）⇒ 「还有效但快到点」的票**续不动**，只能裸铸
- 续期时机：剩余 < `RenewLead` = 600 秒
- **交付只给剩余 ≥ `DeliverFloor` = 300 秒的 pair**，否则先续再发。
  比 `RenewLead` 低一截是刻意的：两个数相等时，一张低于门槛的票同时意味着
  「不能发」和「该续了」，于是供给紧的时候交付路径只能原地等保温
  （`types.DeliverFloor` 的注释里有完整论证）
- 调用方可以用 `min_remaining` **只为这一次请求**把门槛压到 30 秒（见第 10 节）；
  池子自己的健康读数（`/gateways`、空闲网关数）不跟着变

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
1. 排掉调用方自己排除的网关（exclude / exclude_versions）
2. 排掉这个账号此刻不能给满血的 slot（两条闸，见下）
3. 硬筛：只在**此刻有活 pair** 的网关里挑（交付一张现成的票 = 1 个槽位 + 0 发请求，
   而铸一张 = 1 发请求 + 两个槽位：铸票者的 + 消费者的）
   一张活票都没有时才现铸，而且只抽一次签
4. 选 slot（歇得最久；从没碰过的最优先）
5. 开了 verify_full_at_deliver 才 state-echo 验一次（**默认关**）
   满血 / 没验 → 交付，有效期 = min(窗口剩余, pair 剩余)，窗口 150 秒
                （实测 183s，留 18% 余量；从**首次接触**起算，不是从响应回来起算）
   降级 → 标记该 slot，退避一个满血窗口，换下一个 slot
6. 挑不出来 → 503，并按错误码说清是哪一种（第 10 节那张表）
```

**不能给满血的两条闸**（`sched.coolingSet`），缺一条就会把正在烧的槽位当新鲜的发出去：

1. `last_deliver` 在 `slot_cooldown_minutes` 内——票真发出去过，窗口用完了
2. `last_touch` 在一个满血窗口（183s）内、**且此刻不在退避中**——预铸、扫描、
   别人的续期、失败的铸票都会 touch，而那一发就在眼前

第 2 条的豁免是「**此刻仍在**退避中」而不是「有过退避记录」：`backoff_until` 只在
该槽位**成功交付**时才清零，按「字段非零」豁免等于让这条闸对任何失败过一次的槽位
永久沉默（退避上限 1 分钟 < 窗口 183 秒，中间那 123 秒谁都不管）。

**单账号并发硬性为 1**。这是已知的封号主因（2026-09-29 实测 5~6 并发 20~30 分钟
废掉一个通道）。用 per-account mutex，不要用全局信号量。

失败的 slot 用**指数退避**，不要快速重试（快重试会让供给更快耗尽）。

**稀缺的是槽位，不是请求**：每账号每小时 54 发请求（9 区 × 6），但槽位只有
「已知网关数 ÷ 冷却」那么多（十几个网关、4 小时冷却 ≈ 3.5 个/小时），紧约 15 倍。
所以任何「多打一发看看」的设计都要先算这笔账。

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
| `GET /cookie` | consumer key | **下发网关 cookie**，参数见下 |
| `GET /gateways` | 同 `/cookie` | 网关清单 + 两个池子侧门槛，给客户端自己挑落点 |
| `POST /release` | 同 `/cookie` | 归还一次「取了票但一发没打」的交付 |
| `GET /admin/*` | admin key | 全量使用记录 |

### 消费端协议（2026-10-02 **冻结**）

**参数名、JSON 字段名、错误码这三样一个字都不许改**（sub2api 侧照同一份实现）。
要改先改这一节，而且两边同时改。

`GET /cookie` 的可选参数：

| 参数 | 含义 |
|---|---|
| `gateway` | 点名一个网关 |
| `force=1` | 「我手上那张不行了」，必须换一个网关 |
| `account` | 替哪个**上游账号**取（槽位按它记）；跨账号/匿名时必填 |
| `exclude=<名>,…` | 跳过这些网关。**最多 64 项、每项 ≤64 字符**，认不出的名字静默忽略。在「有活 pair」硬筛**之前**生效 |
| `exclude_versions=<v>,…` | 跳过这些**具体的票**（`cookie_version`）。同样 64 项上限 |
| `min_remaining=<秒>` | 这一次能接受的 pair 最低剩余寿命，钳到 `[30, 3600]`，缺省 = `DeliverFloor`。**只影响这一次的候选筛选** |
| `wait=<秒>` | 钳到 `[0, 30]`，缺省 0。只在本次会以**可等待**码失败时才等（`no_live_pair` / `no_gateway`），等到票就重跑一次选择。**绝不在 `all_cooling` 上等**（那是几小时） |

`gateway` 和 `exclude` 自相矛盾时**按 `exclude` 办**（拒）：`exclude` 编码的是
「我烧过这个」，照它办永远不会发出一张坏票。

成功响应（现有字段一个字不动，只加了 `cookie_version`）：

```json
{"gateway":"unified-142","cookie":"__cflb=...; __oailb=...",
 "expires_at":"2026-10-01T06:40:00Z","valid_for_s":150,
 "verified_full":true,"ttl_is_advisory":true,"cookie_version":"3f2a9c1b7d04"}
```

- `valid_for_s` = `min(满血窗口剩余, 这张 pair 的剩余寿命)`，**建议值**不是承诺
- `cookie_version` = `sha256(cookie)` 的前 12 个十六进制字符。确定性、不存状态、
  重启后仍一致 ⇒ 调用方拿它回头指名这张票（`exclude_versions`、`/release`）

失败响应（`/cookie` 所有失败路径，状态码沿用原来的）：

```json
{"error":{"code":"all_cooling","message":"人话","retry_after_seconds":7200}}
```

带 `retry_after_seconds` 时同时发 `Retry-After` 头。错误码是**闭集**：

| code | 含义 | retry_after |
|---|---|---|
| `no_gateway` | 池子里一个网关都没有（冷启动） | 无 |
| `all_cooling` | 你的槽位全在冷却里 | 最早出冷却那个槽位的剩余秒数 |
| `no_live_pair` | 没有活票，且现铸没成 | 无 |
| `rate_limited` | 撞速率闸 | 「腾出一发」的时间 |
| `no_exit` | 没有可用出口代理（整池故障） | 熔断剩余 |
| `upstream_rejected` | 上游（OpenAI）否决了池子的账号：400/401/403 | 无 |
| `mint_failed` | 其他铸票失败 | 无 |
| `public_closed` | 匿名：此刻不对外开放 | 60 |
| `degraded` | 验证开着且判了降级 | `FullWindow` |
| `consumer_rejected` | **你的** consumer key 不认（和上面那条不是一件事） | 300 |

请求本身不合法（参数越界、点名的账号不存在）时信封照旧，但**没有 `code`**：
闭集里没有这一项，编一个假的会让按码分流的调用方读到一个假的交付结论。

### `POST /release`：取了票但一发没打

请求体 `{"cookie_version":"…"}`，鉴权与 `/cookie` 完全一致（`account` 语义照搬）。
成功 204；太晚 / 票对不上 / 已经还过了 → 409。

- 只在交付后 `DeliverTTL` 内受理：过了他很可能已经用了，而「我没用过」池子无从验证
- 槽位上存着 `prev_deliver` / `prev_touch`（交付那一刻写的**旧值**），归还就是还原它们。
  **不是清零**：清零会把一次真实的历史接触抹成「从没碰过」
- **失败即安全**：没人调它时行为和以前完全一样
- 乱报只害他自己（拿回一个其实已经烧掉的槽位），所以和 `account` 一样不设防

### `GET /gateways` 顶层带两个门槛数

`deliver_ttl_s` 和 `deliver_floor_s`。消费端要自己定 `min_remaining`，而这两个数是
池子侧的——不报出来它只能瞎猜，猜高了就每发都取不到票，而失败读数和「真的没票」
一模一样。服务端那边也为这件事单留了一条日志。

### 没有消费端回报这回事

票是池子发的、判定也是池子做的——交付那一刻 `Deliver` 自己就写了 slot 的
`last_touch`，`VerifyFull`（开着的话）自己就写了 `last_verdict`。消费端能告诉池子的
东西，池子早就知道了，而它**不**知道的（转发路径上没有降智判据）回报里也没有。
所以不设回报端点：多一个写路径，只会让消费端的 `unknown` 覆盖掉池子的真判定。

**`/release` 不是回报**：它说的是「这张票我**一发都没用**」，那是池子唯一无从得知
的那件事（交付之后的事都在消费端那头），而且它只还原账本、不写任何判定。

### 上传者视图是信任凭证，不是运维面板

明写成这种句子：

> 本服务用你的凭据发起了 **21 次请求**，全部是 `ping`（`reasoning.effort=low`），
> 累计输出 **0 token**，只用过 `gpt-6-astra`，为池子发现了 **5 个新网关**。

让托管的人一眼看出没动他的额度。

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
