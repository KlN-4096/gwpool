package mint

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

// 出站报文有两套形状，一个 switch 分流，不抽接口。
//
// **sub2api 形状（默认）**：照 sub2api 实际出站的那套（`openai_turn_state_hunter.go` 的
// openAITurnStateProbeBody / newOpenAITurnStateProbeContext + `openai_gateway_forward.go`
// 的 buildUpstreamRequest + `openai_codex_body_order.go` 的 codexResponsesFieldOrder）。
// 顾虑是**封号**不是降智：极简形状不像真实客户端。
//
// 与 sub2api 探测体的三处刻意差异，都朝「更像真客户端」的方向：
//  1. 不发 instructions —— 真 tui/Desktop 不发（只有 vscode 发），而 sub2api 那里发的是
//     该模型的真实 base prompt，gwpool 复制不来，发了还要为一发 ping 多付这段输入 token。
//  2. 不发 tools —— 同上，真 tui 把工具放 input 的 additional_tools 里，顶层不发。
//  3. 发 text.verbosity —— 真客户端发，ResponsesApiRequest 里有这个字段。
//
// **mint 形状**：relay 作者那套极简 mintHeaders + 最小 ping。有 170+ 真实样本实证，
// 是已知能用的后路，保留给 sub2api 形状被上游拒掉时切回去。
const (
	// defaultModel 只在 Client.Model 没填时兜底（测试、裸用 mint 包）。
	// 生产的取值来自 config.Model —— 账号能用哪些模型上游随时在动，写死会 400。
	defaultModel = "gpt-6-luna"
	effortLow    = "low"
	pingText     = "ping"

	// 身份三元组必须同源自洽：originator 与 UA 首段配套（否则上游 404），
	// version 等于 UA 的版本段。拼法照 sub2api 的 codexCLIUserAgent。
	// 版本号不能落后：上游按客户端身份分优先级降载，陈旧版本稳定落在被优先丢弃那侧。
	wireOriginator   = "codex-tui"
	wireVersion      = "0.156.1"
	wireUAEnvSuffix  = " (Ubuntu 22.4.0; x86_64) xterm-256color"
	sub2apiUserAgent = wireOriginator + "/" + wireVersion + wireUAEnvSuffix +
		" (" + wireOriginator + "; " + wireVersion + ")"
	mintUserAgent = "codex-tui/0.154.0 (Ubuntu 24.04; x86_64) OVH (codex-tui; 0.154.0)"
)

// mint 形状的请求体照 SPEC 第 2 节原样拼，不经 marshal：
// 字段顺序和实测跑通的那个客户端一致，少一次「Go 重排了字段」的变量。
// 模型名由 config 卡过 isModelName，不含需要转义的字符。
func mintBody(model string) string {
	return `{"model":"` + model + `","instructions":"","stream":true,"store":false,` +
		`"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"ping"}]}],` +
		`"reasoning":{"effort":"low"},"tool_choice":"auto","parallel_tool_calls":false}`
}

// wireIdentity 是一个全新 Codex 会话的身份，形态照 sub2api 的 openAITurnStateProbeIdentity：
// session == thread（新线程）、turn 独立、window = "<thread>:1"，均为 UUIDv7；
// installation 按账号稳定派生（真客户端的 installation_id 是本机固定值）。
type wireIdentity struct {
	session, thread, turn, window, contextWindow, installation string
}

// 两个生成器独立成变量是为了让 golden 测试拿到确定的身份。生产只有这一个实现。
var (
	newUUIDv7 = uuidV7
	newUUIDv4 = uuidV4
)

func newWireIdentity(cr types.Creds) wireIdentity {
	thread := newUUIDv7()
	return wireIdentity{
		session:       thread,
		thread:        thread,
		turn:          newUUIDv7(),
		window:        thread + ":1",
		contextWindow: newUUIDv7(),
		// 哈希种子不出网，只决定派生值；真客户端这个值在一台机器上是固定的。
		installation: stableUUID("gwpool:installation:" + cr.AccountID),
	}
}

// turnMetadata 是 x-codex-turn-metadata 的值，头里和 client_metadata 里是同一份。
// 用 map 让 json 按键名排序落字节 —— sub2api 那边也是 map，两边同形。
// model / reasoning_effort 是 0.156 才有的 extra，跟着 wireVersion 一起带。
func (ids wireIdentity) turnMetadata(model string) string {
	b, _ := json.Marshal(map[string]any{
		"installation_id":   ids.installation,
		"session_id":        ids.session,
		"thread_id":         ids.thread,
		"turn_id":           ids.turn,
		"root_turn_id":      ids.turn,
		"window_id":         ids.window,
		"context_window_id": ids.contextWindow,
		"window_number":     1,
		"model":             model,
		"reasoning_effort":  effortLow,
	})
	return string(b)
}

// buildWire 产出一次请求的请求体和要设置的头。ticket 非空时带上 state-echo 的活票。
func buildWire(fp config.Fingerprint, model string, cr types.Creds, cookie, ticket string) (body []byte, header http.Header) {
	h := http.Header{}
	h.Set("authorization", "Bearer "+cr.AccessToken)
	h.Set("chatgpt-account-id", cr.AccountID)
	h.Set("content-type", "application/json")
	h.Set("accept", "text/event-stream")
	h.Set("originator", wireOriginator)

	switch fp {
	case config.FingerprintMint:
		h.Set("user-agent", mintUserAgent)
		// 显式 identity 同时关掉 Go 自动加的 Accept-Encoding: gzip，
		// 免得出站头形状和实测跑通的那个客户端不一样。
		h.Set("accept-encoding", "identity")
		h.Set("session-id", newUUIDv4())
		body = []byte(mintBody(model))
	default: // config.FingerprintSub2api
		ids := newWireIdentity(cr)
		meta := ids.turnMetadata(model)
		h.Set("user-agent", sub2apiUserAgent)
		h.Set("version", wireVersion)
		h.Set("session-id", ids.session)
		h.Set("thread-id", ids.thread)
		h.Set("x-client-request-id", ids.thread)
		h.Set("x-codex-window-id", ids.window)
		h.Set("x-codex-turn-metadata", meta)
		// 刻意不发的三个头：
		//   x-codex-installation-id —— 真客户端从不作为头发它，只藏在 body 里；
		//   x-codex-beta-features: remote_compaction_v2 —— 0.156.1 已删掉这个特性；
		//   accept-encoding —— 真客户端一条都不发，由 DisableCompression 保证 Go 也不加。
		body = sub2apiBody(model, ids, meta)
	}
	if cookie != "" {
		h.Set("cookie", cookie)
	}
	if ticket != "" {
		// 必须真发一个活票，否则上游必回 state、state-echo 判据失效。
		h.Set(stateHeader, ticket)
	}
	return body, h
}

// sub2apiBody 按 codexResponsesFieldOrder 的顶层键序落字节：真客户端的体是 serde
// 结构体序列化的，键序固定为字段声明序，Go 的 map 会按字典序输出，两者对不上。
func sub2apiBody(model string, ids wireIdentity, meta string) []byte {
	var o jsonObject
	o.add("model", model)
	o.add("input", []any{map[string]any{
		"type": "message", "role": "user",
		"content": []any{map[string]any{"type": "input_text", "text": pingText}},
	}})
	o.add("tool_choice", "auto")
	o.add("parallel_tool_calls", false)
	o.add("reasoning", map[string]string{"effort": effortLow, "summary": "auto"})
	o.add("store", false)
	o.add("stream", true)
	o.add("include", []string{"reasoning.encrypted_content"})
	o.add("prompt_cache_key", ids.session)
	o.add("text", map[string]string{"verbosity": "low"})
	// 内层用 map：真客户端那里是 HashMap<String,String>，Rust 的迭代顺序本身是随机的，
	// 字典序落在它的分布内。guardian_credits_requested 是 0.156 的恒带标记。
	o.add("client_metadata", map[string]string{
		"session_id":                 ids.session,
		"thread_id":                  ids.thread,
		"turn_id":                    ids.turn,
		"root_turn_id":               ids.turn,
		"x-codex-installation-id":    ids.installation,
		"x-codex-window-id":          ids.window,
		"x-codex-turn-metadata":      meta,
		"guardian_credits_requested": "true",
	})
	return o.done()
}

// jsonObject 按写入顺序拼一个 JSON 对象。值走 json.Marshal，转义不用自己操心。
type jsonObject struct{ b []byte }

func (o *jsonObject) add(key string, v any) {
	raw, err := json.Marshal(v)
	if err != nil { // 只会发生在往里塞 chan/func 这种编不出来的值，属于代码写错
		panic("mint: 请求体字段 " + key + " 编不出 JSON: " + err.Error())
	}
	if len(o.b) == 0 {
		o.b = append(o.b, '{')
	} else {
		o.b = append(o.b, ',')
	}
	k, _ := json.Marshal(key)
	o.b = append(o.b, k...)
	o.b = append(o.b, ':')
	o.b = append(o.b, raw...)
}

func (o *jsonObject) done() []byte {
	if len(o.b) == 0 {
		return []byte("{}")
	}
	return append(o.b, '}')
}

// ---------- id 生成 ----------

// uuidV7 是时间有序的 UUID，真客户端的 session / thread / turn 都是这个版本。
func uuidV7() string {
	b := randBytes(16)
	ms := uint64(time.Now().UnixMilli())
	b[0], b[1], b[2] = byte(ms>>40), byte(ms>>32), byte(ms>>24)
	b[3], b[4], b[5] = byte(ms>>16), byte(ms>>8), byte(ms)
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return formatUUID(b)
}

// uuidV4 给 mint 形状的 session-id 头用（relay 那边是 str(uuid.uuid4())）。
func uuidV4() string {
	b := randBytes(16)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return formatUUID(b)
}

// stableUUID 由种子派生一个形状合法的 v4。种子只参与哈希，不出网。
func stableUUID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return formatUUID(b)
}

func formatUUID(b []byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

const sidAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

// randSID 生成 novproxy 的粘性会话标识，8 位 [0-9a-z]（照 geo_sampler.py）。
// sid 和供应商的绑定记在 Client 里，见 resolve。
const proxySessionIDLength = 8

func randSID() string {
	b := randBytes(proxySessionIDLength)
	for i := range b {
		b[i] = sidAlphabet[int(b[i])%len(sidAlphabet)]
	}
	return string(b)
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	// 出错也只影响 id 的随机性，用时间兜一下，不要为了一个会话 id 让整次铸票失败。
	if _, err := rand.Read(b); err != nil {
		seed := uint64(time.Now().UnixNano())
		for i := range b {
			b[i] = byte(seed >> (8 * (i % 8)))
		}
	}
	return b
}
