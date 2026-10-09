package httpapi

// Shared protocol fixtures and diagnostic-string regression.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// failBody 解失败响应那个信封。
type failBody struct {
	Error struct {
		Code              string `json:"code"`
		Message           string `json:"message"`
		RetryAfterSeconds int    `json:"retry_after_seconds"`
	} `json:"error"`
}

func decodeFail(t *testing.T, w *httptest.ResponseRecorder) failBody {
	t.Helper()
	var got failBody
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("失败响应解不开: %v（body=%s）", err, w.Body.String())
	}
	return got
}

// routeCookie 拼一张形状对的完整路由 cookie：__cflb 一串不透明内容，__oailb 是
// payload 解得开的假 JWT（上游的签名密钥在上游手上，池子本来就不验签）。
func routeCookie(cflb, gateway string, oailbExp time.Time) string {
	payload := fmt.Sprintf(`{"host":"chatgpt-prod.edge.%s.internal","exp":%d}`,
		gateway, oailbExp.Unix())
	return "__cflb=" + cflb + "; __oailb=eyJhbGciOiJFUzI1NiJ9." +
		base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".fake-sig"
}

// N3：Detail 按**字符**截，不按字节——它基本全是中文，而它进的是 HTML 页面，
// 按字节切会把半个汉字落进去。
func TestFailDetailTruncatesByRune(t *testing.T) {
	l := &failLog{}
	// 开头故意放一个 ASCII：这样第 300 个**字节**正好落在一个汉字中间。
	l.add(deliverFail{Detail: "x" + strings.Repeat("网关", failDetailMax)})
	got := l.recent(0, 1, false)[0].Detail
	if !utf8.ValidString(got) {
		t.Fatalf("截出了半个汉字: %q", got)
	}
	if n := utf8.RuneCountInString(strings.TrimSuffix(got, "…")); n != failDetailMax {
		t.Fatalf("截了 %d 个字符，应为 %d", n, failDetailMax)
	}
}
