package mint

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// 自己写 SOCKS5 CONNECT 而不是用 golang.org/x/net/proxy：
// go.mod 必须保持零 require（SPEC 第 9 节），而这段握手只有几十行。
const (
	socksVer      = 0x05
	authNone      = 0x00
	authUserPass  = 0x02
	authNoAccept  = 0xff
	cmdConnect    = 0x01
	addrIPv4      = 0x01
	addrDomain    = 0x03
	addrIPv6      = 0x04
	handshakeWait = 20 * time.Second
)

// socks5Connect 经 proxyAddr 连到 target（"host:port"），返回已就绪的隧道。
//
// 域名**原样交给代理解析**（即 socks5h 的 h）：出口在哪个国家，DNS 就该在哪里做，
// 否则本地解析出的 IP 会把请求钉死在本地那条线路上。
func socks5Connect(ctx context.Context, proxyAddr, user, pass, target string) (net.Conn, error) {
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return nil, fmt.Errorf("目标地址 %q: %w", target, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("目标端口 %q 非法", portStr)
	}
	if len(host) > 255 {
		return nil, errors.New("目标主机名超过 255 字节，SOCKS5 放不下")
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("连代理: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = conn.Close()
		}
	}()

	// 握手期间给死线：住宅代理卡住不回是常态，不设就一直挂着占着并发额度。
	deadline := time.Now().Add(handshakeWait)
	if dl, has := ctx.Deadline(); has && dl.Before(deadline) {
		deadline = dl
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	if _, err := conn.Write([]byte{socksVer, 2, authNone, authUserPass}); err != nil {
		return nil, fmt.Errorf("发握手: %w", err)
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, fmt.Errorf("读握手: %w", err)
	}
	if resp[0] != socksVer {
		return nil, fmt.Errorf("代理不是 SOCKS5（版本 0x%02x）", resp[0])
	}
	switch resp[1] {
	case authNone:
	case authUserPass:
		if err := socksAuth(conn, user, pass); err != nil {
			return nil, err
		}
	case authNoAccept:
		return nil, errors.New("代理拒绝了全部认证方式")
	default:
		return nil, fmt.Errorf("代理要求不支持的认证方式 0x%02x", resp[1])
	}

	req := []byte{socksVer, cmdConnect, 0x00, addrDomain, byte(len(host))}
	req = append(req, host...)
	req = binary.BigEndian.AppendUint16(req, uint16(port))
	if _, err := conn.Write(req); err != nil {
		return nil, fmt.Errorf("发 CONNECT: %w", err)
	}
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return nil, fmt.Errorf("读 CONNECT 回复: %w", err)
	}
	if head[1] != 0x00 {
		return nil, fmt.Errorf("代理拒绝 CONNECT：%s", socksErr(head[1]))
	}
	// 绑定地址用不上，但必须读干净，否则它会混进 TLS ClientHello 之后的流里。
	var skip int
	switch head[3] {
	case addrIPv4:
		skip = 4
	case addrIPv6:
		skip = 16
	case addrDomain:
		n := make([]byte, 1)
		if _, err := io.ReadFull(conn, n); err != nil {
			return nil, err
		}
		skip = int(n[0])
	default:
		return nil, fmt.Errorf("CONNECT 回复里的地址类型 0x%02x 不认", head[3])
	}
	if _, err := io.ReadFull(conn, make([]byte, skip+2)); err != nil {
		return nil, fmt.Errorf("读 CONNECT 回复尾部: %w", err)
	}

	// 握手死线不能留给后面的业务流量，HTTP 客户端有自己的超时。
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	ok = true
	return conn, nil
}

func socksAuth(conn net.Conn, user, pass string) error {
	if len(user) > 255 || len(pass) > 255 {
		return errors.New("代理用户名或密码超过 255 字节")
	}
	msg := []byte{0x01, byte(len(user))}
	msg = append(msg, user...)
	msg = append(msg, byte(len(pass)))
	msg = append(msg, pass...)
	if _, err := conn.Write(msg); err != nil {
		return fmt.Errorf("发代理认证: %w", err)
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fmt.Errorf("读代理认证结果: %w", err)
	}
	if resp[1] != 0x00 {
		// 不带上用户名：里面有 sid、region，还紧挨着密码，别让它进日志。
		return errors.New("代理认证失败")
	}
	return nil
}

func socksErr(code byte) string {
	switch code {
	case 0x01:
		return "通用失败"
	case 0x02:
		return "规则不允许"
	case 0x03:
		return "网络不可达"
	case 0x04:
		return "主机不可达"
	case 0x05:
		return "连接被拒"
	case 0x06:
		return "TTL 超时"
	case 0x07:
		return "命令不支持"
	case 0x08:
		return "地址类型不支持"
	}
	return fmt.Sprintf("未知错误 0x%02x", code)
}
