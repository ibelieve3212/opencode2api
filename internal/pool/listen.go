package pool

// 三个下游端口，格式与 Python 版兼容：
//   31081 SOCKS5（无认证，2api 用，走健康池轮询）
//   28080 HTTP 代理（逐出口 token，token = e-<ip>-<port>）
//   38081 HTTP 代理（轮询健康池）

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Listener 把三条监听串在一起，便于 main 统一拉起。
type Listener struct {
	Pool     *Pool
	Log      func(format string, args ...any)
	Host     string // 导出给 nodes.txt 用
	TokenPort int
	RelayPort int
	DialTimeout time.Duration
}

func (l *Listener) log(format string, args ...any) {
	if l.Log != nil {
		l.Log(format, args...)
	}
}

// ServeSOCKS5 提供无认证 SOCKS5，供 2api 使用。
func (l *Listener) ServeSOCKS5(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	l.log("SOCKS5 监听 %s（健康池轮询）", addr)
	go func() { <-ctx.Done(); ln.Close() }()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return nil
		}
		go l.handleSOCKS(conn)
	}
}

func (l *Listener) handleSOCKS(conn net.Conn) {
	defer conn.Close()
	host, port, err := readSOCKSRequest(conn)
	if err != nil {
		return
	}
	timeout := l.dialTimeout()
	for attempt := 0; attempt < 3; attempt++ {
		e := l.Pool.Pick()
		if e == nil {
			conn.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
			return
		}
		remote, err := DialViaExit(context.Background(), e, host, port, timeout)
		if err != nil {
			// 隧道建不起来：压缓冲池，换一个出口。
			until, killed := l.Pool.Buffer(e, "tunnel")
			if killed {
				l.log("x 坏池：%s 转发失败，睡到 %s", e.Token(), until.Format("01-02 15:04"))
			} else {
				l.log("~ 缓冲池：%s 转发失败（%v）", e.Token(), err)
			}
			continue
		}
		conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		relay(conn, remote)
		remote.Close()
		return
	}
	conn.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
}

func readSOCKSRequest(conn net.Conn) (string, int, error) {
	head := make([]byte, 2)
	if _, err := readFull(conn, head); err != nil {
		return "", 0, err
	}
	if head[0] != 0x05 {
		return "", 0, fmt.Errorf("非 SOCKS5")
	}
	if n := int(head[1]); n > 0 {
		if _, err := readFull(conn, make([]byte, n)); err != nil {
			return "", 0, err
		}
	}
	if _, err := conn.Write([]byte{0x05, 0x00}); err != nil {
		return "", 0, err
	}
	req := make([]byte, 4)
	if _, err := readFull(conn, req); err != nil {
		return "", 0, err
	}
	if req[1] != 0x01 {
		conn.Write([]byte{0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return "", 0, fmt.Errorf("仅支持 CONNECT")
	}
	var host string
	switch req[3] {
	case 0x01:
		b := make([]byte, 4)
		if _, err := readFull(conn, b); err != nil {
			return "", 0, err
		}
		host = net.IP(b).String()
	case 0x03:
		l := make([]byte, 1)
		if _, err := readFull(conn, l); err != nil {
			return "", 0, err
		}
		b := make([]byte, int(l[0]))
		if _, err := readFull(conn, b); err != nil {
			return "", 0, err
		}
		host = string(b)
	case 0x04:
		b := make([]byte, 16)
		if _, err := readFull(conn, b); err != nil {
			return "", 0, err
		}
		host = net.IP(b).String()
	default:
		return "", 0, fmt.Errorf("未知 ATYP")
	}
	pb := make([]byte, 2)
	if _, err := readFull(conn, pb); err != nil {
		return "", 0, err
	}
	return host, int(pb[0])<<8 | int(pb[1]), nil
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := conn.Read(buf[n:])
		if err != nil {
			return n, err
		}
		n += m
	}
	return n, nil
}

// ServeTokenProxy 提供逐出口 token 的 HTTP 代理（28080）。
func (l *Listener) ServeTokenProxy(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	l.log("HTTP 代理监听 %s（逐出口 token）", addr)
	go func() { <-ctx.Done(); ln.Close() }()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return nil
		}
		go l.handleToken(conn)
	}
}

func (l *Listener) handleToken(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	if req.Method != http.MethodConnect {
		conn.Write([]byte("HTTP/1.1 501 Not Implemented\r\nContent-Length: 0\r\n\r\n"))
		return
	}
	token := tokenFromAuth(req.Header.Get("Proxy-Authorization"))
	e := l.Pool.ByToken(token)
	if e == nil {
		conn.Write([]byte("HTTP/1.1 410 Gone\r\nContent-Length: 0\r\n\r\n"))
		return
	}
	host, port := splitHostPort(req.Host)
	remote, err := DialViaExit(context.Background(), e, host, port, l.dialTimeout())
	if err != nil {
		l.Pool.DropHealth(e.IP)
		l.Pool.Buffer(e, "tunnel")
		conn.Write([]byte("HTTP/1.1 502 " + reasonTag(err) + "\r\nContent-Length: 0\r\n\r\n"))
		return
	}
	defer remote.Close()
	conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	relay(conn, remote)
}

// ServeRRLB 提供轮询健康池的 HTTP 代理（38081）。
func (l *Listener) ServeRR(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	l.log("HTTP 代理监听 %s（健康池轮询）", addr)
	go func() { <-ctx.Done(); ln.Close() }()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return nil
		}
		go l.handleRR(conn)
	}
}

func (l *Listener) handleRR(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	if req.Method != http.MethodConnect {
		conn.Write([]byte("HTTP/1.1 501 Not Implemented\r\nContent-Length: 0\r\n\r\n"))
		return
	}
	host, port := splitHostPort(req.Host)
	for attempt := 0; attempt < 3; attempt++ {
		e := l.Pool.Pick()
		if e == nil {
			conn.Write([]byte("HTTP/1.1 503 No Exit\r\nContent-Length: 0\r\n\r\n"))
			return
		}
		remote, err := DialViaExit(context.Background(), e, host, port, l.dialTimeout())
		if err != nil {
			l.Pool.Buffer(e, "tunnel")
			continue
		}
		defer remote.Close()
		conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		relay(conn, remote)
		return
	}
	conn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n"))
}

func (l *Listener) dialTimeout() time.Duration {
	if l.DialTimeout > 0 {
		return l.DialTimeout
	}
	return 8 * time.Second
}

func tokenFromAuth(h string) string {
	const prefix = "basic "
	if len(h) == 0 || !strings.HasPrefix(strings.ToLower(h), prefix) {
		return ""
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(h[len(prefix):]))
	if err != nil {
		return ""
	}
	if i := strings.IndexByte(string(raw), ':'); i >= 0 {
		return string(raw[:i])
	}
	return string(raw)
}

// reasonTag 把失败原因压成简短标签写进 502 状态行，便于探针区分。
func reasonTag(err error) string {
	low := strings.ToLower(err.Error())
	switch {
	case strings.Contains(low, "认证"):
		return "pool-auth"
	case strings.Contains(low, "eof"):
		return "pool-eof"
	case strings.Contains(low, "timeout"):
		return "pool-timeout"
	case strings.Contains(low, "被拒"):
		return "pool-refused"
	}
	return "pool-fail"
}

func splitHostPort(v string) (string, int) {
	host, portStr, err := net.SplitHostPort(v)
	if err != nil {
		return v, 443
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port == 0 {
		port = 443
	}
	return host, port
}
