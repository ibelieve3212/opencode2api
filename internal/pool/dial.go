package pool

// 与出口建 SOCKS5 隧道（用户名密码认证）。外部渠道是明文透传，没有雷神那种
// 上行 XOR——这一点在 Python 版踩过坑，这里从源头只走明文。

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// DialViaExit 通过出口连到 host:port。
func DialViaExit(ctx context.Context, e *Exit, host string, port int, timeout time.Duration) (net.Conn, error) {
	if e.Kind == KindLeigod {
		return dialLeigod(ctx, e, host, port, timeout)
	}
	if e.Kind == KindUU {
		return dialUU(ctx, e, host, port, timeout)
	}
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", e.Addr())
	if err != nil {
		return nil, err
	}
	if err := socks5Handshake(conn, e, host, port, timeout); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func socks5Handshake(conn net.Conn, e *Exit, host string, port int, timeout time.Duration) error {
	conn.SetDeadline(time.Now().Add(timeout))

	method := byte(0x00)
	if e.User != "" {
		method = 0x02
	}
	if _, err := conn.Write([]byte{0x05, 0x01, method}); err != nil {
		return fmt.Errorf("握手写入失败: %w", err)
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return fmt.Errorf("握手 EOF: %w", err)
	}
	if reply[0] != 0x05 {
		return fmt.Errorf("非 SOCKS5 服务（首字节 0x%02x）", reply[0])
	}
	switch reply[1] {
	case 0x02:
		buf := []byte{0x01, byte(len(e.User))}
		buf = append(buf, e.User...)
		buf = append(buf, byte(len(e.Secret)))
		buf = append(buf, e.Secret...)
		if _, err := conn.Write(buf); err != nil {
			return fmt.Errorf("认证写入失败: %w", err)
		}
		auth := make([]byte, 2)
		if _, err := io.ReadFull(conn, auth); err != nil {
			return fmt.Errorf("认证 EOF: %w", err)
		}
		if auth[1] != 0x00 {
			return fmt.Errorf("SOCKS5 认证失败（rep=0x%02x）", auth[1])
		}
	case 0x00:
	default:
		return fmt.Errorf("SOCKS5 协商失败（method=0x%02x）", reply[1])
	}

	req := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			req = append(req, 0x01)
			req = append(req, ip4...)
		} else {
			req = append(req, 0x04)
			req = append(req, ip.To16()...)
		}
	} else {
		if len(host) > 255 {
			return fmt.Errorf("域名过长")
		}
		req = append(req, 0x03, byte(len(host)))
		req = append(req, host...)
	}
	var p [2]byte
	binary.BigEndian.PutUint16(p[:], uint16(port))
	req = append(req, p[:]...)
	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("CONNECT 写入失败: %w", err)
	}

	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return fmt.Errorf("CONNECT 应答 EOF: %w", err)
	}
	if head[1] != 0x00 {
		return fmt.Errorf("SOCKS5 CONNECT 被拒（rep=0x%02x）", head[1])
	}
	var skip int
	switch head[3] {
	case 0x01:
		skip = 4
	case 0x04:
		skip = 16
	case 0x03:
		l := make([]byte, 1)
		if _, err := io.ReadFull(conn, l); err != nil {
			return err
		}
		skip = int(l[0])
	default:
		return fmt.Errorf("未知 ATYP 0x%02x", head[3])
	}
	if skip > 0 {
		if _, err := io.ReadFull(conn, make([]byte, skip)); err != nil {
			return err
		}
	}
	if _, err := io.ReadFull(conn, make([]byte, 2)); err != nil {
		return err
	}
	conn.SetDeadline(time.Time{})
	return nil
}

// relay 双向转发（纯透传）。
func relay(a, b net.Conn) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		if c, ok := dst.(*net.TCPConn); ok {
			_ = c.CloseWrite()
		}
		done <- struct{}{}
	}
	go cp(a, b)
	go cp(b, a)
	<-done
	<-done
}
