// dialcheck：验证"经出口建隧道"这段链路。不消耗上游额度，只测隧道与 TLS。
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/ibelieve3212/opencode2api/internal/pool"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "用法: dialcheck socks5://user:pass@host:port host:port [tls]")
		os.Exit(2)
	}
	raw, target := os.Args[1], os.Args[2]
	wantTLS := len(os.Args) > 3 && os.Args[3] == "tls"

	u, err := url.Parse(raw)
	if err != nil {
		fmt.Println("解析代理失败:", err)
		os.Exit(1)
	}
	port, _ := strconv.Atoi(u.Port())
	e := &pool.Exit{IP: u.Hostname(), Port: port}
	if u.User != nil {
		e.User = u.User.Username()
		e.Secret, _ = u.User.Password()
	}
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		fmt.Println("解析目标失败:", err)
		os.Exit(1)
	}
	tport, _ := strconv.Atoi(portStr)

	start := time.Now()
	conn, err := pool.DialViaExit(context.Background(), e, host, tport, 15*time.Second)
	if err != nil {
		fmt.Printf("隧道建立失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("隧道建立成功 %s → %s（%dms）\n", e.Addr(), target, time.Since(start).Milliseconds())
	defer conn.Close()
	if wantTLS {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: host})
		tlsConn.SetDeadline(time.Now().Add(15 * time.Second))
		if err := tlsConn.Handshake(); err != nil {
			fmt.Printf("TLS 握手失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("TLS 握手成功: %s\n", tlsConn.ConnectionState().Version)
	}
}
