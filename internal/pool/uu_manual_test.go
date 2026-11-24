package pool

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestManualUUConcurrent(t *testing.T) {
	ctx := context.Background()
	acc, err := fetchUUAcc(ctx, &http.Client{Timeout: 25 * time.Second})
	if err != nil {
		t.Fatalf("acc: %v", err)
	}
	nodes := uuNodes(acc)
	account, gacc := uuSessionCache.account, uuSessionCache.gacc
	if len(nodes) > 10 {
		nodes = nodes[:10]
	}
	var mu sync.Mutex
	ok, fail := 0, 0
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for _, n := range nodes {
		wg.Add(1)
		go func(n uuNode) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			e := &Exit{IP: n.IP, Port: n.Port, Kind: KindUU, User: account, Secret: gacc}
			conn, err := dialUU(ctx, e, "opencode.ai", 443, 12*time.Second)
			if err != nil {
				mu.Lock()
				fail++
				mu.Unlock()
				t.Logf("%s:%d 失败 %v", n.IP, n.Port, err)
				return
			}
			tc := tls.Client(conn, &tls.Config{ServerName: "opencode.ai", InsecureSkipVerify: true})
			tc.SetDeadline(time.Now().Add(10 * time.Second))
			if err := tc.Handshake(); err != nil {
				mu.Lock()
				fail++
				mu.Unlock()
				t.Logf("%s:%d TLS 失败 %v", n.IP, n.Port, err)
				conn.Close()
				return
			}
			mu.Lock()
			ok++
			mu.Unlock()
			tc.Close()
		}(n)
	}
	wg.Wait()
	t.Logf("并发 8：成功 %d / 失败 %d", ok, fail)
}
