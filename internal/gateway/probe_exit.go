package gateway

// 池子用的出口门槛探针。
//
// 关键点：请求体与头部不是自己搓的，而是走生产同一条路径——
//   probePayload（内部会调 prepareAnonymousBody：强制 stream、补官方 CLI 的
//   system prompt、补齐并改名四件官方工具）+ upstreamRequest（endpoint、头序、
//   公开 key）。手搓 body 会被上游当成"没过指纹重做的客户端"，一律回
//   429 FreeUsageLimitError，即使出口本身完全健康——Python 版踩过这个坑。
//
// 传输层复用 transportPool 的 BunTransport（BoringSSL ClientHello + bun 头序），
// 只是把代理换成待验出口，因此池子可以在一个进程里并发验证成千上万个出口。

import (
	"context"
	"io"
	"time"

	"github.com/ibelieve3212/opencode2api/internal/config"
	"github.com/ibelieve3212/opencode2api/internal/identity"
	"github.com/ibelieve3212/opencode2api/internal/models"
	wire "github.com/ibelieve3212/opencode2api/internal/protocol"
)

// ExitProbeResult 是一次出口门槛探测的结论。
type ExitProbeResult struct {
	Status  int    `json:"status"`
	Body    string `json:"body,omitempty"`
	Err     string `json:"err,omitempty"`
	Latency int64  `json:"latency_ms"`
}

// ProbeExit 经 proxyURL（socks5://user:pass@host:port）发一次真实的免费模型请求。
func ProbeExit(ctx context.Context, proxyURL, baseURL, model string, timeout time.Duration) (out ExitProbeResult) {
	started := time.Now()
	defer func() { out.Latency = time.Since(started).Milliseconds() }()

	secs := int(timeout.Seconds())
	if secs < 1 {
		secs = 1
	}
	perf := config.PerformanceConfig{
		MaxIdleConns:           1,
		MaxIdleConnsPerHost:    1,
		MaxConnsPerHost:        1,
		IdleConnTimeoutSeconds: 5,
		ConnectTimeoutSeconds:  secs,
	}
	pool, err := transportPool([]string{proxyURL}, perf, timeout)
	if err != nil {
		out.Err = err.Error()
		return out
	}
	if len(pool.items) == 0 {
		out.Err = "no transport"
		return out
	}
	client := pool.items[0].bun
	defer client.CloseIdleConnections()

	protocol := wire.Chat
	v1 := models.IsV1Upstream(baseURL)
	body, err := probePayload(model, protocol, "", v1, nil)
	if err != nil {
		out.Err = "payload: " + err.Error()
		return out
	}
	ids := probeRequestIDs()
	ids.Request = identity.RandomID("req", 16)
	req, err := upstreamRequest(ctx, baseURL, protocol, body, ids, anonymousZenKey)
	if err != nil {
		out.Err = "request: " + err.Error()
		return out
	}
	resp, err := client.Do(req)
	if err != nil {
		out.Err = err.Error()
		return out
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	out.Status = resp.StatusCode
	out.Body = string(raw)
	return out
}
