package models

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const metadataFixture = `{"opencode":{"id":"opencode","models":{"big-pickle":{"id":"big-pickle"}}}}`

// TestMetadataRefreshFallsBackToDirectWhenExitsFail 断言出口全部失败时元数据
// 刷新仍能走直连成功。
//
// 这是线上故障的回归测试。健康池被免费的 429 额度墙打空后，provider 交出的
// 出口一个个都连不上，旧实现只要 provider 给出了客户端就完全不用直连，于是
// 每一轮刷新都失败，元数据一直冻结在缓存里的旧值。元数据端点是公开的，聊天
// 被 429 挡住的同一条出口照样能取到它，所以直连必须作为最后一档。
func TestMetadataRefreshFallsBackToDirectWhenExitsFail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(metadataFixture))
	}))
	defer server.Close()

	store := PricingStore("", slog.New(slog.NewTextHandler(io.Discard, nil)))
	store.endpoint = server.URL
	// A provider that hands over an exit which cannot connect anywhere.
	store.SetClientProvider(func() []*http.Client {
		return []*http.Client{{Timeout: time.Second, Transport: failingTransport{}}}
	})

	if err := store.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh failed with only a broken exit: %v", err)
	}
	snapshot := store.Snapshot()
	if !snapshot.Ready || snapshot.Models == 0 {
		t.Fatalf("metadata not ready after the direct fallback: %+v", snapshot)
	}
	if snapshot.LastError != "" {
		t.Fatalf("last error %q, want empty after a successful fallback", snapshot.LastError)
	}
}

// TestMetadataRefreshStopsAfterTheFirstSuccess 断言第一个成功的响应就是最终
// 结果：直连排在最后只作兜底，出口能用时不该再打一次。
func TestMetadataRefreshStopsAfterTheFirstSuccess(t *testing.T) {
	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(metadataFixture))
	}))
	defer server.Close()

	store := PricingStore("", slog.New(slog.NewTextHandler(io.Discard, nil)))
	store.endpoint = server.URL
	store.SetClientProvider(func() []*http.Client {
		return []*http.Client{{Timeout: 5 * time.Second}}
	})

	if err := store.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	if hits != 1 {
		t.Fatalf("endpoint hit %d times, want exactly 1", hits)
	}
}

// failingTransport fails every request, standing in for an exit that cannot
// reach the origin.
type failingTransport struct{}

func (failingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return nil, errors.New("exit unreachable: " + req.URL.Host)
}
