package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ibelieve3212/opencode2api/internal/config"
)

// TestRefreshAnonymousDirectListsWithoutAProxy 断言没有可用出口时，匿名模型
// 列表仍能通过直连取回，并且请求带的是公开凭据。
//
// 这是线上故障的回归测试。健康池被免费的 429 额度墙打空后，匿名泳道的每个
// 出口都在失败，旧实现只打一行 "anonymous model catalog refresh failed" 就
// 返回 nil，目录一直停在旧数据上，/healthz 报 model_catalog_stale；而重新列出
// 模型那条路本身又依赖可用出口，于是永远自愈不了。模型列表是公开元数据，
// 免费额度的 429 只挡 chat completions，不挡 /v1/models，所以直连必须能兜底。
func TestRefreshAnonymousDirectListsWithoutAProxy(t *testing.T) {
	var gotAuth, gotPath, gotClient string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotClient = r.Header.Get("x-opencode-client")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"big-pickle"},{"id":"flint-40b-free"}]}`))
	}))
	defer server.Close()

	g := &Gateway{
		cfg:    config.Config{Upstream: config.UpstreamConfig{Anonymous: server.URL}},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	models := g.refreshAnonymousDirect(context.Background(), server.URL)
	if len(models) != 2 || models[0] != "big-pickle" || models[1] != "flint-40b-free" {
		t.Fatalf("got models %v, want [big-pickle flint-40b-free]", models)
	}
	if gotPath != "/v1/models" {
		t.Fatalf("requested %q, want /v1/models", gotPath)
	}
	if gotAuth != "Bearer "+anonymousZenKey {
		t.Fatalf("authorization %q, want the public anonymous credential", gotAuth)
	}
	if gotClient != "cli" {
		t.Fatalf("x-opencode-client %q, want cli", gotClient)
	}
}

// TestRefreshAnonymousDirectReportsFailure 断言直连也拿不到列表时返回 nil，
// 好让调用方保留上一份目录，而不是把空列表写进去。
func TestRefreshAnonymousDirectReportsFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	g := &Gateway{
		cfg:    config.Config{Upstream: config.UpstreamConfig{Anonymous: server.URL}},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if models := g.refreshAnonymousDirect(context.Background(), server.URL); models != nil {
		t.Fatalf("got %v, want nil so the previous catalog is kept", models)
	}
}
