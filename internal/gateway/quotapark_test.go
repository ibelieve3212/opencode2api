package gateway

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// TestQuotaParkUsesRetryAfter 断言 429 会按 Retry-After 停用到额度重置时刻，
// 而不是被 120 秒的短上限截断。免费层的 Retry-After 恒等于距下一个 00:00 UTC
// 的秒数，实测抓到的是 48495。
func TestQuotaParkUsesRetryAfter(t *testing.T) {
	nodes := &anonymousNodes{cooldown: 15 * time.Second, quotaPark: 24 * time.Hour}
	node := &anonymousNode{}
	response := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"Retry-After": []string{"48495"}},
	}
	nodes.MarkFailure(node, response, nil)

	parked := time.Until(time.Unix(0, node.cooldownUntil.Load()))
	if parked < 13*time.Hour || parked > 14*time.Hour {
		t.Fatalf("429 parked for %v, want about 48495s", parked)
	}
}

// TestQuotaParkIgnoresOtherStatuses 断言非 429 的 Retry-After 仍走短上限：
// 一个异常头部不能把节点停一整天。
func TestQuotaParkIgnoresOtherStatuses(t *testing.T) {
	nodes := &anonymousNodes{cooldown: 15 * time.Second, quotaPark: 24 * time.Hour}
	node := &anonymousNode{}
	response := &http.Response{
		StatusCode: http.StatusInternalServerError,
		Header:     http.Header{"Retry-After": []string{"48495"}},
	}
	nodes.MarkFailure(node, response, nil)

	parked := time.Until(time.Unix(0, node.cooldownUntil.Load()))
	if parked > 3*time.Minute {
		t.Fatalf("500 parked for %v, want the short cool-down", parked)
	}
}

// TestQuotaParkDisabledKeepsShortCooldown 断言默认配置（0）保持旧行为，
// 这样升级本身不会改变任何现有部署的节奏。
func TestQuotaParkDisabledKeepsShortCooldown(t *testing.T) {
	nodes := &anonymousNodes{cooldown: 15 * time.Second}
	node := &anonymousNode{}
	response := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"Retry-After": []string{"48495"}},
	}
	nodes.MarkFailure(node, response, nil)

	parked := time.Until(time.Unix(0, node.cooldownUntil.Load()))
	if parked > 3*time.Minute {
		t.Fatalf("quota park disabled but node parked for %v", parked)
	}
}

// TestParkStoreSurvivesRestart 断言停用状态能跨进程重启恢复：写盘 → 新建 store
// → 恢复到新 runtime 的节点上。没有这一步，重启就会把耗尽的 IP 全部放回来。
func TestParkStoreSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "parks.json")
	until := time.Now().Add(5 * time.Hour)
	newParkStore(path).record("http://127.0.0.1:28100", until)

	nodes := &anonymousNodes{nodes: []*anonymousNode{{proxy: &proxyTransport{name: "http://127.0.0.1:28100"}}}}
	newParkStore(path).restore(nodes)

	remaining := time.Until(time.Unix(0, nodes.nodes[0].cooldownUntil.Load()))
	if remaining < 4*time.Hour {
		t.Fatalf("restored park lasts %v, want about 5h", remaining)
	}
}

// TestParkStoreDropsExpired 断言过期的停用不会被恢复：额度窗口已经重置，
// 那个 IP 应该重新参与轮换。
func TestParkStoreDropsExpired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "parks.json")
	newParkStore(path).record("http://127.0.0.1:28101", time.Now().Add(-time.Minute))

	nodes := &anonymousNodes{nodes: []*anonymousNode{{proxy: &proxyTransport{name: "http://127.0.0.1:28101"}}}}
	newParkStore(path).restore(nodes)

	if until := nodes.nodes[0].cooldownUntil.Load(); until != 0 {
		t.Fatalf("expired park was restored: %v", time.Unix(0, until))
	}
}
