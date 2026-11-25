package pool

import (
	"testing"
	"time"

	"github.com/ibelieve3212/opencode2api/internal/gateway"
)

// 真死和假死必须先按同一条阶梯处理：第一级短，连续判死才逐级加长。
func TestDeadBackoffLadder(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeadBackoff = 15 * time.Minute
	cfg.DeadTTL = 24 * time.Hour
	p, err := New(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &Exit{IP: "203.0.113.9", Port: 6300, Kind: KindExt}

	want := []time.Duration{15 * time.Minute, time.Hour, 4 * time.Hour, 16 * time.Hour, 24 * time.Hour}
	for i, w := range want {
		got := p.Kill(e).Sub(time.Now())
		if d := got - w; d > time.Minute || d < -time.Minute {
			t.Fatalf("第 %d 次判死应睡 %s，实际 %s", i+1, w, got.Round(time.Second))
		}
	}

	// 探通了就说明之前是冤枉的，阶梯必须从头算。
	p.Admit(e)
	if got := p.Kill(e).Sub(time.Now()); got > 16*time.Minute {
		t.Fatalf("探通后再判死应从第一级重来，实际 %s", got.Round(time.Second))
	}
}

// 上游明确给了 Retry-After，就照它等：被 QuotaFallback 钳短只会反复重撞同一道墙。
func TestQuotaResetHonoursRetryAfter(t *testing.T) {
	cfg := DefaultConfig()
	res := gateway.ExitProbeResult{Status: 429, RetryAfter: 20 * time.Hour}

	got := Classify(res, cfg).Reset.Sub(time.Now())
	if got < 19*time.Hour {
		t.Fatalf("Retry-After 20h 应被采信，实际只等 %s", got.Round(time.Minute))
	}
}

// 上游什么都没说时才退回兜底，且荒谬值要钳到一天。
func TestQuotaResetFallbackAndCap(t *testing.T) {
	cfg := DefaultConfig()

	got := Classify(gateway.ExitProbeResult{Status: 429}, cfg).Reset.Sub(time.Now())
	if d := got - cfg.QuotaFallback; d > time.Minute || d < -time.Minute {
		t.Fatalf("没有信号的 429 应等兜底 %s，实际 %s", cfg.QuotaFallback, got.Round(time.Second))
	}

	huge := gateway.ExitProbeResult{Status: 429, RetryAfter: 72 * time.Hour}
	got = Classify(huge, cfg).Reset.Sub(time.Now())
	if got > maxQuotaHold+time.Minute {
		t.Fatalf("荒谬的 Retry-After 应钳到 %s，实际 %s", maxQuotaHold, got.Round(time.Hour))
	}
}
