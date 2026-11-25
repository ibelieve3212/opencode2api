package pool

import (
	"strings"
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

// 429 不是"这个出口当天没额度了"：同一出口 3 次 200 后转 429、几分钟后又回 200，
// 报文的 Retry-After 恒指向次日 08:00 的日窗口重置。所以判决里不许再带"该睡到
// 什么时候"，冷却时长一律按阶梯给。
func TestQuotaLadderIgnoresRetryAfter(t *testing.T) {
	cfg := DefaultConfig()
	cfg.QuotaBackoff = 2 * time.Minute
	cfg.QuotaTTL = 6 * time.Hour
	p, err := New(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &Exit{IP: "203.0.113.9", Port: 6300, Kind: KindExt}

	v := Classify(gateway.ExitProbeResult{Status: 429, RetryAfter: 20 * time.Hour}, cfg)
	if v.Kind != "quota" {
		t.Fatalf("429 应判 quota，实际 %q", v.Kind)
	}
	// 上游的 20 小时只该出现在日志里，不该变成压制时长。
	if !strings.Contains(v.Reason, "仅作参考") {
		t.Fatalf("上游的 Retry-After 只该记成参考信息，实际 %q", v.Reason)
	}

	want := []time.Duration{
		2 * time.Minute, 8 * time.Minute, 32 * time.Minute,
		128 * time.Minute, 6 * time.Hour, 6 * time.Hour,
	}
	for i, w := range want {
		got := p.Cool(e).Sub(time.Now())
		if d := got - w; d > time.Minute || d < -time.Minute {
			t.Fatalf("第 %d 次撞 429 应压 %s，实际 %s", i+1, w, got.Round(time.Second))
		}
	}

	// 探通了就说明那次 429 只是额度一时被打空，阶梯也得从头算。
	p.Admit(e)
	if got := p.Cool(e).Sub(time.Now()); got > 8*time.Minute {
		t.Fatalf("探通后再撞 429 应从第一级重来，实际 %s", got.Round(time.Second))
	}
}

// 探通一次就是一个真实请求，会吃掉出口跟真实用户共用的免费额度。额度用完就必须
// 停止复验，否则 15 分钟一轮的租约复验会把这份额度全烧在门槛自己身上。
func TestProbeBudgetStopsRecheck(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeBudget = 3
	p, err := New(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &Exit{IP: "203.0.113.7", Port: 6300, Kind: KindExt}
	p.UpsertMaster(e)

	for i := 0; i < cfg.ProbeBudget; i++ {
		p.BurnProbe(e.IP)
	}
	if left := p.ProbeLeft(e.IP); left != 0 {
		t.Fatalf("烧完预算后应剩 0 次，实际 %d", left)
	}
	if got := p.candidates(4, time.Now(), false); len(got) != 0 {
		t.Fatalf("预算用完的出口不该再被取样，实际取了 %d 个", len(got))
	}

	// 出租约也不能把它捞回来复验：留在健康池继续供货，到 08:00 重置再说。
	p.Admit(e)
	p.lease[e.Token()] = time.Now().Add(-time.Second)
	if exp := p.ExpireLeases(time.Now()); len(exp) != 0 {
		t.Fatalf("预算用完的出口不该被退租，实际退了 %d 个", len(exp))
	}
	want := NextReset(time.Now(), cfg.ResetHour)
	if got := p.lease[e.Token()]; !got.After(want.Add(-time.Minute)) {
		t.Fatalf("租约应续到重置点 %s，实际 %s", want.Format(time.DateTime), got.Format(time.DateTime))
	}
}

// 额度日翻页（08:00）后预算重新给满，出口重新可探。
func TestProbeBudgetResetsNextDay(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeBudget = 2
	p, err := New(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &Exit{IP: "203.0.113.8", Port: 6300, Kind: KindExt}
	p.BurnProbe(e.IP)
	p.BurnProbe(e.IP)
	if !p.probeSpentLocked(e.IP) {
		t.Fatal("烧完预算后应判为已用完")
	}
	p.mu.Lock()
	p.probeDay = "1970-01-01" // 假装已经翻页
	p.mu.Unlock()
	if !p.probeSpentLocked(e.IP) {
		t.Fatal("未翻页前不该清空")
	}
	if left := p.ProbeLeft(e.IP); left != cfg.ProbeBudget {
		t.Fatalf("翻页后预算应恢复 %d，实际 %d", cfg.ProbeBudget, left)
	}
}
