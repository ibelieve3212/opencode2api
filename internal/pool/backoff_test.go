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

// 探针台账挂在出口上，不挂在凭据上：面板把口令换了，机器还是那台机器，免费额度
// 也还是那个额度。2026-11-26 实测台账和入池日志差 213 条，213/213 都是入池之后
// 凭据被刷新过的。
func TestCredentialRefreshKeepsProbeLedger(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeBudget = 3
	p, err := New(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &Exit{IP: "203.0.113.31", Port: 6300, Kind: KindExt, User: "u1", Secret: "s1"}
	p.UpsertMaster(e)
	p.BurnProbe(e.IP)

	// 链路坏了才轮换凭据，这是它唯一的翻身机会；轮换本身不清台账。
	p.Kill(e)
	rotated := &Exit{IP: "203.0.113.31", Port: 6300, Kind: KindExt, User: "u2", Secret: "s2"}
	if _, rf := p.UpsertMaster(rotated); !rf {
		t.Fatal("坏池里换了口令应算凭据更新")
	}
	if left := p.ProbeLeft(e.IP); left != cfg.ProbeBudget-1 {
		t.Fatalf("凭据轮换不该清探针台账，应剩 %d 次，实际 %d", cfg.ProbeBudget-1, left)
	}
}

// 这次的线上缺陷：面板给同一个出口 IP 挂着 2～6 个账号，池子按"最后写入者赢"
// 合并，一轮刷新就能把 43% 的总池判成"凭据更新"，并顺手清掉冷却、坏池、两条退避
// 阶梯和探针台账。实测 quotaN / deadN 双双卡在 3 级（阶梯各 5 级），从来没出口爬到
// 顶——刚被 429 压下去的出口每轮都被放回池子再撞一次，这就是"一直撞上 429"。
func TestExtSiblingAccountChurnKeepsLedgers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeBudget = 3
	p, err := New(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := &Exit{IP: "203.0.113.41", Port: 6300, Kind: KindExt, User: "u3", Secret: "s3"}
	p.UpsertMaster(base)
	p.BurnProbe(base.IP)
	p.Cool(base) // 撞了一次 429：quotaN=1，进冷却池

	// 面板把同一台机器的兄弟账号轮着报上来，顺序任意。
	siblings := []*Exit{
		{IP: base.IP, Port: 6300, Kind: KindExt, User: "u1", Secret: "s1"},
		{IP: base.IP, Port: 6300, Kind: KindExt, User: "u5", Secret: "s5"},
		{IP: base.IP, Port: 6300, Kind: KindExt, User: "u2", Secret: "s2"},
	}
	for round := 0; round < 3; round++ {
		for _, s := range siblings {
			p.UpsertMaster(s)
		}
		if _, ok := p.cool[base.IP]; !ok {
			t.Fatalf("第 %d 轮：兄弟账号轮换把 429 冷却清掉了", round+1)
		}
		if got := p.quotaN[base.IP]; got != 1 {
			t.Fatalf("第 %d 轮：429 阶梯被清成 %d 级，应保持 1 级", round+1, got)
		}
		if left := p.ProbeLeft(base.IP); left != cfg.ProbeBudget-1 {
			t.Fatalf("第 %d 轮：探针台账被清，应剩 %d 次，实际 %d", round+1, cfg.ProbeBudget-1, left)
		}
	}

	// 收敛：来回换了几轮之后总池里是确定的那一份，再灌也不会被当成"凭据更新"。
	cred := p.master[base.IP]
	if cred.User != "u1" || cred.Secret != "s1" {
		t.Fatalf("应确定性收敛到字典序最小的账号 u1/s1，实际 %s/%s", cred.User, cred.Secret)
	}
	for _, s := range siblings {
		if _, rf := p.UpsertMaster(s); rf {
			t.Fatalf("已收敛后再报 %s/%s 不该再算凭据更新", s.User, s.Secret)
		}
	}
}

// 正在供货的那条不动：同一 ip:port 上的兄弟账号不见得能连（实测有的通、有的
// 一直超时），拿一个"看起来更小"的凭据去换掉一条验通过的链路纯属自伤。
func TestExtServingExitKeepsProvenCredential(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeBudget = 3
	p, err := New(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	good := &Exit{IP: "203.0.113.43", Port: 6300, Kind: KindExt, User: "u9", Secret: "s9"}
	p.UpsertMaster(good)
	p.Admit(good) // 验通过，进健康池供货

	smaller := &Exit{IP: good.IP, Port: 6300, Kind: KindExt, User: "u1", Secret: "s1"}
	if _, rf := p.UpsertMaster(smaller); rf {
		t.Fatal("正在供货的出口不该被换成没验过的兄弟账号")
	}
	if cur := p.master[good.IP]; cur.User != "u9" || cur.Secret != "s9" {
		t.Fatalf("总池凭据应保持验通过的 u9/s9，实际 %s/%s", cur.User, cur.Secret)
	}
	if _, ok := p.health[good.Token()]; !ok {
		t.Fatal("健康池里那条不该被撤掉")
	}
}

// 同一份凭据连续报上来（面板最常见的形态）一次都不该算更新。
func TestExtSameCredentialIsNotAChange(t *testing.T) {
	p, err := New(DefaultConfig(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &Exit{IP: "203.0.113.42", Port: 6300, Kind: KindExt, User: "u1", Secret: "s1", ExpireAt: 1}
	p.UpsertMaster(e)
	for i := 0; i < 5; i++ {
		if _, rf := p.UpsertMaster(e); rf {
			t.Fatalf("第 %d 次：凭据没变不该算更新", i+1)
		}
	}
}

// 换隧道（uu / 雷神）就是换了出口机器，额度、冷却、坏池、探针台账必须从零起算。
func TestTunnelChangeResetsLedgers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeBudget = 3
	for _, tc := range []struct {
		name string
		init *Exit
		next *Exit
	}{
		{
			"uu 换隧道",
			&Exit{IP: "203.0.113.51", Port: 6300, Kind: KindUU, User: "u1", Secret: "s1"},
			&Exit{IP: "203.0.113.51", Port: 6301, Kind: KindUU, User: "u2", Secret: "s2"},
		},
		{
			"雷神换隧道",
			&Exit{IP: "203.0.113.52", Port: 6300, Kind: KindLeigod, User: "u1", Secret: "s1"},
			&Exit{IP: "203.0.113.52", Port: 6301, Kind: KindLeigod, User: "u2", Secret: "s2"},
		},
	} {
		p, err := New(cfg, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		p.UpsertMaster(tc.init)
		p.BurnProbe(tc.init.IP)
		p.Cool(tc.init)
		if _, rf := p.UpsertMaster(tc.next); !rf {
			t.Fatalf("%s: 换隧道应算凭据更新", tc.name)
		}
		if left := p.ProbeLeft(tc.init.IP); left != cfg.ProbeBudget {
			t.Fatalf("%s: 换隧道后探针台账应清零，应剩 %d 次，实际 %d", tc.name, cfg.ProbeBudget, left)
		}
		if _, ok := p.quotaN[tc.init.IP]; ok {
			t.Fatalf("%s: 换隧道后 429 阶梯应清零", tc.name)
		}
		if _, ok := p.cool[tc.init.IP]; ok {
			t.Fatalf("%s: 换隧道后冷却池应清空", tc.name)
		}
	}
}

// 状态行的「200」只该来自 NoteProbe：Admit 也加一次的话，200 会超过「已探」总数，
// 日志上看起来像池子在凭空造出成功。
func TestHealthyCounterCountsOncePerProbe(t *testing.T) {
	cfg := DefaultConfig()
	p, err := New(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &Exit{IP: "203.0.113.33", Port: 6300, Kind: KindExt}
	p.UpsertMaster(e)

	p.FinishProbe(e.IP)
	p.NoteProbe("healthy")
	if !p.Admit(e) {
		t.Fatal("第一次入池应成功")
	}
	st := p.Snapshot()
	if st.Probed != 1 {
		t.Fatalf("已探应为 1，实际 %d", st.Probed)
	}
	if st.Healthy != 1 {
		t.Fatalf("一次探通只能算一次 200，实际 %d", st.Healthy)
	}
	if st.Probed < st.Healthy {
		t.Fatalf("200 (%d) 不该超过已探 (%d)", st.Healthy, st.Probed)
	}
}
