package pool

// 准入门槛：一个出口只有真的打通到上游、并拿到答复，才算可用。
//
// 探针与 2api 共用 internal/httpx 的 bun 指纹传输，所以"探到的"就是"2api
// 真会遇到的"。探测在进程内用 goroutine 并发，不再每出口 fork 一个子进程。
//
// 门槛按"抽样 + 租约"工作，不再全量轮询：
//   - 每轮只从总池里随机取一小把候选，取样量由健康池缺口决定，不随池子变大；
//   - 探通的出口进健康池并挂一个租约，租约到期才回候选复验；
//   - 一轮里换遍所有探针模型仍然 429，才判这个出口没额度。
//
// 为什么不整池扫：2026-11-25 排查发现，出口的免费额度是按出口自己算的，很小。
// 全量轮询把额度烧在探测上，探到自己 429，又把 429 读成"上游整批封禁"，于是整池
// 看着全死——同一分钟用真 opencode CLI 穿同一个出口是 200。探测本身必须是池子的
// 支出里最小的一项。
//
// 429 的口径（2026-11-26 实测，详见 ground-truth.md 的 "The 429 ledger"）：
//   - 不是"当天没额度"：同一出口同一模型 200 200 200 之后转 429 429 429，隔几分钟
//     又回到 200，两个方向都观测到了；
//   - 不是按模型分桶：deepseek 连 9 次 429 之后照样 200，同时刻 big-pickle 反而 429；
//   - 不是按 project 分桶：每次探测都是 identity.RandomProjectID()，全新 project 一样 429；
//   - 不是出口 IP 漂移：同一条 token 出口连打 ipify 8 次，出口公网 IP 8 次相同。
// 剩下唯一说得通的：出口侧一个很小的免费额度，几次成功就打空，分钟级补回。报文里的
// Retry-After 恒指向次日 08:00 的日窗口重置，不能拿它给单个出口定时长。

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ibelieve3212/opencode2api/internal/gateway"
)

// Verdict 是一次探测的结论。
type Verdict struct {
	Kind    string // healthy | quota | buffered | dead
	Status  int
	Note    string // 缓冲池/坏池的错误分类
	Reason  string // 人类可读原因
	Latency time.Duration
}

// Classify 把上游/传输结果归类。
func Classify(res gateway.ExitProbeResult, cfg Config) Verdict {
	v := Verdict{Status: res.Status, Latency: time.Duration(res.Latency) * time.Millisecond}
	switch {
	case res.Status == 200:
		v.Kind, v.Reason = "healthy", "真实请求 200"
	case res.Status == 429:
		v.Kind, v.Reason = "quota", "429 额度墙"
		// 429 ≠ 这个出口当天没额度了。2026-11-26 实测：同一出口同一模型 3 次 200 之后
		// 转 429、几分钟后又回 200；deepseek 连 9 次 429 之后照样 200，同时刻
		// big-pickle 反而 429；每次探测都是全新随机 project、出口公网 IP 八次恒定。
		// 所以不是按模型、不是按 project、也不是按出口 IP 轮换——是出口侧一个很小的
		// 免费额度被打空、分钟级补回。报文里的 Retry-After 恒指向次日 08:00 的日窗口
		// 重置，采信它等于把几分钟后就能用的出口睡一天——2026-11-25 那批睡到 08:00
		// 的出口就是这么来的。这里只把上游的说法记进日志备查，压多久交给 Pool 的
		// 429 阶梯。
		if hint := res.RetryAfter; hint > 0 {
			v.Reason += "（上游称 " + hint.Round(time.Minute).String() + " 后重置，仅作参考）"
		} else if hint := parseQuotaReset(res.Body, time.Now()); !hint.IsZero() {
			v.Reason += "（上游称 " + time.Until(hint).Round(time.Minute).String() + " 后重置，仅作参考）"
		}
	case res.Status == 401 || res.Status == 403:
		v.Kind, v.Note, v.Reason = "buffered", "auth", "认证/形状不符"
	case res.Status >= 500:
		v.Kind, v.Note, v.Reason = "buffered", "upstream", "上游 5xx"
	case res.Status > 0:
		v.Kind, v.Note, v.Reason = "buffered", "other", "非预期状态"
	default:
		low := strings.ToLower(res.Err)
		switch {
		case strings.Contains(low, "refused"), strings.Contains(low, "no route"),
			strings.Contains(low, "no such host"), strings.Contains(low, "network is unreachable"):
			v.Kind, v.Note, v.Reason = "dead", "connect", "连不上"
		case strings.Contains(low, "timeout"):
			v.Kind, v.Note, v.Reason = "buffered", "timeout", "超时"
		default:
			v.Kind, v.Note, v.Reason = "buffered", "tunnel", "隧道失败"
		}
	}
	return v
}

// Gate 是并发门槛。
type Gate struct {
	Pool        *Pool
	Channels    *Channels
	BaseURL     string
	Models      []string // 探针模型：第一个是主用，429 时依次换下一个
	Timeout     time.Duration
	Concurrency int
	Target      int           // >0 时健康池达标就停止取样；0 = 一直扫
	Sample      int           // 每轮取样出口数；0 = 按健康池缺口自动算
	Pause       time.Duration // 两轮之间的间隔
	// Attempts 是同一个出口在判死前最多问几次（拿到非 429 立即停），RetryGap 是
	// 每次之间的间隔。429 是上游按后端随机给的，隔两秒换条连接就有机会 200；
	// 重试只消耗 429（不出 token、不扣额度），所以代价只有时间。
	Attempts int
	RetryGap time.Duration
	Log      func(format string, args ...any)
	// ProbeURL 返回探测该出口时使用的代理地址。变体协议不能直连拨号，
	// 必须经池子自己的 token 代理转发。留空则退回 e.ProxyURL()。
	ProbeURL func(e *Exit) string
	jobs     chan *Exit

	mu         sync.Mutex
	satisfied  bool // 达标提示只打一次
	primary    int  // 主用探针模型的下标
	wallStreak int  // 连续多少个出口在换遍所有模型后仍然 429
}

func (g *Gate) log(format string, args ...any) {
	if g.Log != nil {
		g.Log(format, args...)
	}
}

// Run 启动并发探针，直到 ctx 结束。
//
// 只有"一个"调度器负责取样与投喂 worker：若让几十个 worker 各自去算候选与
// 目标，会把池子的锁抢死（实测 CPU 100%、渠道都入不了库）。
func (g *Gate) Run(ctx context.Context) {
	n := g.Concurrency
	if n <= 0 {
		n = 64
	}
	g.jobs = make(chan *Exit, n*2)
	for i := 0; i < n; i++ {
		go g.worker(ctx)
	}
	go g.rounds(ctx)
}

// rounds 是唯一的取样者：一轮取一小把、等它落地、歇一下，再决定下一轮。
func (g *Gate) rounds(ctx context.Context) {
	pause := g.Pause
	if pause <= 0 {
		pause = 20 * time.Second
	}
	for ctx.Err() == nil {
		if expired := g.Pool.ExpireLeases(time.Now()); len(expired) > 0 {
			g.log("~ 租约到期：%d 个出口回候选复验", len(expired))
		}
		if g.Target > 0 {
			if h := g.Pool.HealthCount(); h >= g.Target {
				g.noteSatisfied(h)
				if !sleepCtx(ctx, pause) {
					break
				}
				continue
			}
		}
		batch := g.Pool.Sample(g.sampleSize(), time.Now())
		if len(batch) == 0 {
			if !sleepCtx(ctx, pause) {
				break
			}
			continue
		}
		g.log("抽样探测：本轮 %d 个出口（健康池 %d/%d）", len(batch), g.Pool.HealthCount(), g.Target)
		for _, e := range batch {
			select {
			case g.jobs <- e:
			case <-ctx.Done():
				g.Pool.FinishProbe(e.IP)
			}
			if ctx.Err() != nil {
				break
			}
		}
		g.waitIdle(ctx)
		if !sleepCtx(ctx, pause) {
			break
		}
	}
	close(g.jobs)
}

// waitIdle 等本轮投出去的探测全部落地，再开下一轮。
func (g *Gate) waitIdle(ctx context.Context) {
	deadline := time.Now().Add(g.Timeout*3 + 30*time.Second)
	for g.Pool.InFlight() > 0 && time.Now().Before(deadline) {
		if !sleepCtx(ctx, 100*time.Millisecond) {
			return
		}
	}
}

// sampleSize 决定这一轮探几个：够补上健康池的缺口就行，绝不整池扫。
func (g *Gate) sampleSize() int {
	if g.Sample > 0 {
		return g.Sample
	}
	if g.Target <= 0 {
		return 300
	}
	need := g.Target - g.Pool.HealthCount()
	if need < 15 {
		need = 15
	}
	// 命中率不确定，取缺口的两倍，一次就够填满。
	return need * 2
}

func (g *Gate) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-g.jobs:
			if !ok {
				return
			}
			g.probe(ctx, e)
		}
	}
}

// modelOrder 给出这次探测要依次试的探针模型，主用在前。
func (g *Gate) modelOrder() []string {
	g.mu.Lock()
	start := g.primary
	g.mu.Unlock()
	n := len(g.Models)
	if n == 0 {
		return []string{""}
	}
	order := make([]string, 0, 3)
	for i := 0; i < n && len(order) < 3; i++ {
		order = append(order, g.Models[(start+i)%n])
	}
	return order
}

func (g *Gate) probe(ctx context.Context, e *Exit) {
	order := g.modelOrder()
	tries := g.Attempts
	if tries < 1 {
		tries = 1
	}
	var v Verdict
	allQuota := true
	for i := 0; i < tries; i++ {
		if i > 0 && !sleepCtx(ctx, g.RetryGap) {
			break
		}
		// 每次重试都是一次独立的新请求，额度补回来的那一下就拿到 200，所以重试是
		// 最便宜的验证手段：429 不出 token、不扣额度，只花时间。重试时轮换模型不是
		// 为了躲"模型额度"（实测 429 与模型无关），而是让 noteWall 还能识别"探针
		// 模型自己整体废了"这种池外故障。
		v = Classify(g.probeOnce(ctx, e, order[i%len(order)]), g.Pool.Config())
		if v.Kind != "quota" {
			allQuota = false
			break
		}
	}
	if ctx.Err() != nil {
		// 被关停/超时打断的探测没有结论，别拿半次抽样给出口定性。
		g.Pool.FinishProbe(e.IP)
		return
	}
	switched := g.noteWall(allQuota)
	if switched != "" {
		g.log("! 探针模型 %s 疑似自己没额度（连续 25 个出口全 429），改用 %s", switched, g.Models[g.modelPrimary()])
	}
	g.Pool.FinishProbe(e.IP)
	g.Pool.NoteProbe(v.Kind)
	g.apply(e, v)
}

func (g *Gate) probeOnce(ctx context.Context, e *Exit, model string) gateway.ExitProbeResult {
	pctx, cancel := context.WithTimeout(ctx, g.Timeout+5*time.Second)
	defer cancel()
	target := e.ProxyURL()
	if g.ProbeURL != nil {
		target = g.ProbeURL(e)
	}
	return gateway.ProbeExit(pctx, target, g.BaseURL, model, g.Timeout)
}

func (g *Gate) modelPrimary() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.primary
}

// noteWall 统计"连续多少个出口在换遍所有探针模型后仍然 429"。这个数字一大，
// 说明问题在探针侧（模型下架、匿名额度整体收紧），不是池子——正是 2026-11-25
// 那次误判的形状。429 本身与模型无关，所以这条只用来发现池外故障。
// 到阈值就自动换主用模型，返回被换下的模型名（没换则返回空串）。
func (g *Gate) noteWall(allQuota bool) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !allQuota {
		g.wallStreak = 0
		return ""
	}
	g.wallStreak++
	if g.wallStreak < 25 || len(g.Models) < 2 {
		return ""
	}
	old := g.Models[g.primary]
	g.primary = (g.primary + 1) % len(g.Models)
	g.wallStreak = 0
	return old
}

func (g *Gate) apply(e *Exit, v Verdict) {
	switch v.Kind {
	case "healthy":
		// 探通一次就是一个真实请求，出口当天的免费额度少一格。记下来，用完预算
		// 就别再拿它复验了——额度过早被门槛烧光是真实用户 429 的根因。
		g.Pool.BurnProbe(e.IP)
		if g.Pool.Admit(e) {
			st := g.Pool.Snapshot()
			g.log("+ 健康池：%s <- %s（%s，%dms，租约 %s，在册 %d/%d）",
				e.Token(), e.Addr(), v.Reason, v.Latency.Milliseconds(),
				g.Pool.Config().HealthLease, st.Health, g.Target)
		}
	case "quota":
		until := g.Pool.Cool(e)
		g.log("- 冷却池：%s %s，待到 %s", e.Token(), v.Reason,
			until.Format("01-02 15:04"))
	case "buffered":
		if e.Kind == KindUU && v.Note == "tunnel" {
			// 节点已经下线，别攒在缓冲池里等 10 分钟再验
			g.Pool.Forget(e.IP)
			g.log("- 丢弃：%s %s（节点已过期）", e.Token(), v.Reason)
			return
		}
		until, killed := g.Pool.Buffer(e, v.Note)
		if killed {
			g.log("x 坏池：%s %s 连续 %d 次，睡到 %s 再体检", e.Token(), v.Reason,
				g.Pool.Config().BufferStrikes, until.Format("01-02 15:04"))
		} else {
			g.log("~ 缓冲池：%s %s，%s 后再验", e.Token(), v.Reason,
				until.Sub(time.Now()).Round(time.Second))
		}
		if v.Note == "auth" && g.Channels != nil && e.Channel != "" {
			g.Channels.MarkDirty(e.Channel)
		}
	case "dead":
		if e.Kind == KindUU {
			g.Pool.Forget(e.IP)
			g.log("- 丢弃：%s %s（连不上，节点已过期）", e.Token(), v.Reason)
			return
		}
		until := g.Pool.Kill(e)
		g.log("x 坏池：%s %s，睡到 %s 再体检", e.Token(), v.Reason,
			until.Format("01-02 15:04"))
	}
}

// quotaResetRe 匹配上游 429 报文里的 "in 19h20m44s (at 2026-11-24 00:14:10 UTC)"。
var quotaResetRe = regexp.MustCompile(`in\s+(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?\s*\(at\s+([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} UTC)\)`)

// parseQuotaReset 取出该出口自己解封的时刻；取不到返回零值，由调用方兜底。
func parseQuotaReset(body string, now time.Time) time.Time {
	m := quotaResetRe.FindStringSubmatch(body)
	if m == nil {
		return time.Time{}
	}
	// 优先用绝对时刻：它是上游算好的，不受往返误差影响。
	if t, err := time.Parse("2006-01-02 15:04:05 MST", m[4]); err == nil && t.After(now) {
		return t
	}
	var hours, minutes, seconds int
	if m[1] != "" {
		hours, _ = strconv.Atoi(m[1])
	}
	if m[2] != "" {
		minutes, _ = strconv.Atoi(m[2])
	}
	if m[3] != "" {
		seconds, _ = strconv.Atoi(m[3])
	}
	d := time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute +
		time.Duration(seconds)*time.Second
	if d <= 0 {
		return time.Time{}
	}
	return now.Add(d)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// noteSatisfied 在健康池达标时提示一次：此时不再取样，把出口当天的免费额度
// 留给真实用户流量，而不是被门槛自己烧掉。
func (g *Gate) noteSatisfied(health int) {
	g.mu.Lock()
	noted := g.satisfied
	g.satisfied = true
	g.mu.Unlock()
	if !noted {
		g.log("健康池达标（%d/%d）：停止取样，额度留给真实流量", health, g.Target)
	}
}
