package pool

// 准入门槛：一个出口只有真的打通到上游、并拿到答复，才算可用。
//
// 探针与 2api 共用 internal/httpx 的 bun 指纹传输，所以"探到的"就是"2api
// 真会遇到的"。探测在进程内用 goroutine 并发，不再每出口 fork 一个子进程。

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
	Reset   time.Time
}

// Classify 把上游/传输结果归类。
func Classify(res gateway.ExitProbeResult, cfg Config) Verdict {
	v := Verdict{Status: res.Status, Latency: time.Duration(res.Latency) * time.Millisecond}
	switch {
	case res.Status == 200:
		v.Kind, v.Reason = "healthy", "真实请求 200"
	case res.Status == 429:
		v.Kind, v.Reason = "quota", "429 额度墙"
		// 上游的 429 报文自带解封时刻，按它冷却比一律押到下一个 08:00 精确得多：
		// 免费额度按 IP 滚动 24 小时，各 IP 的解封点并不相同。
		v.Reset = parseQuotaReset(res.Body, time.Now())
		if v.Reset.IsZero() {
			// 新版 429 报文不再给重置时刻。实测 ext 那 1.6 万个出口一整天
			// 没有任何一个从 429 自行恢复，说明额度是按周期整批重置的，
			// 所以兜底到下一个重置点（08:00），不要用短冷却反复空探。
			v.Reset = NextReset(time.Now(), cfg.ResetHour)
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
	Model       string
	Timeout     time.Duration
	Concurrency int
	Target      int // >0 时只在健康池未满时探测；0 = 一直扫
	Log         func(format string, args ...any)
	// ProbeURL 返回探测该出口时使用的代理地址。变体协议不能直连拨号，
	// 必须经池子自己的 token 代理转发。留空则退回 e.ProxyURL()。
	ProbeURL func(e *Exit) string
	jobs     chan *Exit

	mu        sync.Mutex
	satisfied bool // 达标提示只打一次
}

func (g *Gate) log(format string, args ...any) {
	if g.Log != nil {
		g.Log(format, args...)
	}
}

// Run 启动并发探针，直到 ctx 结束。
//
// 只有"一个"调度器负责取候选、看目标并投喂 worker：若让 96 个 worker 各自
// 去算候选与目标，会把池子的锁抢死（实测 CPU 100%、渠道都入不了库）。
func (g *Gate) Run(ctx context.Context) {
	n := g.Concurrency
	if n <= 0 {
		n = 64
	}
	g.jobs = make(chan *Exit, n*2)
	for i := 0; i < n; i++ {
		go g.worker(ctx)
	}
	go g.dispatch(ctx)
}

// dispatch 是唯一的候选生产者：补满并发槽位，然后交给 worker 去探。
func (g *Gate) dispatch(ctx context.Context) {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			close(g.jobs)
			return
		case <-tick.C:
		}
		need := g.Concurrency - len(g.jobs) - g.Pool.InFlight()
		if need <= 0 {
			continue
		}
		if g.Target > 0 {
			health := g.Pool.HealthCount()
			if health >= g.Target {
				g.noteSatisfied(health)
				continue
			}
			if rest := g.Target - health; need > rest {
				need = rest
			}
		}
		for _, e := range g.Pool.Candidates(need, time.Now()) {
			select {
			case g.jobs <- e:
			default:
				g.Pool.FinishProbe(e.IP) // 队列满，退回在飞标记
			}
		}
	}
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

func (g *Gate) probe(ctx context.Context, e *Exit) {
	pctx, cancel := context.WithTimeout(ctx, g.Timeout+5*time.Second)
	target := e.ProxyURL()
	if g.ProbeURL != nil {
		target = g.ProbeURL(e)
	}
	res := gateway.ProbeExit(pctx, target, g.BaseURL, g.Model, g.Timeout)
	cancel()

	v := Classify(res, g.Pool.Config())
	g.Pool.FinishProbe(e.IP)
	g.Pool.NoteProbe(v.Kind)
	g.apply(e, v)
}

func (g *Gate) apply(e *Exit, v Verdict) {
	switch v.Kind {
	case "healthy":
		if g.Pool.Admit(e) {
			st := g.Pool.Snapshot()
			g.log("+ 健康池：%s <- %s（%s，%dms，在册 %d/%d）",
				e.Token(), e.Addr(), v.Reason, v.Latency.Milliseconds(), st.Health, g.Target)
		}
	case "quota":
		until := g.Pool.Cool(e, v.Reset)
		g.log("- 冷却池：%s %s，待到 %s 额度重置", e.Token(), v.Reason,
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
			g.log("x 坏池：%s %s 连续 %d 次，睡 %s", e.Token(), v.Reason,
				g.Pool.Config().BufferStrikes, g.Pool.Config().DeadTTL)
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

// noteSatisfied 在健康池达标时提示一次：此后不再全量扫描，把每个出口当天的
// 免费额度留给真实用户流量，而不是被门槛自己烧掉。
func (g *Gate) noteSatisfied(health int) {
	g.mu.Lock()
	noted := g.satisfied
	g.satisfied = true
	g.mu.Unlock()
	if !noted {
		g.log("健康池达标（%d/%d）：停止全量扫描，额度留给真实流量", health, g.Target)
	}
}
