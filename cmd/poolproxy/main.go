// poolproxy：代理池本体。五池管理 + 带 bun 指纹的进程内探测，一个二进制搞定。
//
// 端口与文件格式沿用 Python 版，便于和 2api 平滑对接：
//
//	31081 SOCKS5（无认证，健康池轮询）
//	28080 HTTP（逐出口 token）  38081 HTTP（健康池轮询）
//	nodes.txt = http://<token>:x@<host>:<port>
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ibelieve3212/opencode2api/internal/pool"
)

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "启动失败:", err)
		os.Exit(1)
	}
}

type options struct {
	channelsFile  string
	stateDir      string
	bind          string
	socksPort     int
	tokenPort     int
	rrPort        int
	nodesFile     string
	exportHost    string
	healthTarget  int
	conc          int
	probeTimeout  time.Duration
	probeModels   []string
	sample        int
	samplePause   time.Duration
	healthLease   time.Duration
	quotaFallback time.Duration
	gateURL       string
	refresh       time.Duration
	refreshBatch  int
	bufferHold    time.Duration
	bufferStrike  int
	deadBackoff   time.Duration
	deadTTL       time.Duration
	sweep         bool
	releaseBatch  int
}

func run(o options) error {
	cfg := pool.DefaultConfig()
	cfg.HealthTarget = o.healthTarget
	cfg.BufferHold = o.bufferHold
	cfg.BufferStrikes = o.bufferStrike
	cfg.DeadBackoff = o.deadBackoff
	cfg.DeadTTL = o.deadTTL
	cfg.QuotaFallback = o.quotaFallback
	cfg.HealthLease = o.healthLease

	if err := os.MkdirAll(o.stateDir, 0o755); err != nil {
		return err
	}
	p, err := pool.New(cfg, o.stateDir)
	if err != nil {
		return fmt.Errorf("初始化池子: %w", err)
	}
	p.LoadSnapshot()
	st0 := p.Snapshot()
	log.Printf("本地快照恢复：总池 %d / 冷却 %d / 缓冲 %d / 坏池 %d（无需联网）",
		st0.Master, st0.Cool, st0.Buffer, st0.Dead)
	if n := p.Migrated(); n > 0 {
		log.Printf("坏池迁移：%d 个旧版固定 24h 条目压回第一级 %s，将提前复验",
			n, cfg.DeadBackoff)
	}

	ch, err := pool.NewChannels(o.channelsFile)
	if err != nil {
		return fmt.Errorf("读渠道清单: %w", err)
	}
	log.Printf("渠道清单：%d 个启用", ch.Count())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 门槛：进程内并发探测
	target := cfg.HealthTarget
	if o.sweep {
		target = 0
	}
	gate := &pool.Gate{
		Pool: p, Channels: ch,
		BaseURL: o.gateURL, Models: o.probeModels,
		Timeout: o.probeTimeout, Concurrency: o.conc, Target: target,
		Sample: o.sample, Pause: o.samplePause,
		Log: log.Printf,
		// 变体协议（雷神/UU）不能按出口直连拨号，统一经池子自己的 token 代理。
		ProbeURL: func(e *pool.Exit) string {
			return fmt.Sprintf("http://%s:x@127.0.0.1:%d", e.Token(), o.tokenPort)
		},
	}
	go gate.Run(ctx)
	log.Printf("准入门槛：%d 并发进程内探测（bun 指纹，与 2api 同源），目标健康池 %d，"+
		"探针模型 %v，租约 %s，抽样间隔 %s",
		o.conc, target, o.probeModels, o.healthLease, o.samplePause)

	// 下游端口
	ls := &pool.Listener{Pool: p, Log: log.Printf, Host: o.exportHost}
	go func() {
		if err := ls.ServeSOCKS5(ctx, fmt.Sprintf("%s:%d", o.bind, o.socksPort)); err != nil {
			log.Printf("SOCKS5 监听失败: %v", err)
		}
	}()
	go func() {
		if err := ls.ServeTokenProxy(ctx, fmt.Sprintf("%s:%d", o.bind, o.tokenPort)); err != nil {
			log.Printf("token 代理监听失败: %v", err)
		}
	}()
	go func() {
		if err := ls.ServeRR(ctx, fmt.Sprintf("%s:%d", o.bind, o.rrPort)); err != nil {
			log.Printf("轮询代理监听失败: %v", err)
		}
	}()

	// 渠道保鲜：本地快照是主体。但首轮必须全量拉一遍——否则 67 个渠道
	// 按每轮 8 个轮换，要一个多小时才能把所有池子的出口收齐，在这期间
	// 池子只有几千个候选，白白少了大半产能。
	// 首轮全量必须放到后台：67 个第三方面板要几十秒到几分钟，
	// 放在主协程里会把状态日志、nodes.txt 导出全堵死，看起来像卡死。
	go func() {
		refreshChannels(p, ch, ch.Count())
		t := time.NewTicker(o.refresh)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				refreshChannels(p, ch, o.refreshBatch)
			}
		}
	}()

	// UU 是流水线：节点表分钟级轮换，必须持续补新的。
	// 常态 3 分钟一次；健康数掉到目标一半以下时 45 秒补一次；
	// 连续失败按 5/10/20/40/60 分钟退避——被软限流时硬撞只会更糟。
	go func() {
		var failures int
		var blockUntil time.Time
		last := time.Now()
		tick := time.NewTicker(30 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			now := time.Now()
			if now.Before(blockUntil) {
				continue
			}
			interval := 3 * time.Minute
			if p.HealthCountByKind(pool.KindUU) < target/2 {
				interval = 45 * time.Second
			}
			// ±20% 抖动，别让请求节奏像定时机器人
			jitter := time.Duration(rand.Int63n(int64(interval/5 + 1)))
			if now.Sub(last) < interval/2+jitter {
				continue
			}
			last = now
			added, err := refreshKind(p, ch, "uu")
			if err != nil {
				failures++
				back := 5 * time.Minute
				for i := 1; i < failures && back < time.Hour; i++ {
					back *= 2
				}
				if back > time.Hour {
					back = time.Hour
				}
				blockUntil = now.Add(back)
				log.Printf("UU 拉取失败（%v），退避 %s", err, back.Round(time.Minute))
				continue
			}
			failures = 0
			if added > 0 {
				log.Printf("UU 补充节点：+%d（健康 %d/%d）", added,
					p.HealthCountByKind(pool.KindUU), target)
			}
		}
	}()

	// 导出 nodes.txt
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		last := -1
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n, err := p.WriteNodesFile(o.nodesFile, o.exportHost, o.tokenPort)
				if err != nil {
					log.Printf("写 nodes.txt 失败: %v", err)
					continue
				}
				if n != last {
					last = n
					log.Printf("nodes.txt 更新：%d 个出口", n)
				}
			}
		}
	}()

	// 定时唤醒冷却/缓冲/坏池 + 落盘
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c, b, d := p.WakeDue(time.Now(), o.releaseBatch)
				if c+b+d > 0 {
					log.Printf("到点放回候选：冷却 %d、缓冲 %d、坏池 %d", c, b, d)
				}
				p.PruneExpired(time.Now())
				st := p.Snapshot()
				log.Printf("状态：总池 %d / 健康 %d / 缓冲 %d / 冷却 %d / 坏池 %d（已探 %d，200 %d，429 %d）",
					st.Master, st.Health, st.Buffer, st.Cool, st.Dead,
					st.Probed, st.Healthy, st.Quota)
				// 定期落盘：只在优雅退出时保存的话，docker rm -f / 崩溃就会丢掉
				// 这一轮扫出来的总池与各池结论。
				p.Save()
			}
		}
	}()

	<-ctx.Done()
	log.Printf("正在保存状态…")
	p.Save()
	p.SaveSnapshot()
	return nil
}

func refreshChannels(p *pool.Pool, ch *pool.Channels, batch int) {
	picked, why := ch.Plan(batch)
	if len(picked) == 0 {
		return
	}
	// 并发抓取：渠道是不同主机，串行时一个慢面板（25 秒超时）就会把整轮
	// 拖成几十分钟。8 路并发既快又不会把任何单个面板打满。
	type result struct {
		exits []*pool.Exit
		err   error
		name  string
	}
	jobs := make(chan *pool.Channel)
	results := make(chan result, len(picked))
	var wg sync.WaitGroup
	// 4 路足够：面板是第三方基础设施，多个渠道往往在同一批主机上，
	// 并发拉高会被直接掐连接（实测 8 路时大量 unexpected EOF）。
	workers := 4
	if len(picked) < workers {
		workers = len(picked)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range jobs {
				exits, err := ch.Fetch(c)
				results <- result{exits: exits, err: err, name: c.Name}
			}
		}()
	}
	go func() {
		for _, c := range picked {
			jobs <- c
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	added, refreshed, accounts, failed, doneCh := 0, 0, 0, 0, 0
	// 边到边合并：一个慢渠道不该拖住其余渠道的出口入库。整轮最多等 5 分钟，
	// 到点就带着已入库的部分收工——第三方面板拖住我们是没有意义的。
	deadline := time.After(5 * time.Minute)
merge:
	for {
		select {
		case r, ok := <-results:
			if !ok {
				break merge
			}
			doneCh++
			if doneCh%10 == 0 {
				log.Printf("渠道刷新进度 %d/%d（已入库 %d）", doneCh, len(picked), added)
			}
			if r.err != nil {
				failed++
				log.Printf("渠道 %s 拉取失败: %v", r.name, r.err)
				continue
			}
			accounts += len(r.exits)
			for _, e := range r.exits {
				a, rf := p.UpsertMaster(e)
				if a {
					added++
				}
				if rf {
					refreshed++
				}
			}
		case <-deadline:
			log.Printf("渠道刷新超过 5 分钟，放弃剩余 %d 个渠道（已入库 %d）",
				len(picked)-doneCh, added)
			break merge
		}
	}
	st := p.Snapshot()
	log.Printf("外部渠道：%s，账号 %d 个，新增 %d，凭据更新 %d，失败 %d，在册 %d",
		why, accounts, added, refreshed, failed, st.Master)
	p.SaveSnapshot()
}

// refreshKind 只刷新某一类渠道，返回新增出口数。
func refreshKind(p *pool.Pool, ch *pool.Channels, kind string) (int, error) {
	list := ch.ByKind(kind)
	if len(list) == 0 {
		return 0, nil
	}
	added := 0
	var lastErr error
	for _, c := range list {
		exits, err := ch.Fetch(c)
		if err != nil {
			lastErr = err
			continue
		}
		for _, e := range exits {
			if a, _ := p.UpsertMaster(e); a {
				added++
			}
		}
	}
	return added, lastErr
}
