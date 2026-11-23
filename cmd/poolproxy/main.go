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
	"os"
	"os/signal"
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
	channelsFile string
	stateDir     string
	bind         string
	socksPort    int
	tokenPort    int
	rrPort       int
	nodesFile    string
	exportHost   string
	healthTarget int
	conc         int
	probeTimeout time.Duration
	probeModel   string
	gateURL      string
	refresh      time.Duration
	refreshBatch int
	bufferHold   time.Duration
	bufferStrike int
	deadTTL      time.Duration
	sweep        bool
}

func run(o options) error {
	cfg := pool.DefaultConfig()
	cfg.HealthTarget = o.healthTarget
	cfg.BufferHold = o.bufferHold
	cfg.BufferStrikes = o.bufferStrike
	cfg.DeadTTL = o.deadTTL

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
		BaseURL: o.gateURL, Model: o.probeModel,
		Timeout: o.probeTimeout, Concurrency: o.conc, Target: target,
		Log: log.Printf,
	}
	go gate.Run(ctx)
	log.Printf("准入门槛：%d 并发进程内探测（bun 指纹，与 2api 同源），目标健康池 %d", o.conc, target)

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

	// 渠道保鲜：本地快照是主体，网络只补一小批
	go func() {
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
	refreshChannels(p, ch, o.refreshBatch)

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
				c, b, d := p.WakeDue(time.Now())
				if c+b+d > 0 {
					log.Printf("到点放回候选：冷却 %d、缓冲 %d、坏池 %d", c, b, d)
				}
				p.PruneExpired(time.Now())
				st := p.Snapshot()
				log.Printf("状态：总池 %d / 健康 %d / 缓冲 %d / 冷却 %d / 坏池 %d（已探 %d，200 %d，429 %d）",
					st.Master, st.Health, st.Buffer, st.Cool, st.Dead,
					st.Probed, st.Healthy, st.Quota)
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
	added, refreshed, accounts := 0, 0, 0
	for _, c := range picked {
		exits, err := ch.Fetch(c)
		if err != nil {
			log.Printf("渠道 %s 拉取失败: %v", c.Name, err)
			continue
		}
		accounts += len(exits)
		for _, e := range exits {
			a, r := p.UpsertMaster(e)
			if a {
				added++
			}
			if r {
				refreshed++
			}
		}
	}
	st := p.Snapshot()
	log.Printf("外部渠道：%s，账号 %d 个，新增 %d，凭据更新 %d，在册 %d",
		why, accounts, added, refreshed, st.Master)
	p.SaveSnapshot()
}
