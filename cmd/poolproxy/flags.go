package main

import (
	"flag"
	"time"
)

func parseFlags() options {
	var o options
	flag.StringVar(&o.channelsFile, "channels", "/data/channels.json", "渠道清单 JSON")
	flag.StringVar(&o.stateDir, "state-dir", "/data/state", "状态目录")
	flag.StringVar(&o.bind, "bind", "127.0.0.1", "监听地址")
	flag.IntVar(&o.socksPort, "socks-port", 31081, "SOCKS5 端口（2api 用）")
	flag.IntVar(&o.tokenPort, "token-port", 28080, "逐出口 token 的 HTTP 代理端口")
	flag.IntVar(&o.rrPort, "rr-port", 38081, "健康池轮询 HTTP 代理端口")
	flag.StringVar(&o.nodesFile, "nodes-file", "/data/state/nodes.txt", "健康池导出文件")
	flag.StringVar(&o.exportHost, "export-host", "127.0.0.1", "nodes.txt 里写的地址")
	flag.IntVar(&o.healthTarget, "health-target", 200, "健康池目标数量")
	flag.IntVar(&o.conc, "probe-concurrency", 96, "并发探针数")
	flag.DurationVar(&o.probeTimeout, "probe-timeout", 12*time.Second, "单次探测超时")
	flag.StringVar(&o.probeModel, "probe-model", "deepseek-v4.1-flash-free", "探针模型")
	flag.StringVar(&o.gateURL, "gate-url",
		"https://opencode.ai/inference", "门槛上游基址")
	flag.DurationVar(&o.refresh, "refresh-interval", 10*time.Minute, "渠道保鲜间隔")
	flag.IntVar(&o.refreshBatch, "refresh-batch", 8, "每轮轮换刷新几个渠道")
	flag.DurationVar(&o.bufferHold, "buffer-hold", 10*time.Minute, "缓冲池压制时长")
	flag.IntVar(&o.bufferStrike, "buffer-strikes", 3, "同类错误升坏池阈值")
	flag.DurationVar(&o.deadTTL, "dead-ttl", 24*time.Hour, "坏池沉睡时长")
	flag.IntVar(&o.releaseBatch, "release-batch", 300,
		"每 30 秒最多从冷却/缓冲/坏池放回多少个候选（0=不限）")
	flag.BoolVar(&o.sweep, "sweep", false, "全量扫描模式（不受健康池目标限制）")
	flag.Parse()
	return o
}
