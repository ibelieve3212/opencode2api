package main

import (
	"flag"
	"strings"
	"time"
)

func parseFlags() options {
	var o options
	var probeModels string
	flag.StringVar(&o.channelsFile, "channels", "/data/channels.json", "渠道清单 JSON")
	flag.StringVar(&o.stateDir, "state-dir", "/data/state", "状态目录")
	flag.StringVar(&o.bind, "bind", "127.0.0.1", "监听地址")
	flag.IntVar(&o.socksPort, "socks-port", 31081, "SOCKS5 端口（2api 用）")
	flag.IntVar(&o.tokenPort, "token-port", 28080, "逐出口 token 的 HTTP 代理端口")
	flag.IntVar(&o.rrPort, "rr-port", 38081, "健康池轮询 HTTP 代理端口")
	flag.IntVar(&o.healthPort, "health-port", 8082, "健康检查端口（/healthz 存活，/readyz 可派发；0 = 关闭）")
	flag.StringVar(&o.nodesFile, "nodes-file", "/data/state/nodes.txt", "健康池导出文件")
	flag.StringVar(&o.exportHost, "export-host", "127.0.0.1", "nodes.txt 里写的地址")
	flag.IntVar(&o.healthTarget, "health-target", 200, "健康池目标数量")
	flag.IntVar(&o.conc, "probe-concurrency", 96, "并发探针数")
	flag.DurationVar(&o.probeTimeout, "probe-timeout", 12*time.Second, "单次探测超时")
	flag.StringVar(&probeModels, "probe-model",
		"big-pickle,beam-free,iceman-free,tidepool-alpha-free",
		"探针模型，逗号分隔：第一个主用，429 时依次换下一个（一个模型没额度不该给整池判死）")
	flag.IntVar(&o.sample, "sample-batch", 0, "每轮取样的出口数（0 = 按健康池缺口自动算）")
	flag.DurationVar(&o.samplePause, "sample-pause", 20*time.Second, "两轮取样之间的间隔")
	flag.DurationVar(&o.healthLease, "health-lease", 15*time.Minute,
		"健康池租约：探通的出口多久后回候选复验（0 = 不复验）")
	flag.DurationVar(&o.quotaBackoff, "quota-backoff", 2*time.Minute,
		"429 第一次压多久，每再撞一次延长 4 倍，到 -quota-ttl 封顶")
	flag.DurationVar(&o.quotaTTL, "quota-ttl", 6*time.Hour, "429 压制时长上限")
	flag.IntVar(&o.quotaAttempts, "quota-attempts", 3,
		"同一出口判死前最多问几次：拿到非 429 立即停（429 不出 token、不扣额度，重试只花时间）")
	flag.DurationVar(&o.quotaGap, "quota-gap", 2*time.Second,
		"同一出口两次尝试之间的间隔（实测隔两秒，同一出口的 429 会变 200）")
	flag.IntVar(&o.probeBudget, "probe-budget", 3,
		"同一出口每天最多被探通几次：探通即消耗出口额度，用完就留池供货、到 08:00 再复验（0=不限）")
	flag.StringVar(&o.gateURL, "gate-url",
		"https://opencode.ai/inference", "门槛上游基址")
	flag.DurationVar(&o.refresh, "refresh-interval", 10*time.Minute, "渠道保鲜间隔")
	flag.IntVar(&o.refreshBatch, "refresh-batch", 8, "每轮轮换刷新几个渠道")
	flag.DurationVar(&o.bufferHold, "buffer-hold", 10*time.Minute, "缓冲池压制时长")
	flag.IntVar(&o.bufferStrike, "buffer-strikes", 3, "同类错误升坏池阈值")
	flag.DurationVar(&o.deadBackoff, "dead-backoff", 15*time.Minute,
		"坏池第一次沉睡时长，每再判死一次延长 4 倍，到 -dead-ttl 封顶")
	flag.DurationVar(&o.deadTTL, "dead-ttl", 24*time.Hour, "坏池沉睡时长上限")
	flag.IntVar(&o.releaseBatch, "release-batch", 300,
		"每 30 秒最多从冷却/缓冲/坏池放回多少个候选（0=不限）")
	flag.BoolVar(&o.sweep, "sweep", false, "全量扫描模式（不受健康池目标限制）")
	flag.Parse()
	for _, m := range strings.Split(probeModels, ",") {
		if m = strings.TrimSpace(m); m != "" {
			o.probeModels = append(o.probeModels, m)
		}
	}
	return o
}
