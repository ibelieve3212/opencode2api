// exitprobe：出口探测对比工具。用生产同一条请求管道验证一批出口。
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ibelieve3212/opencode2api/internal/gateway"
)

func main() {
	list := flag.String("list", "", "文件：每行 socks5://user:pass@host:port")
	baseURL := flag.String("base-url", "https://opencode.ai/inference", "上游基址")
	model := flag.String("model", "deepseek-v4.1-flash-free", "探针模型")
	timeout := flag.Duration("timeout", 20*time.Second, "单次超时")
	conc := flag.Int("concurrency", 8, "并发数")
	verbose := flag.Bool("v", false, "打印每个出口的结果")
	flag.Parse()

	f, err := os.Open(*list)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var jobs []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if v := sc.Text(); v != "" {
			jobs = append(jobs, v)
		}
	}
	f.Close()

	var done, ok, quota, other int64
	start := time.Now()
	ch := make(chan string)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range ch {
				ctx, cancel := context.WithTimeout(context.Background(), *timeout+5*time.Second)
				res := gateway.ProbeExit(ctx, p, *baseURL, *model, *timeout)
				cancel()
				atomic.AddInt64(&done, 1)
				switch {
				case res.Status == 200:
					atomic.AddInt64(&ok, 1)
				case res.Status == 429:
					atomic.AddInt64(&quota, 1)
				default:
					atomic.AddInt64(&other, 1)
				}
				if *verbose {
					mu.Lock()
					body := res.Body
					if len(body) > 90 {
						body = body[:90]
					}
					fmt.Printf("  %-46s status=%-4d %s\n", p[len(p)-32:], res.Status, body)
					mu.Unlock()
				}
			}
		}()
	}
	for _, j := range jobs {
		ch <- j
	}
	close(ch)
	wg.Wait()
	el := time.Since(start)
	out, _ := json.Marshal(map[string]any{
		"probed": len(jobs), "healthy_200": ok, "quota_429": quota, "other": other,
		"seconds": int(el.Seconds()), "per_minute": float64(len(jobs)) / el.Seconds() * 60,
	})
	fmt.Println(string(out))
}
