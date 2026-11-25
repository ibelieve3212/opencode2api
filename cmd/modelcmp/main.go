// modelcmp：同一出口用多个模型各问一次，判断 429 是"出口没额度"还是
// "这个模型没额度"。2026-11-25 的误判就是这两者分不清造成的。
//
// 用法：modelcmp -list exits.txt -models big-pickle,deepseek-v4.1-flash-free
// 每个出口的模型顺序按出口下标轮转，抵消"先问的模型占便宜"的顺序效应。
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ibelieve3212/opencode2api/internal/gateway"
)

func main() {
	list := flag.String("list", "", "文件：每行一个代理 URL")
	models := flag.String("models", "big-pickle,deepseek-v4.1-flash-free", "逗号分隔的模型表")
	baseURL := flag.String("base-url", "https://opencode.ai/inference", "上游基址")
	timeout := flag.Duration("timeout", 20*time.Second, "单次超时")
	conc := flag.Int("concurrency", 4, "并发出口数")
	flag.Parse()

	var order []string
	for _, m := range strings.Split(*models, ",") {
		if m = strings.TrimSpace(m); m != "" {
			order = append(order, m)
		}
	}
	f, err := os.Open(*list)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var proxies []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if v := strings.TrimSpace(sc.Text()); v != "" {
			proxies = append(proxies, v)
		}
	}
	f.Close()

	type tally struct{ ok, quota, other int }
	stats := make(map[string]*tally, len(order))
	var mu sync.Mutex

	ch := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range ch {
				p := proxies[idx]
				for j := 0; j < len(order); j++ {
					model := order[(idx+j)%len(order)]
					ctx, cancel := context.WithTimeout(context.Background(), *timeout+5*time.Second)
					res := gateway.ProbeExit(ctx, p, *baseURL, model, *timeout)
					cancel()
					body := strings.Join(strings.Fields(res.Body), " ")
					if body == "" {
						body = res.Err
					}
					if len(body) > 160 {
						body = body[:160]
					}
					mu.Lock()
					t := stats[model]
					if t == nil {
						t = &tally{}
						stats[model] = t
					}
					switch {
					case res.Status == 200:
						t.ok++
					case res.Status == 429:
						t.quota++
					default:
						t.other++
					}
					tag := "#" + fmt.Sprint(idx)
					if len(p) > 40 {
						tag += " …" + p[len(p)-26:]
					}
					fmt.Printf("%-10s %-28s status=%-4d %s\n", tag, model, res.Status, body)
					mu.Unlock()
				}
			}
		}()
	}
	for i := range proxies {
		ch <- i
	}
	close(ch)
	wg.Wait()

	fmt.Println("---- 按模型汇总 ----")
	for _, m := range order {
		t := stats[m]
		if t == nil {
			fmt.Printf("%-28s 未探测\n", m)
			continue
		}
		fmt.Printf("%-28s 200=%-3d 429=%-3d 其它=%-3d 共 %d\n", m, t.ok, t.quota, t.other, t.ok+t.quota+t.other)
	}
}
