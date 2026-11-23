package pool

// 渠道同步：本地快照是主体，网络只负责"保鲜"。
//
// 启动时直接读本地快照，一个请求都不发；之后每轮只刷新一小批渠道（轮换），
// 或者定向补刷"刚报认证失败"的那几个——这样几百个渠道也不会把面板打崩。

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Channel 是 channels.json 里的一条。
type Channel struct {
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	URL       string `json:"url"`
	Token     string `json:"token"`
	SocksPort int    `json:"socks_port"`
	Enabled   *bool  `json:"enabled"`
}

func (c *Channel) enabled() bool { return c.Enabled == nil || *c.Enabled }

// Channels 管理渠道清单与抓取。
type Channels struct {
	path string
	mu   sync.Mutex
	list []*Channel
	rr   int                // 轮换游标
	dirty map[string]bool   // 报过认证失败的渠道名
	client *http.Client
}

// NewChannels 读 channels.json（格式与 Python 版一致：{"channels":[...]}） 。
func NewChannels(path string) (*Channels, error) {
	ch := &Channels{path: path, dirty: map[string]bool{},
		client: &http.Client{Timeout: 25 * time.Second}}
	if err := ch.Reload(); err != nil {
		return nil, err
	}
	return ch, nil
}

// Reload 重新读清单。
func (c *Channels) Reload() error {
	data, err := os.ReadFile(c.path)
	if err != nil {
		return err
	}
	var doc struct {
		Channels []*Channel `json:"channels"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("channels.json: %w", err)
	}
	list := make([]*Channel, 0, len(doc.Channels))
	for _, ch := range doc.Channels {
		if ch != nil && ch.enabled() && ch.URL != "" {
			if ch.Kind == "" {
				ch.Kind = "kingstar"
			}
			list = append(list, ch)
		}
	}
	c.mu.Lock()
	c.list = list
	c.mu.Unlock()
	return nil
}

// Count 返回启用渠道数。
func (c *Channels) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.list)
}

// MarkDirty 记下需要定向补刷的渠道。
func (c *Channels) MarkDirty(name string) {
	if name == "" {
		return
	}
	c.mu.Lock()
	c.dirty[name] = true
	c.mu.Unlock()
}

// Plan 选出本轮要刷新的渠道：有脏渠道就只刷脏的，否则按游标轮换一批。
func (c *Channels) Plan(batch int) (picked []*Channel, why string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.list) == 0 {
		return nil, "无渠道"
	}
	if len(c.dirty) > 0 {
		for _, ch := range c.list {
			if c.dirty[ch.Name] {
				picked = append(picked, ch)
			}
		}
		c.dirty = map[string]bool{}
		if len(picked) > 0 {
			return picked, fmt.Sprintf("定向补刷 %d 个报认证失败的渠道", len(picked))
		}
	}
	if batch <= 0 {
		batch = 8
	}
	n := batch
	if n > len(c.list) {
		n = len(c.list)
	}
	for i := 0; i < n; i++ {
		picked = append(picked, c.list[(c.rr+i)%len(c.list)])
	}
	c.rr = (c.rr + n) % len(c.list)
	return picked, fmt.Sprintf("轮换刷新 %d/%d 个渠道", n, len(c.list))
}

// Fetch 抓一个渠道，返回该渠道的出口列表。
func (c *Channels) Fetch(ch *Channel) ([]*Exit, error) {
	switch ch.Kind {
	case "kingstar":
		return c.fetchKingstar(ch)
	case "json":
		return c.fetchJSON(ch)
	case "plain":
		return c.fetchPlain(ch)
	}
	return nil, fmt.Errorf("未知渠道类型 %q", ch.Kind)
}

var socksPortRe = regexp.MustCompile(`socks5://[^"'\s>]*:(\d+)`)

func (c *Channels) fetchKingstar(ch *Channel) ([]*Exit, error) {
	body, _ := json.Marshal(map[string]string{"token": ch.Token})
	req, err := http.NewRequest(http.MethodPost,
		strings.TrimRight(ch.URL, "/")+"/api/list", strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("api/list HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, fmt.Errorf("api/list 解析失败: %w", err)
	}

	port := ch.SocksPort
	if port == 0 {
		// 面板页上带 socks5 端口模板，抓一次。
		if r, err := c.client.Get(strings.TrimRight(ch.URL, "/") + "/"); err == nil {
			page, _ := io.ReadAll(io.LimitReader(r.Body, 4<<20))
			r.Body.Close()
			if m := socksPortRe.FindSubmatch(page); m != nil {
				port, _ = strconv.Atoi(string(m[1]))
			}
		}
	}
	if port == 0 {
		port = 62000
	}

	now := time.Now().Unix()
	out := make([]*Exit, 0, len(rows))
	for _, row := range rows {
		if n, ok := num(row["status"]); !ok || int(n) != 1 {
			continue
		}
		ip := str(row["assigned_ip"])
		user := str(row["username"])
		if ip == "" || user == "" {
			continue
		}
		expire, _ := num(row["expire_at"])
		if int64(expire) != 0 && int64(expire) <= now {
			continue
		}
		out = append(out, &Exit{
			IP: ip, Port: port, Kind: KindExt, User: user,
			Secret: secretOf(row), Channel: ch.Name,
			ExpireAt: int64(expire), Born: time.Now(),
		})
	}
	return out, nil
}

func (c *Channels) fetchJSON(ch *Channel) ([]*Exit, error) {
	var body io.Reader
	if ch.Token != "" {
		body = strings.NewReader(`{"token":"` + ch.Token + `"}`)
	}
	req, err := http.NewRequest(http.MethodGet, ch.URL, body)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		var wrapper struct {
			Data []map[string]any `json:"data"`
		}
		if json.Unmarshal(raw, &wrapper) != nil {
			return nil, fmt.Errorf("json 解析失败: %w", err)
		}
		rows = wrapper.Data
	}
	return c.rowsToExits(rows, ch), nil
}

func (c *Channels) fetchPlain(ch *Channel) ([]*Exit, error) {
	resp, err := c.client.Get(ch.URL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out []*Exit
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if u, err := url.Parse(line); err == nil && u.Host != "" && u.Scheme != "" {
			port, _ := strconv.Atoi(u.Port())
			if port == 0 {
				port = ch.SocksPort
			}
			user := ""
			pass := ""
			if u.User != nil {
				user = u.User.Username()
				pass, _ = u.User.Password()
			}
			out = append(out, &Exit{IP: u.Hostname(), Port: port, Kind: KindExt,
				User: user, Secret: pass, Channel: ch.Name, Born: time.Now()})
			continue
		}
		host, portStr := line, ""
		if i := strings.LastIndex(line, ":"); i > 0 {
			host, portStr = line[:i], line[i+1:]
		}
		user, pass := "", ""
		if i := strings.LastIndex(host, "@"); i > 0 {
			cred := host[:i]
			host = host[i+1:]
			if j := strings.Index(cred, ":"); j > 0 {
				user, pass = cred[:j], cred[j+1:]
			}
		}
		port, _ := strconv.Atoi(portStr)
		if port == 0 {
			port = ch.SocksPort
		}
		if host == "" || port == 0 {
			continue
		}
		out = append(out, &Exit{IP: host, Port: port, Kind: KindExt,
			User: user, Secret: pass, Channel: ch.Name, Born: time.Now()})
	}
	return out, nil
}

func (c *Channels) rowsToExits(rows []map[string]any, ch *Channel) []*Exit {
	now := time.Now()
	out := make([]*Exit, 0, len(rows))
	for _, row := range rows {
		ip := firstNonEmpty(str(row["assigned_ip"]), str(row["ip"]), str(row["host"]))
		if ip == "" {
			continue
		}
		port := ch.SocksPort
		if n, ok := num(row["port"]); ok && int(n) > 0 {
			port = int(n)
		}
		if port == 0 {
			port = 1080
		}
		expire, _ := num(row["expire_at"])
		out = append(out, &Exit{
			IP: ip, Port: port, Kind: KindExt,
			User: firstNonEmpty(str(row["username"]), str(row["user"])),
			Secret: secretOf(row), Channel: ch.Name,
			ExpireAt: int64(expire), Born: now,
		})
	}
	return out
}

// ---------- 本地快照 ----------

type channelSnapshot struct {
	UpdatedAt int64            `json:"updated_at"`
	Exits     map[string]*Exit `json:"exits"`
}

// LoadSnapshot 启动时读本地快照（不联网）。
func (p *Pool) LoadSnapshot() int {
	var snap channelSnapshot
	if !p.loadJSON("pool-channels.json", &snap) {
		return 0
	}
	n := 0
	for ip, e := range snap.Exits {
		if e == nil {
			continue
		}
		p.mu.Lock()
		if _, ok := p.master[ip]; !ok {
			p.master[ip] = e
			n++
		}
		p.mu.Unlock()
	}
	return n
}

// SaveSnapshot 把外部渠道出口落盘。
func (p *Pool) SaveSnapshot() {
	p.mu.Lock()
	snap := channelSnapshot{UpdatedAt: time.Now().Unix(), Exits: map[string]*Exit{}}
	for ip, e := range p.master {
		if e.Kind == KindExt {
			snap.Exits[ip] = e.Clone()
		}
	}
	p.mu.Unlock()
	p.saveJSON("pool-channels.json", snap)
}

func str(v any) string {
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func num(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return f, err == nil
	}
	return 0, false
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// secretOf 取账号密码：面板字段名各异，按常见名优先。
func secretOf(row map[string]any) string {
	for _, key := range []string{"password", "passwd", "pass", "pwd", "secret", "token"} {
		if v := str(row[key]); v != "" {
			return v
		}
	}
	return ""
}

// SnapshotPath 便于测试。
func SnapshotPath(dir string) string { return filepath.Join(dir, "pool-channels.json") }
