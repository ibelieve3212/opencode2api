package pool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Hold 是缓冲池里的一条记录：多久后再验、同类错误累计几次。
type Hold struct {
	Until time.Time `json:"until"`
	N     int       `json:"n"`
	Kind  string    `json:"kind"`
}

// Config 是池子的行为参数。
type Config struct {
	HealthTarget  int           // 健康池目标数量
	BufferHold    time.Duration // 缓冲池压多久
	BufferStrikes int           // 同类错误累计几次升级坏池
	DeadTTL       time.Duration // 坏池睡多久再体检
	QuotaFallback time.Duration // 上游没给重置时刻时的兜底冷却
	ResetHour     int           // 每日额度重置的小时（北京时间 8）
}

// DefaultConfig 与 Python 版线上参数一致。
func DefaultConfig() Config {
	return Config{
		HealthTarget:  200,
		BufferHold:    10 * time.Minute,
		BufferStrikes: 3,
		DeadTTL:       24 * time.Hour,
		QuotaFallback: 24 * time.Hour,
		ResetHour:     8,
	}
}

// Stats 是给日志/状态用的计数。
type Stats struct {
	Master   int   `json:"master"`
	Health   int   `json:"health"`
	Buffer   int   `json:"buffer"`
	Cool     int   `json:"cool"`
	Dead     int   `json:"dead"`
	Probed   int64 `json:"probed"`
	Healthy  int64 `json:"healthy"`
	Quota    int64 `json:"quota"`
	Buffered int64 `json:"buffered"`
	Killed   int64 `json:"killed"`
	Failed   int64 `json:"failed"`
}

// Pool 是五池本体。所有读写都经 mu，探测在锁外进行（只把结论拿回来记账）。
type Pool struct {
	cfg Config

	mu       sync.Mutex
	master   map[string]*Exit     // 总池：ip -> 出口
	health   map[string]*Exit     // 健康池：token -> 出口
	cool     map[string]time.Time // 冷却池：ip -> 到期
	buffer   map[string]*Hold     // 缓冲池
	dead     map[string]time.Time // 坏池
	inFlight map[string]bool      // 正在探测的 ip
	idx      int
	stats    Stats

	dir string
}

// New 建立池子并从磁盘恢复状态（本地快照优先，不联网）。
func New(cfg Config, dir string) (*Pool, error) {
	p := &Pool{
		cfg:      cfg,
		master:   map[string]*Exit{},
		health:   map[string]*Exit{},
		cool:     map[string]time.Time{},
		buffer:   map[string]*Hold{},
		dead:     map[string]time.Time{},
		inFlight: map[string]bool{},
		dir:      dir,
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p.load()
	return p, nil
}

func (p *Pool) Config() Config { return p.cfg }

// ---------- 状态转移 ----------

// IsBlocked 报告该 ip 此刻是否处于任一"不予派发"的池子里。
func (p *Pool) IsBlocked(ip string, now time.Time) bool {
	if t, ok := p.cool[ip]; ok && t.After(now) {
		return true
	}
	if h, ok := p.buffer[ip]; ok && h.Until.After(now) {
		return true
	}
	if t, ok := p.dead[ip]; ok && t.After(now) {
		return true
	}
	return false
}

// Admit 把出口放进健康池（同一个 ip 只留一条）。
func (p *Pool) Admit(e *Exit) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.health[e.Token()] != nil {
		return false
	}
	for tok, cur := range p.health {
		if cur.IP == e.IP {
			delete(p.health, tok)
		}
	}
	p.health[e.Token()] = e.Clone()
	delete(p.cool, e.IP)
	delete(p.buffer, e.IP)
	delete(p.dead, e.IP)
	p.stats.Healthy++
	return true
}

// Cool 把出口放冷却池直到额度重置。
func (p *Pool) Cool(e *Exit, until time.Time) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropHealthLocked(e.IP)
	if until.IsZero() {
		until = NextReset(time.Now(), p.cfg.ResetHour)
	}
	if prev, ok := p.cool[e.IP]; !ok || until.After(prev) {
		p.cool[e.IP] = until
	}
	return until
}

// Buffer 把出口压进缓冲池，同类错误累计到阈值就升级坏池。
func (p *Pool) Buffer(e *Exit, kind string) (until time.Time, killed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropHealthLocked(e.IP)
	prev := p.buffer[e.IP]
	strikes := 1
	if prev != nil && prev.Kind == kind {
		strikes = prev.N + 1
	}
	if strikes >= p.cfg.BufferStrikes {
		delete(p.buffer, e.IP)
		p.dead[e.IP] = time.Now().Add(p.cfg.DeadTTL)
		p.stats.Killed++
		return time.Time{}, true
	}
	until = time.Now().Add(p.cfg.BufferHold)
	p.buffer[e.IP] = &Hold{Until: until, N: strikes, Kind: kind}
	p.stats.Buffered++
	return until, false
}

// Kill 直接把出口扔进坏池。
func (p *Pool) Kill(e *Exit) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropHealthLocked(e.IP)
	delete(p.buffer, e.IP)
	until := time.Now().Add(p.cfg.DeadTTL)
	p.dead[e.IP] = until
	p.stats.Killed++
	return until
}

// DropHealth 把出口移出健康池（转发失败等），不改变其它池的结论。
func (p *Pool) DropHealth(ip string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropHealthLocked(ip)
}

// QuotaDemote 是"真流量撞 429"的路径：直接进冷却池。
func (p *Pool) QuotaDemote(e *Exit) time.Time { return p.Cool(e, time.Time{}) }

func (p *Pool) dropHealthLocked(ip string) {
	for tok, cur := range p.health {
		if cur.IP == ip {
			delete(p.health, tok)
		}
	}
}

// ---------- 取出口 ----------

// Pick 从健康池轮询取一个出口。没人可发时返回 nil（宁可 5xx 也不发错出口）。
func (p *Pool) Pick() *Exit {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.health) == 0 {
		return nil
	}
	tokens := make([]string, 0, len(p.health))
	for t := range p.health {
		tokens = append(tokens, t)
	}
	sort.Strings(tokens)
	p.idx++
	return p.health[tokens[p.idx%len(tokens)]].Clone()
}

// ByToken 按 token 找健康池出口；找不到说明它已经不在服务了。
func (p *Pool) ByToken(token string) *Exit {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.health[token].Clone()
}

// Tokens 返回健康池全部 token（已排序，便于稳定导出）。
func (p *Pool) Tokens() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.health))
	for t := range p.health {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// Snapshot 返回计数。
func (p *Pool) Snapshot() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	s := p.stats
	s.Master = len(p.master)
	s.Health = len(p.health)
	s.Buffer = 0
	for _, h := range p.buffer {
		if h.Until.After(now) {
			s.Buffer++
		}
	}
	s.Cool = 0
	for _, t := range p.cool {
		if t.After(now) {
			s.Cool++
		}
	}
	s.Dead = 0
	for _, t := range p.dead {
		if t.After(now) {
			s.Dead++
		}
	}
	return s
}

// ---------- 候选调度 ----------

// Candidates 返回可以立刻探测的出口（不在任何"不予派发"的池子里、没有在飞）。
func (p *Pool) Candidates(limit int, now time.Time) []*Exit {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*Exit, 0, limit)
	// 从新到旧：刚同步进来的才活得下来。
	ips := make([]string, 0, len(p.master))
	for ip := range p.master {
		ips = append(ips, ip)
	}
	sort.Slice(ips, func(i, j int) bool {
		return p.master[ips[i]].Born.After(p.master[ips[j]].Born)
	})
	for _, ip := range ips {
		e := p.master[ip]
		if e == nil || e.Expired(now) || p.inFlight[ip] || p.IsBlocked(ip, now) {
			continue
		}
		if _, ok := p.health[e.Token()]; ok {
			continue
		}
		p.inFlight[ip] = true
		out = append(out, e.Clone())
		if len(out) >= limit {
			break
		}
	}
	return out
}

// InFlight 返回正在探测中的出口数。
func (p *Pool) InFlight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.inFlight)
}

// FinishProbe 清掉在飞标记。
func (p *Pool) FinishProbe(ip string) {
	p.mu.Lock()
	delete(p.inFlight, ip)
	p.stats.Probed++
	p.mu.Unlock()
}

// NoteProbe 记一次判决计数。
func (p *Pool) NoteProbe(kind string) {
	p.mu.Lock()
	switch kind {
	case "healthy":
		p.stats.Healthy++
	case "quota":
		p.stats.Quota++
	case "buffered":
		p.stats.Buffered++
	case "dead":
		p.stats.Killed++
	default:
		p.stats.Failed++
	}
	p.mu.Unlock()
}

// UpsertMaster 把渠道拉到的出口合并进总池；凭据变了就当新出口重新过门槛。
func (p *Pool) UpsertMaster(e *Exit) (added bool, refreshed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	old := p.master[e.IP]
	if old == nil {
		p.master[e.IP] = e.Clone()
		return true, false
	}
	if old.User != e.User || old.Secret != e.Secret || old.Port != e.Port {
		old.User, old.Secret, old.Port = e.User, e.Secret, e.Port
		old.ExpireAt = e.ExpireAt
		old.Born = time.Now()
		p.dropHealthLocked(e.IP)
		delete(p.buffer, e.IP)
		return false, true
	}
	if e.ExpireAt != 0 {
		old.ExpireAt = e.ExpireAt
	}
	return false, false
}

// PruneExpired 清掉面板上已过期的出口。
func (p *Pool) PruneExpired(now time.Time) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for ip, e := range p.master {
		if e.Expired(now) {
			delete(p.master, ip)
			delete(p.buffer, ip)
			delete(p.cool, ip)
			n++
		}
	}
	return n
}

// WakeDue 把到期的冷却/缓冲/坏池条目放回候选。
func (p *Pool) WakeDue(now time.Time) (cool, buffer, dead int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for ip, t := range p.cool {
		if !t.After(now) {
			delete(p.cool, ip)
			cool++
		}
	}
	for ip, h := range p.buffer {
		if !h.Until.After(now) {
			delete(p.buffer, ip)
			buffer++
		}
	}
	for ip, t := range p.dead {
		if !t.After(now) {
			delete(p.dead, ip)
			dead++
		}
	}
	return
}

// resetLocation 是额度重置所用的时区：免费额度按北京时间每天 08:00 重置。
// 容器默认跑在 UTC，若按进程本地时区算，08:00 会变成北京时间 16:00——整整
// 晚八小时才开始补货，等于白白浪费一个上午。
var resetLocation = func() *time.Location {
	if loc, err := time.LoadLocation("Asia/Shanghai"); err == nil {
		return loc
	}
	return time.FixedZone("CST", 8*3600)
}()

// NextReset 返回下一次额度重置时刻（北京时间 hour 点）。
func NextReset(now time.Time, hour int) time.Time {
	n := now.In(resetLocation)
	t := time.Date(n.Year(), n.Month(), n.Day(), hour, 0, 0, 0, resetLocation)
	if !t.After(n) {
		t = t.Add(24 * time.Hour)
	}
	return t
}

// ---------- 持久化 ----------

type persistExt struct {
	UpdatedAt int64            `json:"updated_at"`
	Exits     map[string]*Exit `json:"exits"`
}

func (p *Pool) saveJSON(name string, v any) {
	tmp := filepath.Join(p.dir, name+".tmp")
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, filepath.Join(p.dir, name))
}

func (p *Pool) loadJSON(name string, v any) bool {
	data, err := os.ReadFile(filepath.Join(p.dir, name))
	if err != nil {
		return false
	}
	return json.Unmarshal(data, v) == nil
}

// Save 落盘：总池快照 + 三个池子的时间戳。
func (p *Pool) Save() {
	p.mu.Lock()
	ext := persistExt{UpdatedAt: time.Now().Unix(), Exits: make(map[string]*Exit, len(p.master))}
	for ip, e := range p.master {
		ext.Exits[ip] = e.Clone()
	}
	cool := make(map[string]int64, len(p.cool))
	for ip, t := range p.cool {
		cool[ip] = t.Unix()
	}
	dead := make(map[string]int64, len(p.dead))
	for ip, t := range p.dead {
		dead[ip] = t.Unix()
	}
	buf := make(map[string]*Hold, len(p.buffer))
	for ip, h := range p.buffer {
		buf[ip] = h
	}
	p.mu.Unlock()

	p.saveJSON("pool-ext.json", ext)
	p.saveJSON("pool-cool.json", cool)
	p.saveJSON("pool-dead.json", dead)
	p.saveJSON("pool-buffer.json", buf)
}

func (p *Pool) load() {
	var ext persistExt
	if p.loadJSON("pool-ext.json", &ext) {
		for ip, e := range ext.Exits {
			if e != nil {
				p.master[ip] = e
			}
		}
	}
	var cool map[string]int64
	if p.loadJSON("pool-cool.json", &cool) {
		for ip, t := range cool {
			p.cool[ip] = time.Unix(t, 0)
		}
	}
	var dead map[string]int64
	if p.loadJSON("pool-dead.json", &dead) {
		for ip, t := range dead {
			p.dead[ip] = time.Unix(t, 0)
		}
	}
	var buf map[string]*Hold
	if p.loadJSON("pool-buffer.json", &buf) {
		for ip, h := range buf {
			if h != nil {
				p.buffer[ip] = h
			}
		}
	}
}
