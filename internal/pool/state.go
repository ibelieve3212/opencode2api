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

	// 候选列表缓存：主池上万条时，每次取候选都整体排序会让 96 个探针把锁抢死。
	// 只在主池变动（masterVer 变化）时重建一次。
	candList  []string
	candScan  int
	candVer   uint64
	masterVer uint64

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
	// 一个出口同一时刻只能待在一个"不予派发"的池子里。否则它会被两个计时器
	// 同时压着，按最长那个算：本该 08:00 回冷却结束的出口，会被残留的坏池
	// 记录多扣一整天，产能白丢；到点后还会被不同池子各验一遍，白烧额度。
	delete(p.buffer, e.IP)
	delete(p.dead, e.IP)
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
	delete(p.cool, e.IP)
	delete(p.dead, e.IP)
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
	delete(p.cool, e.IP)
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
	if e, ok := p.health[token]; ok && e != nil {
		return e.Clone()
	}
	// 门槛要能按 token 找到还没进健康池的出口，否则变体协议永远探不了。
	for _, e := range p.master {
		if e != nil && e.Token() == token {
			return e.Clone()
		}
	}
	return nil
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
	if len(p.master) == 0 {
		return out
	}
	// 只有主池变动时才重排：刚同步进来的优先，别把探针耗在失效的老节点上。
	if p.candVer != p.masterVer || len(p.candList) != len(p.master) {
		p.candList = p.candList[:0]
		for ip := range p.master {
			p.candList = append(p.candList, ip)
		}
		sort.Slice(p.candList, func(a, b int) bool {
			ea, eb := p.master[p.candList[a]], p.master[p.candList[b]]
			ra, rb := kindRank(ea.Kind), kindRank(eb.Kind)
			if ra != rb {
				return ra < rb
			}
			return ea.Born.After(eb.Born)
		})
		p.candVer = p.masterVer
	}
	n := len(p.candList)
	for i := 0; i < n && len(out) < limit; i++ {
		ip := p.candList[(p.candScan+i)%n]
		e := p.master[ip]
		if e == nil || e.Expired(now) || p.inFlight[ip] || p.IsBlocked(ip, now) {
			continue
		}
		if _, ok := p.health[e.Token()]; ok {
			continue
		}
		p.inFlight[ip] = true
		out = append(out, e.Clone())
	}
	p.candScan = (p.candScan + len(out)) % n
	return out
}

// HealthCount 只取健康池大小：探针不该为了看一眼目标是否达成而遍历全池。
func (p *Pool) HealthCount() int {
	p.mu.Lock()
	n := len(p.health)
	p.mu.Unlock()
	return n
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
		p.masterVer++
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
			p.masterVer++
			n++
		}
	}
	return n
}

// WakeDue 把到期的冷却/缓冲/坏池条目放回候选。
//
// maxRelease > 0 时每轮最多放回这么多个，并按到期时刻先后放：额度是 UTC 零点
// 统一重置的，若把上万个出口一次性全放回，门槛会在同一分钟里把它们当天的免费
// 额度全部消耗掉，等于刚解封就被自己烧光。细水长流才留得住产能。
func (p *Pool) WakeDue(now time.Time, maxRelease int) (cool, buffer, dead int) {
	type due struct {
		ip string
		at time.Time
	}
	var pending []due
	p.mu.Lock()
	for ip, t := range p.cool {
		if !t.After(now) {
			pending = append(pending, due{ip, t})
		}
	}
	for ip, h := range p.buffer {
		if !h.Until.After(now) {
			pending = append(pending, due{ip, h.Until})
		}
	}
	for ip, t := range p.dead {
		if !t.After(now) {
			pending = append(pending, due{ip, t})
		}
	}
	if maxRelease > 0 && len(pending) > maxRelease {
		sort.Slice(pending, func(i, j int) bool { return pending[i].at.Before(pending[j].at) })
		pending = pending[:maxRelease]
	}
	for _, d := range pending {
		if _, ok := p.cool[d.ip]; ok {
			delete(p.cool, d.ip)
			cool++
			continue
		}
		if _, ok := p.buffer[d.ip]; ok {
			delete(p.buffer, d.ip)
			buffer++
			continue
		}
		if _, ok := p.dead[d.ip]; ok {
			delete(p.dead, d.ip)
			dead++
		}
	}
	p.mu.Unlock()
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
	p.reconcileConcerned()
}

// reconcileConcerned 清掉历史遗留的"一个出口压在多个池子里"。
// 保留最早到期的那个：出口尽快回到候选、由门槛重新判定，比被旧记录多锁一天好。
// 返回被清理掉的多余记录数。
func (p *Pool) reconcileConcerned() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	ips := make(map[string]struct{}, len(p.cool)+len(p.buffer)+len(p.dead))
	for ip := range p.cool {
		ips[ip] = struct{}{}
	}
	for ip := range p.buffer {
		ips[ip] = struct{}{}
	}
	for ip := range p.dead {
		ips[ip] = struct{}{}
	}
	for ip := range ips {
		var untils []time.Time
		if t, ok := p.cool[ip]; ok {
			untils = append(untils, t)
		}
		if h, ok := p.buffer[ip]; ok {
			untils = append(untils, h.Until)
		}
		if t, ok := p.dead[ip]; ok {
			untils = append(untils, t)
		}
		if len(untils) < 2 {
			continue
		}
		soonest := untils[0]
		for _, t := range untils[1:] {
			if t.Before(soonest) {
				soonest = t
			}
		}
		// 只留最早到期的那一条，其余删掉。
		kept := false
		if t, ok := p.cool[ip]; ok {
			if !kept && t.Equal(soonest) {
				kept = true
			} else {
				delete(p.cool, ip)
				n++
			}
		}
		if h, ok := p.buffer[ip]; ok {
			if !kept && h.Until.Equal(soonest) {
				kept = true
			} else {
				delete(p.buffer, ip)
				n++
			}
		}
		if t, ok := p.dead[ip]; ok {
			if !kept && t.Equal(soonest) {
				kept = true
			} else {
				delete(p.dead, ip)
				n++
			}
		}
	}
	return n
}

// kindRank 决定探测顺序：变体协议（uu/雷神）优先占额度，外部面板垫底。
func kindRank(k Kind) int {
	switch k {
	case KindUU:
		return 0
	case KindLeigod:
		return 1
	default:
		return 2
	}
}
