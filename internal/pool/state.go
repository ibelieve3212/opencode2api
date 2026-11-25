package pool

import (
	"encoding/json"
	"math/rand/v2"
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
	DeadBackoff   time.Duration // 坏池第一次沉睡时长，每再判死一次延长 4 倍
	DeadTTL       time.Duration // 坏池沉睡上限
	QuotaBackoff  time.Duration // 429 第一次压多久，每再撞一次延长 4 倍
	QuotaTTL      time.Duration // 429 压制时长上限
	ResetHour     int           // 每日额度重置的小时（北京时间 8）
	HealthLease   time.Duration // 健康池租约：探通只是"上次探通"，不是永久可用
	// ProbeBudget 是同一个出口每天最多被门槛探通几次（0 = 不限）。
	//
	// 探通一次就是打一个真实请求，会实打实吃掉出口当天的免费额度。实测一个出口
	// 一天的额度只有个位数次成功请求，而 15 分钟一轮的复验一天要探它 90 多次——
	// 额度在复验里就被烧光了，真实用户过来只能拿到 429。这不是"探得不够"，是
	// 探得太多：用完预算的出口继续留在健康池里供货，到 08:00 重置后才复验。
	ProbeBudget int
}

// DefaultConfig 与 Python 版线上参数一致。
func DefaultConfig() Config {
	return Config{
		HealthTarget:  200,
		BufferHold:    10 * time.Minute,
		BufferStrikes: 3,
		DeadBackoff:   15 * time.Minute,
		DeadTTL:       24 * time.Hour,
		QuotaBackoff:  2 * time.Minute,
		QuotaTTL:      6 * time.Hour,
		ResetHour:     8,
		HealthLease:   15 * time.Minute,
		ProbeBudget:   3,
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
	deadN    map[string]int       // 坏池：该 ip 连续被判死了几次，用来算退避
	quotaN   map[string]int       // 冷却池：该 ip 连续撞了几次 429，用来算退避
	probeN   map[string]int       // 该 ip 今天被探通了几次（= 烧掉几个额度）
	probeDay string               // probeN/quotaN 属于哪个额度日（到点整体清零）
	lease    map[string]time.Time // 健康池租约：token -> 到期
	inFlight map[string]bool      // 正在探测的 ip
	idx      int
	stats    Stats
	migrated int // 启动时被压回第一级的坏池条目数（老版本固定 24h 迁移）
	migCool  int // 启动时被压回第一级的冷却条目数（老版本按 Retry-After 睡到 08:00）

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
		deadN:    map[string]int{},
		quotaN:   map[string]int{},
		probeN:   map[string]int{},
		lease:    map[string]time.Time{},
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

// Migrated 报告本次启动把多少个旧版坏池条目压回第一级退避。
func (p *Pool) Migrated() int { return p.migrated }

// MigratedCool 报告本次启动把多少个按 Retry-After 睡到 08:00 的冷却条目压回第一级。
func (p *Pool) MigratedCool() int { return p.migCool }

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
			delete(p.lease, tok)
		}
	}
	p.health[e.Token()] = e.Clone()
	p.lease[e.Token()] = time.Now().Add(p.cfg.HealthLease)
	delete(p.cool, e.IP)
	delete(p.buffer, e.IP)
	delete(p.dead, e.IP)
	// 探通了就说明之前的"连不上"是暂时的，退避阶梯从头算：否则一个刚被冤枉
	// 过的出口会一直背着最长的沉睡时长。
	delete(p.deadN, e.IP)
	delete(p.quotaN, e.IP)
	p.stats.Healthy++
	return true
}

// Cool 把出口压进冷却池，按"连续撞 429 次数"走阶梯退避。
func (p *Pool) Cool(e *Exit) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollDayLocked()
	p.dropHealthLocked(e.IP)
	// 一个出口同一时刻只能待在一个"不予派发"的池子里。否则它会被两个计时器
	// 同时压着，按最长那个算：本该早点回来的出口，会被残留的坏池记录多扣一天，
	// 产能白丢；到点后还会被不同池子各验一遍，白烧额度。
	delete(p.buffer, e.IP)
	delete(p.dead, e.IP)
	until := p.quotaUntilLocked(e.IP)
	if prev, ok := p.cool[e.IP]; !ok || until.After(prev) {
		p.cool[e.IP] = until
	}
	return until
}

// quotaUntilLocked 算这个出口这次该压多久，并把它的"连续撞 429 次数"加一。
//
// 429 不等于这个出口当天没额度了：同一个出口隔两秒重问就 200，响应里的 id 还
// 有两种形态，说明上游是多个后端在轮着接、撞上没额度的那个才回 429。以前按报文
// 里的 Retry-After（实测约 19 小时）把出口睡到第二天 08:00，于是一轮 429 就能让
// 上千个还能用的出口集体下线——2026-11-25 那 172 个就是这么来的。
//
// 改成 2m → 8m → 32m → … 的阶梯：撞得越连续压得越久，真没额度的自然爬到上限，
// 假 429 的两分钟就回来。重试和新退避都只消耗 429（不动额度），所以代价只有时间。
func (p *Pool) quotaUntilLocked(ip string) time.Time {
	p.quotaN[ip]++
	hold := p.cfg.QuotaBackoff
	for i := 1; i < p.quotaN[ip] && hold < p.cfg.QuotaTTL; i++ {
		hold *= 4
	}
	if hold > p.cfg.QuotaTTL {
		hold = p.cfg.QuotaTTL
	}
	return time.Now().Add(hold)
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
		until = p.deadUntilLocked(e.IP)
		p.dead[e.IP] = until
		p.stats.Killed++
		return until, true
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
	until := p.deadUntilLocked(e.IP)
	p.dead[e.IP] = until
	p.stats.Killed++
	return until
}

// deadUntilLocked 算这个出口这次该死多久，并把它"连续判死次数"加一。
//
// 固定 24 小时的问题：面板掐源 IP、链路抖动、对端刚重启都会先表现为"连不上"，
// 一次抖动就把出口按死一整天，等于池子自己在扔产能——2026-11-25 的坏池一度
// 堆到总池的 70%，基本就是这么来的。改成一个 15m → 1h → 4h → 16h → 24h 的
// 阶梯：真死的一分钟就能确认，假死的十几分钟内就回来。
func (p *Pool) deadUntilLocked(ip string) time.Time {
	p.deadN[ip]++
	hold := p.cfg.DeadBackoff
	for i := 1; i < p.deadN[ip] && hold < p.cfg.DeadTTL; i++ {
		hold *= 4
	}
	if hold > p.cfg.DeadTTL {
		hold = p.cfg.DeadTTL
	}
	return time.Now().Add(hold)
}

// DropHealth 把出口移出健康池（转发失败等），不改变其它池的结论。
func (p *Pool) DropHealth(ip string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropHealthLocked(ip)
}

// QuotaDemote 是"真流量撞 429"的路径：直接进冷却池走阶梯。
func (p *Pool) QuotaDemote(e *Exit) time.Time { return p.Cool(e) }

// quotaDay 把时刻归到它所属的"额度日"：免费额度每天 08:00 重置，所以 07:59 和
// 08:01 分属两天。用日期串当键，跨天时把按天累计的计数整体清掉。
func quotaDay(t time.Time, resetHour int) string {
	return t.Add(-time.Duration(resetHour) * time.Hour).Format("2006-01-02")
}

// rollDayLocked 在额度日翻页时清空"今天的"计数。
func (p *Pool) rollDayLocked() {
	day := quotaDay(time.Now(), p.cfg.ResetHour)
	if p.probeDay == day {
		return
	}
	p.probeDay = day
	p.probeN = map[string]int{}
	p.quotaN = map[string]int{}
}

// BurnProbe 记这个出口今天又被探通了一次。探通就是打了一个真实请求，会吃掉它
// 当天的免费额度，所以这个数就是"烧掉了几个额度"。
func (p *Pool) BurnProbe(ip string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollDayLocked()
	p.probeN[ip]++
}

// ProbeLeft 返回这个出口今天还剩几次探针额度。
func (p *Pool) ProbeLeft(ip string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollDayLocked()
	left := p.cfg.ProbeBudget - p.probeN[ip]
	if p.cfg.ProbeBudget <= 0 || left < 0 {
		return 0
	}
	return left
}

// probeSpentLocked 报告这个出口今天的探针额度是否已经用完。
func (p *Pool) probeSpentLocked(ip string) bool {
	return p.cfg.ProbeBudget > 0 && p.probeN[ip] >= p.cfg.ProbeBudget
}

func (p *Pool) dropHealthLocked(ip string) {
	for tok, cur := range p.health {
		if cur.IP == ip {
			delete(p.health, tok)
			delete(p.lease, tok)
		}
	}
}

// ExpireLeases 退役到期的健康池租约，把它们放回候选重新过门槛。
//
// 健康池里的一条只代表"上次探通"。出口的免费额度既会被真实流量吃掉，也会被
// 同一台机器上的其它使用者吃掉，探通之后永不复验，池子就会把 429 一直转给
// 真实请求。租约到期后出口回候选，由门槛抽样复验——探测量由目标数量决定，
// 不再随池子变大而膨胀。
func (p *Pool) ExpireLeases(now time.Time) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollDayLocked()
	var expired []string
	for tok, until := range p.lease {
		if until.After(now) {
			continue
		}
		// 今天的探针额度已经用完：再探一次既不会更准，还会把出口最后一点免费
		// 额度烧掉。留在健康池里继续供货，到 08:00 重置后再复验。
		if e := p.health[tok]; e != nil && p.probeSpentLocked(e.IP) {
			p.lease[tok] = NextReset(now, p.cfg.ResetHour)
			continue
		}
		delete(p.lease, tok)
		delete(p.health, tok)
		expired = append(expired, tok)
	}
	return expired
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
// 从游标处顺序取，供全量扫描使用。
func (p *Pool) Candidates(limit int, now time.Time) []*Exit {
	return p.candidates(limit, now, false)
}

// Sample 从随机位置取一小批候选，供门槛的抽样探测使用：样本散在全池各处，
// 而不是老在候选列表头部打转。
func (p *Pool) Sample(limit int, now time.Time) []*Exit {
	return p.candidates(limit, now, true)
}

func (p *Pool) candidates(limit int, now time.Time, randomStart bool) []*Exit {
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
		// 列表重建后游标必须归零：否则新加进来的高优先级渠道（uu/雷神）
		// 会被跳过去，探针继续在老位置（ext 区）打转。
		p.candScan = 0
	}
	n := len(p.candList)
	if randomStart {
		p.candScan = rand.IntN(n)
	}
	for i := 0; i < n && len(out) < limit; i++ {
		ip := p.candList[(p.candScan+i)%n]
		e := p.master[ip]
		if e == nil || e.Expired(now) || p.inFlight[ip] || p.IsBlocked(ip, now) {
			continue
		}
		if _, ok := p.health[e.Token()]; ok {
			continue
		}
		// 今天已经探通过 ProbeBudget 次：它当天的免费额度基本被门槛自己烧光了，
		// 再拿它过门槛只会重复烧。等 08:00 重置后再算。
		if p.probeSpentLocked(ip) {
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

// Forget 把一个出口从总池和所有状态里彻底删掉。UU 的节点是一次性的：
// 探不通就是已经过期，留在坏池里睡 24 小时没有任何意义。
func (p *Pool) Forget(ip string) {
	p.mu.Lock()
	delete(p.master, ip)
	delete(p.health, ip)
	delete(p.cool, ip)
	delete(p.buffer, ip)
	delete(p.dead, ip)
	delete(p.deadN, ip)
	delete(p.quotaN, ip)
	delete(p.probeN, ip)
	delete(p.inFlight, ip)
	p.masterVer++
	p.mu.Unlock()
}

// HealthCountByKind 统计某类出口在健康池里的数量，用于按需补货。
func (p *Pool) HealthCountByKind(k Kind) int {
	p.mu.Lock()
	n := 0
	for _, e := range p.health {
		if e != nil && e.Kind == k {
			n++
		}
	}
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
		// 端口/凭据变了就是换了一条可用链路：必须把旧的冷却与坏池记录清掉，
		// 否则 UU 这种「IP 不变、端口轮换」的节点会永远躺在坏池里睡 24 小时。
		delete(p.buffer, e.IP)
		delete(p.cool, e.IP)
		delete(p.dead, e.IP)
		delete(p.deadN, e.IP)
		delete(p.quotaN, e.IP)
		delete(p.probeN, e.IP)
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
			delete(p.dead, ip)
			delete(p.deadN, ip)
			delete(p.quotaN, ip)
			delete(p.probeN, ip)
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

// persistDaily 是按天累计的两个计数：撞了几次 429、被探通了几次。都按额度日
// （08:00 翻页）算，所以带上日期；日期对不上就当没有，整体从头累计。
type persistDaily struct {
	Day   string         `json:"day"`
	Quota map[string]int `json:"quota"`
	Probe map[string]int `json:"probe"`
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
	deadN := make(map[string]int, len(p.dead))
	for ip, n := range p.deadN {
		deadN[ip] = n
	}
	daily := persistDaily{Day: p.probeDay, Quota: map[string]int{}, Probe: map[string]int{}}
	for ip, n := range p.quotaN {
		daily.Quota[ip] = n
	}
	for ip, n := range p.probeN {
		daily.Probe[ip] = n
	}
	buf := make(map[string]*Hold, len(p.buffer))
	for ip, h := range p.buffer {
		buf[ip] = h
	}
	p.mu.Unlock()

	p.saveJSON("pool-ext.json", ext)
	p.saveJSON("pool-cool.json", cool)
	p.saveJSON("pool-dead.json", dead)
	p.saveJSON("pool-dead-n.json", deadN)
	p.saveJSON("pool-daily.json", daily)
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
	var deadN map[string]int
	if p.loadJSON("pool-dead-n.json", &deadN) {
		for ip, n := range deadN {
			p.deadN[ip] = n
		}
	} else {
		// 一次性迁移：老版本按固定 24 小时写坏池，那些时间戳在新阶梯下没有依据，
		// 会让整个坏池（当时占七成）再躺一整天。退避记录缺失就是"从没算过阶梯"，
		// 一律压回第一级，让它们十几分钟后重新过一次门槛。
		now, n := time.Now(), 0
		for ip, t := range p.dead {
			if t.After(now.Add(p.cfg.DeadBackoff)) {
				p.dead[ip] = now.Add(p.cfg.DeadBackoff)
				n++
			}
		}
		if n > 0 {
			p.migrated = n
		}
	}
	var daily persistDaily
	if p.loadJSON("pool-daily.json", &daily) && daily.Day == quotaDay(time.Now(), p.cfg.ResetHour) {
		for ip, n := range daily.Quota {
			p.quotaN[ip] = n
		}
		for ip, n := range daily.Probe {
			p.probeN[ip] = n
		}
		p.probeDay = daily.Day
	}
	if p.probeDay == "" {
		// 没有按天计数就说明冷却时间戳还是老版本按 Retry-After 写的（实测约 19
		// 小时，一律睡到第二天 08:00）。那不是这个出口的额度用完，只是它有一次
		// 撞上了没额度的后端。压回第一级，几分钟后重新过门槛。
		now, n := time.Now(), 0
		for ip, t := range p.cool {
			if t.After(now.Add(p.cfg.QuotaBackoff)) {
				p.cool[ip] = now.Add(p.cfg.QuotaBackoff)
				n++
			}
		}
		if n > 0 {
			p.migCool = n
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
