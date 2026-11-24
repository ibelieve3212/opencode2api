package pool

// 网易 UU 加速器的 sproxy TCP 隧道。三层：
//
//	① REST /v3/acc  拉节点表：body = base64(AES-128-ECB(gzip(json)))，头里带
//	   Seed + Sign（MD5(params&seed&key&post_content&key2)）。
//	② mainlink      节点登录：帧 = version(1) + cmd(4 BE) + len(4 BE) + body，
//	   body 用 AES-GCM 封装（key = derive(ip:port)），拿到 sid 与 encrypt_key。
//	③ sproxy        建隧道：首帧 0x10 + iv + AESGCM(key, 目标信息)，数据面按
//	   encrypt_key 首字节做单字节 XOR（双向）。
//
// 额度是「每个出口 IP 每天 40 次」实测所得，所以按节点 IP 去重。

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	uuRESTHost = "mg.uu.163.com"
	uuACCPath  = "/v3/acc"
	uuGameID   = "5a3b3e9fe3a8b2bb04e917ff"
	uuKey1     = "93d42049d126d9ff6a7e4b93c33ff6f3"
	uuKey2     = "F072B57D4433531091CBF664577030B0"
	uuDeviceID = "377525c4949090ae"
	uuPwdField = "password"

	uuCmdLogin     = 1
	uuCmdLoginResp = 2
	uuCmdKeepalive = 4
	uuPktVerGCM    = 2

	uuMaxRounds   = 12
	uuRoundGap    = 1500 * time.Millisecond
	uuStallRounds = 3
)

// uuHeadersJSON 是设备指纹头（真机导出），改 EAD/Model 会被风控。
var uuHeadersJSON = `{"Locale": "zh_CN", "Operator": "460000", "RomVersion": "15", "Product": "PD2241", "Abi": "x86_64", "SystemType": "android", "SystemVersion": "35", "SavedChannel": "official", "AppVersion": "1530", "DualChannelEnabled": "1", "RomName": "Other", "ScreenDpi": "480", "Manufacturer": "vivo", "OtherVpnRunning": "0", "ScreenSize": "3.44", "WiFiEnabled": "1", "Content-Type": "text/plain; charset=utf-8", "SystemDebug": "0", "Root": "0", "ConnectMode": "IP", "Ntes-UU": "4cyaDglTSvRbtvgR", "JWT": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE4MTA5NjUwMjYsInVzZXJfaWQiOiIxNzE1MTIwMjUxMCIsInV1aWQiOjIzNDgxNDIyODksInVpZCI6IjM3NzUyNWM0OTQ5MDkwYWUiLCJzZXNzaW9uX2lkIjoiNGN5YURnbFRTdlJidHZnUiIsInJlZ2lzdGVyX3RpbWUiOjE3OTU0MTMwMjYwMDB9.jszS52lJPHhU3WAk72p56nvAbWVp2pro9zUmy8_LuYw", "Channel": "official", "EAD": "599747e6161212cg", "Brand": "vivo", "Rom": "V417IR release-keys", "BundleId": "com.netease.uu", "AppVersionCode": "11.3.12", "CellularEnabled": "1", "Model": "V2241A", "TimeZoneOffset": "-480", "Resolution": "1080x1920"}`

func uuHeaders() map[string]string {
	out := map[string]string{}
	_ = json.Unmarshal([]byte(uuHeadersJSON), &out)
	return out
}

// ---------- REST 编解码 ----------

func uuAESKey() []byte {
	if len(uuDeviceID) == 16 {
		return []byte(uuDeviceID)
	}
	sum := md5.Sum([]byte(uuDeviceID))
	return sum[:]
}

func uuECB(data []byte, encrypt bool) ([]byte, error) {
	block, err := aes.NewCipher(uuAESKey())
	if err != nil {
		return nil, err
	}
	if encrypt {
		pad := 16 - len(data)%16
		data = append(data, bytes.Repeat([]byte{byte(pad)}, pad)...)
	}
	if len(data)%16 != 0 {
		return nil, fmt.Errorf("ECB 数据长度不是 16 的倍数")
	}
	out := make([]byte, len(data))
	for i := 0; i < len(data); i += 16 {
		if encrypt {
			block.Encrypt(out[i:i+16], data[i:i+16])
		} else {
			block.Decrypt(out[i:i+16], data[i:i+16])
		}
	}
	return out, nil
}

func uuSign(params, seed, post string, hasPost bool) string {
	head := ""
	if params != "" {
		head = params + "&"
	}
	var plain string
	if hasPost {
		plain = fmt.Sprintf("%sseed=%s&key=%s&post_content=%s&key2=%s", head, seed, uuKey1, post, uuKey2)
	} else {
		plain = fmt.Sprintf("%sseed=%s&key=%s&key2=%s", head, seed, uuKey1, uuKey2)
	}
	sum := md5.Sum([]byte(plain))
	return strings.ToUpper(hex.EncodeToString(sum[:]))
}

func uuRestDecrypt(raw []byte) (string, error) {
	s := strings.TrimSpace(string(raw))
	if strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") {
		return s, nil
	}
	for len(s)%4 != 0 {
		s += "="
	}
	blob, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	out, err := uuECB(blob, false)
	if err != nil {
		return "", err
	}
	if n := len(out); n > 0 && int(out[n-1]) > 0 && int(out[n-1]) <= 16 {
		out = out[:n-int(out[n-1])]
	}
	if len(out) > 2 && out[0] == 0x1f && out[1] == 0x8b {
		zr, err := gzip.NewReader(bytes.NewReader(out))
		if err != nil {
			return "", err
		}
		out, err = io.ReadAll(zr)
		if err != nil {
			return "", err
		}
	}
	return string(out), nil
}

// uuSessionCache 缓存匿名会话（Ntes-UU / JWT），失效时自动重换。
var uuSessionCache struct {
	sync.Mutex
	headers map[string]string
	account string
	gacc    string
	at      time.Time
}

// uuBootstrap 用纯设备头 POST /v3/auth，从响应头拿 Ntes-UU 与 Jwt。
func uuBootstrap(ctx context.Context, client *http.Client) (map[string]string, error) {
	base := uuHeaders()
	delete(base, "JWT")
	delete(base, "Ntes-UU")
	text := "{}"
	gz := gzipBytes([]byte(text))
	enc, err := uuECB(gz, true)
	if err != nil {
		return nil, err
	}
	seed := fmt.Sprintf("%d", time.Now().Unix())
	post := url.QueryEscape(text)
	base["Seed"] = seed
	base["Sign"] = uuSign("", seed, post, true)
	base["Content-Type"] = "text/plain; charset=utf-8"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://"+uuRESTHost+"/v3/auth",
		bytes.NewReader([]byte(base64.StdEncoding.EncodeToString(enc))))
	if err != nil {
		return nil, err
	}
	for k, v := range base {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	tok := resp.Header.Get("Ntes-UU")
	jwt := resp.Header.Get("Jwt")
	if tok == "" || jwt == "" {
		return nil, fmt.Errorf("UU /v3/auth 没有返回会话头（HTTP %d）", resp.StatusCode)
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	text2, _ := uuRestDecrypt(raw)
	var meta map[string]any
	_ = json.Unmarshal([]byte(text2), &meta)
	account, _ := meta["account"].(string)
	gacc := ""
	for k, v := range meta {
		if k == "status" || k == "message" || k == "account" {
			continue
		}
		if sv, ok := v.(string); ok && sv != "" {
			gacc = sv
			break
		}
	}
	out := uuHeaders()
	out["Ntes-UU"] = tok
	out["JWT"] = jwt
	uuSessionCache.account = account
	uuSessionCache.gacc = gacc
	return out, nil
}

func gzipBytes(b []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(b)
	_ = zw.Close()
	return buf.Bytes()
}

// uuEnsureSession 返回可用的会话头；超过 20 分钟或强制刷新时重新引导。
func uuEnsureSession(ctx context.Context, client *http.Client, force bool) (map[string]string, error) {
	uuSessionCache.Lock()
	defer uuSessionCache.Unlock()
	if !force && uuSessionCache.headers != nil && time.Since(uuSessionCache.at) < 20*time.Minute {
		return uuSessionCache.headers, nil
	}
	h, err := uuBootstrap(ctx, client)
	if err != nil {
		return nil, err
	}
	uuSessionCache.headers = h
	uuSessionCache.at = time.Now()
	return h, nil
}

// fetchUUAcc 拉一次 /v3/acc。
func fetchUUAcc(ctx context.Context, client *http.Client) (map[string]any, error) {
	for attempt := 0; attempt < 2; attempt++ {
		acc, maintain, err := uuAccOnce(ctx, client, attempt > 0)
		if err != nil {
			return nil, err
		}
		if !maintain {
			return acc, nil
		}
	}
	return nil, fmt.Errorf("UU /v3/acc 一直返回 server maintain")
}

func uuAccOnce(ctx context.Context, client *http.Client, force bool) (map[string]any, bool, error) {
	headers, err := uuEnsureSession(ctx, client, force)
	if err != nil {
		return nil, false, err
	}
	params := url.Values{}
	params.Set("gid", uuGameID)
	params.Set("support_ip_fragment_v2", "true")
	params.Set("tunnel", "true")
	params.Set("virtual", "false")
	qs := params.Encode()

	h := map[string]string{}
	for k, v := range headers {
		h[k] = v
	}
	seed := fmt.Sprintf("%d", time.Now().Unix())
	h["Seed"] = seed
	h["Sign"] = uuSign(qs, seed, "", false)
	if h["Content-Type"] == "" {
		h["Content-Type"] = "text/plain; charset=utf-8"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://"+uuRESTHost+uuACCPath+"?"+qs, nil)
	if err != nil {
		return nil, false, err
	}
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("UU /v3/acc HTTP %d", resp.StatusCode)
	}
	text, err := uuRestDecrypt(raw)
	if err != nil {
		return nil, false, err
	}
	var acc map[string]any
	if err := json.Unmarshal([]byte(text), &acc); err != nil {
		return nil, false, fmt.Errorf("UU /v3/acc 不是 JSON: %v", err)
	}
	if st, _ := acc["status"].(string); st == "server maintain" {
		return nil, true, nil
	}
	return acc, false, nil
}

// ---------- 隧道加解密 ----------

func uuDeriveKey(host string, port int) []byte {
	material := []byte(fmt.Sprintf("%s:%d", host, port))
	if len(material) < 16 {
		sum := md5.Sum(material)
		return sum[:]
	}
	return material[:16]
}

func uuSeal(key, plain []byte) []byte {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil
	}
	gcm, err := cipher.NewGCMWithNonceSize(block, 16)
	if err != nil {
		return nil
	}
	iv := make([]byte, 16)
	_, _ = rand.Read(iv)
	out := make([]byte, 0, 1+16+len(plain)+16)
	out = append(out, byte(len(iv)))
	out = append(out, iv...)
	return gcm.Seal(out, iv, plain, nil)
}

func uuOpen(key, blob []byte) ([]byte, error) {
	if len(blob) < 1 {
		return nil, fmt.Errorf("GCM 包太短")
	}
	ivlen := int(blob[0])
	if len(blob) < 1+ivlen+16 {
		return nil, fmt.Errorf("GCM 包长度异常")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCMWithNonceSize(block, 16)
	if err != nil {
		return nil, err
	}
	return gcm.Open(nil, blob[1:1+ivlen], blob[1+ivlen:], nil)
}

// ---------- mainlink 登录 ----------

type uuSession struct {
	ml     net.Conn
	sid    string
	obfs   byte
	stop   chan struct{}
	closed sync.Once
}

func (s *uuSession) Close() {
	s.closed.Do(func() {
		close(s.stop)
		s.ml.Close()
	})
}

func uuReadExact(c net.Conn, n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(c, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func uuLogin(ip string, port int, account, gacc string, timeout time.Duration) (*uuSession, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, port), timeout)
	if err != nil {
		return nil, err
	}
	key := uuDeriveKey(ip, port)
	body := map[string]any{
		"abi": "x86_64", "app_version": "1530", "brand": "", "channel": "official",
		"dual_channel_on": 1,
		"extra_info":      map[string]any{"remote_smart_boost_enabled": 0, "smart_boost": 0},
		"game_id":         uuGameID, "mtu": 20000, "network_stack": "system",
		"pseudo_boost": 0, "password": gacc, "system_type": "android",
		"system_version": "35", "tcp_proxy_encrypt_on": 1, "tcpip_over_udp": 1,
		"username": account,
	}
	plain, _ := json.Marshal(body)
	sealed := uuSeal(key, plain)
	if sealed == nil {
		conn.Close()
		return nil, fmt.Errorf("UU 登录封装失败")
	}
	frame := make([]byte, 9, 9+len(sealed))
	frame[0] = uuPktVerGCM
	binary.BigEndian.PutUint32(frame[1:5], uuCmdLogin)
	binary.BigEndian.PutUint32(frame[5:9], uint32(len(sealed)))
	frame = append(frame, sealed...)
	conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write(frame); err != nil {
		conn.Close()
		return nil, err
	}
	for {
		head, err := uuReadExact(conn, 9)
		if err != nil {
			conn.Close()
			return nil, err
		}
		ver := head[0]
		cmd := binary.BigEndian.Uint32(head[1:5])
		length := binary.BigEndian.Uint32(head[5:9])
		payload, err := uuReadExact(conn, int(length))
		if err != nil {
			conn.Close()
			return nil, err
		}
		if ver == uuPktVerGCM {
			payload, err = uuOpen(key, payload)
			if err != nil {
				conn.Close()
				return nil, fmt.Errorf("UU 登录解封失败: %v", err)
			}
		}
		if cmd != uuCmdLoginResp {
			continue
		}
		var resp struct {
			Result    int    `json:"result"`
			ErrorCode any    `json:"error_code"`
			SID       any    `json:"session_id"`
			Encrypt   string `json:"encrypt_key"`
			Method    string `json:"encrypt_method"`
		}
		if err := json.Unmarshal(payload, &resp); err != nil {
			conn.Close()
			return nil, err
		}
		if resp.Result != 0 {
			conn.Close()
			return nil, fmt.Errorf("UU 节点登录失败 result=%d error_code=%v", resp.Result, resp.ErrorCode)
		}
		// sid 有时是数字、有时是字符串，统一按文本处理。
		sid := ""
		switch v := resp.SID.(type) {
		case string:
			sid = v
		case float64:
			sid = strconv.FormatInt(int64(v), 10)
		default:
			sid = fmt.Sprintf("%v", v)
		}
		var obfs byte
		if resp.Method == "XOR" && resp.Encrypt != "" {
			obfs = resp.Encrypt[0]
		}
		conn.SetDeadline(time.Time{})
		s := &uuSession{ml: conn, sid: sid, obfs: obfs, stop: make(chan struct{})}
		go s.keepalive(key)
		return s, nil
	}
}

func (s *uuSession) keepalive(key []byte) {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			frame := make([]byte, 9)
			frame[0] = uuPktVerGCM
			binary.BigEndian.PutUint32(frame[1:5], uuCmdKeepalive)
			binary.BigEndian.PutUint32(frame[5:9], 0)
			_ = s.ml.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := s.ml.Write(frame); err != nil {
				return
			}
		}
	}
}

// ---------- sproxy 隧道 ----------

type uuTunnel struct {
	net.Conn
	sess *uuSession
	obfs byte
}

func (t *uuTunnel) Read(p []byte) (int, error) {
	n, err := t.Conn.Read(p)
	for i := 0; i < n; i++ {
		p[i] ^= t.obfs
	}
	return n, err
}

func (t *uuTunnel) Write(p []byte) (int, error) {
	buf := make([]byte, len(p))
	for i := range p {
		buf[i] = p[i] ^ t.obfs
	}
	if _, err := t.Conn.Write(buf); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (t *uuTunnel) Close() error {
	err := t.Conn.Close()
	t.sess.Close()
	return err
}

// dialUU 建一条经 UU 节点的隧道：先 mainlink 登录拿 sid，再 sproxy 首帧。
func dialUU(ctx context.Context, e *Exit, host string, port int, timeout time.Duration) (net.Conn, error) {
	sess, err := uuLogin(e.IP, e.Port, e.User, e.Secret, timeout)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", e.IP, e.Port), timeout)
	if err != nil {
		sess.Close()
		return nil, err
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.To4() == nil {
		ips, err := net.LookupIP(host)
		if err != nil {
			conn.Close()
			sess.Close()
			return nil, err
		}
		for _, cand := range ips {
			if v4 := cand.To4(); v4 != nil {
				ip = v4
				break
			}
		}
		if ip == nil {
			conn.Close()
			sess.Close()
			return nil, fmt.Errorf("UU 目标 %s 没有 IPv4", host)
		}
	}
	u, s := []byte(e.User), []byte(sess.sid)
	plain := make([]byte, 0, 8+len(u)+len(s))
	plain = append(plain, 1, 1)
	plain = append(plain, ip.To4()...)
	plain = append(plain, byte(port>>8), byte(port&0xFF))
	plain = append(plain, byte(len(u)))
	plain = append(plain, u...)
	plain = append(plain, byte(len(s)))
	plain = append(plain, s...)
	frame := uuSeal(uuDeriveKey(e.IP, e.Port), plain)
	if frame == nil {
		conn.Close()
		sess.Close()
		return nil, fmt.Errorf("UU 首帧封装失败")
	}
	conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write(frame); err != nil {
		conn.Close()
		sess.Close()
		return nil, err
	}
	ack, err := uuReadExact(conn, 35)
	if err != nil {
		conn.Close()
		sess.Close()
		return nil, fmt.Errorf("UU 首帧应答失败: %v", err)
	}
	status, err := uuOpen(uuDeriveKey(e.IP, e.Port), ack)
	if err != nil {
		conn.Close()
		sess.Close()
		return nil, fmt.Errorf("UU 首帧应答解封失败: %v", err)
	}
	if len(status) < 2 || status[0] != 0x01 || status[1] != 0x00 {
		conn.Close()
		sess.Close()
		return nil, fmt.Errorf("UU 隧道被拒: % x", status)
	}
	conn.SetDeadline(time.Time{})
	return &uuTunnel{Conn: conn, sess: sess, obfs: sess.obfs}, nil
}

// ---------- 渠道拉取 ----------

// fetchUUExits 饱和式拉取：每轮换一批节点，按节点 IP 去重，连续几轮没新 IP 就停。
func fetchUUExits(ctx context.Context, client *http.Client, channel string) ([]*Exit, error) {
	seen := make(map[string]*Exit, 1024)
	stall := 0
	var lastErr error
	for round := 0; round < uuMaxRounds; round++ {
		if round > 0 {
			select {
			case <-ctx.Done():
				return uuExitList(seen), ctx.Err()
			case <-time.After(uuRoundGap):
			}
		}
		acc, err := fetchUUAcc(ctx, client)
		if err != nil {
			lastErr = err
			stall++
			if stall >= uuStallRounds && len(seen) > 0 {
				break
			}
			continue
		}
		account, gacc := uuSessionCache.account, uuSessionCache.gacc
		if account == "" || gacc == "" {
			account, gacc = uuAccount(acc)
		}
		if account == "" || gacc == "" {
			lastErr = fmt.Errorf("UU /v3/acc 没有 account/gacc")
			stall++
			continue
		}
		added := 0
		for _, n := range uuNodes(acc) {
			if _, ok := seen[n.IP]; ok {
				continue
			}
			seen[n.IP] = &Exit{
				IP: n.IP, Port: n.Port, Kind: KindUU,
				User: account, Secret: gacc,
				Channel: channel, Born: time.Now(),
			}
			added++
		}
		if added == 0 {
			stall++
			if stall >= uuStallRounds {
				break
			}
		} else {
			stall = 0
		}
	}
	if len(seen) == 0 {
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, fmt.Errorf("UU 没有返回任何节点")
	}
	return uuExitList(seen), nil
}

func uuExitList(m map[string]*Exit) []*Exit {
	out := make([]*Exit, 0, len(m))
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out = append(out, m[k])
	}
	return out
}

func uuAccount(acc map[string]any) (string, string) {
	// 响应里账号可能是顶层字段，也可能在 user 子对象里。
	get := func(m map[string]any, k string) string {
		if v, ok := m[k].(string); ok {
			return v
		}
		return ""
	}
	account := get(acc, "account")
	gacc := get(acc, "gacc")
	if user, ok := acc["user"].(map[string]any); ok {
		if account == "" {
			account = get(user, "account")
		}
		if gacc == "" {
			gacc = get(user, "gacc")
		}
	}
	return account, gacc
}

type uuNode struct {
	IP   string
	Port int
}

func uuNodes(acc map[string]any) []uuNode {
	list, _ := acc["acc"].([]any)
	out := make([]uuNode, 0, len(list))
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		ip, _ := m["ip"].(string)
		if ip == "" {
			continue
		}
		port := 0
		switch v := m["port"].(type) {
		case float64:
			port = int(v)
		case string:
			fmt.Sscanf(v, "%d", &port)
		}
		if port == 0 {
			continue
		}
		out = append(out, uuNode{IP: ip, Port: port})
	}
	return out
}
