package pool

// 雷神（leigod）私有协议：不是标准 SOCKS5。
//
//	握手  : 05 04 80 02 82 83 | 01 <len user> user <len pass> pass
//	        | 05 01 <checksum(pass)> 03 <len host> host <port:uint16>
//	应答  : 05 02 | 01 00 | 04 <rep>   (rep==0x01 成功)
//	数据面: 上行（客户端→节点）逐字节 XOR 0x01，下行原样
//
// 校验和只对 16 字节密码有效：(-pass[6] - pass[10] - 0x2C) & 0xFF。

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
)

const (
	leigodAPIHost = "https://opapi.xxghh.biz"
	leigodAPIPath = "/speed/gamemaster/nodeInfo/areaRule"

	leigodToken  = "0185de07af764f9c9de1d29c3319f2a0"
	leigodAppID  = "nnMobile_d0k3duup"
	leigodDevice = "00000000-62b4-9300-0000-00007facd163"
	leigodAnon   = "2acfa01fcd25034d595d1366dec0775b5"
	leigodBusi   = "nn_aksjfdasoifnkls"

	leigodDefUser = "USER831173"
	leigodDefPass = "PASS331250181986"
)

func leigodHeaders() map[string]string {
	return map[string]string{
		"token":         leigodToken,
		"appId":         leigodAppID,
		"timeStamp":     strconv.FormatInt(time.Now().UnixMilli(), 10),
		"signType":      "1",
		"sign":          "",
		"version":       "700",
		"reqChannel":    "2",
		"deviceId":      leigodDevice,
		"appName":       "leigod_accelerator",
		"mobileBrand":   "vivo",
		"mobileModel":   "V2241A",
		"osVersion":     "15",
		"platform":      "2",
		"registerCanal": "common",
		"anonymousId":   leigodAnon,
		"busiType":      leigodBusi,
		"cpuArch":       "[x86_64, arm64-v8a, x86]",
		"User-Agent":    "okhttp/4.9.2",
		"Content-Type":  "application/json; charset=UTF-8",
	}
}

type leigodAreaRule struct {
	Success bool   `json:"success"`
	RetMsg  string `json:"retMsg"`
	RetData map[string]struct {
		Nodes []struct {
			ProxyIP   string `json:"proxyIp"`
			ProxyPort int    `json:"proxyPort"`
			ProxyUser string `json:"proxyUser"`
			ProxyPass string `json:"proxyPass"`
		} `json:"nodes"`
	} `json:"retData"`
}

// 雷神节点表每次只回一批，必须多轮拉取才能攒够；接口有 403 风控，
// 所以轮间隔要留够，连续几轮没有新 IP 就认为已经饱和。
const (
	leigodMaxRounds   = 25
	leigodRoundGap    = 1200 * time.Millisecond
	leigodStallRounds = 3
)

// fetchLeigodExits 饱和式拉取雷神节点：按 IP 去重，连续 stallRounds 轮
// 没有新 IP 就停。返回的每个 IP 只保留一个出口（额度是按 IP 算的）。
func fetchLeigodExits(ctx context.Context, client *http.Client, channel string) ([]*Exit, error) {
	seen := make(map[string]*Exit, 512)
	stall := 0
	var lastErr error
	for round := 0; round < leigodMaxRounds; round++ {
		if round > 0 {
			select {
			case <-ctx.Done():
				return exitsOf(seen), ctx.Err()
			case <-time.After(leigodRoundGap):
			}
		}
		batch, err := leigodRound(ctx, client, channel)
		if err != nil {
			lastErr = err
			stall++
			if stall >= leigodStallRounds && len(seen) > 0 {
				break
			}
			continue
		}
		added := 0
		for _, e := range batch {
			if _, ok := seen[e.IP]; ok {
				continue
			}
			seen[e.IP] = e
			added++
		}
		if added == 0 {
			stall++
			if stall >= leigodStallRounds {
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
		return nil, fmt.Errorf("雷神没有返回任何节点")
	}
	return exitsOf(seen), nil
}

func exitsOf(m map[string]*Exit) []*Exit {
	out := make([]*Exit, 0, len(m))
	for _, e := range m {
		out = append(out, e)
	}
	return out
}

// leigodRound 拉一次 areaRule，把 retData 里各分区的 nodes 摊平。
func leigodRound(ctx context.Context, client *http.Client, channel string) ([]*Exit, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		leigodAPIHost+leigodAPIPath, bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, err
	}
	for k, v := range leigodHeaders() {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("雷神接口 HTTP %d: %s", resp.StatusCode, truncate(string(body), 80))
	}
	var parsed leigodAreaRule
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("雷神接口返回不是 JSON: %v", err)
	}
	if !parsed.Success {
		return nil, fmt.Errorf("雷神接口返回失败: %s", parsed.RetMsg)
	}
	now := time.Now()
	out := make([]*Exit, 0, 256)
	for region, area := range parsed.RetData {
		for _, n := range area.Nodes {
			if n.ProxyIP == "" {
				continue
			}
			port := n.ProxyPort
			if port == 0 {
				port = 5438
			}
			user, pass := n.ProxyUser, n.ProxyPass
			if user == "" {
				user = leigodDefUser
			}
			if pass == "" {
				pass = leigodDefPass
			}
			out = append(out, &Exit{
				IP: n.ProxyIP, Port: port, Kind: KindLeigod,
				User: user, Secret: pass,
				Channel: "leigod-" + region, Born: now,
			})
		}
	}
	return out, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// leigodChecksum 是雷神私有的口令校验和，只对 16 字节密码有定义。
func leigodChecksum(pass string) byte {
	if len(pass) != 16 {
		return 0
	}
	return byte((-(int(pass[6])) - int(pass[10]) - 0x2C) & 0xFF)
}

// dialLeigod 走雷神私有握手建立隧道。返回的连接写入时会自动做 XOR 0x01。
func dialLeigod(ctx context.Context, e *Exit, host string, port int, timeout time.Duration) (net.Conn, error) {
	if len(e.Secret) != 16 {
		return nil, fmt.Errorf("雷神节点密码长度异常(%d)", len(e.Secret))
	}
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", e.Addr())
	if err != nil {
		return nil, err
	}
	if err := leigodHandshake(conn, e, host, port, timeout); err != nil {
		conn.Close()
		return nil, err
	}
	return &xorConn{Conn: conn, key: 1}, nil
}

func leigodHandshake(conn net.Conn, e *Exit, host string, port int, timeout time.Duration) error {
	conn.SetDeadline(time.Now().Add(timeout))
	u, p := []byte(e.User), []byte(e.Secret)
	h := []byte(host)
	if len(h) > 255 {
		return fmt.Errorf("目标域名过长")
	}
	buf := make([]byte, 0, 32+len(u)+len(p)+len(h))
	buf = append(buf, 5, 4, 0x80, 0x02, 0x82, 0x83)
	buf = append(buf, 1, byte(len(u)))
	buf = append(buf, u...)
	buf = append(buf, byte(len(p)))
	buf = append(buf, p...)
	buf = append(buf, 5, 1, leigodChecksum(e.Secret), 3, byte(len(h)))
	buf = append(buf, h...)
	buf = append(buf, byte(port>>8), byte(port&0xFF))
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	reply := make([]byte, 6)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return fmt.Errorf("握手 EOF: %v", err)
	}
	if reply[0] != 5 || reply[1] != 2 {
		return fmt.Errorf("方法协商失败: % x", reply)
	}
	if reply[2] != 1 || reply[3] != 0 {
		return fmt.Errorf("认证失败: % x", reply)
	}
	if reply[5] != 1 {
		return fmt.Errorf("CONNECT 被拒 REP=0x%02x", reply[5])
	}
	// 应答不止 6 字节：服务端还会跟一段定长尾巴（实测 8 字节）。留着不读，
	// 它会被当成隧道里的第一段数据，TLS 就会报 "first record does not look
	// like a TLS handshake"。这里用短超时把它读干净。
	conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	drain := make([]byte, 64)
	for {
		if _, err := conn.Read(drain); err != nil {
			break
		}
	}
	conn.SetDeadline(time.Time{})
	return nil
}

// xorConn 只在上行方向做逐字节 XOR，下行是明文。
type xorConn struct {
	net.Conn
	key byte
}

func (c *xorConn) Write(p []byte) (int, error) {
	buf := make([]byte, len(p))
	for i := range p {
		buf[i] = p[i] ^ c.key
	}
	if _, err := c.Conn.Write(buf); err != nil {
		return 0, err
	}
	return len(p), nil
}
