// Package pool 是代理池本体：五池管理 + 带 bun 指纹的进程内探测。
//
// 与 2api 共用 internal/httpx，所以"门槛探到的"和"2api 真发出去的"是同一条
// 请求形状，不存在两套实现漂移的问题。
package pool

import (
	"fmt"
	"net/url"
	"time"
)

// Kind 区分出口来源：外部渠道（8888 面板，标准 SOCKS5 认证）与雷神私有协议。
type Kind string

const (
	KindExt    Kind = "ext"
	KindLeigod Kind = "leigod"
	KindUU     Kind = "uu"
)

// Exit 是一个出口。ip 用面板给的 hostname 或数字 IP 均可。
type Exit struct {
	IP       string    `json:"ip"`
	Port     int       `json:"port"`
	Kind     Kind      `json:"kind"`
	User     string    `json:"user,omitempty"`
	Secret   string    `json:"secret,omitempty"`
	Channel  string    `json:"channel,omitempty"`
	ExpireAt int64     `json:"expire_at,omitempty"`
	Born     time.Time `json:"born"`
}

// Token 是导出给下游的标识，格式与 Python 版一致：e-<ip>-<port>。
func (e *Exit) Token() string {
	return fmt.Sprintf("%s-%s-%d", tokenPrefix(e.Kind), e.IP, e.Port)
}

func tokenPrefix(k Kind) string {
	if k == KindLeigod {
		return "l"
	}
	return "e"
}

// Addr 是出口的 host:port。
func (e *Exit) Addr() string { return fmt.Sprintf("%s:%d", e.IP, e.Port) }

// ProxyURL 供 internal/httpx 直接当上游代理用。
func (e *Exit) ProxyURL() string {
	u := url.URL{Scheme: "socks5", Host: e.Addr()}
	if e.User != "" {
		u.User = url.UserPassword(e.User, e.Secret)
	}
	return u.String()
}

// Expired 报告面板上标注的到期时刻是否已过。
func (e *Exit) Expired(now time.Time) bool {
	return e.ExpireAt > 0 && e.ExpireAt <= now.Unix()
}

// Clone 返回副本，避免把内部指针交给外部。
func (e *Exit) Clone() *Exit {
	if e == nil {
		return nil
	}
	c := *e
	return &c
}
