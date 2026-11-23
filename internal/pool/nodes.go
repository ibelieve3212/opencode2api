package pool

// nodes.txt 导出：格式与 Python 版完全一致
//   http://<token>:x@<host>:<port>
// 2api 的 proxyfile 可以直接吃这个文件。

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// WriteNodesFile 原子写一次 nodes.txt，返回写入条目数。
func (p *Pool) WriteNodesFile(path, host string, port int) (int, error) {
	tokens := p.Tokens()
	var sb strings.Builder
	for _, t := range tokens {
		fmt.Fprintf(&sb, "http://%s:x@%s:%d\n", t, host, port)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(sb.String()), 0o644); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return 0, err
	}
	return len(tokens), nil
}
