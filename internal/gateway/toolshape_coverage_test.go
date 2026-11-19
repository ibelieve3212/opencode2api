package gateway

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ibelieve3212/opencode2api/internal/protocol"
)

// 各客户端真实声明的工具名。来源见 OfficialToolCandidates 的注释表。
var clientToolSets = map[string][]string{
	"pi":        {"bash", "read", "write", "edit", "glob", "grep"},
	"claude":    {"Read", "Write", "Edit", "MultiEdit", "Bash", "Glob", "Grep", "WebFetch", "WebSearch", "TodoWrite", "Task", "AskUserQuestion", "Skill"},
	"cursor":    {"read_file", "edit_file", "run_terminal_cmd", "list_dir", "grep_search", "file_search", "codebase_search", "reapply"},
	"cline":     {"read_file", "write_to_file", "replace_in_file", "execute_command", "list_files", "search_files", "ask_followup_question", "new_task", "insert_content", "search_and_replace"},
	"gemini":    {"read_file", "write_file", "replace", "glob", "search_file_content", "list_directory", "read_many_files", "run_shell_command", "web_fetch", "google_web_search", "write_todos"},
	"codex":     {"shell", "apply_patch", "update_plan", "web_search", "read_file"},
	"zed":       {"read_file", "edit_file", "create_file", "list_directory", "find_path", "grep", "run_command", "fetch", "web_search", "spawn_agent", "update_plan"},
	"goose":     {"shell", "read_file", "write_file", "list_directory", "search", "web_fetch"},
	"openhands": {"execute_bash", "execute_ipython_cell", "browse", "finish"},
	"sweagent":  {"bash", "search_dir", "search_file", "find_file", "open", "edit", "create"},
	"crush":     {"bash", "edit", "view", "write", "fetch", "glob", "grep", "ls"},
	"warp":      {"run_command", "read_files", "edit_file", "search_codebase", "grep"},
	"copilot":   {"read_file", "edit_file", "run_in_terminal", "list_dir", "file_search", "grep_search", "fetch_webpage", "semantic_search"},
	"amazonq":   {"fs_read", "fs_write", "execute_bash"},
	"amp":       {"Bash", "Read", "Edit", "Write", "Glob", "Grep", "Task", "WebFetch", "WebSearch", "TodoWrite"},
	"dsh":       {"read", "read_image", "write", "edit", "glob", "grep", "pwsh", "web_fetch", "todo_write", "subagent", "skill", "ask_user_question", "present", "create_goal", "get_goal", "update_goal", "list_agents", "send_message", "wait_agent", "interrupt_agent", "job_output", "job_kill", "job_list", "team_task_create", "schedule_create", "ralph", "workflow", "working_directory", "load_workspace_dependencies"},
}

func shapeFor(t *testing.T, names []string, v1 bool) (map[string]string, []string) {
	t.Helper()
	list := make([]any, 0, len(names))
	for _, n := range names {
		list = append(list, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        n,
				"description": "client " + n,
				"parameters":  map[string]any{"type": "object"},
			},
		})
	}
	payload := map[string]any{"tools": list}
	renames, ok := renameAndCompleteTools(payload, protocol.Chat, v1, "")
	if !ok {
		t.Fatalf("shaping declined")
	}
	shaped := payload["tools"].([]any)
	wireNames := make([]string, 0, len(shaped))
	for _, raw := range shaped {
		e := raw.(map[string]any)
		fn := e["function"].(map[string]any)
		wireNames = append(wireNames, fn["name"].(string))
	}
	seen := map[string]int{}
	for _, n := range wireNames {
		seen[n]++
	}
	dups := []string{}
	for n, c := range seen {
		if c > 1 {
			dups = append(dups, fmt.Sprintf("%s x%d", n, c))
		}
	}
	if len(dups) > 0 {
		t.Errorf("duplicate wire names for %v: %v", names, dups)
	}
	// 客户端的工具定义按顺序排在最前面，因此 wireNames[i] 就是 names[i] 在线上真正的名字。
	// 用真实输出判定映射，而不是依赖 rename 表里是否留了记录（恒等映射本就不记录）。
	back := map[string]string{}
	for i, decl := range names {
		if i >= len(wireNames) {
			break
		}
		back[decl] = wireNames[i]
	}
	if renames != nil {
		for decl, wire := range back {
			if strings.HasPrefix(wire, "mcp_") {
				continue
			}
			if got, ok := renames.Client(wire); ok && got != decl {
				t.Errorf("wire %q restored %q, want %q", wire, got, decl)
			}
		}
	}
	return back, wireNames
}

func TestAliasTableSize(t *testing.T) {
	total := 0
	for _, name := range allOfficialToolNames() {
		total += len(OfficialToolCandidates(name))
	}
	fmt.Printf("\n=== 别名表 ===\n  官方能力(两代并集)=%d  别名条目(含重复)=%d  去重后索引=%d\n",
		len(allOfficialToolNames()), total, len(toolAliasIndex))
	if len(toolAliasIndex) < 200 {
		t.Errorf("index too small: %d", len(toolAliasIndex))
	}
}

func TestCrossCapabilityConflicts(t *testing.T) {
	owner := map[string]string{}
	conflicts := map[string][]string{}
	for _, name := range allOfficialToolNames() {
		capability := capabilityOf(name)
		for _, alias := range OfficialToolCandidates(name) {
			folded := foldToolForMatch(alias)
			if folded == "" {
				continue
			}
			if prior, ok := owner[folded]; ok && prior != capability {
				if _, seen := conflicts[folded]; !seen {
					conflicts[folded] = []string{prior}
				}
				conflicts[folded] = append(conflicts[folded], capability)
				continue
			}
			owner[folded] = capability
		}
	}
	keys := make([]string, 0, len(conflicts))
	for k := range conflicts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf("\n=== 跨能力冲突（已从索引剔除，客户端保留自有名）: %d ===\n", len(keys))
	for _, k := range keys {
		caps := conflicts[k]
		// 去重展示
		uniq := []string{}
		seen := map[string]bool{}
		for _, c := range caps {
			if !seen[c] {
				seen[c] = true
				uniq = append(uniq, c)
			}
		}
		dropped := "kept:" + owner[k]
		if _, inIndex := toolAliasIndex[k]; !inIndex {
			dropped = "dropped"
		}
		fmt.Printf("  %-24s %v  ->  %s\n", k, uniq, dropped)
	}
}

func TestCoverageAllClients(t *testing.T) {
	for _, v1 := range []bool{false, true} {
		lane := "v2"
		if v1 {
			lane = "v1"
		}
		fmt.Printf("\n########## lane %s ##########\n", lane)
		keys := make([]string, 0, len(clientToolSets))
		for k := range clientToolSets {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, client := range keys {
			names := clientToolSets[client]
			back, wireNames := shapeFor(t, names, v1)
			opaque := []string{}
			for _, n := range wireNames {
				if strings.HasPrefix(n, "mcp_") {
					opaque = append(opaque, n)
				}
			}
			fmt.Printf("  %-9s declared=%2d mapped=%2d opaque=%2d wire=%2d\n",
				client, len(names), len(back), len(opaque), len(wireNames))
			if len(opaque) > 0 {
				// 打印哪个客户端工具落到了 opaque
				declaredOpaque := []string{}
				for _, decl := range names {
					if _, ok := back[decl]; !ok {
						declaredOpaque = append(declaredOpaque, decl)
					}
				}
				fmt.Printf("            未绑定: %s\n", strings.Join(declaredOpaque, ", "))
			}
		}
	}
}

func TestPiAndDshExact(t *testing.T) {
	// pi 与 dsh 是实际在用的两个客户端，逐一断言它们的映射结果。
	want := map[string]map[string]string{
		"pi": {
			"bash": "shell", "read": "read", "write": "write",
			"edit": "edit", "glob": "glob", "grep": "grep",
		},
		"dsh": {
			"pwsh": "shell", "read": "read", "write": "write", "edit": "edit",
			"glob": "glob", "grep": "grep", "web_fetch": "webfetch",
			"subagent": "subagent", "skill": "skill", "ask_user_question": "question",
		},
	}
	for client, expect := range want {
		back, _ := shapeFor(t, clientToolSets[client], false)
		for decl, wireWant := range expect {
			wire, ok := back[decl]
			if !ok {
				t.Errorf("%s: %q was not mapped (want wire %q)", client, decl, wireWant)
				continue
			}
			if wire != wireWant {
				t.Errorf("%s: %q -> %q, want %q", client, decl, wire, wireWant)
			}
		}
	}
	fmt.Printf("\n=== pi / dsh 精确映射断言通过 ===\n")
}
