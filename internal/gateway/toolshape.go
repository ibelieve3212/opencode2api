package gateway

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/ibelieve3212/opencode2api/internal/jsonutil"
	wire "github.com/ibelieve3212/opencode2api/internal/protocol"
)

// The free tier only accepts agent-shaped traffic, and each lane validates the
// tool definitions against its own CLI generation. These assets are the
// genuine definitions captured from the official clients: opencode_tools.json
// for the 2.x lanes (/inference and /zen/go) and opencode_tools_v1.json for the
// 1.x lane (/zen).
//
//go:embed opencode_tools.json
var opencodeToolsV2JSON []byte

//go:embed opencode_tools_v1.json
var opencodeToolsV1JSON []byte

// opencodeToolDefinition is the description and schema of one official tool.
type opencodeToolDefinition struct {
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

// v2OfficialToolOrder is the key order of opencode_tools.json, made explicit
// because Go maps do not preserve it. The order and the set come from a 2.3.1
// capture: 2.3.1 carries experimental_image_generate and no longer carries
// patch, which an earlier 2.3.0 build still did.
var v2OfficialToolOrder = []string{
	"edit", "execute", "experimental_image_generate", "glob", "grep",
	"question", "read", "shell", "skill", "subagent", "webfetch",
	"websearch", "write",
}

var (
	opencodeToolsOnce    sync.Once
	opencodeToolsV2      map[string]opencodeToolDefinition
	opencodeToolsV1      map[string]opencodeToolDefinition
	opencodeToolsV1Order []string
)

// loadOfficialTools decodes both embedded tool sets once. A file that fails to
// decode leaves that generation with no known definitions, which only degrades
// the definition back to a placeholder; the tool names are still appended.
func loadOfficialTools() {
	opencodeToolsOnce.Do(func() {
		opencodeToolsV2 = map[string]opencodeToolDefinition{}
		if err := json.Unmarshal(opencodeToolsV2JSON, &opencodeToolsV2); err != nil {
			opencodeToolsV2 = map[string]opencodeToolDefinition{}
		}
		refreshVolatileDescriptions(opencodeToolsV2)
		opencodeToolsV1 = map[string]opencodeToolDefinition{}
		opencodeToolsV1Order = nil
		var entries []struct {
			Function struct {
				Name        string         `json:"name"`
				Description string         `json:"description"`
				Parameters  map[string]any `json:"parameters"`
			} `json:"function"`
		}
		if err := json.Unmarshal(opencodeToolsV1JSON, &entries); err == nil {
			for _, entry := range entries {
				name := entry.Function.Name
				if name == "" {
					continue
				}
				opencodeToolsV1[name] = opencodeToolDefinition{
					Description: entry.Function.Description,
					Parameters:  entry.Function.Parameters,
				}
				opencodeToolsV1Order = append(opencodeToolsV1Order, name)
			}
		}
	})
}

// refreshVolatileDescriptions rewrites the parts of a captured description that the client
// computes at request time, so the asset can stay a faithful capture of one build without the
// value going stale in the code.
//
// The websearch description carries the current year — the client renders it from
// `${new Date().getFullYear()}` — and is the only tool description with a volatile part. Every
// other description in either asset is a fixed literal.
func refreshVolatileDescriptions(definitions map[string]opencodeToolDefinition) {
	definition, ok := definitions["websearch"]
	if !ok || definition.Description == "" {
		return
	}
	year := regexp.MustCompile(`The current year is \d{4}\.`)
	if !year.MatchString(definition.Description) {
		return
	}
	definition.Description = year.ReplaceAllString(
		definition.Description,
		fmt.Sprintf("The current year is %d.", time.Now().Year()),
	)
	definitions["websearch"] = definition
}

// officialToolNames returns the names of the official set for the lane.
func officialToolNames(v1 bool) []string {
	loadOfficialTools()
	if v1 {
		return opencodeToolsV1Order
	}
	return v2OfficialToolOrder
}

// OfficialToolCandidates returns every client spelling this gateway recognizes for one official
// capability. The table covers the mainstream agent families field by field, because a gateway
// that knows only one naming convention works for one client: a request declaring read_file,
// run_command and search_files used to match nothing, so the built-ins stayed unbound, the model
// called read or shell, and the client received a tool call it had never declared. That is the
// difference between an agent that works through this gateway and one that errors on every tool
// turn.
//
// Provenance of each row, so the table stays maintainable instead of accreting guesses:
//
//	Claude Code      Read Write Edit MultiEdit NotebookEdit Bash Glob Grep LS WebFetch WebSearch
//	                 Task TodoWrite AskUserQuestion Skill
//	Cursor           read_file edit_file run_terminal_cmd list_dir grep_search file_search
//	                 codebase_search search_symbols reapply
//	Cline/Roo/Kilo   read_file write_to_file replace_in_file execute_command list_files
//	                 search_files ask_followup_question new_task insert_content search_and_replace
//	Gemini/Qwen Code read_file write_file replace glob search_file_content list_directory
//	                 read_many_files run_shell_command web_fetch google_web_search write_todos
//	Codex            shell apply_patch update_plan web_search read_file view_image
//	Zed              read_file edit_file create_file list_directory find_path grep run_command
//	                 fetch web_search spawn_agent update_plan
//	Goose            shell read_file write_file str_replace list_directory search web_fetch
//	OpenHands        execute_bash execute_ipython_cell str_replace_editor browse finish
//	SWE-agent        bash search_dir search_file find_file open edit create
//	Crush            bash edit view write fetch glob grep ls
//	Warp             run_command read_files edit_file search_codebase grep
//	VS Code Copilot  read_file edit_file run_in_terminal list_dir file_search grep_search
//	                 fetch_webpage semantic_search
//	Amazon Q         fs_read fs_write execute_bash
//	Devin            shell editor browse search fetch
//	Amp              Bash Read Edit Write Glob Grep Task WebFetch WebSearch TodoWrite
//	dsh              pwsh read read_image write edit glob grep web_fetch todo_write subagent
//	                 skill ask_user_question
//	pi               bash read write edit glob grep
//
// Matching is case-insensitive and shape-folded, so one row covers read_file, ReadFile and
// read-file. A spelling two capabilities both claim is dropped by toolAliasIndex rather than
// resolved by guessing.
func OfficialToolCandidates(officialName string) []string {
	switch capabilityOf(officialName) {
	case "shell":
		return []string{
			// Claude Code, Amp, Crush, SWE-agent.
			"Bash", "Shell", "bash", "shell", "sh", "zsh", "fish",
			// dsh names its command tool after the shell it runs.
			"pwsh", "powershell", "cmd", "cmd_exe",
			// Cursor, Warp, Zed, VS Code Copilot.
			"run_command", "run_terminal_cmd", "run_terminal_command", "run_in_terminal",
			// Cline, Roo, Kilo, OpenHands, Amazon Q.
			"execute_command", "execute_shell", "execute_bash", "execute_terminal_command",
			// Gemini CLI, Qwen Code.
			"run_shell_command", "run_shell", "run_bash", "run_cmd",
			// Generic function-calling agents.
			"command", "terminal", "exec", "exec_command", "shell_exec", "bash_exec",
			"shell_command", "terminal_command", "system_command", "run_terminal",
			"spawn_terminal", "open_terminal",
		}
	case "read":
		return []string{
			"Read", "read", "readfile", "read_file", "read_files", "read_many_files",
			"read_multiple_files", "read_entire_file", "read_text_file", "read_file_content",
			"read_lines", "read_document", "view", "view_file", "open", "open_file", "cat",
			"file_read", "get_file", "get_file_contents", "get_file_content", "show_file",
			"display_file", "print_file", "peek_file", "inspect_file",
			// Amazon Q.
			"fs_read",
		}
	case "write":
		return []string{
			"Write", "write", "writefile", "write_file", "write_files", "write_text_file",
			"write_to_file", "create_file", "create_files", "create_new_file",
			"create_or_update_file", "create", "save_file", "save_text_file", "put_file", "new_file",
			"output_file", "file_write", "add_file", "emit_file",
			// Amazon Q.
			"fs_write",
		}
	case "edit":
		return []string{
			"Edit", "edit", "editfile", "edit_file", "edit_files", "edit_lines",
			"edit_file_content", "multiedit", "multi_edit", "str_replace", "string_replace",
			"str_replace_editor", "replace_string", "replace_text", "replace_in_file",
			"replace_in_file_content", "modify_file", "update_file", "change_file",
			"apply_patch", "apply_diff", "apply_edit", "insert_content", "search_and_replace",
			"replace",
			"reapply", "refactor_file", "rewrite_file",
		}
	case "glob":
		return []string{
			"Glob", "glob", "glob_files", "list_dir", "listdir", "list_files",
			"list_directory", "list_folder", "list_paths", "list_tree", "ls", "dir",
			"find_files", "find_file", "find_path", "file_search", "directory_tree",
			"dir_tree", "show_directory", "browse_directory", "scan_directory",
			"enumerate_files", "directory_listing", "list_repository_files", "search_dir",
		}
	case "grep":
		return []string{
			"Grep", "grep", "rg", "ripgrep", "grep_search", "search", "search_code",
			"code_search", "codebase_search", "search_codebase", "semantic_search",
			"search_files", "search_file_content", "search_in_files", "search_repository",
			"search_workspace", "search_text", "search_pattern",
			"find_in_files", "find_string", "find_text", "find_all", "find_references",
			"search_definitions",
		}
	case "question":
		return []string{
			"AskUserQuestion", "question", "ask", "askuser", "ask_user",
			"ask_user_question", "ask_followup_question", "ask_human", "ask_user_input",
			"ask_user_for_input", "ask_question", "prompt_user", "request_user_input",
			"request_clarification", "user_input", "user_question", "clarify",
		}
	case "skill":
		return []string{
			"Skill", "skill", "skills", "load_skill", "use_skill", "get_skill",
			"invoke_skill", "skill_load", "apply_skill", "read_skill",
		}
	case "subagent":
		return []string{
			"Agent", "Task", "Subagent", "subagent", "sub_agent", "task", "sub_task",
			"subtask", "spawn_agent", "spawn_subagent", "delegate", "delegate_task",
			"run_agent", "run_task", "invoke_agent", "dispatch_agent", "launch_agent",
			"create_agent", "new_task", "agent_task", "parallel_agents",
		}
	case "todowrite":
		return []string{
			"TodoWrite", "TodoWriteList", "todo_write", "todowrite", "todo", "todos",
			"todo_list", "todo_update", "update_todo", "update_todos", "write_todos",
			"write_todo", "task_write", "task_list", "plan", "plans", "update_plan",
			"create_plan", "edit_plan", "manage_todos", "set_todos", "track_tasks",
			"checklist_write",
		}
	case "webfetch":
		return []string{
			"WebFetch", "webfetch", "web_fetch", "fetch", "fetch_url", "fetch_page",
			"fetch_webpage", "fetch_content", "get_url", "get_webpage", "url_fetch",
			"open_url", "visit_url", "read_url", "retrieve_url", "browse_url",
			"http_fetch", "load_url", "download_webpage", "web_get", "browse",
			"visit_webpage", "browser_use", "browse_webpage", "goto_url", "web_scrape",
			"scrape_url", "open_webpage", "read_webpage",
		}
	case "websearch":
		return []string{
			"WebSearch", "websearch", "web_search", "search_web", "search_the_web",
			"google_search", "google_web_search", "internet_search", "search_internet",
			"online_search", "search_online", "web_query", "web_lookup", "search_engine",
			// Search integrations MCP servers expose under their own product names.
			"brave_search", "tavily_search", "exa_search", "serper_search",
			"duckduckgo_search", "bing_search", "searxng_search", "kagi_search",
		}
	case "execute":
		return []string{
			"execute", "run_code", "code_execute", "execute_code", "eval",
			"evaluate_code", "python_exec", "python_execute", "execute_python",
			"run_python", "python", "ipython", "repl", "interpreter", "code_interpreter",
			"javascript_exec", "execute_javascript", "run_javascript", "node_exec",
			"js_exec", "run_node", "execute_script", "run_script", "sandbox_exec",
			"python_interpreter", "python_repl", "execute_python_code", "run_python_code",
			"generate_and_execute_code", "execute_ipython_cell", "execute_python_cell",
			"notebook_execute",
		}
	case "experimental_image_generate":
		return []string{
			"experimental_image_generate", "generate_image", "image_generate", "image_gen",
			"create_image", "make_image", "draw", "draw_image", "generate_picture",
			"render_image", "image_generation", "generate_visual", "picture_generate",
		}

	default:
		return nil
	}
}

// capabilityOf groups the official names that are one capability under a different CLI
// generation. The 2.x line renamed bash to shell and task to subagent, so matching has to treat a
// renamed pair as one thing or a client's bash cannot bind the 2.x shell.
func capabilityOf(officialName string) string {
	switch strings.ToLower(officialName) {
	case "bash", "shell":
		return "shell"
	case "task", "subagent":
		return "subagent"
	default:
		return strings.ToLower(officialName)
	}
}

// allOfficialToolNames is the union of both lanes' official names, in a stable order.
func allOfficialToolNames() []string {
	seen := map[string]bool{}
	all := make([]string, 0, 16)
	for _, name := range append(append([]string{}, officialToolNames(false)...), officialToolNames(true)...) {
		if seen[name] {
			continue
		}
		seen[name] = true
		all = append(all, name)
	}
	return all
}

// toolAliasIndex folds every recognized client spelling to the one capability that owns it.
//
// Two capabilities claiming one spelling would make the binding a coin flip, which is how
// search_files once landed on glob for one agent and stayed opaque for another. Such a spelling is
// dropped from the index instead: the client keeps its own wire name, which still dispatches to a
// handler it actually has. An official name always means its own capability and outranks an alias.
var toolAliasIndex = buildToolAliasIndex()

func buildToolAliasIndex() map[string]string {
	names := allOfficialToolNames()
	index := make(map[string]string, 320)
	reserved := make(map[string]bool, len(names))
	for _, name := range names {
		folded := foldToolForMatch(name)
		index[folded] = capabilityOf(name)
		reserved[folded] = true
	}
	conflicted := map[string]bool{}
	for _, name := range names {
		capability := capabilityOf(name)
		for _, alias := range OfficialToolCandidates(name) {
			folded := foldToolForMatch(alias)
			if folded == "" || conflicted[folded] || reserved[folded] {
				continue
			}
			if existing, taken := index[folded]; taken && existing != capability {
				delete(index, folded)
				conflicted[folded] = true
				continue
			}
			index[folded] = capability
		}
	}
	return index
}

// officialToolDefinition returns the captured definition for an official name.
func officialToolDefinition(name string, v1 bool) (opencodeToolDefinition, bool) {
	loadOfficialTools()
	if v1 {
		definition, ok := opencodeToolsV1[name]
		return definition, ok
	}
	definition, ok := opencodeToolsV2[name]
	return definition, ok
}

// strictToolSchema reports whether the 2.x Chat shape marks every tool strict:false.
//
// It does not, so this is false and the field is omitted. Evidence: the installed
// 2.3.0 release binary carries no "strict" token anywhere near its tool-definition
// code (every hit is a TypeScript config template, a Bun internal, or a linter
// message), and the client's own tool contract says a definition is a description
// plus parameters — serialized by the protocol layer, where `strict` is optional and
// nothing sets it (specs/v2/tools.md, "Tools"). An earlier comment here claimed the
// client sent the field; that claim traced back to a proxy's own renderer rather
// than to the client. Flip this only if the upstream starts rejecting definitions
// that omit it.
const strictToolSchema = false

// officialTool renders one official definition in the wire shape the protocol
// expects. The captured 1.x entries carry no strict field and are rendered verbatim;
// the 2.x Chat shape omits it too, for the reason above.
func officialTool(protocol wire.Protocol, name string, v1 bool, platform string) map[string]any {
	description := "Agent tool " + name
	parameters := map[string]any{"type": "object", "properties": map[string]any{}}
	if definition, ok := officialToolDefinition(name, v1); ok {
		description = definition.Description
		if definition.Parameters != nil {
			parameters = definition.Parameters
		}
	}
	if v1 {
		if name == "bash" {
			description = v1BashDescription(description, platform)
		}
	} else if name == "shell" {
		description = v2ShellDescription(description, platform)
	}
	switch protocol {
	case wire.Chat:
		function := map[string]any{
			"name":        name,
			"description": description,
			"parameters":  parameters,
		}
		if !v1 && strictToolSchema {
			function["strict"] = false
		}
		return map[string]any{"type": "function", "function": function}
	case wire.Anthropic:
		return map[string]any{
			"name":         name,
			"description":  description,
			"input_schema": parameters,
		}
	default:
		return map[string]any{
			"type":        "function",
			"name":        name,
			"description": description,
			"parameters":  parameters,
		}
	}
}

// v2ShellDescription renders the configured platform's shell description,
// retaining the host-based behavior when no platform override is supplied.
func v2ShellDescription(base, platform string) string {
	if platform == "" {
		platform = runtime.GOOS
		if platform != "windows" {
			return base
		}
	}
	switch platform {
	case "win32", "windows":
		return strings.Replace(base, "on Linux using bash", "on Windows using powershell", 1)
	case "darwin":
		return strings.Replace(base, "on Linux using bash", "on macOS using bash", 1)
	default:
		return base
	}
}

// v1BashDescription applies the same platform rewrite to the 1.x bash
// definition, which states its OS and shell inline.
func v1BashDescription(base, platform string) string {
	if platform == "" {
		platform = runtime.GOOS
		if platform != "windows" {
			return base
		}
	}
	switch platform {
	case "win32", "windows":
		return strings.Replace(base, "OS: linux, Shell: bash", "OS: windows, Shell: powershell", 1)
	case "darwin":
		return strings.Replace(base, "OS: linux, Shell: bash", "OS: darwin, Shell: bash", 1)
	default:
		return base
	}
}

// renameAndCompleteTools makes the tool list read as an official CLI session:
// every named client tool is renamed to an opaque wire name, legacy functions
// arrays are converted, and the full official set for the lane is appended.
// The returned map restores the client names in the upstream response.
func renameAndCompleteTools(payload map[string]any, protocol wire.Protocol, v1 bool, platform string) (*wire.ToolRenames, bool) {
	switch protocol {
	case wire.Chat, wire.Responses, wire.Anthropic:
	default:
		return nil, false
	}
	renames := wire.NewToolRenames()
	used := map[string]bool{}
	var items []any
	if raw, exists := payload["tools"]; exists {
		if list, ok := raw.([]any); ok {
			items = list
		}
	}
	declaredNames := make([]string, 0, len(items))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if name, ok := renamableToolName(protocol, entry); ok {
			declaredNames = append(declaredNames, name)
		}
	}
	if protocol == wire.Chat || protocol == wire.Responses {
		if legacy, ok := payload["functions"].([]any); ok {
			for _, rawEntry := range legacy {
				entry, ok := rawEntry.(map[string]any)
				if ok {
					if name := jsonutil.StringAt(entry, "name"); name != "" {
						declaredNames = append(declaredNames, name)
					}
				}
			}
			items = append(items, convertLegacyFunctions(legacy, protocol)...)
			delete(payload, "functions")
		}
	}
	officialNames := officialToolNames(v1)
	// A client tool that means the same thing as a built-in takes the built-in's name rather than an
	// opaque one. Sending both is what broke agents: the request carried mcp_ab12cd34 plus the
	// legible built-in read, the model reliably picked read, and the client got back a tool call it
	// had never declared. One name per capability means the model has no wrong choice to make, and
	// the response maps straight back to the client's own name.
	standIn := make(map[string]string, len(officialNames))  // official name -> client tool standing in
	byClient := make(map[string]string, len(declaredNames)) // client tool -> official name it takes
	bind := func(accept func(official, declared string) bool) {
		for _, official := range officialNames {
			if _, done := standIn[official]; done {
				continue
			}
			for _, declared := range declaredNames {
				if _, taken := byClient[declared]; taken {
					continue
				}
				if !accept(official, declared) {
					continue
				}
				standIn[official] = declared
				byClient[declared] = official
				break
			}
		}
	}
	// A client's own spelling of a built-in outranks an alias, so a request declaring both read and
	// read_file keeps read_file on its own name instead of taking the name read already holds.
	bind(func(official, declared string) bool {
		return foldToolForMatch(declared) == foldToolForMatch(official)
	})
	bind(toolNamesMatch)
	shaped := make([]any, 0, len(items)+len(officialNames))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			shaped = append(shaped, item)
			continue
		}
		name, ok := renamableToolName(protocol, entry)
		if !ok {
			shaped = append(shaped, item)
			continue
		}
		if official, ok := byClient[name]; ok {
			// The whole definition has to become the official one, not just the name. The free
			// tier validates the argument schema too: a definition that carries another client's
			// property spellings is refused exactly like one that carries its tool names, and the
			// refusal is the same 429 the quota returns. The model therefore answers in the
			// official schema, and the response walk re-keys the arguments back to the spellings
			// the client's own handler accepts.
			declaredParameters := declaredToolParameters(protocol, entry)
			argumentRename := toolArgumentRename(official, declaredParameters, v1)
			shaped = append(shaped, officialTool(protocol, official, v1, platform))
			renames.Add(name, official)
			renames.BindOfficial(official, name)
			renames.BindArgs(official, argumentRename)
			renames.BindRequired(official, toolRequiredBackfill(official, declaredParameters, v1, argumentRename))
			continue
		}
		if wireName := clientWireName(renames, used, name); wireName != name {
			setToolName(protocol, entry, wireName)
		}
		shaped = append(shaped, entry)
	}
	// 官方工具集必须补齐：少一个，上游就按伪造的请求回同一个 429，和额度耗尽无法区分。
	// 客户端派发不了的那几个，由提示词里的 # Tool routing 段点名，让模型别去调。
	for _, official := range officialNames {
		if _, covered := standIn[official]; covered {
			continue
		}
		shaped = append(shaped, officialTool(protocol, official, v1, platform))
	}
	payload["tools"] = shaped
	declared := make(map[string]string, len(declaredNames))
	for _, name := range declaredNames {
		if wireName, ok := renames.Wire(name); ok {
			declared[name] = wireName
			continue
		}
		// A tool already spelled the built-in's way is not a rename, so the rename table has no
		// entry for it. The history walk still needs the mapping: without it the call is treated as
		// unknown and dropped, and a multi-step turn loses the tool it just ran.
		if official, ok := byClient[name]; ok {
			declared[name] = official
		}
	}
	sanitizeToolHistory(payload, protocol, declared, renames, officialToolNameSet(v1))
	rewriteToolReferences(payload, protocol, renames)
	return renames, true
}

// toolNamesMatch reports whether a declared client tool names the same capability as an official
// tool. The lookup goes through toolAliasIndex, which folds case, separators and punctuation, so
// read_file, ReadFile and read-file all reach one row, and a spelling two capabilities claim maps
// to neither rather than to whichever row happened to be checked first.
func toolNamesMatch(official, declared string) bool {
	folded := foldToolForMatch(declared)
	if folded == "" {
		return false
	}
	if folded == foldToolForMatch(official) {
		return true
	}
	owner, ok := toolAliasIndex[folded]
	return ok && owner == capabilityOf(official)
}

// foldToolForMatch lowercases a tool name and drops every character that is not a letter or digit.
func foldToolForMatch(name string) string {
	var builder strings.Builder
	for _, char := range strings.ToLower(name) {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') {
			builder.WriteRune(char)
		}
	}
	return builder.String()
}

// payloadHasTool reports whether the request declares the named tool, so the prompt
// only describes tools the model will actually see.
func payloadHasTool(payload map[string]any, protocol wire.Protocol, name string) bool {
	items, _ := payload["tools"].([]any)
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if got, ok := renamableToolName(protocol, entry); ok && got == name {
			return true
		}
	}
	return false
}

// unaliasedOfficialTools lists the official tools that were appended to the request but that no
// client tool was bound to. The model can see them and may call them, yet the client never
// declared anything by that name and cannot dispatch the call, so the prompt names them as
// unavailable. A lane whose client already declares every counterpart gets an empty list and no
// extra prompt text at all.
func unaliasedOfficialTools(renames *wire.ToolRenames, v1 bool) []string {
	official := officialToolNames(v1)
	if renames == nil {
		// No tools were shaped (a title request), so nothing was appended either.
		return nil
	}
	var missing []string
	for _, name := range official {
		if _, bound := renames.Official(name); !bound {
			missing = append(missing, name)
		}
	}
	return missing
}
func officialToolNameSet(v1 bool) map[string]bool {
	set := make(map[string]bool)
	for _, name := range officialToolNames(v1) {
		set[name] = true
	}
	return set
}

func sanitizeToolHistory(payload map[string]any, protocol wire.Protocol, declared map[string]string, renames *wire.ToolRenames, laneOfficial map[string]bool) {
	dropped := map[string]bool{}
	switch protocol {
	case wire.Anthropic:
		messages := jsonutil.SliceAt(payload, "messages")
		for _, rawMessage := range messages {
			message, _ := rawMessage.(map[string]any)
			for _, rawBlock := range jsonutil.SliceAt(message, "content") {
				block, _ := rawBlock.(map[string]any)
				if jsonutil.StringAt(block, "type") != "tool_use" {
					continue
				}
				name := jsonutil.StringAt(block, "name")
				if wireName, ok := historyToolWireName(name, declared, renames, laneOfficial); ok {
					block["name"] = wireName
				} else if id := jsonutil.StringAt(block, "id"); id != "" {
					dropped[id] = true
				}
			}
		}
		keptMessages := make([]any, 0, len(messages))
		for _, rawMessage := range messages {
			message, _ := rawMessage.(map[string]any)
			content, ok := message["content"].([]any)
			if !ok {
				keptMessages = append(keptMessages, rawMessage)
				continue
			}
			keptContent := make([]any, 0, len(content))
			for _, rawBlock := range content {
				block, _ := rawBlock.(map[string]any)
				switch jsonutil.StringAt(block, "type") {
				case "tool_use":
					if _, ok := historyToolWireName(jsonutil.StringAt(block, "name"), declared, renames, laneOfficial); !ok {
						continue
					}
				case "tool_result":
					if dropped[jsonutil.StringAt(block, "tool_use_id")] {
						continue
					}
				}
				keptContent = append(keptContent, rawBlock)
			}
			if len(keptContent) == 0 {
				continue
			}
			message["content"] = keptContent
			keptMessages = append(keptMessages, rawMessage)
		}
		payload["messages"] = keptMessages
	case wire.Chat:
		messages := jsonutil.SliceAt(payload, "messages")
		dropEmptyMessages := make(map[int]bool)
		for index, rawMessage := range messages {
			message, _ := rawMessage.(map[string]any)
			calls, ok := message["tool_calls"].([]any)
			if !ok {
				continue
			}
			keptCalls := make([]any, 0, len(calls))
			for _, rawCall := range calls {
				call, _ := rawCall.(map[string]any)
				function, _ := call["function"].(map[string]any)
				name := jsonutil.StringAt(function, "name")
				if wireName, ok := historyToolWireName(name, declared, renames, laneOfficial); ok {
					function["name"] = wireName
					keptCalls = append(keptCalls, rawCall)
				} else if id := jsonutil.StringAt(call, "id"); id != "" {
					dropped[id] = true
				}
			}
			if len(keptCalls) == 0 {
				delete(message, "tool_calls")
				dropEmptyMessages[index] = len(calls) > 0
			} else {
				message["tool_calls"] = keptCalls
			}
		}
		keptMessages := make([]any, 0, len(messages))
		for index, rawMessage := range messages {
			message, _ := rawMessage.(map[string]any)
			if jsonutil.StringAt(message, "role") == "tool" && dropped[jsonutil.StringAt(message, "tool_call_id")] {
				continue
			}
			if dropEmptyMessages[index] && !chatMessageHasContent(message) {
				continue
			}
			keptMessages = append(keptMessages, rawMessage)
		}
		payload["messages"] = keptMessages
	case wire.Responses:
		items := jsonutil.SliceAt(payload, "input")
		for _, rawItem := range items {
			item, _ := rawItem.(map[string]any)
			switch jsonutil.StringAt(item, "type") {
			case "function_call", "custom_tool_call":
				name := jsonutil.StringAt(item, "name")
				if wireName, ok := historyToolWireName(name, declared, renames, laneOfficial); ok {
					item["name"] = wireName
				} else if id := jsonutil.FirstString(jsonutil.StringAt(item, "call_id"), jsonutil.StringAt(item, "id")); id != "" {
					dropped[id] = true
				}
			}
		}
		keptItems := make([]any, 0, len(items))
		for _, rawItem := range items {
			item, _ := rawItem.(map[string]any)
			if jsonutil.StringAt(item, "type") == "function_call_output" && dropped[jsonutil.StringAt(item, "call_id")] {
				continue
			}
			if kind := jsonutil.StringAt(item, "type"); kind == "function_call" || kind == "custom_tool_call" {
				if _, ok := historyToolWireName(jsonutil.StringAt(item, "name"), declared, renames, laneOfficial); !ok {
					continue
				}
			}
			keptItems = append(keptItems, rawItem)
		}
		payload["input"] = keptItems
	}
	sanitizeToolChoice(payload, protocol, declared, renames, laneOfficial)
}

func historyToolWireName(name string, declared map[string]string, renames *wire.ToolRenames, laneOfficial map[string]bool) (string, bool) {
	if wireName, ok := declared[name]; ok {
		return wireName, true
	}
	if _, ok := renames.Client(name); ok {
		return name, true
	}
	if laneOfficial[name] {
		client, ok := renames.Official(name)
		if !ok {
			client, _ = renames.Resolve(name)
		}
		if wireName, ok := declared[client]; ok {
			return wireName, true
		}
		return renames.Wire(client)
	}
	client, _ := renames.Resolve(name)
	if wireName, ok := declared[client]; ok {
		return wireName, true
	}
	return "", false
}

func chatMessageHasContent(message map[string]any) bool {
	// Every key the client's unions accept counts as content, so a message carrying only
	// reasoning (in any of its spellings) is not dropped as empty.
	for _, key := range []string{
		"content", "refusal",
		"reasoning_content", "reasoning", "reasoning_text",
		"reasoning_opaque", "reasoning_details",
	} {
		switch value := message[key].(type) {
		case string:
			if value != "" {
				return true
			}
		case []any:
			if len(value) > 0 {
				return true
			}
		case map[string]any:
			if len(value) > 0 {
				return true
			}
		case nil:
		default:
			return true
		}
	}
	return false
}

func sanitizeToolChoice(payload map[string]any, protocol wire.Protocol, declared map[string]string, renames *wire.ToolRenames, laneOfficial map[string]bool) {
	// The gate refuses a tool_choice of "none" outright. The string form is the one a
	// client actually sends for a final step, and it used to fall straight through the
	// object-form assertion below. Dropping it costs the model nothing: declaring no
	// tools already carries the same meaning. The object form with type "none" is
	// dropped for the same reason.
	if text, ok := payload["tool_choice"].(string); ok && text == "none" {
		delete(payload, "tool_choice")
		return
	}
	choice, ok := payload["tool_choice"].(map[string]any)
	if !ok {
		return
	}
	kind := jsonutil.StringAt(choice, "type")
	if kind == "none" {
		delete(payload, "tool_choice")
		return
	}
	if kind != "tool" && kind != "function" && kind != "custom" {
		return
	}
	target := choice
	if protocol == wire.Chat {
		if function, ok := choice["function"].(map[string]any); ok {
			target = function
		}
	}
	name := jsonutil.StringAt(target, "name")
	if name == "" {
		return
	}
	wireName, ok := historyToolWireName(name, declared, renames, laneOfficial)
	if !ok {
		delete(payload, "tool_choice")
		return
	}
	target["name"] = wireName
}

// renamableToolName returns the client tool name of one definition. Responses
// built-in tools (web search and similar) have no client name and are left
// untouched.
func renamableToolName(protocol wire.Protocol, entry map[string]any) (string, bool) {
	switch protocol {
	case wire.Chat:
		name := jsonutil.StringAt(entry, "function", "name")
		return name, name != ""
	case wire.Responses:
		switch jsonutil.StringAt(entry, "type") {
		case "", "function", "custom":
		default:
			return "", false
		}
		name := jsonutil.StringAt(entry, "name")
		return name, name != ""
	default:
		// Anthropic server tools (web search and similar) carry a type; a
		// plain client tool has only name/description/input_schema.
		switch jsonutil.StringAt(entry, "type") {
		case "", "custom":
		default:
			return "", false
		}
		name := jsonutil.StringAt(entry, "name")
		return name, name != ""
	}
}

// clientWireName returns the existing wire name for a client tool, or allocates
// a new one. A duplicate client definition reuses the first allocation so every
// reference keeps pointing at one name.
func clientWireName(renames *wire.ToolRenames, used map[string]bool, client string) string {
	if wireName, ok := renames.Wire(client); ok {
		return wireName
	}
	wireName := wire.ClientToolWireName(client, used)
	renames.Add(client, wireName)
	return wireName
}

// setToolName writes the wire name into one tool definition in place.
func setToolName(protocol wire.Protocol, entry map[string]any, name string) {
	if protocol == wire.Chat {
		if function, ok := entry["function"].(map[string]any); ok {
			function["name"] = name
		}
		return
	}
	entry["name"] = name
}

// convertLegacyFunctions turns the pre-tools "functions" array into entries of
// the protocol's tool shape for the shared renaming pass.
func convertLegacyFunctions(entries []any, protocol wire.Protocol) []any {
	converted := make([]any, 0, len(entries))
	for _, raw := range entries {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		name := jsonutil.StringAt(entry, "name")
		if name == "" {
			continue
		}
		description := jsonutil.StringAt(entry, "description")
		switch protocol {
		case wire.Chat:
			function := map[string]any{
				"name":        name,
				"description": description,
				"parameters":  legacyParameters(entry),
			}
			if strict, ok := entry["strict"]; ok {
				function["strict"] = strict
			}
			converted = append(converted, map[string]any{"type": "function", "function": function})
		case wire.Responses:
			tool := map[string]any{
				"type":        "function",
				"name":        name,
				"description": description,
				"parameters":  legacyParameters(entry),
			}
			if strict, ok := entry["strict"]; ok {
				tool["strict"] = strict
			}
			converted = append(converted, tool)
		}
	}
	return converted
}

// legacyParameters returns a legacy function's parameters, defaulting to an
// empty object schema.
func legacyParameters(entry map[string]any) any {
	if parameters, ok := entry["parameters"]; ok && parameters != nil {
		return parameters
	}
	return map[string]any{"type": "object", "properties": map[string]any{}}
}

// rewriteToolReferences renames every outbound reference to a client tool so
// the request stays self-consistent: named tool_choice (including legacy Chat
// and Responses allowed_tools forms), legacy function_call, and the tool-call
// history each protocol keeps. String choices pass through untouched.
func rewriteToolReferences(payload map[string]any, protocol wire.Protocol, renames *wire.ToolRenames) {
	if renames.Empty() {
		return
	}
	rename := func(container map[string]any) {
		name, ok := container["name"].(string)
		if !ok {
			return
		}
		if wireName, ok := renames.Wire(name); ok {
			container["name"] = wireName
		}
	}
	renameAllowedTools := func(choice map[string]any) {
		for _, rawEntry := range jsonutil.SliceAt(choice, "allowed_tools") {
			if entry, ok := rawEntry.(map[string]any); ok {
				rename(entry)
			}
		}
	}
	switch protocol {
	case wire.Chat:
		if choice, ok := payload["tool_choice"].(map[string]any); ok {
			if function, ok := choice["function"].(map[string]any); ok {
				rename(function)
			} else {
				rename(choice)
			}
		}
		if call, ok := payload["function_call"].(map[string]any); ok {
			rename(call)
		}
		for _, rawMessage := range jsonutil.SliceAt(payload, "messages") {
			message, _ := rawMessage.(map[string]any)
			if call, ok := message["function_call"].(map[string]any); ok {
				rename(call)
			}
			for _, rawCall := range jsonutil.SliceAt(message, "tool_calls") {
				call, _ := rawCall.(map[string]any)
				if function, ok := call["function"].(map[string]any); ok {
					rename(function)
				}
			}
		}
	case wire.Responses:
		if choice, ok := payload["tool_choice"].(map[string]any); ok {
			rename(choice)
			renameAllowedTools(choice)
		}
		for _, rawItem := range jsonutil.SliceAt(payload, "input") {
			item, _ := rawItem.(map[string]any)
			switch jsonutil.StringAt(item, "type") {
			case "function_call", "custom_tool_call":
				rename(item)
			}
		}
	case wire.Anthropic:
		if choice, ok := payload["tool_choice"].(map[string]any); ok {
			rename(choice)
		}
		for _, rawMessage := range jsonutil.SliceAt(payload, "messages") {
			message, _ := rawMessage.(map[string]any)
			for _, rawBlock := range jsonutil.SliceAt(message, "content") {
				block, _ := rawBlock.(map[string]any)
				if jsonutil.StringAt(block, "type") == "tool_use" {
					rename(block)
				}
			}
		}
	}
}

// declaredToolParameters returns the parameters schema of one declared client
// tool, in whichever shape the protocol puts it.
func declaredToolParameters(protocol wire.Protocol, entry map[string]any) any {
	switch protocol {
	case wire.Chat:
		if function, ok := entry["function"].(map[string]any); ok {
			return function["parameters"]
		}
		return nil
	case wire.Anthropic:
		return entry["input_schema"]
	default:
		return entry["parameters"]
	}
}

// officialArgAliases lists, per folded official argument name, the folded
// client spellings that mean the same argument but do not fold together. The
// fold already pairs oldString with old_string and replaceAll with replace_all,
// so only genuinely different words need a row here.
var officialArgAliases = map[string][]string{
	"path":        {"filepath", "file", "filename", "targetfile", "absolutepath", "pathname", "target"},
	"oldstring":   {"oldtext", "old", "search", "find", "original", "before"},
	"newstring":   {"newtext", "new", "replace", "replacement", "after"},
	"replaceall":  {"all", "global", "replaceevery"},
	"content":     {"text", "filetext", "data", "body", "contents"},
	"command":     {"cmd", "script", "shellcommand", "bash", "commandline"},
	"workdir":     {"cwd", "workingdirectory", "dir", "directory"},
	"timeout":     {"timeoutms", "timeoutseconds", "timeoutmillis", "maxtime"},
	"background":  {"runinbackground", "async", "nohangup"},
	"id":          {"name", "skill", "skillid"},
	"pattern":     {"query", "regex", "glob", "searchpattern"},
	"offset":      {"start", "from", "linenumber", "startline"},
	"limit":       {"count", "maxlines", "lines", "max", "numlines"},
	"description": {"justification", "reason", "note", "summary"},
	"prompt":      {"task", "instruction", "instructions", "message"},
	"agent":       {"subagent", "type", "kind"},
}

// schemaProperties returns the property table of one JSON schema, or nil when
// the value is not an object schema with properties.
func schemaProperties(schema any) map[string]any {
	object, ok := schema.(map[string]any)
	if !ok {
		return nil
	}
	properties, _ := object["properties"].(map[string]any)
	return properties
}

// toolArgumentRename maps the official property names of one tool onto the ones
// the client declared for it. A pair is recorded only when the two spellings
// differ, so a tool whose schemas already agree costs nothing.
func toolArgumentRename(official string, declared any, v1 bool) map[string]string {
	definition, ok := officialToolDefinition(official, v1)
	if !ok {
		return nil
	}
	officialProperties := schemaProperties(definition.Parameters)
	declaredProperties := schemaProperties(declared)
	if len(officialProperties) == 0 || len(declaredProperties) == 0 {
		return nil
	}
	byFold := make(map[string]string, len(declaredProperties))
	for name := range declaredProperties {
		key := foldToolForMatch(name)
		if key == "" {
			continue
		}
		if existing, seen := byFold[key]; seen && existing != name {
			// Two declared properties fold together: neither can be guessed for.
			byFold[key] = ""
			continue
		}
		byFold[key] = name
	}
	rename := map[string]string{}
	for name := range officialProperties {
		if _, same := declaredProperties[name]; same {
			continue
		}
		folded := foldToolForMatch(name)
		match := byFold[folded]
		if match == "" {
			for _, candidate := range officialArgAliases[folded] {
				if client, hit := byFold[candidate]; hit && client != "" {
					match = client
					break
				}
			}
		}
		if match == "" || match == name {
			continue
		}
		rename[name] = match
	}
	if len(rename) == 0 {
		return nil
	}
	return rename
}

// requiredNames reads a schema's required list. A payload that came through
// encoding/json carries []any, but a schema built in Go may carry []string, and
// both mean the same thing here.
func requiredNames(value any) []string {
	switch typed := value.(type) {
	case []any:
		names := make([]string, 0, len(typed))
		for _, raw := range typed {
			if name, ok := raw.(string); ok {
				names = append(names, name)
			}
		}
		return names
	case []string:
		return typed
	default:
		return nil
	}
}

// cloneSchema deep-copies one decoded JSON schema so a per-request edit cannot
// reach the shared embedded definition.
func cloneSchema(schema map[string]any) map[string]any {
	encoded, err := json.Marshal(schema)
	if err != nil {
		return schema
	}
	var clone map[string]any
	if err := json.Unmarshal(encoded, &clone); err != nil {
		return schema
	}
	return clone
}

// setEmittedToolParameters writes a parameters object back into a rendered tool
// definition, in whichever place the protocol keeps it.
func setEmittedToolParameters(protocol wire.Protocol, tool map[string]any, parameters map[string]any) {
	switch protocol {
	case wire.Chat:
		if function, ok := tool["function"].(map[string]any); ok {
			function["parameters"] = parameters
		}
	case wire.Anthropic:
		tool["input_schema"] = parameters
	default:
		tool["parameters"] = parameters
	}
}

// toolRequiredBackfill returns the client-required arguments the official schema
// cannot carry, each with the zero value of its declared type. The official schema
// has to go out byte for byte — adding a property to it is refused exactly like a
// foreign tool name — so the model cannot supply these, and the client's own
// validation would reject the call on arrival. The response walk adds them instead.
func toolRequiredBackfill(official string, declared any, v1 bool, argumentRename map[string]string) map[string]any {
	declaredSchema, _ := declared.(map[string]any)
	if declaredSchema == nil {
		return nil
	}
	declaredProperties := schemaProperties(declaredSchema)
	required := requiredNames(declaredSchema["required"])
	if len(declaredProperties) == 0 || len(required) == 0 {
		return nil
	}
	definition, ok := officialToolDefinition(official, v1)
	if !ok {
		return nil
	}
	officialProperties := schemaProperties(definition.Parameters)
	renamed := make(map[string]bool, len(argumentRename))
	for _, client := range argumentRename {
		renamed[client] = true
	}
	backfill := map[string]any{}
	for _, argument := range required {
		if argument == "" || renamed[argument] {
			continue
		}
		if _, official := officialProperties[argument]; official {
			continue
		}
		backfill[argument] = schemaZeroValue(declaredProperties[argument])
	}
	if len(backfill) == 0 {
		return nil
	}
	return backfill
}

// schemaZeroValue returns the value to send for an argument the model could not
// supply, chosen from the type the client declared for it.
func schemaZeroValue(schema any) any {
	object, _ := schema.(map[string]any)
	switch object["type"] {
	case "boolean":
		return false
	case "integer", "number":
		return 0
	case "array":
		return []any{}
	case "object":
		return map[string]any{}
	default:
		return ""
	}
}
