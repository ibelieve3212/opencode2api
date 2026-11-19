package gateway

import (
	_ "embed"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/ibelieve3212/opencode2api/internal/config"
	wire "github.com/ibelieve3212/opencode2api/internal/protocol"
)

// The free tier accepts only agent-shaped requests, and its abuse checks expect
// the system prompt the official CLI sends. These assets are the CLI's own
// wording: the v1 text for /zen (OpenCode 1.x tool names) and the v2 text for
// /inference and /zen/go (OpenCode 2.x tool names). The v2 file is the v1 file
// with the tool names the 2.x CLI uses.
//
//go:embed opencode_prompt_v1.txt
var opencodePromptV1 string

//go:embed opencode_prompt_v2.txt
var opencodePromptV2 string

// opencodePromptCodeMode is the "# Code Mode" block a 2.3.x client appends after the
// identity block: the section that introduces the `execute` tool and lists the built-in
// `opencode` namespace it can reach. Without it the prompt describes fewer capabilities
// than the tool set declares, because `execute` is in that set.
//
//go:embed opencode_prompt_v2_codemode.txt
var opencodePromptCodeMode string

// opencodePromptMarkers identify a body that already carries an official CLI system prompt,
// so a client that sends one itself is left alone and repeated shaping cannot duplicate it.
//
// Three openings cover every family that REPLACES the base prompt, and the client's
// replacements are the ones that can reach a gateway without the base line:
//
//	base, gpt, gpt-astra   You are an AI agent running in OpenCode
//	trinity                You are opencode, an interactive CLI tool
//	meta, kimi             You are OpenCode,
//
// claude only APPENDS its supplement, so a claude body still carries a base opening. Checking
// solely the first line meant the guard never matched the 2.x prompt this package injects, so a
// body that already carried it was given a second copy.
var opencodePromptMarkers = []string{
	"You are opencode, an interactive CLI tool",
	"You are an AI agent running in OpenCode",
	"You are OpenCode,",
}

// toolRoutingBlock names the built-in tools this request carries but the client cannot dispatch.
//
// The official set is appended so the request matches a real CLI session, which means the model
// can see names the caller never declared. A call it makes to one of those comes back to a client
// that has no handler for it — the failure an agent shows as "unknown tool" on every tool turn.
// Naming them here is cheaper and steadier than pruning the tool set and giving up the shape.
func toolRoutingBlock(disabled []string) string {
	return strings.Join([]string{
		"# Tool routing",
		"",
		"Call only the tools declared in this request. These built-in names belong to the harness and are not available to you; do not call them even though they appear in the tool list:",
		"",
		"- " + strings.Join(disabled, ", "),
	}, "\n")
}

// opencodeSkills is the built-in skill listing the CLI appends after its
// environment block. It is static, so it is part of the injected prompt.
const opencodeSkills = `Skills provide specialized instructions and workflows for specific tasks.
Use the skill tool to load a skill when a task matches its description.
<available_skills>
  <skill>
    <id>opencode</id>
    <name>OpenCode</name>
    <description>Use this skill for any question about OpenCode itself, including how OpenCode works, using or configuring it, migrating from V1 to V2, troubleshooting it, developing plugins or integrations, using the OpenCode SDK, clients, server, or API, and contributing to the OpenCode codebase. Also use it for OpenCode agents, commands, skills, tools, permissions, MCP servers, providers, models, themes, keybinds, formatters, the CLI, TUI, desktop app, and web app.</description>
  </skill>
  <skill>
    <id>report</id>
    <name>Report</name>
    <description>Use when the user wants to report an opencode issue or bug. Collect standard diagnostics, add user-specific reproduction context, and publish the issue with GitHub CLI.</description>
  </skill>
</available_skills>`

var (
	opencodeEnvOnce sync.Once
	opencodeWorkDir string
)

// opencodeEnvironment renders the <env> block the CLI puts after its base
// prompt. Configured values replace the host defaults without changing the
// field names or order; the host directory is resolved once per process.
func opencodeEnvironment(model string, environment *config.PromptEnvironmentConfig) string {
	opencodeEnvOnce.Do(func() {
		directory := ""
		if current, err := os.Getwd(); err == nil {
			directory = filepath.Clean(strings.TrimSpace(current))
		}
		if directory == "" || directory == "." {
			directory = "/workspace"
		}
		opencodeWorkDir = directory
	})
	// The model name is no longer part of the environment block: the client reports it
	// in its own "# Your Model" system part, which identityBlock renders.
	platform := runtime.GOOS
	if platform == "windows" {
		platform = "win32"
	}
	directory := opencodeWorkDir
	workspace := directory
	gitRepo := "yes"
	if environment != nil {
		if environment.WorkingDirectory != "" {
			directory = environment.WorkingDirectory
			workspace = directory
		}
		if environment.WorkspaceRoot != "" {
			workspace = environment.WorkspaceRoot
		}
		if environment.Platform != "" {
			platform = environment.Platform
		}
		if environment.IsGitRepo != nil && !*environment.IsGitRepo {
			gitRepo = "no"
		}
	}
	return strings.Join([]string{
		"Here is some useful information about the environment you are running in:",
		"<env>",
		"  Working directory: " + directory,
		"  Workspace root folder: " + workspace,
		"  Is directory a git repo: " + gitRepo,
		"  Platform: " + platform,
		"  Prefer " + opencodeTempDirectory + " over generic system temporary directories such as /tmp; it is pre-created and approved for external access.",
		"</env>",
	}, "\n")
}

// opencodeTempDirectory stands in for the CLI's global tmp directory, which it names
// in the environment block. The free tier compares the block's shape rather than the
// path, so a host-derived path keeps the line honest without inventing a constant.
var opencodeTempDirectory = filepath.Join(os.TempDir(), "opencode")

// opencodeSystemPrompt assembles the full CLI system prompt the free tier's abuse
// checks expect. The parts and their order mirror the client's own system array, read from
// the 2.3.0 binary rather than from a checkout:
//
//	[0] the base prompt with ${OPENCODE_TOOL_GUIDANCE} resolved per present tool.
//	    The renderer is Z0e in chunk-rp2adgpa.js, called as
//	    Z0e(basePromptAsset, Object.keys(session.tools)); it has exactly three
//	    conditions (shell, write, edit) and joins its lines with "\n".
//	[1] the "# Your Model" identity block, which the identity plugin splices at index 1
//	    and joins internally with "\n".
//	[2] the instruction block: "Today's date: <toDateString()>" and the <env> block are
//	    separate system parts, each rendered by its own renderer.
//	[3] the worktree guidance the tools plugin pushes onto the end of the array.
//	[4] the built-in skill listing.
//
// The parts are joined with a single "\n" because that is what the client does when it
// folds the array onto the wire: chunk-95hz31gz.js defines
//
//	$U = (e) => e.map((r) => r.text).join("\n")
//
// and the openai-chat protocol module (chunk-a3k2v7ew.js) builds
// `[{ role: "system", content: $U(system) }]`. An empty array emits no system message.
// This was "\n\n" here until it was checked against the binary.
func opencodeSystemPrompt(model string, v1 bool, tools []string, environment *config.PromptEnvironmentConfig, disabled []string) string {
	base := renderBasePrompt(opencodePromptV2, tools)
	if v1 {
		base = renderBasePrompt(opencodePromptV1, tools)
	}
	parts := []string{base, identityBlock(model)}
	// The order below is the order a 2.3.1 capture shows, which is not the order the
	// parts are written in the binary: the base prompt and identity first, then the
	// Code Mode block, then the skill listing, then the date, the environment block and
	// the worktree guidance. 1.x has no Code Mode, so that part is v2-only.
	if !v1 {
		parts = append(parts, strings.TrimRight(opencodePromptCodeMode, "\n"))
	}
	if len(disabled) > 0 {
		parts = append(parts, toolRoutingBlock(disabled))
	}
	parts = append(parts,
		opencodeSkills,
		"Today's date: "+time.Now().UTC().Format("Mon Jan 02 2006"),
		opencodeEnvironment(model, environment),
		opencodeWorktreeGuidance,
	)
	return strings.Join(parts, "\n")
}

// opencodeWorktreeGuidance is the system part the tools plugin pushes onto the array
// (packages/core/src/tool/plugin/opencode.ts), so it lands after the instructions
// rather than beside the identity block.
const opencodeWorktreeGuidance = "When you create a worktree outside the current working directory and intend to use it as your primary working directory, consider using `execute` to call `tools.opencode.session_move` and make the worktree the session's working directory."

// opencodeToolNames lists the tool names a shaped Chat payload declares, so the base
// prompt renders guidance for exactly the tools the request carries.
//
// The official set is always included, because renameAndCompleteTools appends it
// unconditionally after this runs. Rendering from the caller's names alone gave the wrong
// answer for every foreign agent: a client that declares run_command, write_file and
// apply_patch instead of shell, write and edit matched none of the three guidance lines, so
// the injected prompt carried an empty ${OPENCODE_TOOL_GUIDANCE} while the tool set it was
// sent beside declared all three. The section a real CLI always fills was silently blank.
func opencodeToolNames(payload map[string]any, v1 bool) []string {
	items, _ := payload["tools"].([]any)
	names := make([]string, 0, len(items)+len(officialToolNames(v1)))
	seen := make(map[string]bool, len(items)+len(officialToolNames(v1)))
	add := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		names = append(names, name)
	}
	for _, name := range officialToolNames(v1) {
		add(name)
	}
	for _, item := range items {
		// A Chat tool is {"type":"function","function":{"name":…}}, which is the shape
		// setToolName writes and the shape readToolName reads.
		entry, _ := item.(map[string]any)
		function, _ := entry["function"].(map[string]any)
		name, _ := function["name"].(string)
		add(name)
	}
	return names
}

// identityBlock renders the "# Your Model" system part the identity plugin splices
// into every request (packages/core/src/plugin/identity.ts), naming the exact
// provider and model the client asked for.
func identityBlock(model string) string {
	apiModel := strings.TrimSpace(strings.TrimPrefix(model, "opencode/"))
	if apiModel == "" {
		apiModel = "unknown"
	}
	return strings.Join([]string{
		"# Your Model",
		"- Name: " + apiModel,
		"- Provider ID: opencode",
		"- Model ID: " + apiModel,
	}, "\n")
}

// renderBasePrompt substitutes ${OPENCODE_TOOL_GUIDANCE} with the lines the client
// derives from the tools present in the request
// (packages/core/src/session/system-prompt.ts).
func renderBasePrompt(prompt string, tools []string) string {
	has := func(name string) bool {
		for _, tool := range tools {
			if tool == name {
				return true
			}
		}
		return false
	}
	lines := make([]string, 0, 4)
	if has("shell") {
		lines = append(lines,
			"- Prefer dedicated tools over shell commands; fall back to the shell when a tool cannot do what you need.",
			"- Do not chain shell commands with separators like `echo \"====\";` or `printf '---'`; the output becomes noisy in a way that makes the user's side of the conversation worse.",
		)
	}
	if has("write") {
		lines = append(lines,
			"- Use the write tool to create files or completely replace their content. Prefer using the edit tool for targeted changes.",
		)
	}
	if has("edit") {
		lines = append(lines,
			"- Use the edit tool for targeted changes to existing text files. It replaces the exact text in `oldString` with `newString`, and the values must differ. By default, `oldString` must occur exactly once. If it occurs multiple times, include more surrounding context to make it unique or set `replaceAll` to true to replace every occurrence.",
		)
	}
	return strings.Replace(prompt, "${OPENCODE_TOOL_GUIDANCE}", strings.Join(lines, "\n"), 1)
}

// injectOfficialPrompt prepends the official CLI system prompt to a shaped
// free-tier payload. It reports whether the payload changed; a body that
// already carries the marker is left untouched, so repeated shaping cannot
// duplicate the prompt. System One payloads are not chat bodies and are never
// touched.
func injectOfficialPrompt(payload map[string]any, protocol wire.Protocol, v1 bool, environment *config.PromptEnvironmentConfig, disabled []string) bool {
	if payload == nil || protocol == wire.SystemOne {
		return false
	}
	model, _ := payload["model"].(string)
	prompt := opencodeSystemPrompt(model, v1, opencodeToolNames(payload, v1), environment, disabled)
	switch protocol {
	case wire.Chat:
		return injectChatPrompt(payload, prompt)
	case wire.Anthropic:
		return injectAnthropicPrompt(payload, prompt)
	case wire.Responses:
		return injectResponsesPrompt(payload, prompt)
	default:
		return false
	}
}

// injectChatPrompt folds every system/developer message into the single system
// message the 1.x and 2.x CLIs send, then prepends it. Client instructions keep
// their order and are separated from the official prompt by a blank line.
func injectChatPrompt(payload map[string]any, prompt string) bool {
	messages, ok := payload["messages"].([]any)
	if !ok {
		return false
	}
	kept := make([]any, 0, len(messages)+1)
	var instructions []string
	for _, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			kept = append(kept, raw)
			continue
		}
		role, _ := message["role"].(string)
		if role != "system" && role != "developer" {
			kept = append(kept, raw)
			continue
		}
		content := message["content"]
		if carriesOpenCodePrompt(content) {
			return false
		}
		for _, part := range textFragments(content) {
			if part = strings.TrimSpace(part); part != "" {
				instructions = append(instructions, part)
			}
		}
	}
	// The client would carry these as further system parts, so the fold separator
	// is the same single newline $U uses, not a paragraph break.
	system := prompt
	if joined := strings.Join(instructions, "\n"); joined != "" {
		system += "\n" + joined
	}
	payload["messages"] = append([]any{map[string]any{"role": "system", "content": system}}, kept...)
	return true
}

// injectAnthropicPrompt prepends the prompt to the top-level system value,
// keeping a caller's string or block list intact after it.
func injectAnthropicPrompt(payload map[string]any, prompt string) bool {
	system, exists := payload["system"]
	if exists && carriesOpenCodePrompt(system) {
		return false
	}
	if !exists {
		payload["system"] = prompt
		return true
	}
	switch value := system.(type) {
	case string:
		if strings.TrimSpace(value) == "" {
			payload["system"] = prompt
		} else {
			payload["system"] = prompt + "\n" + value
		}
	case []any:
		payload["system"] = append([]any{map[string]any{"type": "text", "text": prompt}}, value...)
	default:
		payload["system"] = prompt
	}
	return true
}

// injectResponsesPrompt prepends the prompt to the Responses instructions.
func injectResponsesPrompt(payload map[string]any, prompt string) bool {
	instructions, exists := payload["instructions"]
	if exists && carriesOpenCodePrompt(instructions) {
		return false
	}
	if text, ok := instructions.(string); exists && ok {
		if strings.TrimSpace(text) == "" {
			payload["instructions"] = prompt
		} else {
			payload["instructions"] = prompt + "\n" + text
		}
		return true
	}
	payload["instructions"] = prompt
	return true
}

// textFragments flattens a system or instruction value into its text: a plain
// string, a list of content blocks, or a block object carrying text or content.
func textFragments(value any) []string {
	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return []string{typed}
	case []any:
		var parts []string
		for _, item := range typed {
			parts = append(parts, textFragments(item)...)
		}
		return parts
	case map[string]any:
		if text, ok := typed["text"].(string); ok {
			return textFragments(text)
		}
		if content, ok := typed["content"]; ok {
			return textFragments(content)
		}
	}
	return nil
}

// carriesOpenCodePrompt reports whether any text inside value already contains
// the marker.
func carriesOpenCodePrompt(value any) bool {
	for _, part := range textFragments(value) {
		for _, marker := range opencodePromptMarkers {
			if strings.Contains(part, marker) {
				return true
			}
		}
	}
	return false
}
