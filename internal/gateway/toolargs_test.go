package gateway

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ibelieve3212/opencode2api/internal/protocol"
)

// dshSchemaTools 是 dsh 真实声明的一组工具：名字是官方的，参数却是它自己的拼写。
// 这正是被上游判成「非 OpenCode 会话」的那种定义——名字改对了，schema 还露馅。
func dshSchemaTools() []any {
	object := func(properties map[string]any, required ...string) map[string]any {
		schema := map[string]any{
			"type":                 "object",
			"properties":           properties,
			"additionalProperties": false,
		}
		if len(required) > 0 {
			schema["required"] = required
		}
		return schema
	}
	stringProperty := func(description string) map[string]any {
		return map[string]any{"type": "string", "description": description}
	}
	return []any{
		map[string]any{"type": "function", "function": map[string]any{
			"name":        "edit",
			"description": "Edit an existing UTF-8 text file by replacing literal text.",
			"parameters": object(map[string]any{
				"file_path":           stringProperty("Path to edit"),
				"old_string":          stringProperty("Literal text to replace"),
				"new_string":          stringProperty("Literal replacement text"),
				"replace_all":         map[string]any{"type": "boolean"},
				"sandbox_permissions": stringProperty("Sandbox mode"),
				"justification":       stringProperty("Why this edit needs access"),
			}, "file_path", "old_string", "new_string"),
		}},
		map[string]any{"type": "function", "function": map[string]any{
			"name":        "read",
			"description": "Read a UTF-8 text file",
			"parameters": object(map[string]any{
				"file_path": stringProperty("File to read"),
				"offset":    map[string]any{"type": "integer"},
				"limit":     map[string]any{"type": "integer"},
			}, "file_path"),
		}},
		map[string]any{"type": "function", "function": map[string]any{
			"name":        "shell",
			"description": "Run a PowerShell command",
			"parameters": object(map[string]any{
				"command":           stringProperty("Command to run"),
				"timeoutMs":         map[string]any{"type": "integer"},
				"run_in_background": map[string]any{"type": "boolean"},
				"description":       stringProperty("What the command does"),
			}, "command", "description"),
		}},
	}
}

// TestShapedDefinitionIsTheOfficialOne 断言发出去的定义整块是官方的：名字、描述、
// 参数 schema 全部一致。上游校验的就是这三样，留下任何一处客户端拼写都会被判成
// 别的会话并以 429 拒绝。
func TestShapedDefinitionIsTheOfficialOne(t *testing.T) {
	payload := map[string]any{"tools": dshSchemaTools()}
	if _, ok := renameAndCompleteTools(payload, protocol.Chat, false, ""); !ok {
		t.Fatalf("shaping declined")
	}
	shaped := payload["tools"].([]any)
	for _, raw := range shaped {
		entry := raw.(map[string]any)
		function := entry["function"].(map[string]any)
		name := function["name"].(string)
		definition, official := officialToolDefinition(name, false)
		if !official {
			// 客户端独有的工具仍走不透明名，不属于本条断言。
			continue
		}
		if function["description"] != definition.Description {
			t.Errorf("%s: description is the client's, want the official one", name)
		}
		// Byte for byte: the free tier refuses a definition that carries even one property
		// the official one does not, so nothing client-specific may survive here.
		if !reflect.DeepEqual(function["parameters"], definition.Parameters) {
			t.Errorf("%s: parameters are the client's schema, want the official one", name)
		}
	}
}

// TestToolArgumentRename 固定住参数映射：官方拼写回到客户端拼写。
func TestToolArgumentRename(t *testing.T) {
	declared := dshSchemaTools()
	byName := map[string]any{}
	for _, raw := range declared {
		entry := raw.(map[string]any)
		function := entry["function"].(map[string]any)
		byName[function["name"].(string)] = function["parameters"]
	}
	cases := map[string]map[string]string{
		"edit":  {"path": "file_path", "oldString": "old_string", "newString": "new_string", "replaceAll": "replace_all"},
		"read":  {"path": "file_path"},
		"shell": {"timeout": "timeoutMs", "background": "run_in_background"},
	}
	for official, want := range cases {
		got := toolArgumentRename(official, byName[official], false)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v, want %v", official, got, want)
		}
	}
}

// TestToolArgumentRenameSkipsAgreeingSchemas 断言参数已经一致的工具不产生映射，
// 这样响应侧对这些工具完全不动。
func TestToolArgumentRenameSkipsAgreeingSchemas(t *testing.T) {
	definition, ok := officialToolDefinition("glob", false)
	if !ok {
		t.Fatalf("glob definition missing")
	}
	if mapping := toolArgumentRename("glob", definition.Parameters, false); len(mapping) != 0 {
		t.Errorf("glob already agrees, got mapping %v", mapping)
	}
}

// TestResponseRestoresClientArguments 走一遍完整往返：按官方 schema 发出去，模型
// 用官方参数名回答，响应侧必须还原成客户端自己的参数名。
func TestResponseRestoresClientArguments(t *testing.T) {
	payload := map[string]any{"tools": dshSchemaTools()}
	renames, ok := renameAndCompleteTools(payload, protocol.Chat, false, "")
	if !ok {
		t.Fatalf("shaping declined")
	}
	answer := []byte(`{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"edit","arguments":"{\"path\":\"/tmp/a.txt\",\"oldString\":\"a\",\"newString\":\"b\",\"replaceAll\":true}"}}]}}]}`)
	restored := protocol.RenameResponseTool(answer, protocol.Chat, renames)

	var decoded struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(restored, &decoded); err != nil {
		t.Fatalf("decode restored response: %v", err)
	}
	call := decoded.Choices[0].Message.ToolCalls[0].Function
	if call.Name != "edit" {
		t.Errorf("name = %q, want edit", call.Name)
	}
	var arguments map[string]any
	if err := json.Unmarshal([]byte(call.Arguments), &arguments); err != nil {
		t.Fatalf("decode arguments: %v", err)
	}
	want := map[string]any{
		"file_path":   "/tmp/a.txt",
		"old_string":  "a",
		"new_string":  "b",
		"replace_all": true,
	}
	if !reflect.DeepEqual(arguments, want) {
		t.Errorf("arguments = %v, want %v", arguments, want)
	}
}

// TestResponseBackfillsClientRequiredArguments 断言客户端声明必填、但官方 schema 装不下的
// 参数，会在响应侧补上零值。dsh 的 shell 要求 description，官方 shell 没有这个属性，模型
// 不可能给出它 —— 不补，客户端就以 missing required property 拒掉整次调用。
func TestResponseBackfillsClientRequiredArguments(t *testing.T) {
	payload := map[string]any{"tools": dshSchemaTools()}
	renames, ok := renameAndCompleteTools(payload, protocol.Chat, false, "")
	if !ok {
		t.Fatalf("shaping declined")
	}
	answer := []byte(`{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"shell","arguments":"{\"command\":\"pwd\"}"}}]}}]}`)
	restored := protocol.RenameResponseTool(answer, protocol.Chat, renames)

	var decoded struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					Function struct {
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(restored, &decoded); err != nil {
		t.Fatalf("decode restored response: %v", err)
	}
	var arguments map[string]any
	if err := json.Unmarshal([]byte(decoded.Choices[0].Message.ToolCalls[0].Function.Arguments), &arguments); err != nil {
		t.Fatalf("decode arguments: %v", err)
	}
	description, present := arguments["description"]
	if !present {
		t.Fatalf("client-required description was not backfilled: %v", arguments)
	}
	if description != "" {
		t.Errorf("description = %v, want the empty string", description)
	}
	if arguments["command"] != "pwd" {
		t.Errorf("command = %v, want pwd", arguments["command"])
	}
}

// v1SchemaTools 是 1.x 通道（/zen）的一组客户端工具：名字是 v1 官方的，参数是 dsh 的拼写。
// v1 的 edit 用 filePath 而不是 v2 的 path，所以参数映射必须按各代的定义分别算。
func v1SchemaTools() []any {
	object := func(properties map[string]any, required ...string) map[string]any {
		schema := map[string]any{"type": "object", "properties": properties}
		if len(required) > 0 {
			schema["required"] = required
		}
		return schema
	}
	text := func(description string) map[string]any {
		return map[string]any{"type": "string", "description": description}
	}
	return []any{
		map[string]any{"type": "function", "function": map[string]any{
			"name":        "edit",
			"description": "client edit",
			"parameters": object(map[string]any{
				"file_path":   text("File to edit"),
				"old_string":  text("Text to find"),
				"new_string":  text("Text to replace"),
				"replace_all": map[string]any{"type": "boolean"},
			}, "file_path", "old_string", "new_string"),
		}},
		map[string]any{"type": "function", "function": map[string]any{
			"name":        "bash",
			"description": "client bash",
			"parameters": object(map[string]any{
				"command":     text("Command"),
				"timeoutMs":   map[string]any{"type": "integer"},
				"description": text("What it does"),
			}, "command", "description"),
		}},
		map[string]any{"type": "function", "function": map[string]any{
			"name":        "read",
			"description": "client read",
			"parameters": object(map[string]any{
				"file_path": text("File"),
				"offset":    map[string]any{"type": "integer"},
				"limit":     map[string]any{"type": "integer"},
			}, "file_path"),
		}},
	}
}

// TestV1ShapedDefinitionIsTheOfficialOne 断言 1.x 通道发出的定义也是整块官方 v1 定义，
// 参数映射按 v1 的 filePath 计算，而不是照搬 v2 的 path。
func TestV1ShapedDefinitionIsTheOfficialOne(t *testing.T) {
	payload := map[string]any{"tools": v1SchemaTools()}
	if _, ok := renameAndCompleteTools(payload, protocol.Chat, true, ""); !ok {
		t.Fatalf("shaping declined")
	}
	shaped := payload["tools"].([]any)
	seen := map[string]bool{}
	for _, raw := range shaped {
		function := raw.(map[string]any)["function"].(map[string]any)
		name := function["name"].(string)
		definition, official := officialToolDefinition(name, true)
		if !official {
			continue
		}
		seen[name] = true
		if function["description"] != definition.Description {
			t.Errorf("%s: description is the client's, want the official v1 one", name)
		}
		if !reflect.DeepEqual(function["parameters"], definition.Parameters) {
			t.Errorf("%s: parameters are the client's schema, want the official v1 one", name)
		}
	}
	for _, name := range []string{"edit", "bash", "read"} {
		if !seen[name] {
			t.Errorf("%s did not take the official v1 definition", name)
		}
	}
}

// TestV1ToolArgumentRename 固定住 v1 的参数映射：filePath 对 file_path，而不是 v2 的 path。
func TestV1ToolArgumentRename(t *testing.T) {
	byName := map[string]any{}
	for _, raw := range v1SchemaTools() {
		function := raw.(map[string]any)["function"].(map[string]any)
		byName[function["name"].(string)] = function["parameters"]
	}
	cases := map[string]map[string]string{
		"edit": {"filePath": "file_path", "oldString": "old_string", "newString": "new_string", "replaceAll": "replace_all"},
		"read": {"filePath": "file_path"},
		"bash": {"timeout": "timeoutMs"},
	}
	for official, want := range cases {
		got := toolArgumentRename(official, byName[official], true)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("v1 %s: got %v, want %v", official, got, want)
		}
	}
}

// TestV1BackfillCoversBashDescription 断言 v1 的 bash 也补齐客户端必填的 description。
func TestV1BackfillCoversBashDescription(t *testing.T) {
	payload := map[string]any{"tools": v1SchemaTools()}
	renames, ok := renameAndCompleteTools(payload, protocol.Chat, true, "")
	if !ok {
		t.Fatalf("shaping declined")
	}
	values := renames.Required("bash")
	if values == nil {
		t.Fatalf("no backfill recorded for v1 bash")
	}
	if zero, present := values["description"]; !present || zero != "" {
		t.Errorf("v1 bash backfill = %v, want description empty string", values)
	}
}
