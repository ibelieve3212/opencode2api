package protocol

import (
	"encoding/json"
	"io"
	"strings"
	"testing"
)

// argRenames builds a rename set that sends the official "edit" definition and
// maps its arguments back onto the spellings a client like dsh declares.
func argRenames() *ToolRenames {
	renames := NewToolRenames()
	renames.BindOfficial("edit", "edit")
	renames.BindArgs("edit", map[string]string{
		"path":       "file_path",
		"oldString":  "old_string",
		"newString":  "new_string",
		"replaceAll": "replace_all",
	})
	return renames
}

// readAll drains a stream reader and returns everything it produced.
func readAll(t *testing.T, reader io.Reader) string {
	t.Helper()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	return string(data)
}

// toolCallArguments collects every tool_calls function in a rewritten Chat
// stream, concatenating argument fragments per call index.
func chatToolCalls(t *testing.T, stream string) map[string]map[string]string {
	t.Helper()
	calls := map[string]map[string]string{}
	for _, line := range strings.Split(stream, "\n") {
		body, ok := strings.CutPrefix(line, "data: ")
		if !ok || body == "[DONE]" {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(body), &payload); err != nil {
			continue
		}
		choices, _ := payload["choices"].([]any)
		for _, rawChoice := range choices {
			choice, _ := rawChoice.(map[string]any)
			delta, _ := choice["delta"].(map[string]any)
			list, _ := delta["tool_calls"].([]any)
			for position, rawCall := range list {
				call, _ := rawCall.(map[string]any)
				function, _ := call["function"].(map[string]any)
				key := "0"
				if index, ok := call["index"]; ok {
					key = jsonIndex(index, position)
				}
				entry := calls[key]
				if entry == nil {
					entry = map[string]string{}
					calls[key] = entry
				}
				if name, ok := function["name"].(string); ok && name != "" {
					entry["name"] += name
				}
				if arguments, ok := function["arguments"].(string); ok {
					entry["arguments"] += arguments
				}
			}
		}
	}
	return calls
}

func jsonIndex(value any, fallback int) string {
	switch typed := value.(type) {
	case float64:
		return strings.TrimSuffix(strings.TrimSuffix(jsonNumber(typed), ".0"), ".")
	default:
		return jsonNumber(float64(fallback))
	}
}

func jsonNumber(value float64) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

// TestChatStreamReKeysArguments 断言 Chat 流里的参数分片被整块缓冲，并在调用结束
// 时以客户端的参数名一次性写出。分片本身无法按键改名，所以只能等它完整。
func TestChatStreamReKeysArguments(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"edit","arguments":"{\"path\":\"/tmp/a.txt\",\"old"}}]}}]}`,
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"String\":\"a\",\"newString\":\"b\"}"}}]}}]}`,
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		`data: [DONE]`,
		``,
	}, "\n")

	rewritten := readAll(t, RenameStreamReader(strings.NewReader(stream), Chat, argRenames()))
	calls := chatToolCalls(t, rewritten)
	call := calls["0"]
	if call == nil {
		t.Fatalf("no tool call in rewritten stream:\n%s", rewritten)
	}
	if call["name"] != "edit" {
		t.Errorf("name = %q, want edit", call["name"])
	}
	var arguments map[string]any
	if err := json.Unmarshal([]byte(call["arguments"]), &arguments); err != nil {
		t.Fatalf("arguments are not one complete object: %q", call["arguments"])
	}
	if arguments["file_path"] != "/tmp/a.txt" || arguments["old_string"] != "a" || arguments["new_string"] != "b" {
		t.Errorf("arguments = %v, want the client's spellings", arguments)
	}
	if _, leaked := arguments["path"]; leaked {
		t.Errorf("official argument name reached the client: %v", arguments)
	}
}

// TestChatStreamLeavesAgreeingToolsAlone 断言没有参数映射的工具分片照原样透传，
// 不因为改写机制而改变分帧。
func TestChatStreamLeavesAgreeingToolsAlone(t *testing.T) {
	renames := NewToolRenames()
	renames.BindOfficial("glob", "glob")
	stream := strings.Join([]string{
		`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"glob","arguments":"{\"pattern\":\"*.go\"}"}}]}}]}`,
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		`data: [DONE]`,
		``,
	}, "\n")
	rewritten := readAll(t, RenameStreamReader(strings.NewReader(stream), Chat, renames))
	if !strings.Contains(rewritten, `{\"pattern\":\"*.go\"}`) {
		t.Errorf("an agreeing tool's arguments were rewritten:\n%s", rewritten)
	}
}

// anthropicInput returns every tool_use input the rewritten Anthropic stream
// delivered, plus the block names, after applying the input deltas in order.
func anthropicInput(t *testing.T, stream string) (string, string) {
	t.Helper()
	name := ""
	var input strings.Builder
	for _, line := range strings.Split(stream, "\n") {
		body, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(body), &payload); err != nil {
			continue
		}
		switch payload["type"] {
		case "content_block_start":
			block, _ := payload["content_block"].(map[string]any)
			if typed, ok := block["name"].(string); ok {
				name = typed
			}
		case "content_block_delta":
			delta, _ := payload["delta"].(map[string]any)
			if typed, ok := delta["partial_json"].(string); ok {
				input.WriteString(typed)
			}
		}
	}
	return name, input.String()
}

// TestAnthropicStreamReKeysArguments 断言 Anthropic 流里的 input_json_delta 被缓冲，
// 并在 content_block_stop 之前以一整块改写后的对象发出。
func TestAnthropicStreamReKeysArguments(t *testing.T) {
	stream := strings.Join([]string{
		`event: content_block_start`,
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"edit","input":{}}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"/tmp/a.txt\","}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"oldString\":\"a\",\"newString\":\"b\"}"}}`,
		``,
		`event: content_block_stop`,
		`data: {"type":"content_block_stop","index":0}`,
		``,
	}, "\n")

	rewritten := readAll(t, RenameStreamReader(strings.NewReader(stream), Anthropic, argRenames()))
	name, input := anthropicInput(t, rewritten)
	if name != "edit" {
		t.Errorf("name = %q, want edit", name)
	}
	var arguments map[string]any
	if err := json.Unmarshal([]byte(input), &arguments); err != nil {
		t.Fatalf("input is not one complete object: %q\nstream:\n%s", input, rewritten)
	}
	if arguments["file_path"] != "/tmp/a.txt" || arguments["old_string"] != "a" || arguments["new_string"] != "b" {
		t.Errorf("arguments = %v, want the client's spellings", arguments)
	}
	if strings.Index(rewritten, `"file_path"`) > strings.Index(rewritten, "content_block_stop") {
		t.Errorf("the re-keyed input did not arrive before the block closed:\n%s", rewritten)
	}
}

// TestAnthropicObjectArguments 断言非流式 Anthropic 响应里的 tool_use input 对象
// 也按键还原。
func TestAnthropicObjectArguments(t *testing.T) {
	body := []byte(`{"type":"message","content":[{"type":"tool_use","id":"toolu_1","name":"edit","input":{"path":"/tmp/a","oldString":"a","newString":"b"}}]}`)
	rewritten := RenameResponseTool(body, Anthropic, argRenames())
	var payload struct {
		Content []struct {
			Name  string         `json:"name"`
			Input map[string]any `json:"input"`
		} `json:"content"`
	}
	if err := json.Unmarshal(rewritten, &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	input := payload.Content[0].Input
	if input["file_path"] != "/tmp/a" || input["old_string"] != "a" || input["new_string"] != "b" {
		t.Errorf("input = %v, want the client's spellings", input)
	}
}
