package models

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/ibelieve3212/opencode2api/internal/config"
	"github.com/ibelieve3212/opencode2api/internal/httpx"
	wire "github.com/ibelieve3212/opencode2api/internal/protocol"
)

// CapabilitiesURL is OpenCode's machine-readable model catalog. It feeds
// FetchCapabilities and is also the OpenCode metadata endpoint the
// PricingRegistry reads. ZenDocsURL and GoDocsURL are the Zen and Go
// documentation pages whose endpoint tables FetchCapabilities reads with
// FetchProtocolDocs.
//
// The type filter mirrors the 2.3.0 client, which requests
// `?type=chat,decision,image,video` against the same host. Without it the
// endpoint returns chat models only: measured 2026-11-17, the filter raised the
// opencode provider from 140 to 142 models, adding `jev-1.13` and
// `jev-1.13-free`.
//
// Both additions are type `decision` and speak the SystemOne questions/answers
// protocol (`@ai-sdk/typesafe-ai`), so neither is routable here. The filter is
// kept for parity with what the release build itself knew about, not for routing:
// the number of free models this gateway can actually serve is 52 either way.
//
// The path stays at /api.json, but the decoders now accept BOTH schema generations, so
// this constant can be pointed at /experimental/v2.0/api.json without losing protocol
// detection or pricing. catalog_shape.go holds the side-by-side mapping and the adapter.
//
//	           /api.json (used here)       /experimental/v2.0
//	cost       cost.input, cost.cache_read cost.text.input.price, .cache.read
//	tool_call  present                     absent (capabilities.tools.supported)
//	provider.api  string                   map of protocol -> base_url
//	provider.npm  present                  absent (api[shape].sdk.aisdk)
//
// Both are read; a source may be switched without touching the callers. The seven models
// the v2 endpoint adds beyond the filtered legacy one are all non-chat (image, decision,
// live), which is why the legacy path is still the default.
const (
	CapabilitiesURL = "https://models.opencode.ai/api.json?type=chat,decision,image,video"
	ZenDocsURL      = "https://raw.githubusercontent.com/anomalyco/opencode/dev/packages/web/src/content/docs/zen.mdx"
	GoDocsURL       = "https://raw.githubusercontent.com/anomalyco/opencode/dev/packages/web/src/content/docs/go.mdx"
	// V1Base is the v1 lane's origin: the /zen console (serving /zen/v1/*) that
	// expects the OpenCode 1.x request shape. It is the default keyed Zen
	// upstream and the anonymous lane's fallback after the v2 /inference
	// origin. It also serves the models listing.
	V1Base = "https://opencode.ai/zen"
)

// IsV1Upstream reports whether baseURL targets the v1 /zen gate, which expects
// the OpenCode 1.x request shape, rather than a v2 gate (/inference or
// /zen/go), which expects the OpenCode 2.x CLI shape.
func IsV1Upstream(baseURL string) bool {
	trimmed := strings.ToLower(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if strings.EqualFold(trimmed, strings.TrimRight(V1Base, "/")) {
		return true
	}
	return strings.HasSuffix(trimmed, "/zen")
}

// protocolDocEndpointPattern matches a row of a documentation endpoint table,
// "| name | `model-id` | `.../v1/<endpoint>` |", capturing the model ID and the
// endpoint.
var protocolDocEndpointPattern = regexp.MustCompile("\\|[^|]+\\|\\s*`?([^|`\\s]+)`?\\s*\\|\\s*`[^`]+/v1/(chat/completions|responses|messages|systemone)`")

// Capabilities is the per-tier result of FetchCapabilities: the native
// protocol of every model, the models whose SDK maps to no supported protocol,
// and the metadata of every listed model.
type Capabilities struct {
	Protocols   map[config.Tier]map[string]wire.Protocol
	Unsupported map[config.Tier]map[string]bool
	Metadata    map[config.Tier]map[string]Metadata
}

type capabilityProvider struct {
	ID     string                     `json:"id"`
	API    string                     `json:"api"`
	NPM    string                     `json:"npm"`
	Models map[string]capabilityModel `json:"models"`
}

// UnmarshalJSON accepts the provider api field as either shape: a plain string (legacy)
// or a lane map (v2). Everything else maps directly.
func (p *capabilityProvider) UnmarshalJSON(data []byte) error {
	var wire struct {
		ID     string                     `json:"id"`
		API    json.RawMessage            `json:"api"`
		NPM    string                     `json:"npm"`
		Models map[string]capabilityModel `json:"models"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	p.ID = wire.ID
	p.NPM = wire.NPM
	p.Models = wire.Models
	p.API = decodeProviderAPI(wire.API)
	return nil
}

// capabilityModel carries both schema generations. The legacy fields are what the
// decoders read when present; the v2 block fills in when they are not. Accessors in
// catalog_shape.go resolve the pair, so no caller branches on the generation.
type capabilityModel struct {
	ID               string                     `json:"id"`
	Provider         *capabilityModelProvider   `json:"provider"`
	Limit            *capabilityModelLimit      `json:"limit"`
	Reasoning        bool                       `json:"reasoning"`
	ToolCall         bool                       `json:"tool_call"`
	StructuredOutput bool                       `json:"structured_output"`
	Modalities       *capabilityModelModalities `json:"modalities"`
	// Legacy-only field: the reasoning field as the legacy catalog spells it.
	Interleaved *capabilityModelInterleaved `json:"interleaved"`
	// v2-only fields.
	Type         string                             `json:"type"`
	Capabilities *capabilityModelCapabilities       `json:"capabilities"`
	API          map[string]capabilityModelAPIEntry `json:"api"`
	Cost         json.RawMessage                    `json:"cost"`
}

type capabilityModelModalities struct {
	Input  []string `json:"input"`
	Output []string `json:"output"`
}

type capabilityModelLimit struct {
	Context int `json:"context"`
	Input   int `json:"input"`
	Output  int `json:"output"`
}

// Metadata carries the per-model capability fields surfaced through
// /v1/models so consumers (harnesses like Pi or jcode) get real context
// windows and feature flags from the catalog instead of guessing. It is
// purely additive: routing does not depend on any of these fields.
type Metadata struct {
	ContextWindow    int      `json:"context_window,omitempty"`
	MaxInput         int      `json:"max_input,omitempty"`
	MaxOutput        int      `json:"max_output,omitempty"`
	Reasoning        bool     `json:"reasoning,omitempty"`
	ToolCall         bool     `json:"tool_call,omitempty"`
	StructuredOutput bool     `json:"structured_output,omitempty"`
	InputModalities  []string `json:"input_modalities,omitempty"`
	OutputModalities []string `json:"output_modalities,omitempty"`
	// ReasoningField names the response key that carries reasoning for this model, taken
	// from whichever catalog generation declared it. Empty when the model publishes none.
	ReasoningField string `json:"reasoning_field,omitempty"`
	// Cost is filled from the OpenCode pricing metadata at read time, in USD per
	// million tokens.
	Cost *Cost `json:"cost,omitempty"`
}

// Cost is the published per-million-token price of a model. A nil field means
// the metadata did not state it.
type Cost struct {
	Input      *float64 `json:"input,omitempty"`
	Output     *float64 `json:"output,omitempty"`
	CacheRead  *float64 `json:"cache_read,omitempty"`
	CacheWrite *float64 `json:"cache_write,omitempty"`
}

// metadata reads whichever generation declared each field. The accessors fall back from
// the v2 block to the legacy booleans, so a source may change shape without changing what
// consumers see.
//
// Modalities are passed through as published: the legacy shape lists bare kinds ("image")
// while v2 lists media types ("image/jpeg"). That matches the client, which does not
// normalise them either.
func (m *capabilityModel) metadata(providerNPM string) Metadata {
	md := Metadata{
		Reasoning:        m.supportsReasoning(),
		ToolCall:         m.toolCall(),
		StructuredOutput: m.supportsStructuredOutput(),
	}
	if m.Modalities != nil {
		md.InputModalities = m.Modalities.Input
		md.OutputModalities = m.Modalities.Output
	}
	if m.Limit != nil {
		md.ContextWindow = m.Limit.Context
		md.MaxInput = m.Limit.Input
		md.MaxOutput = m.Limit.Output
	}
	md.ReasoningField = m.reasoningField(providerNPM)
	return md
}

type capabilityModelProvider struct {
	NPM string `json:"npm"`
}

// FetchCapabilities reads OpenCode's machine-readable provider catalog from
// endpoint (normally CapabilitiesURL). Unlike /v1/models, this source includes
// the SDK selected for each model, which is the upstream's protocol
// declaration. The Zen and Go endpoint tables then refine it. Protocol
// discovery is data-driven: no model IDs are embedded here (the fixed SOTA
// rotation list in config.SOTAModels is unrelated). It fails when the catalog
// cannot be read or yields no Zen or Go model.
func FetchCapabilities(ctx context.Context, client *http.Client, endpoint string) (Capabilities, error) {
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Capabilities{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", httpx.V2UserAgent())
	resp, err := client.Do(req)
	if err != nil {
		return Capabilities{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return Capabilities{}, fmt.Errorf("OpenCode capability endpoint returned HTTP %d", resp.StatusCode)
	}
	var providers map[string]capabilityProvider
	dec := json.NewDecoder(io.LimitReader(resp.Body, 64<<20))
	if err := dec.Decode(&providers); err != nil {
		return Capabilities{}, err
	}
	result := Capabilities{
		Protocols:   map[config.Tier]map[string]wire.Protocol{config.TierZen: {}, config.TierGo: {}},
		Unsupported: map[config.Tier]map[string]bool{config.TierZen: {}, config.TierGo: {}},
		Metadata:    map[config.Tier]map[string]Metadata{config.TierZen: {}, config.TierGo: {}},
	}
	for providerID, provider := range providers {
		tier, ok := capabilityTier(providerID, provider.API)
		if !ok {
			continue
		}
		for modelID, model := range provider.Models {
			if model.ID != "" {
				modelID = model.ID
			}
			// Resolves whichever generation declared it: the legacy shape names the SDK on
			// the provider (sometimes overridden per model), the v2 shape nests it under
			// the lane as api[shape].sdk.aisdk.
			npm := model.npm(provider.NPM)
			// Lane-first: the catalog's own lane key decides, the SDK is the fallback.
			if protocol, ok := model.protocol(npm); ok {
				result.Protocols[tier][modelID] = protocol
			} else {
				result.Unsupported[tier][modelID] = true
			}
			result.Metadata[tier][modelID] = model.metadata(npm)
		}
	}
	// The machine catalog is the primary source. The upstream endpoint tables
	// are a supplemental source for models whose provider inherits a default SDK
	// but whose published endpoint is more specific (for example a Messages
	// route), so a listed endpoint wins over the SDK and clears an unsupported
	// mark. A table that cannot be fetched is skipped. This remains
	// data-driven: no model IDs are embedded here.
	for _, doc := range []struct {
		tier config.Tier
		url  string
	}{
		{config.TierZen, ZenDocsURL},
		{config.TierGo, GoDocsURL},
	} {
		protocols, err := FetchProtocolDocs(ctx, client, doc.url)
		if err != nil {
			continue
		}
		for modelID, protocol := range protocols {
			result.Protocols[doc.tier][modelID] = protocol
			delete(result.Unsupported[doc.tier], modelID)
		}
	}
	if len(result.Protocols[config.TierZen]) == 0 && len(result.Protocols[config.TierGo]) == 0 && len(result.Unsupported[config.TierZen]) == 0 && len(result.Unsupported[config.TierGo]) == 0 {
		return Capabilities{}, errors.New("OpenCode capability endpoint returned no Zen or Go models")
	}
	return result, nil
}

// FetchProtocolDocs parses the endpoint table of an OpenCode documentation
// page into a model-to-protocol map, deriving the protocol from the endpoint
// path of each row. It fails when no row matches. A nil client uses
// http.DefaultClient.
func FetchProtocolDocs(ctx context.Context, client *http.Client, endpoint string) (map[string]wire.Protocol, error) {
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/plain, text/markdown, */*")
	req.Header.Set("User-Agent", httpx.V2UserAgent())
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("OpenCode endpoint documentation returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	result := make(map[string]wire.Protocol)
	for _, line := range strings.Split(string(body), "\n") {
		match := protocolDocEndpointPattern.FindStringSubmatch(line)
		if len(match) != 3 {
			continue
		}
		modelID := strings.TrimSpace(match[1])
		if modelID == "" || strings.ContainsAny(modelID, " `|") {
			continue
		}
		var protocol wire.Protocol
		switch match[2] {
		case "chat/completions":
			protocol = wire.Chat
		case "responses":
			protocol = wire.Responses
		case "messages":
			protocol = wire.Anthropic
		case "systemone":
			protocol = wire.SystemOne
		}
		if protocol != "" {
			result[modelID] = protocol
		}
	}
	if len(result) == 0 {
		return nil, errors.New("OpenCode endpoint documentation returned no protocol rows")
	}
	return result, nil
}

// capabilityTier maps a catalog provider to a tier by its ID and API URL: an
// opencode-go provider or a /go/ URL is Go, any other OpenCode provider or a
// /zen/ URL is Zen, and every other provider is skipped.
func capabilityTier(providerID, api string) (config.Tier, bool) {
	value := strings.ToLower(strings.TrimSpace(providerID + " " + api))
	if strings.Contains(value, "opencode-go") || strings.Contains(value, "/go/") {
		return config.TierGo, true
	}
	if strings.Contains(value, "opencode") || strings.Contains(value, "/zen/") {
		return config.TierZen, true
	}
	return "", false
}

// responsesSDKs are the SDK packages the 2.3.0 client serves through the Responses
// shape by default. It is the client's own set, from the catalog upgrade function:
//
//	NB = new Set(["@ai-sdk/openai", "@ai-sdk/azure", "@ai-sdk/xai"])
//	shape = provider.shape === "responses" || (provider.shape === undefined && NB.has(npm))
//	        ? "responses" : "chat-completions"
//
// azure and xai were missing here, so their models fell through to the unsupported
// branch and were dropped from routing even though the client serves them.
var responsesSDKs = map[string]bool{
	"@ai-sdk/openai": true,
	"@ai-sdk/azure":  true,
	"@ai-sdk/xai":    true,
}

// protocolForSDK maps the AI SDK package a model is served through to its wire
// protocol. It reports false for a package the gateway cannot speak.
//
// Note a deliberate divergence: the client defaults an unrecognised SDK to
// chat-completions, this returns unsupported so the model is left unrouted rather
// than sent down a lane that may not accept it.
func protocolForSDK(npm string) (wire.Protocol, bool) {
	value := strings.ToLower(strings.TrimSpace(npm))
	switch {
	case strings.Contains(value, "anthropic"):
		return wire.Anthropic, true
	case responsesSDKs[value] || strings.HasSuffix(value, "/openai"):
		return wire.Responses, true
	case strings.Contains(value, "openai-compatible"):
		return wire.Chat, true
	case value == "@ai-sdk/mistral":
		// Mistral's API is OpenAI-compatible, and the catalog places it on the
		// chat-completions lane. Without this the legacy catalog — which has no per-model lane
		// map — drops mistral-large-4 even though the gateway can serve it.
		return wire.Chat, true
	default:
		return "", false
	}
}

type modelsResponse struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

// FetchModels lists the model IDs at baseURL's /v1/models with key, sending
// the v1 or v2 User-Agent that matches the lane. It also returns the HTTP
// status, 0 when no response arrived, so callers can judge the proxy. An empty
// list is an error.
func FetchModels(ctx context.Context, client *http.Client, baseURL, key string) ([]string, int, error) {
	trimmed := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, trimmed+"/v1/models", nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	if IsV1Upstream(trimmed) {
		req.Header.Set("User-Agent", httpx.V1UserAgent())
	} else {
		req.Header.Set("User-Agent", httpx.V2UserAgent())
	}
	req.Header.Set("x-opencode-client", "cli")
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, resp.StatusCode, fmt.Errorf("models endpoint returned HTTP %d", resp.StatusCode)
	}
	var payload modelsResponse
	dec := json.NewDecoder(io.LimitReader(resp.Body, 8<<20))
	if err := dec.Decode(&payload); err != nil {
		return nil, resp.StatusCode, err
	}
	models := make([]string, 0, len(payload.Data))
	for _, item := range payload.Data {
		if item.ID != "" {
			models = append(models, item.ID)
		}
	}
	if len(models) == 0 {
		return nil, resp.StatusCode, errors.New("models endpoint returned an empty list")
	}
	return models, resp.StatusCode, nil
}
