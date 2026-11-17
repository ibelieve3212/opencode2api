package models

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/ibelieve3212/opencode2api/internal/jsonutil"
	wire "github.com/ibelieve3212/opencode2api/internal/protocol"
)

// This file adapts the two catalog schema generations the OpenCode model catalog has
// shipped in, so the rest of the package reads one shape.
//
// The client itself does exactly this conversion. In chunk-c2xa869t.js the release
// binary upgrades a legacy entry to its internal form:
//
//	NB = new Set(["@ai-sdk/openai", "@ai-sdk/azure", "@ai-sdk/xai"])
//	c  = provider.shape === "responses" || (provider.shape === undefined && NB.has(npm))
//	     ? "responses" : "chat-completions"
//	return {
//	  id, type: e.type ?? "chat", name, family, release_date, status,
//	  modalities: e.modalities ?? { input: ["text"], output: ["text"] },
//	  capabilities: {
//	    tools: { supported: e.tool_call ?? false },
//	    reasoning: UB(e),
//	    temperature: e.temperature,
//	  },
//	  limit: e.limit,
//	  cost: e.cost && q5(e.cost),
//	  api: { [c]: { base_url: e.provider?.api ?? a.api ?? "", sdk: { aisdk: s } } },
//	}
//
// The fields below are the two shapes side by side:
//
//	                 legacy (/api.json)          v2 (/experimental/v2.0)
//	cost             cost.input, cost.cache_read cost.text.input.price, .cache.read
//	tools            tool_call: true             capabilities.tools.supported
//	reasoning        reasoning: true             capabilities.reasoning.supported
//	structured out   structured_output: true     capabilities.structured_output
//	provider api     provider.api (string)       model.api[shape].base_url (map)
//	provider sdk     provider.npm (string)       model.api[shape].sdk.aisdk (map)
//
// Reading both is not an upgrade for its own sake: the legacy endpoint without the
// client's type filter omits models the release build itself knew about, and the v2
// endpoint carries seven more chat-relevant ones. Consuming both means the source can
// change without touching protocol detection or pricing.

// capabilityModelCapabilities is the v2 capability block. It replaces the legacy
// top-level booleans.
type capabilityModelCapabilities struct {
	Tools            *capabilityModelToolSupport `json:"tools"`
	Reasoning        *capabilityModelReasoning   `json:"reasoning"`
	StructuredOutput bool                        `json:"structured_output"`
	Temperature      bool                        `json:"temperature"`
}

type capabilityModelToolSupport struct {
	Supported bool `json:"supported"`
}

// capabilityModelReasoning mirrors the descriptor the client's UB() builds from the
// legacy reasoning_options list. Supported is what routing reads; the rest is kept so
// an effort or budget setting survives the trip.
type capabilityModelReasoning struct {
	Supported bool     `json:"supported"`
	Toggle    bool     `json:"toggle"`
	Effort    []string `json:"effort"`
	Budget    *struct {
		Min *float64 `json:"min"`
		Max *float64 `json:"max"`
	} `json:"budget"`
}

// capabilityModelAPIEntry is one lane in the v2 api map: the base URL and the SDK that
// speaks it. The map key is the shape ("chat-completions" or "responses").
type capabilityModelAPIEntry struct {
	BaseURL string `json:"base_url"`
	SDK     *struct {
		AISDK      string `json:"aisdk"`
		OpenCodeAI string `json:"opencodeai"`
	} `json:"sdk"`
	Compatibility *struct {
		ReasoningField string `json:"reasoning_field"`
	} `json:"compatibility"`
}

// providerShapeKeys orders the v2 lanes when a model offers several. chat-completions
// is the shape the /inference OpenAI lane speaks, so it wins; otherwise the first key
// in a stable order is used.
var providerShapeKeys = []string{"chat-completions", "responses", "messages"}

// shapeEntry picks the lane a v2 model should be read through.
func (m *capabilityModel) shapeEntry() (shape string, entry *capabilityModelAPIEntry) {
	if len(m.API) == 0 {
		return "", nil
	}
	for _, key := range providerShapeKeys {
		if candidate, ok := m.API[key]; ok {
			return key, candidate
		}
	}
	// Unknown lane names still carry a base URL; take one deterministically.
	keys := make([]string, 0, len(m.API))
	for key := range m.API {
		keys = append(keys, key)
	}
	sortStrings(keys)
	key := keys[0]
	return key, m.API[key]
}

// baseURL reports the lane URL from whichever shape declared it.
func (m *capabilityModel) baseURL() string {
	if _, entry := m.shapeEntry(); entry != nil {
		return strings.TrimSpace(entry.BaseURL)
	}
	return ""
}

// npm reports the SDK package that serves this model. The legacy shape names it on the
// provider (sometimes overridden per model); the v2 shape nests it under the lane.
func (m *capabilityModel) npm(providerNPM string) string {
	if m.Provider != nil && strings.TrimSpace(m.Provider.NPM) != "" {
		return m.Provider.NPM
	}
	if _, entry := m.shapeEntry(); entry != nil && entry.SDK != nil && entry.SDK.AISDK != "" {
		return entry.SDK.AISDK
	}
	return providerNPM
}

// toolCall reports whether the model accepts tool definitions.
func (m *capabilityModel) toolCall() bool {
	if m.Capabilities != nil && m.Capabilities.Tools != nil {
		return m.Capabilities.Tools.Supported
	}
	return m.ToolCall
}

// supportsReasoning reports whether the model exposes reasoning.
func (m *capabilityModel) supportsReasoning() bool {
	if m.Capabilities != nil && m.Capabilities.Reasoning != nil {
		return m.Capabilities.Reasoning.Supported
	}
	return m.Reasoning
}

// supportsStructuredOutput reports whether the model accepts a structured output schema.
func (m *capabilityModel) supportsStructuredOutput() bool {
	if m.Capabilities != nil {
		return m.Capabilities.StructuredOutput
	}
	return m.StructuredOutput
}

// interleaved is the legacy spelling of the reasoning field. It is either a string, a
// boolean, or an object with a `field` key, and the client collapses all three:
//
//	typeof e === "string"  ? e
//	: typeof e === "object" && "field" in e ? e.field
//	: undefined
//
// A bare `true` names no field: it only says reasoning is interleaved, so it yields the
// default name rather than a value.
type capabilityModelInterleaved struct {
	Field   string
	Boolean bool
}

// UnmarshalJSON accepts the string, boolean and object spellings the catalog uses.
func (i *capabilityModelInterleaved) UnmarshalJSON(data []byte) error {
	var asString string
	if json.Unmarshal(data, &asString) == nil {
		i.Field = strings.TrimSpace(asString)
		return nil
	}
	var asBool bool
	if json.Unmarshal(data, &asBool) == nil {
		i.Boolean = asBool
		return nil
	}
	var asObject struct {
		Field string `json:"field"`
	}
	if err := json.Unmarshal(data, &asObject); err != nil {
		return err
	}
	i.Field = strings.TrimSpace(asObject.Field)
	return nil
}

// reasoningField reports the response key that carries reasoning for this model, and only
// on the lane where such a key exists.
//
// Measured across the v2 catalog, reasoning_field is declared on the chat-completions lane
// (50 models) and on no other: the Responses lane carries reasoning as a structured object,
// the Anthropic Messages lane as thinking blocks, and generate-content, systemone, images
// and live declare none. The legacy catalog sets `interleaved` without regard to lane, so
// reading it unconditionally reports a field the model does not have — which is how
// claude-sonnet-4-5, a Messages model, seemed to disagree between generations.
//
// The gate is the wire protocol resolved from the SDK, so both generations answer alike.
func (m *capabilityModel) reasoningField(providerNPM string) string {
	protocol, known := m.protocol(providerNPM)
	if !known || protocol != wire.Chat {
		return ""
	}
	if _, entry := m.shapeEntry(); entry != nil && entry.Compatibility != nil {
		if field := strings.TrimSpace(entry.Compatibility.ReasoningField); field != "" {
			return field
		}
	}
	if m.Interleaved != nil {
		if field := strings.TrimSpace(m.Interleaved.Field); field != "" {
			return field
		}
		if m.Interleaved.Boolean {
			return defaultReasoningField
		}
	}
	return ""
}

// defaultReasoningField is the key the client falls back to when a model is interleaved but
// does not name a field (chunk-a3k2v7ew.js computes "reasoning_content" on that path).
const defaultReasoningField = "reasoning_content"

// laneProtocol maps a v2 lane name to its wire protocol. The three chat-bearing lanes are
// the only ones this gateway speaks; every other lane is a different API shape entirely.
func laneProtocol(lane string) (wire.Protocol, bool) {
	switch strings.ToLower(strings.TrimSpace(lane)) {
	case "chat-completions":
		return wire.Chat, true
	case "responses":
		return wire.Responses, true
	case "messages":
		return wire.Anthropic, true
	default:
		// generate-content, systemone, images and live are real lanes with their own request
		// shapes, which this gateway does not implement.
		return "", false
	}
}

// protocol resolves the wire protocol for a model, preferring the lane the catalog names.
//
// Lane-first matters because the SDK alone is not decisive. Measured against the live v2
// catalog: @ai-sdk/mistral serves the chat-completions lane but no SDK rule recognises it,
// and @ai-sdk/xai and @ai-sdk/openai serve an images lane that the Responses rule would
// otherwise claim. A model's own lane key is the upstream's declaration; the SDK is the
// fallback for the legacy catalog, which has no per-model lane map.
func (m *capabilityModel) protocol(providerNPM string) (wire.Protocol, bool) {
	if lane, _ := m.shapeEntry(); lane != "" {
		return laneProtocol(lane)
	}
	return protocolForSDK(m.npm(providerNPM))
}

// modelType reports the v2 type tag, defaulting to chat the way the client does.
func (m *capabilityModel) modelType() string {
	if strings.TrimSpace(m.Type) != "" {
		return m.Type
	}
	return "chat"
}

// decodeProviderAPI reads the provider-level api field, which is a plain string in the
// legacy shape and absent (or a lane map) in v2.
func decodeProviderAPI(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var asString string
	if json.Unmarshal(raw, &asString) == nil {
		return strings.TrimSpace(asString)
	}
	var asMap map[string]capabilityModelAPIEntry
	if json.Unmarshal(raw, &asMap) != nil {
		return ""
	}
	for _, key := range providerShapeKeys {
		if entry, ok := asMap[key]; ok {
			return strings.TrimSpace(entry.BaseURL)
		}
	}
	for _, entry := range asMap {
		return strings.TrimSpace(entry.BaseURL)
	}
	return ""
}

// sortStrings is a tiny insertion sort so this file needs no extra import.
func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

// readCost reads a model's price and any context-length overrides from either catalog
// generation. It mirrors what the client's q5 normaliser produces when it upgrades an
// entry, in the opposite direction.
//
//	legacy  { "input": 0, "output": 0, "cache_read": 0, "cache_write": 0,
//	          "tiers": [ { "input": …, "output": …, "tier": { "size": 272000 } } ],
//	          "context_over_200k": { … } }
//	v2      { "text": { "input": { "price": 0, "cache": { "read": 0,
//	                      "write": { "30m": 0.25, "12h": 0.5 } },
//	                      "overrides": [ { "when": { "context": { "gt": 272000 } },
//	                                       "price": 0.4, "cache": { "read": 0.04 } } ] },
//	                    "output": { "price": 0, "overrides": [ … ] } } }
//
// Three details that a base-only read gets wrong on live data:
//
//   - context overrides. 18 of 142 legacy and 19 of 149 v2 models price by context length,
//     and the override can be double the base, so stopping at the base understates a long
//     request. Both spellings are collected into the same tier list.
//   - cache.write is a scalar in the legacy shape but a TTL map in v2 on 8 models
//     ({ "30m": …, "12h": … }). The shortest TTL is kept as the representative write price.
//   - the v2 form keys cost by modality. "text" is used, and the first modality present
//     otherwise, so an entry is never silently unpriced.
func readCost(cost map[string]any) priceParts {
	if len(cost) == 0 {
		return priceParts{}
	}

	// Legacy: the price sits flat on the cost object and the overrides live under "tiers".
	if _, nested := cost["text"]; !nested {
		flat := false
		for _, key := range []string{"input", "output", "cache_read", "cache_write"} {
			if _, ok := cost[key]; ok {
				flat = true
				break
			}
		}
		if flat {
			return priceParts{
				Input:       numberPointer(cost, "input"),
				Output:      numberPointer(cost, "output"),
				CacheRead:   numberPointer(cost, "cache_read"),
				CacheWrite:  numberPointer(cost, "cache_write"),
				Tiers:       legacyTiers(jsonutil.SliceAt(cost, "tiers")),
				Multipliers: readMultipliers(jsonutil.SliceAt(cost, "multipliers")),
			}
		}
	}

	block := jsonutil.MapAt(cost, "text")
	if len(block) == 0 {
		keys := make([]string, 0, len(cost))
		for key := range cost {
			keys = append(keys, key)
		}
		sortStrings(keys)
		for _, key := range keys {
			if candidate := jsonutil.MapAt(cost, key); len(candidate) > 0 {
				block = candidate
				break
			}
		}
	}
	if len(block) == 0 {
		return priceParts{}
	}

	inputBlock := jsonutil.MapAt(block, "input")
	outputBlock := jsonutil.MapAt(block, "output")
	cache := jsonutil.MapAt(inputBlock, "cache")
	return priceParts{
		Input:       numberPointer(inputBlock, "price"),
		Output:      numberPointer(outputBlock, "price"),
		CacheRead:   numberPointer(cache, "read"),
		CacheWrite:  cacheWritePrice(cache["write"]),
		Tiers:       mergeOverrides(jsonutil.SliceAt(inputBlock, "overrides"), jsonutil.SliceAt(outputBlock, "overrides")),
		Multipliers: readMultipliers(jsonutil.SliceAt(cost, "multipliers")),
	}
}

// readMultipliers reads cost.multipliers, a conditional price factor.
//
// The schema (chunk-c2xa869t.js) permits a factor that is a number or an object keyed by a
// price path, and four condition kinds. Measured over the binary's embedded snapshot: 421
// entries — service_tier 432 conditions, region 14, date 23, utc_hours 8 — while the live
// catalog carries only five, all date with a numeric factor. A reader that kept just the date
// and a number recorded nil for the other 400-odd.
//
// One important limit: the client applies these at request time only when the single condition
// is service_tier (cF looks up E.when.service_tier explicitly). Date, region and utc_hours
// entries are carried but not applied by that path, so they are recorded here rather than
// treated as a price change.
func readMultipliers(entries []any) []PriceMultiplier {
	multipliers := make([]PriceMultiplier, 0, len(entries))
	for _, raw := range entries {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		multiplier := PriceMultiplier{
			Factor:       numberPointer(entry, "factor"),
			FactorByPath: factorByPath(entry["factor"]),
		}
		// A condition is { equals: <value> } or, for date and utc_hours, a bound.
		//
		// Only a single-condition entry is resolved. The client tests
		// Object.keys(when).length === 1 before matching anything, so an entry with several
		// conditions is one it would never apply — and ranging over the map to take the first key
		// would make this reader answer differently on two runs. Such an entry keeps its factor
		// and is left with no condition.
		if when, ok := entry["when"].(map[string]any); ok && len(when) == 1 {
			for kind, condition := range when {
				multiplier.When = kind
				if block, ok := condition.(map[string]any); ok {
					if value, ok := block["equals"]; ok {
						multiplier.Equals = scalarString(value)
					}
					multiplier.Bound = scalarString(block["gte"])
				}
			}
		}
		multipliers = append(multipliers, multiplier)
	}
	return multipliers
}

// factorByPath flattens a factor that is keyed by price path, such as {"text": 2} or
// {"text": {"input": {"price": 1.1}}}, keeping the first numeric leaf. The client walks the
// path it is pricing and falls back to 1 when it finds nothing, so a flat map is enough to
// report what was published.
func factorByPath(value any) map[string]float64 {
	block, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	out := map[string]float64{}
	var walk func(prefix string, node any)
	walk = func(prefix string, node any) {
		inner, ok := node.(map[string]any)
		if !ok {
			if f, ok := node.(float64); ok && prefix != "" {
				out[prefix] = f
			}
			return
		}
		for key, child := range inner {
			next := key
			if prefix != "" {
				next = prefix + "." + key
			}
			walk(next, child)
		}
	}
	walk("", block)
	if len(out) == 0 {
		return nil
	}
	return out
}

// scalarString renders a JSON scalar for the condition fields.
func scalarString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		return ""
	}
}

// legacyTiers reads the legacy "tiers" list, where each entry carries the full price set
// and names its threshold under tier.size.
func legacyTiers(entries []any) []PriceTier {
	tiers := make([]PriceTier, 0, len(entries))
	for _, raw := range entries {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		tier := jsonutil.MapAt(entry, "tier")
		tiers = append(tiers, PriceTier{
			AboveContext: jsonutil.IntAt(tier, "size"),
			Input:        numberPointer(entry, "input"),
			Output:       numberPointer(entry, "output"),
			CacheRead:    numberPointer(entry, "cache_read"),
			CacheWrite:   numberPointer(entry, "cache_write"),
		})
	}
	return tiers
}

// mergeOverrides joins the v2 input and output override lists, which are separate arrays
// keyed by the same context threshold.
func mergeOverrides(input, output []any) []PriceTier {
	byThreshold := map[int]*PriceTier{}
	order := []int{}
	claim := func(threshold int) *PriceTier {
		if existing, ok := byThreshold[threshold]; ok {
			return existing
		}
		byThreshold[threshold] = &PriceTier{AboveContext: threshold}
		order = append(order, threshold)
		return byThreshold[threshold]
	}
	for _, raw := range input {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		threshold := jsonutil.IntAt(jsonutil.MapAt(entry, "when"), "context", "gt")
		tier := claim(threshold)
		tier.Input = numberPointer(entry, "price")
		cache := jsonutil.MapAt(entry, "cache")
		tier.CacheRead = numberPointer(cache, "read")
		tier.CacheWrite = cacheWritePrice(cache["write"])
	}
	for _, raw := range output {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		threshold := jsonutil.IntAt(jsonutil.MapAt(entry, "when"), "context", "gt")
		claim(threshold).Output = numberPointer(entry, "price")
	}
	tiers := make([]PriceTier, 0, len(order))
	for _, threshold := range order {
		tiers = append(tiers, *byThreshold[threshold])
	}
	return tiers
}

// cacheWritePrice reads cost.text.input.cache.write, which the catalog publishes in two
// shapes: a scalar, or a map of hold time to price.
//
// The client's P5 takes Object.values(write)[0] — the first entry in document order. Go maps do
// not preserve order, so this picks by a short-TTL preference instead. Measured over the live
// catalog every published map is {"30m": x, "12h": 2x} and the two rules agree on all 8; they
// would diverge only if a catalog ever listed the longer hold time first.
func cacheWritePrice(value any) *float64 {
	if value == nil {
		return nil
	}
	if number, ok := value.(float64); ok {
		return &number
	}
	byTTL, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	keys := make([]string, 0, len(byTTL))
	for key := range byTTL {
		keys = append(keys, key)
	}
	sortStrings(keys)
	for _, preferred := range []string{"30m", "1h", "5m"} {
		if number, ok := byTTL[preferred].(float64); ok {
			return &number
		}
	}
	for _, key := range keys {
		if number, ok := byTTL[key].(float64); ok {
			return &number
		}
	}
	return nil
}
