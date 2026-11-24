package httpx

// v2ClientVersion is the OpenCode 2.x CLI release the gateway claims to be on
// the v2 lanes (/inference and /zen/go). Upstream accepts a range of releases
// (the version number itself is not validated), so this only needs bumping
// when it starts rejecting older ones.
//
// Ground truth for the installed client: 2.4.1 builds its User-Agent as
// `opencode/${channel}/${version}/${name}` (packages/core/src/app.ts), with
// name defaulting to the build artefact, which is `cli` for CLI builds
// (packages/cli/src/version.ts), giving `opencode/latest/2.4.1/cli`.
//
// Diffing 2.3.2 against 2.4.1 shows the wire shape is unchanged: the header
// block, tool schemas and system prompt are identical and model-request.ts
// differs only in formatting. So the version string is the whole fingerprint
// delta; bump it whenever the released CLI moves.
const v2ClientVersion = "2.4.1"

// v1ClientVersion is the OpenCode 1.x release the gateway claims to be on the
// v1 lane (/zen), and v1BunVersion is the Bun runtime that release ships with.
// Bump them together with the provider-utils versions below, all taken from a
// captured request of the same release.
//
// Read off the published 1.19.6 linux-x64 binary: it is compiled with Bun
// 1.4.4 (was 1.3.14 in 1.19.3), and the AI SDK's runtime suffix is
// `runtime/${navigator.userAgent.toLowerCase()}`, so the client now reports
// `runtime/bun/1.4.4`. Every other wire detail is unchanged: the thirteen
// bundled provider-utils versions are byte-identical between 1.19.3 and
// 1.19.6, and the x-opencode-* header set only loses x-opencode-title (which
// this gateway never sent on the v1 lane).
const (
	v1ClientVersion = "1.19.6"
	v1BunVersion    = "1.4.4"
)

// The AI SDK provider packages of OpenCode 1.19.6 each bundle their own
// provider-utils, whose version ends up in the User-Agent of every v1 model
// request, so the version depends on the wire protocol.
const (
	// V1ChatSDK is bundled by @ai-sdk/openai-compatible (chat completions).
	V1ChatSDK = "4.0.23"
	// V1ResponsesSDK is bundled by @ai-sdk/openai (responses).
	V1ResponsesSDK = "4.0.40"
	// V1AnthropicSDK is bundled by @ai-sdk/anthropic (messages).
	V1AnthropicSDK = "4.0.46"
)

// V2UserAgent reports the gateway as an OpenCode 2.x CLI using the shape the
// v2 gate expects: "opencode/<channel>/<version>/<name>".
func V2UserAgent() string {
	return "opencode/latest/" + v2ClientVersion + "/cli"
}

// V1UserAgent is the OpenCode 1.x client's own User-Agent, a bare
// "opencode/<version>", which it sends as-is on requests that bypass the AI
// SDK. The earlier "(<os> <arch>; <runtime>)" suffix carried the gateway's Go
// runtime, which no real OpenCode client sends.
func V1UserAgent() string {
	return "opencode/" + v1ClientVersion
}

// V1ModelUserAgent is the User-Agent of a v1 model request: the AI SDK appends
// the provider-utils version of the package serving the request and the
// runtime to the client's own User-Agent, e.g.
// "opencode/1.19.6 ai-sdk/provider-utils/4.0.23 runtime/bun/1.4.4".
func V1ModelUserAgent(providerUtils string) string {
	return V1UserAgent() + " ai-sdk/provider-utils/" + providerUtils + " runtime/bun/" + v1BunVersion
}
