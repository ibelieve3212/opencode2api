package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ibelieve3212/opencode2api/internal/httpx"
)

// errFreeTierIPBlocked reports that OpenCode's free gateway refused the request
// because it flagged the egress IP, not the request, the key or the session.
// Unlike the shape gates, another lane, key or retry cannot fix it: the same
// IP gets the same answer, and retrying only extends the penalty. The request
// must therefore leave the anonymous lane instead of walking its upstreams,
// and the exit must stay out of the cursor until the block expires.
var errFreeTierIPBlocked = errors.New("free tier blocked the egress IP")

// freeTierBlockedMessage is what a client receives while its exit is blocked.
// The upstream message tells people to turn off a VPN, which is wrong for a
// server that egresses through a proxy; this one names the real cause and the
// two ways out.
const freeTierBlockedMessage = "OpenCode is blocking free-model traffic from the IP this gateway leaves from, so retrying now cannot succeed. Route the request through another exit or wait for the block to expire."

// errAnonymousLanePaused reports that the anonymous lane's circuit breaker has
// paused the whole lane. Unlike an upstream rejection, another exit or a retry
// cannot succeed while the lane is paused; the caller should use an
// authenticated tier or report the pause itself.
var errAnonymousLanePaused = errors.New("anonymous lane paused by its circuit breaker")

// anonymousLanePausedMessage is what a client receives while the circuit
// breaker holds the anonymous lane shut. It names the cause so the operator is
// not sent looking for a credential or model problem.
const anonymousLanePausedMessage = "The anonymous free tier is paused after repeated upstream failures. Configure an authenticated key, wait for the pause to expire, or restart the gateway after confirming the upstream block has lifted."

// freeTierBlockTTL keeps a blocked exit out of the anonymous cursor. Upstream
// does not publish the block duration, so this is a conservative backoff: long
// enough to stop the retry storm, short enough that an unblocked IP recovers
// without a restart.
const freeTierBlockTTL = 15 * time.Minute

// freeTierBlockTTL returns the backoff applied to an exit the free tier flagged.
func (g *Gateway) freeTierBlockTTL() time.Duration {
	if g != nil && g.freeTierBlock > 0 {
		return g.freeTierBlock
	}
	return freeTierBlockTTL
}

// abuseProbeLimit bounds how much of a 403 body is read to classify it.
const abuseProbeLimit = 16 << 10

// isNetworkAbuseBlock reports whether status and body are the upstream's IP
// abuse rejection. The response is a 403 whose message names the egress
// network, in one of the wordings the gateway has been served:
//
//   - "…free models aren't available from your current network because we've
//     detected abusive traffic from this IP address…"
//   - "Forbidden, OpenCode Free Model Gateway Detect Abuse Traffic"
//   - "Abuse of traffic"
//
// The shape gate's 403 ("free tier can only be used from within OpenCode") is
// deliberately not matched: that one is a request-shape problem, which another
// lane or a refreshed body can still fix.
func isNetworkAbuseBlock(status int, body []byte) bool {
	if status != http.StatusForbidden {
		return false
	}
	text := strings.ToLower(string(body))
	if strings.Contains(text, "abusive traffic") ||
		strings.Contains(text, "abuse traffic") ||
		strings.Contains(text, "abuse of traffic") {
		return true
	}
	return strings.Contains(text, "free models aren't available") &&
		strings.Contains(text, "from your current network")
}

// peekResponseBody reads the start of a response body and puts it back in
// front of the rest, so classification and downstream error handling both see
// the full payload.
func peekResponseBody(resp *http.Response) []byte {
	if resp == nil || resp.Body == nil {
		return nil
	}
	prefix, err := io.ReadAll(io.LimitReader(resp.Body, abuseProbeLimit))
	resp.Body = replayBody(prefix, resp.Body)
	if err != nil {
		return nil
	}
	return prefix
}

// bufferRetryResponse releases a response's transport before the next attempt,
// retaining at most 64 KiB of its body, including any prefix read before an error.
func bufferRetryResponse(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(body))
}

// replaceRetryResponse keeps the latest HTTP response across transport failures
// and closes a superseded response before giving ownership to its replacement.
func replaceRetryResponse(previous, current *http.Response) *http.Response {
	if current == nil {
		return previous
	}
	if previous != nil && previous != current {
		httpx.DrainAndClose(previous.Body)
	}
	return current
}

// peekNetworkAbuseBlock classifies a response without consuming its body.
func peekNetworkAbuseBlock(resp *http.Response) bool {
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		return false
	}
	return isNetworkAbuseBlock(resp.StatusCode, peekResponseBody(resp))
}

// errDeterministicClientRejection reports that the upstream rejected the
// request itself: the body is malformed, too long for the model, or fails the
// same validation on every credential. Retrying on another key, lane or tier
// cannot change that, so the request must end and the original 4xx must reach
// the client. Without this, the first tier's real error is replaced by
// whatever the next tier answers — typically a 401 from an unconfigured key,
// which tells the client to re-authenticate instead of compacting.
var errDeterministicClientRejection = errors.New("upstream rejected the request deterministically")

// deterministicMarkers are the error types and codes an upstream uses for a
// request that will fail identically everywhere. The status must be 400 or 422
// as well: a 401, 403 or 429 can be specific to the credential or the moment.
var deterministicMarkers = []string{
	"invalid_request_error",
	"invalid_request",
	"context_length_exceeded",
	"maximum context length",
	"string_above_max_length",
	"prompt is too long",
}

// isDeterministicClientResponse reports whether status and body describe a
// request-level rejection rather than a credential or availability problem.
//
// A retired model belongs here. The upstream answers HTTP 410 ModelDeprecated for a
// withdrawn model, naming its replacement:
//
//	{"id":"minimax-m3.1-flash-free","status":410,"type":"ModelDeprecated",
//	 "message":"Model minimax-m3.1-flash-free has been deprecated. Use big-pickle instead.",
//	 "metadata":{"model":"minimax-m3.1-flash-free","replacement":"big-pickle"}}
//
// Rotating an exit, a key or a lane can never serve that model, so the request must
// end and the upstream's own document must reach the client. Treating it as
// retryable instead makes every caller walk its whole attempt budget on a model that
// is gone, which also extends the window it looks blocked for.
func isDeterministicClientResponse(status int, body []byte) bool {
	if _, retired := retiredModelFrom(status, body); retired {
		return true
	}
	// The transient endpoint fault belongs on this path too, despite not being
	// "deterministic" in the usual sense. The upstream sends it under BOTH 429 and 403
	// with a byte-identical body, and it clears on its own; walking the attempt budget
	// on it spends credentials and exits that cannot change the answer. Sending the
	// upstream document straight through also keeps the form visible to the caller,
	// instead of replacing it with whichever credential's 401 came next.
	if isEndpointUnavailable(status, body) {
		return true
	}
	if status != http.StatusBadRequest && status != http.StatusUnprocessableEntity {
		return false
	}
	text := strings.ToLower(string(body))
	for _, marker := range deterministicMarkers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

// endpointUnavailableMarker is the message the upstream uses for a transient fault. It
// arrives under both 429 and 403, so the status cannot be the predicate: a rule keyed
// on 429 sends it down the quota path, and a rule that reads every 403 as a shape
// problem passes it to the client as though the request were at fault.
//
// Note what deliberately is NOT the predicate: the surrounding `type: "server_error"`.
// The same type carries `Model is unavailable`, which is a model that is gone for good.
const endpointUnavailableMarker = "Endpoint is unavailable"

// isEndpointUnavailable reports the transient upstream fault from either status it uses.
func isEndpointUnavailable(status int, body []byte) bool {
	if status != http.StatusTooManyRequests && status != http.StatusForbidden {
		return false
	}
	return strings.Contains(string(body), endpointUnavailableMarker)
}

// retiredModel is the structured withdrawal the upstream returns with HTTP 410.
type retiredModel struct {
	Model       string
	Replacement string
	Message     string
}

// retiredModelFrom reads a 410 ModelDeprecated document. The replacement is worth
// keeping rather than flattening into a generic 4xx: it is the model the caller
// should switch to, and the upstream answers 410 rather than 404 so that the
// migration target survives the trip.
func retiredModelFrom(status int, body []byte) (retiredModel, bool) {
	if status != http.StatusGone {
		return retiredModel{}, false
	}
	var payload struct {
		Type     string `json:"type"`
		Message  string `json:"message"`
		Metadata struct {
			Model       string `json:"model"`
			Replacement string `json:"replacement"`
		} `json:"metadata"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return retiredModel{}, false
	}
	if payload.Type != "ModelDeprecated" && payload.Metadata.Replacement == "" {
		return retiredModel{}, false
	}
	return retiredModel{
		Model:       payload.Metadata.Model,
		Replacement: payload.Metadata.Replacement,
		Message:     payload.Message,
	}, true
}

// replayBody returns a body that yields prefix and then the rest of original.
func replayBody(prefix []byte, original io.ReadCloser) io.ReadCloser {
	return &prefixBody{Reader: io.MultiReader(bytes.NewReader(prefix), original), closer: original}
}

type prefixBody struct {
	io.Reader
	closer io.Closer
}

func (b *prefixBody) Close() error { return b.closer.Close() }
