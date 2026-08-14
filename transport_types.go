package bitfab

import "time"

type traceOperation string

const (
	operationExternalSpan  traceOperation = "external_span"
	operationExternalTrace traceOperation = "external_trace"
	operationInternalTrace traceOperation = "internal_trace"
)

// directBatchSender posts one fully-encoded request body to a Bitfab endpoint
// and returns the parsed JSON response. Supplied by httpClient so the transport
// reuses the client's auth, base URL, and error semantics rather than opening
// its own network path.
//
// The body arrives already encoded: the exporter assembles it from per-span
// encodes it has to produce anyway to size a request, so handing over a map
// here would encode the same batch a second time.
type directBatchSender func(endpoint string, request preparedRequest, timeout time.Duration) (map[string]any, error)

// traceTransport is the boundary every instrumentation path crosses to hand a
// Bitfab payload to the network.
type traceTransport interface {
	// submit queues a payload. encoded is the caller's already-encoded body,
	// reused verbatim as the carrier attribute so a span is never encoded
	// twice; nil asks the transport to encode payload itself. Never panics;
	// delivery failures degrade silently.
	submit(operation traceOperation, payload map[string]any, encoded []byte)
	// flush drains the queue within timeout. False on export failure or timeout.
	flush(timeout time.Duration) bool
	// shutdown flushes, then permanently stops this transport.
	shutdown(timeout time.Duration) bool
}
