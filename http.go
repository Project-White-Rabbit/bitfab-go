package bitfab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type httpClient struct {
	apiKey     string
	serviceURL string
	client     *http.Client

	transportMu sync.Mutex
	transport   traceTransport
	closed      bool
}

func newHTTPClient(apiKey, serviceURL string) *httpClient {
	return &httpClient{
		apiKey:     apiKey,
		serviceURL: serviceURL,
		client: &http.Client{
			Timeout: 120 * time.Second,
		},
	}
}

// httpStatusError is a non-2xx response. The OTLP exporter reads the status to
// decide whether a failed request is worth retrying.
type httpStatusError struct {
	StatusCode int
	Body       string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("bitfab: HTTP %d: %s", e.StatusCode, e.Body)
}

func warnForStubbedBody(dropped []string) {
	if len(dropped) == 0 {
		return
	}
	warnOnce(
		"request-body-stubbed",
		fmt.Sprintf(
			"a request body held non-serializable value(s) (e.g. %s); "+
				"they were stubbed so the span still sends, but the trace may be "+
				"incomplete or not replayable",
			strings.Join(uniqueStrings(dropped), ", "),
		),
	)
}

// apiResponseError is a 2xx response whose body reports an error. The server
// understood the request and rejected it, so retrying cannot help.
type apiResponseError struct {
	Message string
}

func (e *apiResponseError) Error() string {
	return e.Message
}

func statusCode(err error) int {
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode
	}
	return 0
}

func isRetryableStatus(err error) bool {
	var apiErr *apiResponseError
	if errors.As(err, &apiErr) {
		return false
	}
	status := statusCode(err)
	if status == 0 {
		return true
	}
	return status == http.StatusRequestTimeout ||
		status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests ||
		status >= 500
}

// request makes a single POST request to the Bitfab API and returns the parsed
// JSON response.
func (h *httpClient) request(
	ctx context.Context,
	endpoint string,
	payload map[string]any,
	timeout time.Duration,
) (map[string]any, error) {
	// Encode defensively so a stray non-encodable value can never abort the
	// send and silently drop the span. Strays are stubbed in place; a degraded
	// payload warns loudly that the trace may not be replayable.
	body, dropped := marshalPayloadSafe(payload)
	warnForStubbedBody(dropped)
	return h.send(ctx, endpoint, body, timeout)
}

// send POSTs an already-encoded body and returns the parsed JSON response.
// Callers that encode their own body use this so a payload is never encoded
// twice.
func (h *httpClient) send(
	ctx context.Context,
	endpoint string,
	body []byte,
	timeout time.Duration,
) (map[string]any, error) {
	client := h.client
	if timeout > 0 {
		client = &http.Client{Timeout: timeout}
	}

	encodedBody, contentEncoding := encodeRequestBody(body)

	req, err := http.NewRequestWithContext(ctx, "POST", h.serviceURL+endpoint, bytes.NewReader(encodedBody))
	if err != nil {
		return nil, fmt.Errorf("bitfab: failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.apiKey)
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &httpStatusError{StatusCode: resp.StatusCode, Body: string(respBody)}
	}

	result := map[string]any{}
	if json.Unmarshal(respBody, &result) == nil {
		if errMsg, ok := result["error"].(string); ok {
			if url, ok := result["url"].(string); ok {
				return nil, &apiResponseError{Message: fmt.Sprintf("%s Configure it at: %s%s", errMsg, h.serviceURL, url)}
			}
			return nil, &apiResponseError{Message: errMsg}
		}
	}
	return result, nil
}

func (h *httpClient) get(ctx context.Context, endpoint string, result any) error {
	req, err := http.NewRequestWithContext(ctx, "GET", h.serviceURL+endpoint, nil)
	if err != nil {
		return fmt.Errorf("bitfab: failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+h.apiKey)

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("bitfab: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return &httpStatusError{StatusCode: resp.StatusCode, Body: string(body)}
	}
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("bitfab: failed to decode response: %w", err)
	}
	return nil
}

// traceTransportOrNil returns this client's transport, building it on first use
// so a client that never sends a span starts no OpenTelemetry worker.
func (h *httpClient) traceTransportOrNil() traceTransport {
	h.transportMu.Lock()
	defer h.transportMu.Unlock()
	if h.closed {
		return nil
	}
	if h.transport == nil {
		h.transport = createTraceTransport(h.sendTransportRequest)
	}
	return h.transport
}

func (h *httpClient) sendTransportRequest(
	endpoint string,
	body []byte,
	timeout time.Duration,
) (map[string]any, error) {
	return h.send(context.Background(), endpoint, body, timeout)
}

// submit queues a payload on this client's trace transport. extraDropped
// carries losses detected during capture (a value the size cap stubbed) so the
// span can be marked non-replayable. The body encoded here is handed to the
// transport so the carrier attribute never re-encodes it.
func (h *httpClient) submit(operation traceOperation, payload map[string]any, extraDropped ...string) {
	merged := make(map[string]any, len(payload)+1)
	for k, v := range payload {
		merged[k] = v
	}
	merged["sdkVersion"] = Version

	merged, body, dropped := marshalSpanBody(merged, extraDropped...)
	warnForStubbedBody(dropped)

	transport := h.traceTransportOrNil()
	if transport == nil {
		warnOnce("otel-submit-after-close", "Bitfab client is closed; dropping spans")
		return
	}
	transport.submit(operation, merged, body)
}

// sendExternalSpan queues a span payload on this client's trace transport.
func (h *httpClient) sendExternalSpan(payload map[string]any, extraDropped ...string) {
	h.submit(operationExternalSpan, payload, extraDropped...)
}

// sendExternalTrace queues a trace payload on this client's trace transport.
func (h *httpClient) sendExternalTrace(payload map[string]any) {
	h.submit(operationExternalTrace, payload)
}

// flush drains this client's transport within timeout. It reports false when an
// export failed or the deadline expired.
func (h *httpClient) flush(timeout time.Duration) bool {
	h.transportMu.Lock()
	transport := h.transport
	h.transportMu.Unlock()
	if transport == nil {
		return true
	}
	return transport.flush(timeout)
}

// close flushes and permanently shuts down this client's transport. Idempotent.
func (h *httpClient) close(timeout time.Duration) bool {
	h.transportMu.Lock()
	if h.closed {
		h.transportMu.Unlock()
		return true
	}
	h.closed = true
	transport := h.transport
	h.transport = nil
	h.transportMu.Unlock()

	if transport == nil {
		return true
	}
	return transport.shutdown(timeout)
}
