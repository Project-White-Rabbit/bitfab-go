package bitfab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var carrierSubmissionSequence atomic.Uint64

type httpClient struct {
	apiKey     string
	serviceURL string
	client     *http.Client

	transportMu sync.Mutex
	transport   traceTransport
	closed      bool

	deliveryMu      sync.Mutex
	traceDeliveries map[string]*traceDelivery
}

type traceDelivery struct {
	submittedSpanIDs map[string]struct{}
	ackedSpanIDs     map[string]struct{}
	closed           bool
	closingAcked     bool
	serverTraceID    string
}

type deliveryReport struct {
	spanCount     int
	closed        bool
	delivered     bool
	serverTraceID string
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
	// How long the server asked us to wait, when it said so.
	RetryAfter time.Duration
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

// Retry-After comes in two forms: whole seconds, or an HTTP date. Anything
// else, including a date already in the past, means the server did not give us
// a wait we can act on.
func parseRetryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}
	if seconds, err := strconv.ParseFloat(header, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds * float64(time.Second))
	}
	when, err := http.ParseTime(header)
	if err != nil {
		return 0
	}
	if wait := time.Until(when); wait > 0 {
		return wait
	}
	return 0
}

func retryAfter(err error) time.Duration {
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.RetryAfter
	}
	return 0
}

// OTLP's retryable set, plus 500. Every other 4xx is the server's verdict on
// the payload and will be the same next time.
//
// 500 is a deliberate deviation: OTLP treats it as the app being broken, which
// assumes a collector that fails deterministically. Bitfab ingestion answers
// every unhandled error with 500, so a connection blip or a cold start arrives
// here indistinguishable from a real fault, and giving up on the first one
// drops spans a second attempt would deliver.
func isRetryableStatus(err error) bool {
	var apiErr *apiResponseError
	if errors.As(err, &apiErr) {
		return false
	}
	switch statusCode(err) {
	case 0, http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
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
	return h.sendPrepared(ctx, endpoint, prepareRequestBody(body), timeout)
}

func (h *httpClient) sendPrepared(
	ctx context.Context,
	endpoint string,
	prepared preparedRequest,
	timeout time.Duration,
) (map[string]any, error) {
	client := h.client
	if timeout > 0 {
		client = &http.Client{Timeout: timeout}
	}

	req, err := http.NewRequestWithContext(ctx, "POST", h.serviceURL+endpoint, bytes.NewReader(prepared.body))
	if err != nil {
		return nil, fmt.Errorf("bitfab: failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.apiKey)
	if prepared.contentEncoding != "" {
		req.Header.Set("Content-Encoding", prepared.contentEncoding)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &httpStatusError{
			StatusCode: resp.StatusCode,
			Body:       string(respBody),
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
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
		return &httpStatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
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
		h.transport = createTraceTransport(h.sendTransportRequest, h.recordDeliveredCarriers)
	}
	return h.transport
}

func (h *httpClient) sendTransportRequest(
	endpoint string,
	request preparedRequest,
	timeout time.Duration,
) (map[string]any, error) {
	response, err := h.sendPrepared(context.Background(), endpoint, request, timeout)
	if err == nil {
		h.recordServerTraceIDs(response)
	}
	return response, err
}

// submit queues a payload on this client's trace transport. extraDropped
// carries losses detected during capture (a value the size cap stubbed) so the
// span can be marked non-replayable. The body encoded here is handed to the
// transport so the carrier attribute never re-encodes it.
func (h *httpClient) submit(operation traceOperation, payload map[string]any, meta carrierMeta, extraDropped ...string) {
	merged := make(map[string]any, len(payload)+1)
	for k, v := range payload {
		merged[k] = v
	}
	merged["sdkVersion"] = Version

	merged, body, dropped := marshalSpanBodyWithLimit(
		merged,
		maxCompressibleSpanCarrierBytes,
		extraDropped...,
	)
	warnForStubbedBody(dropped)

	transport := h.traceTransportOrNil()
	if transport == nil {
		warnOnce("otel-submit-after-close", "Bitfab client is closed; dropping spans")
		return
	}
	transport.submit(operation, merged, body, meta)
}

// sendExternalSpan queues a span payload on this client's trace transport.
func (h *httpClient) sendExternalSpan(payload map[string]any, extraDropped ...string) {
	ref := carrierRefForPayload(payload)
	h.recordSubmittedCarrier(ref)
	h.submit(operationExternalSpan, payload, carrierMeta{ref: ref}, extraDropped...)
}

func carrierRefForPayload(payload map[string]any) *carrierRef {
	traceID, _ := payload["sourceTraceId"].(string)
	if traceID == "" {
		for _, field := range []string{"externalTrace", "rawTrace"} {
			rawTrace, _ := payload[field].(map[string]any)
			if rawTrace != nil {
				traceID, _ = rawTrace["id"].(string)
			}
			if traceID != "" {
				break
			}
		}
	}
	if traceID == "" {
		return nil
	}
	ref := &carrierRef{traceID: traceID}
	if rawSpan, _ := payload["rawSpan"].(map[string]any); rawSpan != nil {
		ref.spanID, _ = rawSpan["id"].(string)
		if ref.spanID == "" {
			ref.spanID = fmt.Sprintf("submission-%d", carrierSubmissionSequence.Add(1))
		}
	}
	return ref
}

func (h *httpClient) recordSubmittedCarrier(ref *carrierRef) {
	if ref == nil {
		return
	}
	h.deliveryMu.Lock()
	defer h.deliveryMu.Unlock()
	delivery := h.traceDeliveries[ref.traceID]
	if delivery == nil {
		return
	}
	if ref.spanID == "" {
		delivery.closed = true
		return
	}
	delivery.submittedSpanIDs[ref.spanID] = struct{}{}
}

func (h *httpClient) recordDeliveredCarriers(refs []carrierRef) {
	h.deliveryMu.Lock()
	defer h.deliveryMu.Unlock()
	for _, ref := range refs {
		delivery := h.traceDeliveries[ref.traceID]
		if delivery == nil {
			continue
		}
		if ref.spanID == "" {
			delivery.closingAcked = true
		} else {
			delivery.ackedSpanIDs[ref.spanID] = struct{}{}
		}
	}
}

func (h *httpClient) recordServerTraceIDs(response map[string]any) {
	if response == nil {
		return
	}
	raw, ok := response["traceIds"]
	if !ok {
		return
	}
	traceIDs := make(map[string]string)
	switch values := raw.(type) {
	case map[string]any:
		for sourceTraceID, value := range values {
			if serverTraceID, ok := value.(string); ok {
				traceIDs[sourceTraceID] = serverTraceID
			}
		}
	case map[string]string:
		traceIDs = values
	}
	if len(traceIDs) == 0 {
		return
	}
	h.deliveryMu.Lock()
	defer h.deliveryMu.Unlock()
	for sourceTraceID, serverTraceID := range traceIDs {
		if delivery := h.traceDeliveries[sourceTraceID]; delivery != nil {
			delivery.serverTraceID = serverTraceID
		}
	}
}

func (h *httpClient) trackTraceDeliveries(traceIDs []string) {
	h.deliveryMu.Lock()
	defer h.deliveryMu.Unlock()
	if h.traceDeliveries == nil {
		h.traceDeliveries = make(map[string]*traceDelivery, len(traceIDs))
	}
	for _, traceID := range traceIDs {
		if h.traceDeliveries[traceID] == nil {
			h.traceDeliveries[traceID] = &traceDelivery{
				submittedSpanIDs: make(map[string]struct{}),
				ackedSpanIDs:     make(map[string]struct{}),
			}
		}
	}
}

func (h *httpClient) peekServerTraceID(traceID string) string {
	h.deliveryMu.Lock()
	defer h.deliveryMu.Unlock()
	if delivery := h.traceDeliveries[traceID]; delivery != nil {
		return delivery.serverTraceID
	}
	return ""
}

func (h *httpClient) hasClosedDeliveries(traceIDs []string) bool {
	h.deliveryMu.Lock()
	defer h.deliveryMu.Unlock()
	for _, traceID := range traceIDs {
		if delivery := h.traceDeliveries[traceID]; delivery != nil && delivery.closed {
			return true
		}
	}
	return false
}

func (h *httpClient) takeTraceDeliveries(traceIDs []string) map[string]deliveryReport {
	h.deliveryMu.Lock()
	defer h.deliveryMu.Unlock()
	reports := make(map[string]deliveryReport, len(traceIDs))
	for _, traceID := range traceIDs {
		delivery := h.traceDeliveries[traceID]
		if delivery == nil {
			continue
		}
		delete(h.traceDeliveries, traceID)
		delivered := delivery.closingAcked
		if delivered {
			for spanID := range delivery.submittedSpanIDs {
				if _, ok := delivery.ackedSpanIDs[spanID]; !ok {
					delivered = false
					break
				}
			}
		}
		reports[traceID] = deliveryReport{
			spanCount:     len(delivery.submittedSpanIDs),
			closed:        delivery.closed,
			delivered:     delivered,
			serverTraceID: delivery.serverTraceID,
		}
	}
	return reports
}

// sendExternalTrace queues a trace payload on this client's trace transport.
func (h *httpClient) sendExternalTrace(payload map[string]any) {
	ref := carrierRefForPayload(payload)
	if completed, _ := payload["completed"].(bool); !completed {
		ref = nil
	}
	h.recordSubmittedCarrier(ref)
	h.submit(operationExternalTrace, payload, carrierMeta{ref: ref})
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
