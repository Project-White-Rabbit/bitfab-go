package bitfab

import (
	"bytes"
	"context"
	"encoding/json"
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
	wg         sync.WaitGroup
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

// request makes a POST request to the Bitfab API.
func (h *httpClient) request(endpoint string, payload map[string]any, opts ...requestOption) error {
	cfg := requestConfig{
		timeout:    0, // use default client timeout
		maxRetries: 1,
		retryDelay: 100 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	// Encode defensively so a stray non-encodable value can never abort the
	// send and silently drop the span. Strays are stubbed in place; a degraded
	// payload warns loudly that the trace may not be replayable.
	body, dropped := marshalPayloadSafe(payload)
	if len(dropped) > 0 {
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

	var lastErr error
	for attempt := 0; attempt < cfg.maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(cfg.retryDelay)
		}

		client := h.client
		if cfg.timeout > 0 {
			client = &http.Client{Timeout: cfg.timeout}
		}

		req, err := http.NewRequest("POST", h.serviceURL+endpoint, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("bitfab: failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+h.apiKey)

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastErr = fmt.Errorf("bitfab: HTTP %d: %s", resp.StatusCode, string(respBody))
			continue
		}

		// Check for error in response body
		var result map[string]any
		if json.Unmarshal(respBody, &result) == nil {
			if errMsg, ok := result["error"].(string); ok {
				if url, ok := result["url"].(string); ok {
					return fmt.Errorf("%s Configure it at: %s%s", errMsg, h.serviceURL, url)
				}
				return fmt.Errorf("%s", errMsg)
			}
		}

		return nil
	}

	return lastErr
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
		return fmt.Errorf("bitfab: HTTP %d: %s", resp.StatusCode, string(body))
	}
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("bitfab: failed to decode response: %w", err)
	}
	return nil
}

// sendExternalSpan sends a span payload in the background and returns a channel
// that is closed when the HTTP request completes. This allows callers to await
// span delivery before sending trace completion.
func (h *httpClient) sendExternalSpan(payload map[string]any) <-chan struct{} {
	merged := make(map[string]any, len(payload)+1)
	for k, v := range payload {
		merged[k] = v
	}
	merged["sdkVersion"] = Version

	done := make(chan struct{})
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				warnOnce("panic-background-request", fmt.Sprintf("a background request panicked and was recovered: %v", r))
			}
		}()
		if err := h.request("/api/sdk/externalSpans", merged, withTimeout(30*time.Second)); err != nil {
			warnOnce("send-external-span-failed", fmt.Sprintf("failed to send a span to the backend (further occurrences suppressed): %v", err))
		}
	}()
	return done
}

// sendExternalTrace sends a trace payload in the background (fire-and-forget).
func (h *httpClient) sendExternalTrace(payload map[string]any) {
	merged := make(map[string]any, len(payload)+1)
	for k, v := range payload {
		merged[k] = v
	}
	merged["sdkVersion"] = Version

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				warnOnce("panic-background-request", fmt.Sprintf("a background request panicked and was recovered: %v", r))
			}
		}()
		if err := h.request("/api/sdk/externalTraces", merged, withTimeout(10*time.Second)); err != nil {
			warnOnce("send-external-trace-failed", fmt.Sprintf("failed to send a trace to the backend (further occurrences suppressed): %v", err))
		}
	}()
}

// runBackground runs fn in a tracked background goroutine that never crashes the
// host on panic. flush() waits for these via the wait group, so work moved off
// the user's hot path (e.g. draining a root trace's child spans) still completes
// before FlushTraces returns.
func (h *httpClient) runBackground(fn func()) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				warnOnce("panic-background-task", fmt.Sprintf("a background task panicked and was recovered: %v", r))
			}
		}()
		fn()
	}()
}

// flush waits for all pending background goroutines to complete.
func (h *httpClient) flush(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
	}
}

// requestOption configures a single request.
type requestOption func(*requestConfig)

type requestConfig struct {
	timeout    time.Duration
	maxRetries int
	retryDelay time.Duration
}

func withTimeout(d time.Duration) requestOption {
	return func(c *requestConfig) { c.timeout = d }
}
