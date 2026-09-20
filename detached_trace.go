package bitfab

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// DetachedTrace updates a persisted trace without an active tracing context.
// Each operation waits for the server response, returns any rejection, and
// writes whether or not capture is on.
type DetachedTrace struct {
	client  *Client
	traceID string
}

// GetTrace returns a handle for a canonical Bitfab trace ID. It validates the
// ID locally; the server checks existence and ownership on each update.
func (c *Client) GetTrace(traceID string) (*DetachedTrace, error) {
	if !traceIDPattern.MatchString(traceID) {
		return nil, fmt.Errorf("bitfab: invalid trace ID")
	}
	return &DetachedTrace{client: c, traceID: traceID}, nil
}

// TraceID returns the canonical Bitfab trace ID addressed by this handle.
func (t *DetachedTrace) TraceID() string {
	return t.traceID
}

// AddContext appends a context entry without replacing existing entries.
func (t *DetachedTrace) AddContext(ctx context.Context, value map[string]any) error {
	if value == nil {
		return nil
	}
	return t.patch(ctx, map[string]any{"appendContexts": []map[string]any{value}})
}

// SetMetadata merges keys into existing metadata, replacing matching keys.
func (t *DetachedTrace) SetMetadata(ctx context.Context, value map[string]any) error {
	if value == nil {
		return nil
	}
	return t.patch(ctx, map[string]any{"mergeMetadata": value})
}

// SetSessionID replaces the trace's session ID. Empty values are ignored.
func (t *DetachedTrace) SetSessionID(ctx context.Context, value string) error {
	if value == "" {
		return nil
	}
	return t.patch(ctx, map[string]any{"setSessionId": value})
}

// SetName replaces the trace's name. Empty values are ignored.
func (t *DetachedTrace) SetName(ctx context.Context, value string) error {
	if value == "" {
		return nil
	}
	return t.patch(ctx, map[string]any{"setName": value})
}

func (t *DetachedTrace) patch(ctx context.Context, payload map[string]any) error {
	body, dropped := marshalPayloadSafe(payload)
	warnForStubbedBody(dropped)
	_, err := t.client.httpClient.sendPreparedMethod(ctx, http.MethodPatch,
		"/api/sdk/traces/"+url.PathEscape(t.traceID), prepareRequestBody(body), 10*time.Second)
	return err
}
