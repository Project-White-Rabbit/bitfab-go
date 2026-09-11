package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
)

// TraceSearchParams filters traces by up to 10 exact caller metadata pairs.
// Every pair must match case-insensitively. Cursor is the opaque NextCursor
// from a prior page.
type TraceSearchParams struct {
	CallerMetadata   map[string]string
	TraceFunctionKey string
	Cursor           string
	Limit            int
}

// TraceSearchEntry is one stable summary returned by a trace search.
type TraceSearchEntry struct {
	ID               string            `json:"id"`
	TraceFunctionKey *string           `json:"traceFunctionKey"`
	Name             *string           `json:"name"`
	Status           string            `json:"status"`
	CreatedAt        string            `json:"createdAt"`
	CallerMetadata   map[string]string `json:"callerMetadata"`
}

// TraceSearchResult is one page of matching traces.
type TraceSearchResult struct {
	Traces     []TraceSearchEntry `json:"traces"`
	NextCursor *string            `json:"nextCursor"`
	HasMore    bool               `json:"hasMore"`
}

// TracesClient searches traces for the authenticated organization.
type TracesClient struct {
	httpClient *httpClient
}

// Search finds traces by exact caller metadata.
func (t *TracesClient) Search(ctx context.Context, params TraceSearchParams) (*TraceSearchResult, error) {
	payload := map[string]any{"callerMetadata": params.CallerMetadata}
	if params.TraceFunctionKey != "" {
		payload["traceFunctionKey"] = params.TraceFunctionKey
	}
	if params.Cursor != "" {
		payload["cursor"] = params.Cursor
	}
	if params.Limit != 0 {
		payload["limit"] = params.Limit
	}
	response, err := t.httpClient.request(ctx, "/api/sdk/traces/search", payload, 0)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("bitfab: decode trace search response: %w", err)
	}
	var result TraceSearchResult
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, fmt.Errorf("bitfab: decode trace search response: %w", err)
	}
	return &result, nil
}
