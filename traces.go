package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
)

type TraceSearchParams struct {
	CallerMetadata   map[string]string
	Name             string
	NameContains     string
	TraceFunctionKey string
	Cursor           string
	Limit            int
}

type TraceSearchEntry struct {
	ID               string            `json:"id"`
	TraceFunctionKey *string           `json:"traceFunctionKey"`
	Name             *string           `json:"name"`
	Status           string            `json:"status"`
	CreatedAt        string            `json:"createdAt"`
	CallerMetadata   map[string]string `json:"callerMetadata"`
}

type TraceSearchResult struct {
	Traces     []TraceSearchEntry `json:"traces"`
	NextCursor *string            `json:"nextCursor"`
	HasMore    bool               `json:"hasMore"`
}

type TracesClient struct {
	httpClient *httpClient
}

func (t *TracesClient) Search(ctx context.Context, params TraceSearchParams) (*TraceSearchResult, error) {
	payload := map[string]any{}
	if len(params.CallerMetadata) > 0 {
		payload["callerMetadata"] = params.CallerMetadata
	}
	if params.Name != "" {
		payload["name"] = params.Name
	}
	if params.NameContains != "" {
		payload["nameContains"] = params.NameContains
	}
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
