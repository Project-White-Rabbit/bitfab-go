package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
)

type SpanListParams struct {
	SpanType string
	SpanName string
	Fields   []string
	Cursor   string
	Limit    int
}

type ListedSpan struct {
	ID           string  `json:"id"`
	TraceID      string  `json:"traceId"`
	ParentSpanID *string `json:"parentSpanId"`
	Name         *string `json:"name"`
	Type         *string `json:"type"`
	StartedAt    *string `json:"startedAt"`
	EndedAt      *string `json:"endedAt"`
	Input        any     `json:"input,omitempty"`
	Output       any     `json:"output,omitempty"`
	Reasoning    any     `json:"reasoning,omitempty"`
	Content      any     `json:"content,omitempty"`
	Errors       any     `json:"errors,omitempty"`
	Contexts     any     `json:"contexts,omitempty"`
}

type SpanListResult struct {
	Spans      []ListedSpan `json:"spans"`
	NextCursor *string      `json:"nextCursor"`
	HasMore    bool         `json:"hasMore"`
}

type SpansClient struct {
	httpClient *httpClient
}

func (s *SpansClient) List(ctx context.Context, traceID string, params SpanListParams) (*SpanListResult, error) {
	if !traceIDPattern.MatchString(traceID) {
		return nil, fmt.Errorf("bitfab: invalid trace ID")
	}
	payload := map[string]any{"traceId": traceID}
	if params.SpanType != "" {
		payload["spanType"] = params.SpanType
	}
	if params.SpanName != "" {
		payload["spanName"] = params.SpanName
	}
	if len(params.Fields) > 0 {
		payload["fields"] = params.Fields
	}
	if params.Cursor != "" {
		payload["cursor"] = params.Cursor
	}
	if params.Limit != 0 {
		payload["limit"] = params.Limit
	}
	response, err := s.httpClient.request(ctx, "/api/sdk/spans/list", payload, 0)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("bitfab: decode span list response: %w", err)
	}
	var result SpanListResult
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, fmt.Errorf("bitfab: decode span list response: %w", err)
	}
	return &result, nil
}
