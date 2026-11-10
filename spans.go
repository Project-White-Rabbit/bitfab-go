package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
)

const (
	spanListMaxPageSize           = 200
	spanListMaxPageSizeWithFields = 50
)

// SpanListFilters narrows the spans returned by SpansClient.List and SpansClient.ListAll.
type SpanListFilters struct {
	SpanType string
	SpanName string
	Fields   []string
	// Metadata keeps only spans whose metadata holds every one of these keys with these values, matched exactly and case-insensitively. Up to 10 keys. Span metadata is set with GetCurrentSpan(ctx).SetMetadata.
	Metadata map[string]string
}

// SpanListParams selects one page of SpansClient.List.
type SpanListParams struct {
	SpanListFilters
	Cursor string
	// Limit is spans per page, 1 to 200, or at most 50 when Fields is set, since each span's content is fetched. A larger value is refused with an error naming the cap. The server defaults to 50. Use ListAll to read every span without choosing a page size.
	Limit int
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
	// Metadata is the span's metadata, keys and values lowercased. Empty when none was set.
	Metadata map[string]string `json:"metadata"`
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
	if len(params.Metadata) > 0 {
		payload["metadata"] = params.Metadata
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

// ListAll returns every span of one trace in the order they ran, fetched page by page until none are left, so no span is missed by forgetting to follow NextCursor. Pages hold 200 spans, or 50 when Fields is set. Returns an HTTP 404 error when the trace is not in your organization.
func (s *SpansClient) ListAll(ctx context.Context, traceID string, filters SpanListFilters) ([]ListedSpan, error) {
	limit := spanListMaxPageSize
	if len(filters.Fields) > 0 {
		limit = spanListMaxPageSizeWithFields
	}
	spans := []ListedSpan{}
	cursor := ""
	for {
		page, err := s.List(ctx, traceID, SpanListParams{SpanListFilters: filters, Cursor: cursor, Limit: limit})
		if err != nil {
			return nil, err
		}
		spans = append(spans, page.Spans...)
		if !page.HasMore || page.NextCursor == nil || *page.NextCursor == "" {
			return spans, nil
		}
		cursor = *page.NextCursor
	}
}
