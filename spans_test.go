package bitfab

import (
	"context"
	"net/http"
	"testing"
)

func TestSpans_ExposedOnClient(t *testing.T) {
	client := NewClient("test-key")
	if client.Spans == nil {
		t.Fatal("client.Spans is nil")
	}
}

func TestSpans_ListPostsOnlyTheTraceIDWhenNothingElseIsSet(t *testing.T) {
	traceID := "11111111-1111-4111-8111-111111111111"
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"spans": []any{}, "nextCursor": nil, "hasMore": false}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	result, err := client.Spans.List(context.Background(), traceID, SpanListParams{})
	if err != nil {
		t.Fatal(err)
	}
	requests := server.recorded()
	if len(requests) != 1 || requests[0].method != http.MethodPost || requests[0].path != "/api/sdk/spans/list" ||
		len(requests[0].body) != 1 || requests[0].body["traceId"] != traceID {
		t.Fatalf("requests = %+v", requests)
	}
	if len(result.Spans) != 0 || result.NextCursor != nil || result.HasMore {
		t.Fatalf("result = %+v", result)
	}
}

func TestSpans_ListPostsEveryArgumentAndDecodesThePage(t *testing.T) {
	traceID := "11111111-1111-4111-8111-111111111111"
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{
			"spans": []any{map[string]any{
				"id": "span-2", "traceId": traceID, "parentSpanId": "span-1", "name": "plan", "type": "llm",
				"startedAt": "2026-09-20T00:00:00.000Z", "endedAt": "2026-09-20T00:00:01.000Z",
				"input": map[string]any{"prompt": "hi"}, "output": nil,
			}},
			"nextCursor": "next", "hasMore": true,
		}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	result, err := client.Spans.List(context.Background(), traceID, SpanListParams{
		SpanType: "llm", SpanName: "plan", Fields: []string{"input", "output"}, Cursor: "cursor-1", Limit: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	requests := server.recorded()
	if len(requests) != 1 || requests[0].path != "/api/sdk/spans/list" {
		t.Fatalf("requests = %+v", requests)
	}
	body := requests[0].body
	fields, _ := body["fields"].([]any)
	if len(body) != 6 || body["traceId"] != traceID || body["spanType"] != "llm" || body["spanName"] != "plan" ||
		body["cursor"] != "cursor-1" || body["limit"] != float64(25) ||
		len(fields) != 2 || fields[0] != "input" || fields[1] != "output" {
		t.Fatalf("body = %+v", body)
	}
	if len(result.Spans) != 1 || result.NextCursor == nil || *result.NextCursor != "next" || !result.HasMore {
		t.Fatalf("result = %+v", result)
	}
	span := result.Spans[0]
	input, _ := span.Input.(map[string]any)
	if span.ID != "span-2" || span.TraceID != traceID || *span.ParentSpanID != "span-1" || *span.Name != "plan" ||
		*span.Type != "llm" || *span.StartedAt != "2026-09-20T00:00:00.000Z" || *span.EndedAt != "2026-09-20T00:00:01.000Z" ||
		input["prompt"] != "hi" || span.Output != nil || span.Reasoning != nil {
		t.Fatalf("span = %+v", span)
	}
}

func TestSpans_ListPassesTheCursorBackForTheNextPage(t *testing.T) {
	traceID := "11111111-1111-4111-8111-111111111111"
	server := newDatasetsServer(t, func(request datasetRequest) any {
		if request.body["cursor"] == nil {
			return map[string]any{"spans": []any{}, "nextCursor": "page-2", "hasMore": true}
		}
		return map[string]any{"spans": []any{}, "nextCursor": nil, "hasMore": false}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	params := SpanListParams{SpanType: "function"}
	first, err := client.Spans.List(context.Background(), traceID, params)
	if err != nil {
		t.Fatal(err)
	}
	params.Cursor = *first.NextCursor
	second, err := client.Spans.List(context.Background(), traceID, params)
	if err != nil {
		t.Fatal(err)
	}
	requests := server.recorded()
	if len(requests) != 2 || requests[1].body["cursor"] != "page-2" || requests[1].body["spanType"] != "function" {
		t.Fatalf("requests = %+v", requests)
	}
	if second.HasMore || second.NextCursor != nil {
		t.Fatalf("second = %+v", second)
	}
}

func TestSpans_ListRejectsAnInvalidTraceIDBeforeAnyRequest(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	_, err := client.Spans.List(context.Background(), "not-a-trace", SpanListParams{})
	if err == nil || err.Error() != "bitfab: invalid trace ID" {
		t.Fatalf("err = %v", err)
	}
	if len(server.recorded()) != 0 {
		t.Fatalf("requests = %+v", server.recorded())
	}
}
