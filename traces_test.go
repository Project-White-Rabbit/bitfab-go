package bitfab

import (
	"context"
	"testing"
)

func TestTraces_ExposedOnClient(t *testing.T) {
	client := NewClient("test-key")
	if client.Traces == nil {
		t.Fatal("client.Traces is nil")
	}
}

func TestTraces_SearchPostsCallerMetadata(t *testing.T) {
	server := newDatasetsServer(t, func(request datasetRequest) any {
		return map[string]any{"traces": []any{}, "nextCursor": nil, "hasMore": false}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))

	result, err := client.Traces.Search(context.Background(), TraceSearchParams{
		CallerMetadata:   map[string]string{"ticket_id": "ABC-123"},
		TraceFunctionKey: "support-agent",
		Limit:            10,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(result.Traces) != 0 || result.HasMore {
		t.Errorf("result = %+v", result)
	}
	req := server.recorded()[0]
	if req.path != "/api/sdk/traces/search" {
		t.Errorf("path = %q", req.path)
	}
	if req.body["traceFunctionKey"] != "support-agent" || req.body["limit"] != float64(10) {
		t.Errorf("body = %+v", req.body)
	}
	metadata, ok := req.body["callerMetadata"].(map[string]any)
	if !ok || metadata["ticket_id"] != "ABC-123" {
		t.Errorf("callerMetadata = %+v", req.body["callerMetadata"])
	}
}

func TestTraces_SearchPostsNameFilters(t *testing.T) {
	server := newDatasetsServer(t, func(request datasetRequest) any {
		return map[string]any{"traces": []any{}, "nextCursor": nil, "hasMore": false}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))

	if _, err := client.Traces.Search(context.Background(), TraceSearchParams{
		Name:         "Nightly Checkout",
		NameContains: "checkout",
	}); err != nil {
		t.Fatalf("Search: %v", err)
	}
	req := server.recorded()[0]
	if req.body["name"] != "Nightly Checkout" {
		t.Errorf("name = %+v", req.body["name"])
	}
	if req.body["nameContains"] != "checkout" {
		t.Errorf("nameContains = %+v", req.body["nameContains"])
	}
	if _, present := req.body["callerMetadata"]; present {
		t.Errorf("callerMetadata should be omitted, body = %+v", req.body)
	}
}
