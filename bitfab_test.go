package bitfab

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestClient(serverURL string) *Client {
	return NewClient("test-key", WithServiceURL(serverURL))
}

func TestNewClient_Defaults(t *testing.T) {
	c := NewClient("test-key")
	if c.apiKey != "test-key" {
		t.Errorf("apiKey = %q, want test-key", c.apiKey)
	}
	if c.serviceURL != DefaultServiceURL {
		t.Errorf("serviceURL = %q, want %q", c.serviceURL, DefaultServiceURL)
	}
}

func TestNewClient_WithServiceURL(t *testing.T) {
	c := NewClient("test-key", WithServiceURL("https://custom.example.com"))
	if c.serviceURL != "https://custom.example.com" {
		t.Errorf("serviceURL = %q, want https://custom.example.com", c.serviceURL)
	}
}

func TestSpan_BasicExecution(t *testing.T) {
	server := newSpanCaptureServer(t)
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	result, err := client.Span(ctx, "test-service", func(ctx context.Context) (any, error) {
		return "hello", nil
	})

	client.FlushTraces(5 * time.Second)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "hello" {
		t.Errorf("result = %v, want hello", result)
	}
}

func TestSpan_WithNameAndType(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "order-service", func(ctx context.Context) (any, error) {
		return map[string]any{"total": 100}, nil
	}, WithName("ProcessOrder"), WithType("function"), WithFunctionName("processOrder"))

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if captured == nil {
		t.Fatal("no span captured")
	}

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)

	if spanData["name"] != "ProcessOrder" {
		t.Errorf("name = %v, want ProcessOrder", spanData["name"])
	}
	if spanData["type"] != "function" {
		t.Errorf("type = %v, want function", spanData["type"])
	}
	if spanData["function_name"] != "processOrder" {
		t.Errorf("function_name = %v, want processOrder", spanData["function_name"])
	}
	if captured["traceFunctionKey"] != "order-service" {
		t.Errorf("traceFunctionKey = %v, want order-service", captured["traceFunctionKey"])
	}
	if captured["source"] != "go-sdk-function" {
		t.Errorf("source = %v, want go-sdk-function", captured["source"])
	}
}

func TestSpan_CapturesError(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	_, err := client.Span(ctx, "failing-service", func(ctx context.Context) (any, error) {
		return nil, errors.New("something went wrong")
	})

	client.FlushTraces(5 * time.Second)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "something went wrong" {
		t.Errorf("error = %q, want 'something went wrong'", err.Error())
	}

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	if spanData["error"] != "something went wrong" {
		t.Errorf("span error = %v, want 'something went wrong'", spanData["error"])
	}
}

func TestSpan_InvalidType(t *testing.T) {
	// An invalid span type must never fail the user's call. The span degrades to
	// "custom" and fn still runs and returns its real value.
	client := NewClient("test-key")
	ctx := context.Background()

	ran := false
	result, err := client.Span(ctx, "test", func(ctx context.Context) (any, error) {
		ran = true
		return "real-value", nil
	}, WithType("invalid"))

	if err != nil {
		t.Fatalf("invalid type must not surface an error, got %v", err)
	}
	if !ran {
		t.Fatal("fn must still run when the span type is invalid")
	}
	if result != "real-value" {
		t.Fatalf("expected the function's real return value, got %v", result)
	}
}

func TestSpan_NestedSpans_ShareTraceID(t *testing.T) {
	var mu sync.Mutex
	var payloads []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			payloads = append(payloads, payload)
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "outer", func(ctx context.Context) (any, error) {
		return client.Span(ctx, "inner", func(ctx context.Context) (any, error) {
			return "inner-result", nil
		})
	})

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(payloads) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(payloads))
	}

	// Both should share the same trace ID
	traceID0 := payloads[0]["rawSpan"].(map[string]any)["trace_id"].(string)
	traceID1 := payloads[1]["rawSpan"].(map[string]any)["trace_id"].(string)
	if traceID0 != traceID1 {
		t.Errorf("trace IDs differ: %q vs %q", traceID0, traceID1)
	}
}

func TestSpan_NestedSpans_HaveParentID(t *testing.T) {
	var mu sync.Mutex
	var payloads []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			payloads = append(payloads, payload)
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "outer", func(ctx context.Context) (any, error) {
		return client.Span(ctx, "inner", func(ctx context.Context) (any, error) {
			return "result", nil
		})
	})

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Find inner and outer by traceFunctionKey
	var innerSpan, outerSpan map[string]any
	for _, p := range payloads {
		if p["traceFunctionKey"] == "inner" {
			innerSpan = p
		}
		if p["traceFunctionKey"] == "outer" {
			outerSpan = p
		}
	}

	if innerSpan == nil || outerSpan == nil {
		t.Fatal("could not find inner/outer spans")
	}

	innerRaw := innerSpan["rawSpan"].(map[string]any)
	outerRaw := outerSpan["rawSpan"].(map[string]any)

	// Inner should have parent_id pointing to outer
	if innerRaw["parent_id"] != outerRaw["id"] {
		t.Errorf("inner parent_id = %v, want %v (outer id)", innerRaw["parent_id"], outerRaw["id"])
	}

	// Outer should not have parent_id
	if _, ok := outerRaw["parent_id"]; ok {
		t.Error("outer span should not have parent_id")
	}
}

func TestSpan_IndependentCalls_DifferentTraceIDs(t *testing.T) {
	var mu sync.Mutex
	var payloads []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			payloads = append(payloads, payload)
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "service-1", func(ctx context.Context) (any, error) {
		return "a", nil
	})
	client.Span(ctx, "service-2", func(ctx context.Context) (any, error) {
		return "b", nil
	})

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(payloads) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(payloads))
	}

	traceID0 := payloads[0]["rawSpan"].(map[string]any)["trace_id"].(string)
	traceID1 := payloads[1]["rawSpan"].(map[string]any)["trace_id"].(string)
	if traceID0 == traceID1 {
		t.Error("independent calls should have different trace IDs")
	}
}

func TestGetFunction_Span(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	fn := client.GetFunction("order-processing")
	ctx := context.Background()

	result, err := fn.Span(ctx, func(ctx context.Context) (any, error) {
		return "done", nil
	}, WithName("ProcessOrder"), WithType("function"))

	client.FlushTraces(5 * time.Second)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "done" {
		t.Errorf("result = %v, want done", result)
	}

	mu.Lock()
	defer mu.Unlock()

	if captured["traceFunctionKey"] != "order-processing" {
		t.Errorf("traceFunctionKey = %v, want order-processing", captured["traceFunctionKey"])
	}
}

func TestSpan_AllTypes(t *testing.T) {
	server := newSpanCaptureServer(t)
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	types := []string{"llm", "agent", "function", "guardrail", "handoff", "custom"}
	for _, st := range types {
		_, err := client.Span(ctx, "test", func(ctx context.Context) (any, error) {
			return nil, nil
		}, WithType(st))
		if err != nil {
			t.Errorf("unexpected error for type %q: %v", st, err)
		}
	}

	client.FlushTraces(5 * time.Second)
}

func TestSpan_CapturesOutput(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "test", func(ctx context.Context) (any, error) {
		return map[string]any{"total": 42}, nil
	})

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	output := spanData["output"].(map[string]any)
	if output["total"] != float64(42) {
		t.Errorf("output total = %v, want 42", output["total"])
	}
}

func TestSpan_WithInput(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "test", func(ctx context.Context) (any, error) {
		return "result", nil
	}, WithInput("order-123", 99.99))

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	input := spanData["input"].([]any)
	if len(input) != 2 {
		t.Fatalf("input len = %d, want 2", len(input))
	}
	if input[0] != "order-123" {
		t.Errorf("input[0] = %v, want order-123", input[0])
	}
	if input[1] != 99.99 {
		t.Errorf("input[1] = %v, want 99.99", input[1])
	}
}

func TestSpan_WithInputSingleArg(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "test", func(ctx context.Context) (any, error) {
		return nil, nil
	}, WithInput("order-123"))

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	if spanData["input"] != "order-123" {
		t.Errorf("input = %v, want order-123", spanData["input"])
	}
}

func TestStart_BasicExecution(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	ctx, span := client.Start(ctx, "order-service", "ProcessOrder", WithType("function"))
	span.SetInput("order-123")
	span.SetOutput(map[string]any{"total": 100})
	span.End()

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if captured == nil {
		t.Fatal("no span captured")
	}

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)

	if spanData["name"] != "ProcessOrder" {
		t.Errorf("name = %v, want ProcessOrder", spanData["name"])
	}
	if spanData["type"] != "function" {
		t.Errorf("type = %v, want function", spanData["type"])
	}
	if spanData["input"] != "order-123" {
		t.Errorf("input = %v, want order-123", spanData["input"])
	}
	output := spanData["output"].(map[string]any)
	if output["total"] != float64(100) {
		t.Errorf("output total = %v, want 100", output["total"])
	}
	if captured["traceFunctionKey"] != "order-service" {
		t.Errorf("traceFunctionKey = %v, want order-service", captured["traceFunctionKey"])
	}

	// Verify context was updated (span should not be nil)
	if currentSpan(ctx) == nil {
		t.Error("expected span in context after Start")
	}
}

func TestStart_CapturesError(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	_, span := client.Start(ctx, "failing-service", "RiskyOp")
	span.SetError(errors.New("something went wrong"))
	span.End()

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	if spanData["error"] != "something went wrong" {
		t.Errorf("error = %v, want 'something went wrong'", spanData["error"])
	}
}

func TestStart_IdempotentEnd(t *testing.T) {
	var mu sync.Mutex
	var spanCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only count span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			spanCount++
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	_, span := client.Start(ctx, "test", "Test")
	span.End()
	span.End() // second call should be a no-op
	span.End() // third call should be a no-op

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if spanCount != 1 {
		t.Errorf("span sent %d times, want 1", spanCount)
	}
}

func TestStart_NestedSpans(t *testing.T) {
	var mu sync.Mutex
	var payloads []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			payloads = append(payloads, payload)
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	ctx, outerSpan := client.Start(ctx, "pipeline", "Outer")
	ctx, innerSpan := client.Start(ctx, "pipeline", "Inner")
	innerSpan.End()
	outerSpan.End()

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(payloads) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(payloads))
	}

	// Find inner and outer
	var inner, outer map[string]any
	for _, p := range payloads {
		raw := p["rawSpan"].(map[string]any)
		sd := raw["span_data"].(map[string]any)
		if sd["name"] == "Inner" {
			inner = p
		}
		if sd["name"] == "Outer" {
			outer = p
		}
	}

	if inner == nil || outer == nil {
		t.Fatal("could not find inner/outer spans")
	}

	// Same trace ID
	innerTraceID := inner["rawSpan"].(map[string]any)["trace_id"].(string)
	outerTraceID := outer["rawSpan"].(map[string]any)["trace_id"].(string)
	if innerTraceID != outerTraceID {
		t.Errorf("trace IDs differ: %q vs %q", innerTraceID, outerTraceID)
	}

	// Inner has parent_id pointing to outer
	innerParentID := inner["rawSpan"].(map[string]any)["parent_id"].(string)
	outerID := outer["rawSpan"].(map[string]any)["id"].(string)
	if innerParentID != outerID {
		t.Errorf("inner parent_id = %v, want %v", innerParentID, outerID)
	}
}

func TestFunction_Start(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	fn := client.GetFunction("order-processing")
	ctx := context.Background()

	_, span := fn.Start(ctx, "ProcessOrder", WithType("function"))
	span.SetInput("order-456")
	span.SetOutput("done")
	span.End()

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if captured["traceFunctionKey"] != "order-processing" {
		t.Errorf("traceFunctionKey = %v, want order-processing", captured["traceFunctionKey"])
	}
	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	if spanData["name"] != "ProcessOrder" {
		t.Errorf("name = %v, want ProcessOrder", spanData["name"])
	}
}

func TestDisabled_SpanExecutesButDoesNotSend(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := NewClient("test-key", WithServiceURL(server.URL), WithEnabled(false))
	ctx := context.Background()

	result, err := client.Span(ctx, "test-service", func(ctx context.Context) (any, error) {
		return "executed", nil
	})

	client.FlushTraces(1 * time.Second)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "executed" {
		t.Errorf("result = %v, want executed", result)
	}
	if requestCount != 0 {
		t.Errorf("sent %d requests, want 0", requestCount)
	}
}

func TestDisabled_StartReturnsNoOpSpan(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := NewClient("test-key", WithServiceURL(server.URL), WithEnabled(false))
	ctx := context.Background()

	_, span := client.Start(ctx, "test-service", "TestSpan", WithType("function"))
	span.SetInput("hello")
	span.SetOutput("world")
	span.End()

	client.FlushTraces(1 * time.Second)

	if requestCount != 0 {
		t.Errorf("sent %d requests, want 0", requestCount)
	}
}

func TestEnabled_DefaultsToTrue(t *testing.T) {
	client := NewClient("test-key")
	if !client.enabled {
		t.Error("enabled should default to true")
	}
}

func TestSpan_UnserializableOutput_DoesNotCrash(t *testing.T) {
	server := newSpanCaptureServer(t)
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	// Channels cannot be serialized by json.Marshal - this must not crash the app
	result, err := client.Span(ctx, "test", func(ctx context.Context) (any, error) {
		return map[string]any{"ch": make(chan int)}, nil
	})

	client.FlushTraces(5 * time.Second)

	if err != nil {
		t.Fatalf("span should not return error for unserializable output: %v", err)
	}
	// The result should still be returned to the caller
	m := result.(map[string]any)
	if m["ch"] == nil {
		t.Error("result should contain the channel value")
	}
}

func TestStart_UnserializableOutput_DoesNotCrash(t *testing.T) {
	server := newSpanCaptureServer(t)
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	_, span := client.Start(ctx, "test", "Test")
	span.SetOutput(map[string]any{"fn": func() {}})
	// End() must not panic even though func() can't be serialized
	span.End()

	client.FlushTraces(5 * time.Second)
}

func TestSpan_ServerDown_ReturnsResult(t *testing.T) {
	// Point at a server that isn't listening - span sending will fail,
	// but the user's result and error must still be returned.
	client := newTestClient("http://127.0.0.1:1")
	ctx := context.Background()

	result, err := client.Span(ctx, "test", func(ctx context.Context) (any, error) {
		return "my-result", nil
	})

	client.FlushTraces(1 * time.Second)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "my-result" {
		t.Errorf("result = %v, want my-result", result)
	}
}

func TestSpan_ServerDown_ReturnsUserError(t *testing.T) {
	client := newTestClient("http://127.0.0.1:1")
	ctx := context.Background()

	result, err := client.Span(ctx, "test", func(ctx context.Context) (any, error) {
		return nil, errors.New("user error")
	})

	client.FlushTraces(1 * time.Second)

	if err == nil || err.Error() != "user error" {
		t.Fatalf("err = %v, want 'user error'", err)
	}
	if result != nil {
		t.Errorf("result = %v, want nil", result)
	}
}

func TestSpan_WithMetadata(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	// Closure-style spans don't have access to AddContext (no ActiveSpan handle)
	// Context can only be added via Start/End style with ActiveSpan
	client.Span(ctx, "test", func(ctx context.Context) (any, error) {
		return "result", nil
	})

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	if _, ok := spanData["contexts"]; ok {
		t.Error("contexts should not be present for closure-style span without AddContext")
	}
}

func TestSpan_NoContext(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "test", func(ctx context.Context) (any, error) {
		return "result", nil
	})

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	if _, ok := spanData["contexts"]; ok {
		t.Error("contexts should not be present when not set")
	}
}

func TestStart_WithContext(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	_, span := client.Start(ctx, "test", "TestSpan", WithType("function"))
	span.AddContext(map[string]any{"request_id": "req-456", "env": "staging"})
	span.End()

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	contexts := spanData["contexts"].([]any)
	if len(contexts) != 1 {
		t.Errorf("expected 1 context entry, got %d", len(contexts))
	}
	// Each AddContext call pushes the entire map as one entry
	entry := contexts[0].(map[string]any)
	if entry["request_id"] != "req-456" {
		t.Error("expected request_id to be 'req-456'")
	}
	if entry["env"] != "staging" {
		t.Error("expected env to be 'staging'")
	}
}

func TestStart_ContextAccumulation(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Only capture span requests, not trace requests
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	_, span := client.Start(ctx, "test", "TestSpan", WithType("function"))
	span.AddContext(map[string]any{"step": "validation"})
	span.AddContext(map[string]any{"step": "processing"})
	span.End()

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	contexts := spanData["contexts"].([]any)
	if len(contexts) != 2 {
		t.Errorf("expected 2 context entries, got %d", len(contexts))
	}
	// Each AddContext call pushes the entire map as one entry
	entry1 := contexts[0].(map[string]any)
	entry2 := contexts[1].(map[string]any)
	if entry1["step"] != "validation" {
		t.Error("expected first entry step to be 'validation'")
	}
	if entry2["step"] != "processing" {
		t.Error("expected second entry step to be 'processing'")
	}
}

func TestNewClient_EmptyAPIKeyAutoDisables(t *testing.T) {
	t.Setenv("BITFAB_API_KEY", "") // ignore any ambient key so the fallback is empty
	client := NewClient("")
	if client.enabled {
		t.Error("client with empty apiKey should be auto-disabled")
	}
}

func TestNewClient_WhitespaceAPIKeyAutoDisables(t *testing.T) {
	t.Setenv("BITFAB_API_KEY", "")
	client := NewClient("   ")
	if client.enabled {
		t.Error("client with whitespace apiKey should be auto-disabled")
	}
}

func TestSpan_EmptyAPIKeyRunsFunctionSilently(t *testing.T) {
	t.Setenv("BITFAB_API_KEY", "")
	client := NewClient("")
	ctx := context.Background()

	result, err := client.Span(ctx, "test", func(ctx context.Context) (any, error) {
		return "result", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "result" {
		t.Errorf("result = %v, want result", result)
	}
}

func TestStart_EmptyAPIKeyReturnsNoOpSpan(t *testing.T) {
	t.Setenv("BITFAB_API_KEY", "")
	client := NewClient("")
	ctx := context.Background()

	_, span := client.Start(ctx, "test", "Test")
	span.End() // should not panic
}

func TestNewClient_ExplicitDisabledWithEmptyAPIKeyStaysDisabled(t *testing.T) {
	client := NewClient("", WithEnabled(false))
	if client.enabled {
		t.Error("explicitly disabled client should stay disabled")
	}
}

func TestNewClient_FallsBackToEnvAPIKey(t *testing.T) {
	t.Setenv("BITFAB_API_KEY", "env-key")
	client := NewClient("")
	if !client.enabled {
		t.Error("client should be enabled when key comes from the environment")
	}
	if client.apiKey != "env-key" {
		t.Errorf("apiKey = %q, want env-key", client.apiKey)
	}
}

func TestNewClient_ExplicitAPIKeyBeatsEnv(t *testing.T) {
	t.Setenv("BITFAB_API_KEY", "env-key")
	client := NewClient("arg-key")
	if client.apiKey != "arg-key" {
		t.Errorf("apiKey = %q, want arg-key (explicit arg wins over env)", client.apiKey)
	}
}

func TestNewClient_WithAPIKeyOption(t *testing.T) {
	t.Setenv("BITFAB_API_KEY", "")
	client := NewClient("", WithAPIKey("opt-key"))
	if !client.enabled || client.apiKey != "opt-key" {
		t.Errorf("WithAPIKey did not set the key: enabled=%v apiKey=%q", client.enabled, client.apiKey)
	}
}

func TestNewClient_StrictPanicsWithoutKey(t *testing.T) {
	t.Setenv("BITFAB_API_KEY", "")
	defer func() {
		if recover() == nil {
			t.Error("strict client with no key should panic")
		}
	}()
	NewClient("", WithStrict(true))
}

func TestNewClient_StrictDoesNotPanicWithKey(t *testing.T) {
	client := NewClient("present", WithStrict(true))
	if !client.enabled {
		t.Error("strict client with a key should be enabled")
	}
}

func newSpanCaptureServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
}

func TestGetCurrentTrace_OutsideSpan(t *testing.T) {
	ctx := context.Background()
	trace := GetCurrentTrace(ctx)
	if trace != nil {
		t.Error("GetCurrentTrace should return nil outside of span context")
	}
}

func TestGetCurrentTrace_InsideSpan(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	var trace *CurrentTrace
	client.Span(ctx, "test", func(ctx context.Context) (any, error) {
		trace = GetCurrentTrace(ctx)
		return nil, nil
	})

	if trace == nil {
		t.Error("GetCurrentTrace should return non-nil inside span context")
	}
}

func TestTraceCompletion_SessionID(t *testing.T) {
	clearAllTraceStates()
	var mu sync.Mutex
	var tracePayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if strings.Contains(r.URL.Path, "externalTraces") {
			mu.Lock()
			tracePayload = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "test-function", func(ctx context.Context) (any, error) {
		trace := GetCurrentTrace(ctx)
		trace.SetSessionID("sess-123")
		return "done", nil
	})

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if tracePayload == nil {
		t.Fatal("no trace payload captured")
	}
	// Session ID is at payload level, not in externalTrace
	if tracePayload["sessionId"] != "sess-123" {
		t.Errorf("sessionId = %v, want sess-123", tracePayload["sessionId"])
	}
}

func TestTraceCompletion_Metadata(t *testing.T) {
	clearAllTraceStates()
	var mu sync.Mutex
	var tracePayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if strings.Contains(r.URL.Path, "externalTraces") {
			mu.Lock()
			tracePayload = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "test-function", func(ctx context.Context) (any, error) {
		trace := GetCurrentTrace(ctx)
		trace.SetMetadata(map[string]any{"user_id": "u-123", "region": "us-east"})
		return "done", nil
	})

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if tracePayload == nil {
		t.Fatal("no trace payload captured")
	}
	rawTrace := tracePayload["externalTrace"].(map[string]any)
	metadata := rawTrace["metadata"].(map[string]any)
	if metadata["user_id"] != "u-123" {
		t.Errorf("metadata user_id = %v, want u-123", metadata["user_id"])
	}
	if metadata["region"] != "us-east" {
		t.Errorf("metadata region = %v, want us-east", metadata["region"])
	}
}

func TestTraceCompletion_Contexts(t *testing.T) {
	clearAllTraceStates()
	var mu sync.Mutex
	var tracePayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if strings.Contains(r.URL.Path, "externalTraces") {
			mu.Lock()
			tracePayload = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "test-function", func(ctx context.Context) (any, error) {
		trace := GetCurrentTrace(ctx)
		trace.AddContext(map[string]any{"user_type": "premium", "tier": "gold"})
		return "done", nil
	})

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if tracePayload == nil {
		t.Fatal("no trace payload captured")
	}
	rawTrace := tracePayload["externalTrace"].(map[string]any)
	contexts := rawTrace["contexts"].([]any)
	if len(contexts) != 1 {
		t.Errorf("expected 1 context entry, got %d", len(contexts))
	}
	// Each AddContext call pushes the entire map as one entry
	entry := contexts[0].(map[string]any)
	if entry["user_type"] != "premium" {
		t.Error("expected user_type to be 'premium'")
	}
	if entry["tier"] != "gold" {
		t.Error("expected tier to be 'gold'")
	}
}

func TestTraceCompletion_AllMethods(t *testing.T) {
	clearAllTraceStates()
	var mu sync.Mutex
	var tracePayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if strings.Contains(r.URL.Path, "externalTraces") {
			mu.Lock()
			tracePayload = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "test-function", func(ctx context.Context) (any, error) {
		trace := GetCurrentTrace(ctx)
		trace.SetSessionID("sess-789")
		trace.SetMetadata(map[string]any{"workflow": "checkout"})
		trace.AddContext(map[string]any{"step": "payment"})
		return "done", nil
	})

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if tracePayload == nil {
		t.Fatal("no trace payload captured")
	}
	if tracePayload["sessionId"] != "sess-789" {
		t.Errorf("sessionId = %v, want sess-789", tracePayload["sessionId"])
	}
	rawTrace := tracePayload["externalTrace"].(map[string]any)
	metadata := rawTrace["metadata"].(map[string]any)
	if metadata["workflow"] != "checkout" {
		t.Errorf("metadata workflow = %v, want checkout", metadata["workflow"])
	}
	contexts := rawTrace["contexts"].([]any)
	if len(contexts) != 1 {
		t.Errorf("expected 1 context entry, got %d", len(contexts))
	}
}

func TestTraceCompletion_OnlyForRootSpans(t *testing.T) {
	clearAllTraceStates()
	var mu sync.Mutex
	var spanCount, traceCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if strings.Contains(r.URL.Path, "externalSpans") {
			spanCount++
		}
		if strings.Contains(r.URL.Path, "externalTraces") {
			traceCount++
		}
		mu.Unlock()
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "outer", func(ctx context.Context) (any, error) {
		return client.Span(ctx, "inner", func(ctx context.Context) (any, error) {
			return "inner-result", nil
		})
	})

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if spanCount != 2 {
		t.Errorf("expected 2 spans, got %d", spanCount)
	}
	if traceCount != 1 {
		t.Errorf("expected 1 trace completion, got %d", traceCount)
	}
}

func TestTraceCompletion_HasEndedAt(t *testing.T) {
	clearAllTraceStates()
	var mu sync.Mutex
	var tracePayload map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if strings.Contains(r.URL.Path, "externalTraces") {
			mu.Lock()
			tracePayload = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "test-function", func(ctx context.Context) (any, error) {
		return "done", nil
	})

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if tracePayload == nil {
		t.Fatal("no trace payload captured")
	}
	rawTrace := tracePayload["externalTrace"].(map[string]any)
	endedAt, ok := rawTrace["ended_at"].(string)
	if !ok || endedAt == "" {
		t.Error("ended_at should be present in trace completion")
	}
}

func TestStart_SetPrompt(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	_, span := client.Start(ctx, "test", "TestSpan", WithType("llm"))
	span.SetPrompt("You are a helpful assistant.")
	span.End()

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	if spanData["prompt"] != "You are a helpful assistant." {
		t.Errorf("prompt = %v, want 'You are a helpful assistant.'", spanData["prompt"])
	}
}

func TestStart_PromptOmittedWhenNotCalled(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	_, span := client.Start(ctx, "test", "TestSpan")
	span.SetOutput("result")
	span.End()

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	if _, ok := spanData["prompt"]; ok {
		t.Error("prompt should not be present when SetPrompt was not called")
	}
}

func TestStart_SetPromptLastCallWins(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	_, span := client.Start(ctx, "test", "TestSpan", WithType("llm"))
	span.SetPrompt("first prompt")
	span.SetPrompt("second prompt")
	span.SetPrompt("final prompt")
	span.End()

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	if spanData["prompt"] != "final prompt" {
		t.Errorf("prompt = %v, want 'final prompt'", spanData["prompt"])
	}
}

func TestSetPrompt_NilReceiver(t *testing.T) {
	var span *ActiveSpan
	span.SetPrompt("should not panic")
}

func TestSetPrompt_EmptyStringIgnored(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	_, span := client.Start(ctx, "test", "TestSpan")
	span.SetPrompt("")
	span.End()

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	rawSpan := captured["rawSpan"].(map[string]any)
	spanData := rawSpan["span_data"].(map[string]any)
	if _, ok := spanData["prompt"]; ok {
		t.Error("prompt should not be present when SetPrompt was called with empty string")
	}
}

// TestSpan_MarksNonReplayableOnLossyInput verifies the public Span path runs the
// payload through finalizeSpanPayload: an unserializable input (a channel) still
// ships, marked serialization_degraded (non-replayable), with routing intact.
func TestSpan_MarksNonReplayableOnLossyInput(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	client.Span(ctx, "lossy-service", func(ctx context.Context) (any, error) {
		return "ok", nil
	}, WithName("Lossy"), WithType("function"), WithInput(make(chan int)))

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if captured == nil {
		t.Fatal("no span captured")
	}
	errs, ok := captured["errors"].([]any)
	if !ok || len(errs) == 0 {
		t.Fatalf("expected a serialization_degraded error, got errors=%v", captured["errors"])
	}
	first, _ := errs[0].(map[string]any)
	if first["step"] != serializationDegradedStep {
		t.Fatalf("step = %v, want %s", first["step"], serializationDegradedStep)
	}
	if captured["traceFunctionKey"] != "lossy-service" {
		t.Errorf("traceFunctionKey lost on collapse: %v", captured["traceFunctionKey"])
	}
}

// TestSpan_MarksNonReplayableOnOversizeInput verifies that an oversize input
// (which capValue stubs into a JSON-clean placeholder) is still marked
// serialization_degraded - the gap where the cap would otherwise hide the loss.
func TestSpan_MarksNonReplayableOnOversizeInput(t *testing.T) {
	var mu sync.Mutex
	var captured map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if strings.Contains(r.URL.Path, "externalSpans") {
			mu.Lock()
			captured = payload
			mu.Unlock()
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	ctx := context.Background()

	big := strings.Repeat("x", MaxSerializedValueBytes+1)
	client.Span(ctx, "oversize-service", func(ctx context.Context) (any, error) {
		return "ok", nil
	}, WithName("Oversize"), WithType("function"), WithInput(big))

	client.FlushTraces(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if captured == nil {
		t.Fatal("no span captured")
	}
	errs, ok := captured["errors"].([]any)
	if !ok || len(errs) == 0 {
		t.Fatalf("oversize input should be marked non-replayable, got errors=%v", captured["errors"])
	}
	first, _ := errs[0].(map[string]any)
	if first["step"] != serializationDegradedStep {
		t.Fatalf("step = %v, want %s", first["step"], serializationDegradedStep)
	}
}
