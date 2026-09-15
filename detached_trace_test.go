package bitfab

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

const detachedTraceID = "11111111-1111-4111-8111-111111111111"

func TestDetachedTraceUpdates(t *testing.T) {
	var payloads []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/api/sdk/traces/"+detachedTraceID {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Error("missing authentication")
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		payloads = append(payloads, payload)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer server.Close()
	client := NewClient("test-key", WithServiceURL(server.URL))
	trace, err := client.GetTrace(detachedTraceID)
	if err != nil || trace.TraceID() != detachedTraceID {
		t.Fatalf("handle = %#v, error = %v", trace, err)
	}
	ctx := context.Background()
	for _, err := range []error{
		trace.AddContext(ctx, map[string]any{"review": "complete"}),
		trace.SetMetadata(ctx, map[string]any{"region": "west"}),
		trace.SetSessionID(ctx, "session-2"),
		trace.SetName(ctx, "Reviewed case"),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	want := []map[string]any{
		{"appendContexts": []any{map[string]any{"review": "complete"}}},
		{"mergeMetadata": map[string]any{"region": "west"}},
		{"setSessionId": "session-2"},
		{"setName": "Reviewed case"},
	}
	if !reflect.DeepEqual(payloads, want) {
		t.Fatalf("payloads = %#v", payloads)
	}
}

func TestDetachedTraceNoOpsAndValidation(t *testing.T) {
	client := NewClient("test-key", WithServiceURL(":invalid"), WithEnabled(false))
	for _, id := range []string{"", "not-uuid", "../labels"} {
		if _, err := client.GetTrace(id); err == nil {
			t.Errorf("accepted invalid ID %q", id)
		}
	}
	trace, err := client.GetTrace(detachedTraceID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, err := range []error{
		trace.AddContext(ctx, nil), trace.SetMetadata(ctx, nil),
		trace.SetName(ctx, ""), trace.SetSessionID(ctx, ""),
		trace.AddContext(ctx, map[string]any{"x": true}),
		trace.SetMetadata(ctx, map[string]any{"x": true}),
		trace.SetName(ctx, "name"), trace.SetSessionID(ctx, "session"),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDetachedTracePropagatesRejectionAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()
	trace, err := NewClient("test-key", WithServiceURL(server.URL)).GetTrace(detachedTraceID)
	if err != nil {
		t.Fatal(err)
	}
	var statusErr *httpStatusError
	if err := trace.SetName(context.Background(), "name"); !errors.As(err, &statusErr) || statusErr.StatusCode != 404 {
		t.Fatalf("expected 404 error, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := trace.SetName(ctx, "name"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}
