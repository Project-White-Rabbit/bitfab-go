package bitfab

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfiguration_LazyCredentialsRecoverAndCache(t *testing.T) {
	t.Setenv("BITFAB_API_KEY", "")
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer late-key" {
			t.Errorf("authorization: %q", r.Header.Get("Authorization"))
		}
		requests.Add(1)
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer server.Close()
	client := NewClient("", WithServiceURL(server.URL))
	defer client.Close(time.Second)
	calls := 0
	call := func(ctx context.Context) (any, error) { calls++; return "result", nil }
	if result, err := client.Span(context.Background(), "test", call); err != nil || result != "result" {
		t.Fatalf("untraced call %v %v", result, err)
	}
	client.FlushTraces(time.Second)
	if requests.Load() != 0 {
		t.Fatal("sent without key")
	}
	t.Setenv("BITFAB_API_KEY", "late-key")
	if _, err := client.Span(context.Background(), "test", call); err != nil {
		t.Fatal(err)
	}
	if !client.FlushTraces(time.Second) {
		t.Fatal("flush failed")
	}
	if requests.Load() == 0 || calls != 2 {
		t.Fatal("late credentials did not enable capture")
	}
	t.Setenv("BITFAB_API_KEY", "changed-key")
	if _, err := client.Span(context.Background(), "test", call); err != nil {
		t.Fatal(err)
	}
	client.FlushTraces(time.Second)
}

func TestConfiguration_CallableKeyAndCaptureOff(t *testing.T) {
	t.Setenv("BITFAB_API_KEY", "")
	var resolved atomic.Int32
	client := NewClient("", WithAPIKeyFunc(func() string { resolved.Add(1); return "key" }), WithCaptureEnabled(false), WithStrict(true))
	defer client.Close(time.Second)
	called := false
	result, err := client.Span(context.Background(), "test", func(context.Context) (any, error) { called = true; return 7, nil })
	if err != nil || result != 7 || !called {
		t.Fatalf("capture-off call %v %v", result, err)
	}
	if resolved.Load() != 0 {
		t.Fatal("ordinary capture-off call resolved credentials")
	}
	// Resource requests still resolve credentials independently of capture policy.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer key" {
			t.Error("callable key missing")
		}
		json.NewEncoder(w).Encode(map[string]any{"labels": []any{}})
	}))
	defer server.Close()
	client.httpClient.serviceURL = server.URL
	if _, err := client.Labels.GetAll(context.Background(), []string{"trace"}); err != nil {
		t.Fatal(err)
	}
	if resolved.Load() != 1 {
		t.Errorf("resolution calls %d", resolved.Load())
	}
}

func TestConfiguration_StrictIsLazy(t *testing.T) {
	t.Setenv("BITFAB_API_KEY", "")
	client := NewClient("", WithStrict(true))
	t.Setenv("BITFAB_API_KEY", "loaded-after-construction")
	if !client.CaptureEnabled() {
		t.Fatal("strict client did not resolve late key")
	}
}
