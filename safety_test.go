package bitfab

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRandomUUID_ValidV4(t *testing.T) {
	id := randomUUID()
	// 8-4-4-4-12 layout, version 4, variant 8/9/a/b.
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		t.Fatalf("not a uuid: %q", id)
	}
	lens := []int{8, 4, 4, 4, 12}
	for i, p := range parts {
		if len(p) != lens[i] {
			t.Fatalf("segment %d wrong length in %q", i, id)
		}
	}
	if parts[2][0] != '4' {
		t.Fatalf("expected version 4, got %q", id)
	}
	if v := parts[3][0]; v != '8' && v != '9' && v != 'a' && v != 'b' {
		t.Fatalf("expected variant 8/9/a/b, got %q", id)
	}
}

func TestRandomUUID_Unique(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := randomUUID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate uuid generated: %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestFallbackUUIDv4_Layout(t *testing.T) {
	// The fallback path must produce the same valid v4 shape as the crypto path
	// so trace/span correlation still works when crypto/rand is unavailable.
	id := fallbackUUIDv4()
	parts := strings.Split(id, "-")
	if len(parts) != 5 || parts[2][0] != '4' {
		t.Fatalf("fallback uuid malformed: %q", id)
	}
}

func TestCapValue_PassesSmallValuesThrough(t *testing.T) {
	v := map[string]any{"a": 1, "b": "small"}
	got := capValue(v)
	// Same value returned unchanged.
	if _, ok := got.(map[string]any); !ok {
		t.Fatalf("small value should pass through unchanged, got %T", got)
	}
}

func TestCapValue_StubsOversizedValue(t *testing.T) {
	big := strings.Repeat("x", MaxSerializedValueBytes+1)
	got := capValue(big)
	s, ok := got.(string)
	if !ok || !strings.Contains(s, "too_large") {
		t.Fatalf("oversized value should be stubbed, got %#v", got)
	}
	// The stub must itself be JSON-encodable so the span still ships.
	if _, err := json.Marshal(got); err != nil {
		t.Fatalf("stub is not JSON-encodable: %v", err)
	}
}

func TestCapValue_NilStaysNil(t *testing.T) {
	if got := capValue(nil); got != nil {
		t.Fatalf("nil should stay nil, got %#v", got)
	}
}

func TestSpan_RootDrainDoesNotBlockCaller(t *testing.T) {
	// A slow backend must not stall the user's traced call. The root-trace drain
	// (waiting on child spans + trace completion) now runs in the background, so
	// Span returns as soon as fn does regardless of network latency.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()

	start := time.Now()
	_, _ = client.Span(ctx, "root", func(ctx context.Context) (any, error) {
		_, _ = client.Span(ctx, "child", func(ctx context.Context) (any, error) {
			return "child", nil
		})
		return "root", nil
	})
	elapsed := time.Since(start)

	if elapsed > 1*time.Second {
		t.Fatalf("Span blocked the caller for %v; the root drain must be async", elapsed)
	}
	// Flush so the background goroutines settle before the server closes.
	client.FlushTraces(5 * time.Second)
}

func TestWarnOnce_DedupsByKey(t *testing.T) {
	resetWarnOnce()
	// Calling twice with the same key must not panic and must be idempotent.
	warnOnce("test-key", "first")
	warnOnce("test-key", "second-should-be-suppressed")
	if _, loaded := warnedKeys.Load("test-key"); !loaded {
		t.Fatal("key should be recorded after first warn")
	}
	resetWarnOnce()
	if _, loaded := warnedKeys.Load("test-key"); loaded {
		t.Fatal("resetWarnOnce should clear recorded keys")
	}
}
