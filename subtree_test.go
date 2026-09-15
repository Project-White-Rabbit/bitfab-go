package bitfab

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func subtreeClient(t *testing.T) (*Client, func() []map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var spans []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reader io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			defer gz.Close()
			reader = gz
		}
		body, _ := io.ReadAll(reader)
		mu.Lock()
		for _, carrier := range decodeOtlpCarriers(t, body) {
			spans = append(spans, carrier.payload)
		}
		mu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	}))
	c := newTestClient(server.URL)
	t.Cleanup(func() { c.Close(time.Second); server.Close() })
	return c, func() []map[string]any { mu.Lock(); defer mu.Unlock(); return append([]map[string]any(nil), spans...) }
}

func autoTestCall(ctx context.Context, symbol string, fn func() int) (result int) {
	node := EnterAutoNode(ctx, symbol, symbol, nil, []any{&result})
	if node != nil {
		defer func() {
			p := recover()
			node.End(p)
			if p != nil {
				panic(p)
			}
		}()
		if node.Mocked() {
			node.AssignMock()
			return
		}
	}
	return fn()
}

func TestSubtree_NestedMirrorsAndReparenting(t *testing.T) {
	c, requests := subtreeClient(t)
	no := false
	if err := c.Node("hidden", NodeOptions{Capture: &no}); err != nil {
		t.Fatal(err)
	}
	_, err := c.Trace(context.Background(), "outer", func(ctx context.Context) (any, error) {
		return c.Trace(ctx, "inner", func(ctx context.Context) (any, error) {
			return autoTestCall(ctx, "hidden", func() int { return autoTestCall(ctx, "leaf", func() int { return 7 }) }), nil
		}, TraceOptions{})
	}, TraceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	var spans []map[string]any
	for _, request := range requests() {
		if raw, ok := request["rawSpan"].(map[string]any); ok {
			spans = append(spans, raw)
		}
	}
	if len(spans) != 5 {
		t.Fatalf("spans = %#v", spans)
	}
	var innerRoot, innerCopy map[string]any
	for _, span := range spans {
		data := span["span_data"].(map[string]any)
		if data["name"] == "hidden" {
			t.Fatal("hidden node emitted")
		}
		if data["name"] == "inner" {
			if span["parent_id"] == nil {
				innerRoot = span
			} else {
				innerCopy = span
			}
		}
	}
	if innerRoot == nil || innerCopy == nil {
		t.Fatalf("missing linked roots: %#v", spans)
	}
	if innerCopy["span_data"].(map[string]any)["nested_trace_id"] != innerRoot["trace_id"] {
		t.Fatal("missing nested link")
	}
	for _, span := range spans {
		if span["span_data"].(map[string]any)["name"] == "leaf" && span["parent_id"] != innerRoot["id"] && span["parent_id"] != innerCopy["id"] {
			t.Fatal("child was not reparented")
		}
	}
}

func TestSubtree_FinalizerDeadlineAndManagedRoot(t *testing.T) {
	c, requests := subtreeClient(t)
	release := make(chan struct{})
	ctx := withManagedTraceRoot(context.Background())
	value, err := c.Span(ctx, "managed", func(ctx context.Context) (any, error) {
		return c.Trace(ctx, "managed", func(context.Context) (any, error) { return "actual", nil }, TraceOptions{Finalize: func(any) (any, error) { <-release; return "recorded", nil }})
	}, WithInput("original input"))
	if value != "actual" || err != nil {
		t.Fatalf("result=%v, %v", value, err)
	}
	if c.FlushTraces(time.Millisecond) {
		t.Fatal("flush ignored finalizer")
	}
	close(release)
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	spanCount, traceCount := 0, 0
	for _, request := range requests() {
		if raw, ok := request["rawSpan"].(map[string]any); ok {
			spanCount++
			if raw["span_data"].(map[string]any)["output"] != "recorded" {
				t.Fatalf("raw=%#v", raw)
			}
			if raw["span_data"].(map[string]any)["input"] != "original input" {
				t.Fatalf("managed input = %#v", raw)
			}
			if request["rootTraceFunctionKey"] != "managed" {
				t.Fatal("root key")
			}
		}
		if request["completed"] == true {
			traceCount++
		}
	}
	if spanCount != 1 || traceCount != 1 {
		t.Fatalf("spans=%d traces=%d", spanCount, traceCount)
	}
}

func TestSubtree_LimitsAndMixedGuard(t *testing.T) {
	c, requests := subtreeClient(t)
	zero := 0
	_, err := c.Trace(context.Background(), "limited", func(ctx context.Context) (any, error) {
		autoTestCall(ctx, "child", func() int { return 1 })
		_, err := c.Span(ctx, "wrong", func(context.Context) (any, error) { t.Fatal("mixed body ran"); return nil, nil })
		var mixed *MixedTracingError
		if !errors.As(err, &mixed) {
			t.Fatalf("mixed error=%v", err)
		}
		return 1, nil
	}, TraceOptions{MaxSpans: &zero})
	if err != nil {
		t.Fatal(err)
	}
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	spans := 0
	for _, request := range requests() {
		if request["rawSpan"] != nil {
			spans++
		}
	}
	if spans != 1 {
		t.Fatalf("spans=%d", spans)
	}
}

func TestSubtree_MockOverrideAndGoroutineContext(t *testing.T) {
	c, _ := subtreeClient(t)
	if err := c.Node("leaf", NodeOptions{Finalize: func(any) (any, error) { t.Error("mock output was finalized again"); return nil, nil }}); err != nil {
		t.Fatal(err)
	}
	replay := &replayContext{mockTree: &mockTree{spans: map[string]mockSpan{}}, callCounters: map[string]int{}, mockStrategy: MockNone, mockOverrides: []MockOverride{{Match: func(SpanNodeMeta) bool { return true }, Value: 9}}}
	ctx := withReplayContext(context.Background(), replay)
	_, err := c.Trace(ctx, "root", func(ctx context.Context) (any, error) {
		result := make(chan int, 1)
		snapshot := CaptureAutoContext()
		go RunAutoContext(snapshot, func() { result <- autoTestCall(nil, "leaf", func() int { t.Error("mocked body ran"); return 0 }) })
		if value := <-result; value != 9 {
			t.Fatalf("value=%d", value)
		}
		return nil, nil
	}, TraceOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSubtree_RejectsUncapturedMockNode(t *testing.T) {
	c, _ := subtreeClient(t)
	yes, no := true, false
	if err := c.Node("leaf", NodeOptions{Capture: &no, MockOnReplay: &yes}); err == nil {
		t.Fatal("uncaptured mock node was accepted")
	}
}

func TestSubtree_ReplayCanMockAbsorbedNestedRoot(t *testing.T) {
	c, _ := subtreeClient(t)
	replay := &replayContext{mockTree: &mockTree{spans: map[string]mockSpan{}}, callCounters: map[string]int{}, mockStrategy: MockNone, mockOverrides: []MockOverride{{Match: func(node SpanNodeMeta) bool { return node.SpanName == "nested" }, Value: 4}}}
	ctx := withReplayContext(context.Background(), replay)
	value, err := c.Trace(ctx, "root", func(ctx context.Context) (any, error) {
		return c.Trace(ctx, "nested", func(context.Context) (any, error) { t.Error("mocked nested root ran"); return 0, nil }, TraceOptions{})
	}, TraceOptions{})
	if value != 4 || err != nil {
		t.Fatalf("value=%v error=%v", value, err)
	}
}

func TestSubtree_SeedNestedRootStaysInOwnedTrace(t *testing.T) {
	c, requests := subtreeClient(t)
	ctx := withManagedTraceRoot(context.WithValue(context.Background(), seedContextKey{}, &seedContext{traceID: randomUUID()}))
	_, err := c.Span(ctx, "seed", func(ctx context.Context) (any, error) {
		return c.Trace(ctx, "seed", func(ctx context.Context) (any, error) {
			return c.Trace(ctx, "nested", func(ctx context.Context) (any, error) {
				return autoTestCall(ctx, "leaf", func() int { return 1 }), nil
			}, TraceOptions{})
		}, TraceOptions{})
	})
	if err != nil {
		t.Fatal(err)
	}
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	traceIDs := map[any]bool{}
	spanCount, completions := 0, 0
	for _, request := range requests() {
		if raw, ok := request["rawSpan"].(map[string]any); ok {
			spanCount++
			traceIDs[raw["trace_id"]] = true
		}
		if request["completed"] == true {
			completions++
		}
	}
	if len(traceIDs) != 1 || spanCount != 3 || completions != 1 {
		t.Fatalf("traces=%v spans=%d completions=%d", traceIDs, spanCount, completions)
	}
}

func TestSubtree_FinalizerFailurePreservesReturnAndPanic(t *testing.T) {
	c, requests := subtreeClient(t)
	if err := c.Node("leaf", NodeOptions{Finalize: func(any) (any, error) { panic("finalizer") }}); err != nil {
		t.Fatal(err)
	}
	value, err := c.Trace(context.Background(), "root", func(ctx context.Context) (any, error) {
		return autoTestCall(ctx, "leaf", func() int { return 3 }), nil
	}, TraceOptions{})
	if value != 3 || err != nil {
		t.Fatalf("value=%v err=%v", value, err)
	}
	func() {
		defer func() {
			if recover() != "original" {
				t.Error("panic changed")
			}
		}()
		_, _ = c.Trace(context.Background(), "panic", func(context.Context) (any, error) { panic("original") }, TraceOptions{})
	}()
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	found := false
	for _, request := range requests() {
		if raw, ok := request["rawSpan"].(map[string]any); ok {
			data := raw["span_data"].(map[string]any)
			if data["name"] == "leaf" {
				found = data["error"] == "finalize failed: finalizer"
			}
		}
	}
	if !found {
		t.Fatal("finalizer failure was not recorded")
	}
}

func TestSubtree_UnfinishedTaskClosesOnceWithoutLateFinalizer(t *testing.T) {
	c, requests := subtreeClient(t)
	if err := c.Node("slow", NodeOptions{Finalize: func(any) (any, error) { t.Error("late finalizer ran"); return nil, nil }}); err != nil {
		t.Fatal(err)
	}
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	_, err := c.Trace(context.Background(), "root", func(context.Context) (any, error) {
		snapshot := CaptureAutoContext()
		go RunAutoContext(snapshot, func() {
			defer close(done)
			autoTestCall(nil, "slow", func() int { close(started); <-release; return 1 })
		})
		<-started
		return nil, nil
	}, TraceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	<-done
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	count := 0
	for _, request := range requests() {
		if raw, ok := request["rawSpan"].(map[string]any); ok {
			data := raw["span_data"].(map[string]any)
			if data["name"] == "slow" {
				count++
				if data["error"] == nil {
					t.Fatal("unfinished node missing error")
				}
			}
		}
	}
	if count != 1 {
		t.Fatalf("slow nodes=%d", count)
	}
}

func TestSubtree_NodeGuardUnderOptInSpan(t *testing.T) {
	c, _ := subtreeClient(t)
	if err := c.Node("leaf", NodeOptions{}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, ok := recover().(*MixedTracingError); !ok {
			t.Fatal("node inside span did not raise mixed tracing error")
		}
	}()
	_, _ = c.Span(context.Background(), "root", func(ctx context.Context) (any, error) {
		return autoTestCall(ctx, "leaf", func() int { t.Error("mixed node body ran"); return 1 }), nil
	})
}

func TestSubtree_CloseTimeoutStillClosesTransport(t *testing.T) {
	c, requests := subtreeClient(t)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	_, err := c.Trace(context.Background(), "root", func(context.Context) (any, error) { return 1, nil }, TraceOptions{Finalize: func(v any) (any, error) { <-release; return v, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if c.Close(time.Millisecond) {
		t.Fatal("close ignored unfinished finalizer")
	}
	if c.shouldRecord(context.Background()) || c.CaptureEnabled() {
		t.Fatal("timed out close left recording enabled")
	}
	close(release)
	if !c.waitAutoFinalizers(time.Second) {
		t.Fatal("finalizer did not finish")
	}
	_, _ = c.Trace(context.Background(), "later", func(context.Context) (any, error) { return 2, nil }, TraceOptions{})
	if len(requests()) != 0 {
		t.Fatal("closed client sent a span")
	}
}

type subtreePanicError struct{}

func (*subtreePanicError) Error() string { panic("error formatter failed") }

func TestSubtree_BrokenErrorFormatterDoesNotChangeResult(t *testing.T) {
	c, _ := subtreeClient(t)
	original := &subtreePanicError{}
	value, err := c.Trace(context.Background(), "root", func(context.Context) (any, error) { return 7, original }, TraceOptions{})
	if value != 7 || err != original {
		t.Fatal("instrumentation changed the application result")
	}
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
}
