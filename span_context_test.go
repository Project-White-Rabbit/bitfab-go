package bitfab

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
)

func TestCurrentSpan_EmptyContext(t *testing.T) {
	ctx := context.Background()
	if got := currentSpan(ctx); got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
}

func TestWithSpanContext_SingleSpan(t *testing.T) {
	ctx := context.Background()
	ctx = withSpanContext(ctx, "trace-1", "span-1", "")

	got := currentSpan(ctx)
	if got == nil {
		t.Fatal("expected span, got nil")
	}
	if got.traceID != "trace-1" {
		t.Errorf("traceID = %q, want %q", got.traceID, "trace-1")
	}
	if got.spanID != "span-1" {
		t.Errorf("spanID = %q, want %q", got.spanID, "span-1")
	}
}

func TestWithSpanContext_NestedSpans(t *testing.T) {
	ctx := context.Background()
	ctx = withSpanContext(ctx, "trace-1", "span-1", "")
	ctx = withSpanContext(ctx, "trace-1", "span-2", "")

	got := currentSpan(ctx)
	if got == nil {
		t.Fatal("expected span, got nil")
	}
	if got.spanID != "span-2" {
		t.Errorf("spanID = %q, want %q (top of stack)", got.spanID, "span-2")
	}
}

func TestWithSpanContext_DoesNotMutateParent(t *testing.T) {
	ctx := context.Background()
	parent := withSpanContext(ctx, "trace-1", "span-1", "")
	_ = withSpanContext(parent, "trace-1", "span-2", "")

	got := currentSpan(parent)
	if got == nil {
		t.Fatal("expected span, got nil")
	}
	if got.spanID != "span-1" {
		t.Errorf("parent spanID = %q, want %q", got.spanID, "span-1")
	}
}

func TestWithSpanContext_GoroutineIsolation(t *testing.T) {
	ctx := context.Background()
	ctx = withSpanContext(ctx, "trace-main", "span-main", "")

	done := make(chan string)
	go func() {
		freshCtx := context.Background()
		s := currentSpan(freshCtx)
		if s != nil {
			done <- s.spanID
		} else {
			done <- ""
		}
	}()

	result := <-done
	if result != "" {
		t.Errorf("goroutine with fresh context saw span %q, expected none", result)
	}
}

func TestCurrentTrace_TraceID(t *testing.T) {
	ctx := withSpanContext(context.Background(), "trace-1", "span-1", "")

	if got := GetCurrentTrace(ctx).TraceID(); got != "trace-1" {
		t.Errorf("TraceID() = %q, want trace-1", got)
	}
	var trace *CurrentTrace
	if got := trace.TraceID(); got != "" {
		t.Errorf("nil TraceID() = %q, want empty", got)
	}
}

func TestCurrentSpan_IDs(t *testing.T) {
	ctx := withSpanContext(context.Background(), "trace-1", "span-1", "")
	span := GetCurrentSpan(ctx)

	if got := span.ID(); got != "span-1" {
		t.Errorf("ID() = %q, want span-1", got)
	}
	if got := span.TraceID(); got != "trace-1" {
		t.Errorf("TraceID() = %q, want trace-1", got)
	}
	var noSpan *CurrentSpan
	if noSpan.ID() != "" || noSpan.TraceID() != "" {
		t.Error("nil CurrentSpan should return empty IDs")
	}
}

func newTraceFunctionKeyTestClient(t *testing.T) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	c := newTestClient(server.URL)
	t.Cleanup(func() { c.Close(0) })
	return c
}

type traceFunctionKeysSeen struct {
	traceKey string
	spanKeys []string
}

func seeTraceFunctionKeys(ctx context.Context) traceFunctionKeysSeen {
	return traceFunctionKeysSeen{traceKey: GetCurrentTrace(ctx).TraceFunctionKey(), spanKeys: GetCurrentSpan(ctx).TraceFunctionKeys()}
}

func assertTraceFunctionKeys(t *testing.T, label string, got traceFunctionKeysSeen, wantTraceKey string, wantSpanKeys []string) {
	t.Helper()
	if got.traceKey != wantTraceKey {
		t.Errorf("%s: TraceFunctionKey() = %q, want %q", label, got.traceKey, wantTraceKey)
	}
	if !reflect.DeepEqual(got.spanKeys, wantSpanKeys) {
		t.Errorf("%s: TraceFunctionKeys() = %#v, want %#v", label, got.spanKeys, wantSpanKeys)
	}
}

func TestTraceFunctionKeys_OutsideTrace(t *testing.T) {
	var nilTrace *CurrentTrace
	var nilSpan *CurrentSpan
	if got := nilTrace.TraceFunctionKey(); got != "" {
		t.Errorf("nil TraceFunctionKey() = %q, want empty", got)
	}
	if got := nilSpan.TraceFunctionKeys(); got != nil {
		t.Errorf("nil TraceFunctionKeys() = %#v, want nil", got)
	}
	assertTraceFunctionKeys(t, "background", seeTraceFunctionKeys(context.Background()), "", nil)
}

func TestTraceFunctionKeys_SingleTraceFunction(t *testing.T) {
	c := newTraceFunctionKeyTestClient(t)
	var got traceFunctionKeysSeen
	c.Span(context.Background(), "answer-question", func(ctx context.Context) (any, error) {
		got = seeTraceFunctionKeys(ctx)
		return nil, nil
	})
	assertTraceFunctionKeys(t, "single", got, "answer-question", []string{"answer-question"})
}

func TestTraceFunctionKeys_NestedSpanJoinsOuterTrace(t *testing.T) {
	c := newTraceFunctionKeyTestClient(t)
	var inner, started traceFunctionKeysSeen
	var outerTrace, innerTrace string
	c.Span(context.Background(), "outer-agent", func(ctx context.Context) (any, error) {
		outerTrace = GetCurrentTrace(ctx).TraceID()
		c.GetFunction("inner-tool").Span(ctx, func(ctx context.Context) (any, error) {
			inner = seeTraceFunctionKeys(ctx)
			innerTrace = GetCurrentTrace(ctx).TraceID()
			return nil, nil
		})
		startCtx, span := c.Start(ctx, "inner-started", "step")
		started = seeTraceFunctionKeys(startCtx)
		span.End()
		return nil, nil
	})
	if outerTrace == "" || innerTrace != outerTrace {
		t.Fatalf("inner trace %q did not join outer trace %q", innerTrace, outerTrace)
	}
	assertTraceFunctionKeys(t, "nested Span", inner, "outer-agent", []string{"outer-agent", "inner-tool"})
	assertTraceFunctionKeys(t, "nested Start", started, "outer-agent", []string{"outer-agent", "inner-started"})
}

func TestTraceFunctionKeys_TraceInsideTraceStartsNewTrace(t *testing.T) {
	c := newTraceFunctionKeyTestClient(t)
	var outer, inner traceFunctionKeysSeen
	var outerTrace, innerTrace string
	_, err := c.Trace(context.Background(), "outer-agent", func(ctx context.Context) (any, error) {
		outer = seeTraceFunctionKeys(ctx)
		outerTrace = GetCurrentTrace(ctx).TraceID()
		return c.Trace(ctx, "inner-tool", func(ctx context.Context) (any, error) {
			inner = seeTraceFunctionKeys(ctx)
			innerTrace = GetCurrentTrace(ctx).TraceID()
			return nil, nil
		}, TraceOptions{})
	}, TraceOptions{})
	if err != nil {
		t.Fatalf("Trace returned %v", err)
	}
	if outerTrace == "" || innerTrace == "" || innerTrace == outerTrace {
		t.Fatalf("inner trace %q should differ from outer trace %q", innerTrace, outerTrace)
	}
	assertTraceFunctionKeys(t, "outer Trace", outer, "outer-agent", []string{"outer-agent"})
	assertTraceFunctionKeys(t, "inner Trace", inner, "inner-tool", []string{"outer-agent", "inner-tool"})
}

func TestTraceFunctionKeys_RecursionListsKeyOnce(t *testing.T) {
	c := newTraceFunctionKeyTestClient(t)
	var got traceFunctionKeysSeen
	var recurse func(ctx context.Context, depth int) (any, error)
	recurse = func(ctx context.Context, depth int) (any, error) {
		if depth == 0 {
			got = seeTraceFunctionKeys(ctx)
			return nil, nil
		}
		return c.Span(ctx, "recursive-step", func(ctx context.Context) (any, error) {
			return recurse(ctx, depth-1)
		})
	}
	c.Span(context.Background(), "planner", func(ctx context.Context) (any, error) {
		return recurse(ctx, 3)
	})
	assertTraceFunctionKeys(t, "recursion", got, "planner", []string{"planner", "recursive-step"})
}

func TestTraceFunctionKeys_PerGoroutine(t *testing.T) {
	c := newTraceFunctionKeyTestClient(t)
	keys := []string{"trace-a", "trace-b"}
	got := make([]traceFunctionKeysSeen, len(keys))
	var ready, done sync.WaitGroup
	ready.Add(len(keys))
	release := make(chan struct{})
	for i, key := range keys {
		done.Add(1)
		go func() {
			defer done.Done()
			c.Span(context.Background(), key, func(ctx context.Context) (any, error) {
				ready.Done()
				<-release
				c.Span(ctx, "shared-child", func(ctx context.Context) (any, error) {
					got[i] = seeTraceFunctionKeys(ctx)
					return nil, nil
				})
				return nil, nil
			})
		}()
	}
	ready.Wait()
	close(release)
	done.Wait()
	for i, key := range keys {
		assertTraceFunctionKeys(t, key, got[i], key, []string{key, "shared-child"})
	}
}
