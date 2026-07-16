package bitfab

import (
	"context"
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
	ctx = withSpanContext(ctx, "trace-1", "span-1")

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
	ctx = withSpanContext(ctx, "trace-1", "span-1")
	ctx = withSpanContext(ctx, "trace-1", "span-2")

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
	parent := withSpanContext(ctx, "trace-1", "span-1")
	_ = withSpanContext(parent, "trace-1", "span-2")

	// Parent context should still see span-1
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
	ctx = withSpanContext(ctx, "trace-main", "span-main")

	done := make(chan string)
	go func() {
		// New goroutine with a fresh context should not see the parent span
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
	ctx := withSpanContext(context.Background(), "trace-1", "span-1")

	if got := GetCurrentTrace(ctx).TraceID(); got != "trace-1" {
		t.Errorf("TraceID() = %q, want trace-1", got)
	}
	var trace *CurrentTrace
	if got := trace.TraceID(); got != "" {
		t.Errorf("nil TraceID() = %q, want empty", got)
	}
}

func TestCurrentSpan_IDs(t *testing.T) {
	ctx := withSpanContext(context.Background(), "trace-1", "span-1")
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
