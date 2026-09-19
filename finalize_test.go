package bitfab

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestSpanFinalize_PreservesRawReturnAndWaitsForOutput(t *testing.T) {
	c, requests := subtreeClient(t)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	raw := &struct{ Text string }{Text: "live"}
	var calls atomic.Int32
	value, err := c.GetFunction("root").Span(context.Background(), func(context.Context) (any, error) { return raw, nil }, WithFinalize(func(value any) (any, error) {
		calls.Add(1)
		<-release
		return map[string]any{"text": value.(*struct{ Text string }).Text}, nil
	}))
	if value != raw || err != nil {
		t.Fatal("raw value changed")
	}
	if c.FlushTraces(time.Millisecond) {
		t.Fatal("flush ignored pending output")
	}
	close(release)
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	if calls.Load() != 1 {
		t.Fatal("finalizer did not run once")
	}
	spans, traces := 0, 0
	for _, request := range requests() {
		if raw, ok := request["rawSpan"].(map[string]any); ok {
			spans++
			output := raw["span_data"].(map[string]any)["output"].(map[string]any)
			if output["text"] != "live" {
				t.Fatalf("output=%v", output)
			}
		}
		if request["completed"] == true {
			traces++
		}
	}
	if spans != 1 || traces != 1 {
		t.Fatalf("spans=%d traces=%d", spans, traces)
	}
}

func TestSpanFinalize_ErrorPolicies(t *testing.T) {
	for _, test := range []struct {
		name     string
		finalize SpanFinalizer
		message  string
		partial  any
	}{
		{"error", func(any) (any, error) { return "discard", errors.New("failed") }, "finalize failed: failed", nil},
		{"panic", func(any) (any, error) { panic("failed") }, "finalize failed: failed", nil},
		{"partial", func(any) (any, error) {
			return nil, &StreamFinalizationError{Output: "partial", Err: errors.New("stream failed")}
		}, "stream failed", "partial"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, requests := subtreeClient(t)
			value, err := c.Span(context.Background(), "root", func(context.Context) (any, error) { return "raw", nil }, WithFinalize(test.finalize))
			if value != "raw" || err != nil {
				t.Fatal("finalizer changed application result")
			}
			if !c.FlushTraces(time.Second) {
				t.Fatal("flush")
			}
			for _, request := range requests() {
				if raw, ok := request["rawSpan"].(map[string]any); ok {
					data := raw["span_data"].(map[string]any)
					if data["error"] != test.message || data["output"] != test.partial {
						t.Fatalf("data=%v", data)
					}
				}
			}
		})
	}
}

func TestSpanFinalize_SkipsErrorsAndMockedOutputs(t *testing.T) {
	c, _ := subtreeClient(t)
	finalize := WithFinalize(func(any) (any, error) { t.Error("finalizer should be skipped"); return nil, nil })
	original := errors.New("application")
	_, err := c.Span(context.Background(), "error", func(context.Context) (any, error) { return nil, original }, finalize)
	if err != original {
		t.Fatal("error changed")
	}
	replay := &replayContext{mockStrategy: MockNone, mockTree: &mockTree{spans: map[string]mockSpan{}}, callCounters: map[string]int{}, mockOverrides: []MockOverride{{Match: func(SpanNodeMeta) bool { return true }, Value: "recorded"}}}
	ctx := withReplayContext(context.Background(), replay)
	_, err = c.Span(ctx, "root", func(ctx context.Context) (any, error) {
		return c.Span(ctx, "child", func(context.Context) (any, error) { t.Error("mocked body ran"); return nil, nil }, finalize)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
}

func TestSpanFinalize_RootWaitsForDroppedChildAndMetadata(t *testing.T) {
	for _, drop := range []bool{false, true} {
		t.Run(map[bool]string{false: "metadata", true: "dropped"}[drop], func(t *testing.T) {
			c, requests := subtreeClient(t)
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			var traceID string
			_, err := c.Span(context.Background(), "root", func(ctx context.Context) (any, error) {
				trace := GetCurrentTrace(ctx)
				traceID = trace.TraceID()
				_, err := c.Span(ctx, "child", func(context.Context) (any, error) { return "raw", nil }, WithFinalize(func(any) (any, error) {
					<-release
					trace.SetMetadata(map[string]any{"late": true})
					return "recorded", nil
				}))
				if drop {
					trace.Drop()
				}
				return "root", err
			})
			if err != nil {
				t.Fatal(err)
			}
			if getTraceState(traceID) == nil {
				t.Fatal("root retired before child finalized")
			}
			close(release)
			if !c.FlushTraces(time.Second) {
				t.Fatal("flush")
			}
			completion := false
			for _, request := range requests() {
				if raw, ok := request["rawSpan"].(map[string]any); ok && drop && raw["span_data"].(map[string]any)["name"] == "child" {
					t.Fatal("dropped child uploaded after finalization")
				}
				if request["completed"] == true {
					completion = true
					trace := request["externalTrace"].(map[string]any)
					if trace["metadata"].(map[string]any)["late"] != true {
						t.Fatal("late metadata lost")
					}
				}
			}
			if !completion || getTraceState(traceID) != nil {
				t.Fatal("completion did not retire state")
			}
		})
	}
}

func TestSpan_PanicIsRecordedAndRethrown(t *testing.T) {
	c, requests := subtreeClient(t)
	original := &struct{ Message string }{"panic"}
	var traceID string
	func() {
		defer func() {
			if recover() != original {
				t.Error("panic identity changed")
			}
		}()
		_, _ = c.Span(context.Background(), "root", func(ctx context.Context) (any, error) { traceID = GetCurrentTrace(ctx).TraceID(); panic(original) }, WithFinalize(func(any) (any, error) { t.Error("panic finalized"); return nil, nil }))
	}()
	if getTraceState(traceID) != nil {
		t.Fatal("panic leaked trace state")
	}
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	found := false
	for _, request := range requests() {
		if raw, ok := request["rawSpan"].(map[string]any); ok {
			found = raw["span_data"].(map[string]any)["error"] != nil
		}
	}
	if !found {
		t.Fatal("panic was not recorded")
	}
}

func TestSpanExperimentID_ReplayWinsAndExplicitAppliesOutsideReplay(t *testing.T) {
	for _, replaying := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit", true: "replay"}[replaying], func(t *testing.T) {
			c, requests := subtreeClient(t)
			ctx := context.Background()
			want := "explicit"
			if replaying {
				ctx = withReplayContext(ctx, &replayContext{experimentID: "experiment"})
				want = "experiment"
			}
			_, err := c.Span(ctx, "closure", func(context.Context) (any, error) { return 1, nil }, WithExperimentID("explicit"))
			if err != nil {
				t.Fatal(err)
			}
			_, span := c.Start(ctx, "handle", "handle", WithExperimentID("explicit"), WithFinalize(func(any) (any, error) { return "final", nil }))
			span.SetOutput("raw")
			span.End()
			span.End()
			if !c.FlushTraces(time.Second) {
				t.Fatal("flush")
			}
			for _, request := range requests() {
				if request["experimentId"] != want || request["testRunId"] != want {
					t.Fatalf("attribution=%v legacy=%v want=%s", request["experimentId"], request["testRunId"], want)
				}
			}
		})
	}
}

func TestSpanDeprecatedTestRunIDMatchesExperimentIDAndConflictsFail(t *testing.T) {
	c, requests := subtreeClient(t)
	ctx := context.Background()
	if _, err := c.Span(ctx, "closure", func(context.Context) (any, error) { return 1, nil }, WithTestRunID("legacy")); err != nil {
		t.Fatal(err)
	}
	_, span := c.Start(ctx, "handle", "handle", WithExperimentID("legacy"), WithTestRunID("legacy"))
	span.End()
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	for _, request := range requests() {
		if request["experimentId"] != "legacy" || request["testRunId"] != "legacy" {
			t.Fatalf("attribution=%v legacy=%v", request["experimentId"], request["testRunId"])
		}
	}
	ran := false
	_, err := c.Span(ctx, "closure", func(context.Context) (any, error) { ran = true; return 1, nil }, WithExperimentID("one"), WithTestRunID("two"))
	if err == nil || ran {
		t.Fatalf("conflict err=%v ran=%v", err, ran)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Start accepted conflicting experiment IDs")
			}
		}()
		c.Start(ctx, "handle", "handle", WithExperimentID("one"), WithTestRunID("two"))
	}()
	if _, err := c.Trace(ctx, "trace", func(context.Context) (any, error) { return 1, nil }, TraceOptions{ExperimentID: "one", TestRunID: "two"}); err == nil {
		t.Fatal("Trace accepted conflicting experiment IDs")
	}
	if err := c.Node(func() {}, NodeOptions{ExperimentID: "one", TestRunID: "two"}); err == nil {
		t.Fatal("Node accepted conflicting experiment IDs")
	}
}
