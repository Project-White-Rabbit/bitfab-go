package bitfab

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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
	one := 1
	_, err := c.Trace(context.Background(), "limited", func(ctx context.Context) (any, error) {
		autoTestCall(ctx, "child", func() int { return 1 })
		_, err := c.Span(ctx, "wrong", func(context.Context) (any, error) { t.Fatal("mixed body ran"); return nil, nil })
		var mixed *MixedTracingError
		if !errors.As(err, &mixed) {
			t.Fatalf("mixed error=%v", err)
		}
		return 1, nil
	}, TraceOptions{MaxSpans: &one})
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

type planMarshalCounter struct{ calls *atomic.Int32 }

func (m planMarshalCounter) MarshalJSON() ([]byte, error) {
	m.calls.Add(1)
	return []byte(`"counted"`), nil
}

func loadContentOffPlan(t *testing.T, c *Client, off map[string][]string) {
	t.Helper()
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	var nodes []any
	for key, names := range off {
		for _, name := range names {
			nodes = append(nodes, map[string]any{"traceFunctionKey": key, "name": name, "captureContent": false})
		}
	}
	body := map[string]any{"nodes": nodes}
	plan := newSimulationPlan(func() (map[string]any, error) { return body, nil }, true)
	parsed, err := parseSimulationPlan(body)
	if err != nil {
		t.Fatal(err)
	}
	plan.contentOff = parsed
	plan.refreshAfter = time.Now().Add(time.Hour)
	c.httpClient.simulationPlan.stop()
	c.httpClient.simulationPlan = plan
}

func planTestNode(ctx context.Context, name string, calls *atomic.Int32) (result planMarshalCounter, err error) {
	node := EnterAutoNode(ctx, name, name, []any{planMarshalCounter{calls}}, []any{&result, &err})
	if node != nil {
		defer node.End(nil)
	}
	return planMarshalCounter{calls}, errors.New(name + " failed")
}

func sentSpansByName(requests []map[string]any) map[string][]map[string]any {
	byName := map[string][]map[string]any{}
	for _, request := range requests {
		if raw, ok := request["rawSpan"].(map[string]any); ok {
			data := raw["span_data"].(map[string]any)
			name, _ := data["name"].(string)
			byName[name] = append(byName[name], data)
		}
	}
	return byName
}

func TestSimulationPlan_LoadedPlanNeverBuildsContentOffContent(t *testing.T) {
	c, requests := subtreeClient(t)
	loadContentOffPlan(t, c, map[string][]string{"planned": {"secret"}, "optin": {"secret-span", "secret-start"}, "managed": {"managed"}})
	var secretCalls, publicCalls atomic.Int32
	_, err := c.Trace(context.Background(), "planned", func(ctx context.Context) (any, error) {
		_, _ = planTestNode(ctx, "secret", &secretCalls)
		_, _ = planTestNode(ctx, "public", &publicCalls)
		return nil, nil
	}, TraceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Span(context.Background(), "optin", func(context.Context) (any, error) {
		return planMarshalCounter{&secretCalls}, errors.New("secret-span failed")
	}, WithName("secret-span"), WithInput(planMarshalCounter{&secretCalls}))
	_, active := c.Start(context.Background(), "optin", "secret-start", WithInput(planMarshalCounter{&secretCalls}))
	active.SetOutput(planMarshalCounter{&secretCalls})
	active.SetError(errors.New("secret-start failed"))
	active.End()
	_, _ = c.Span(withManagedTraceRoot(context.Background()), "managed", func(ctx context.Context) (any, error) {
		return c.Trace(ctx, "managed", func(context.Context) (any, error) { return planMarshalCounter{&secretCalls}, nil }, TraceOptions{})
	}, WithInput(planMarshalCounter{&secretCalls}))
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	if calls := secretCalls.Load(); calls != 0 {
		t.Errorf("content-off values were serialized %d times", calls)
	}
	if publicCalls.Load() == 0 {
		t.Error("listed sibling content was never built")
	}
	byName := sentSpansByName(requests())
	for _, name := range []string{"secret", "secret-span", "secret-start", "managed"} {
		if len(byName[name]) != 1 {
			t.Fatalf("%s spans = %#v", name, byName[name])
		}
		data := byName[name][0]
		for _, field := range []string{"input", "output", "input_meta", "output_meta", "input_serialized", "output_serialized"} {
			if _, ok := data[field]; ok {
				t.Errorf("%s carried %s", name, field)
			}
		}
		if data["content_off_by_simulation_plan"] != true {
			t.Errorf("%s missing content-off marker: %#v", name, data)
		}
		if name != "managed" && data["error"] != name+" failed" {
			t.Errorf("%s lost error: %#v", name, data)
		}
	}
	if len(byName["public"]) != 1 {
		t.Fatalf("public spans = %#v", byName["public"])
	}
	public := byName["public"][0]
	if public["input"] == nil || public["output"] == nil || public["content_off_by_simulation_plan"] != nil || public["error"] != "public failed" {
		t.Errorf("public span lost content: %#v", public)
	}
}

func TestSubtree_ContentOffSpansCountTowardMaxSpansOnly(t *testing.T) {
	c, requests := subtreeClient(t)
	loadContentOffPlan(t, c, map[string][]string{"limited": {"secret"}})
	total, subtree := 10, 2
	_, err := c.Trace(context.Background(), "limited", func(ctx context.Context) (any, error) {
		for _, symbol := range []string{"secret", "public", "secret"} {
			for i := 0; i < 5; i++ {
				autoTestCall(ctx, symbol, func() int { return i })
			}
		}
		return nil, nil
	}, TraceOptions{MaxSpans: &total, MaxCapturedSubtreeSpans: &subtree})
	if err != nil {
		t.Fatal(err)
	}
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	byName := sentSpansByName(requests())
	if len(byName["secret"]) != 7 || len(byName["public"]) != subtree || len(byName["limited"]) != 1 {
		t.Fatalf("secret=%d public=%d root=%d", len(byName["secret"]), len(byName["public"]), len(byName["limited"]))
	}
}

func autoTraceTruncation(requests []map[string]any, traceID any) map[string]any {
	for _, request := range requests {
		trace, _ := request["externalTrace"].(map[string]any)
		if trace == nil || request["traceId"] != traceID && trace["id"] != traceID {
			continue
		}
		metadata, _ := trace["metadata"].(map[string]any)
		if _, ok := metadata["bitfab.truncated_by"]; ok {
			return metadata
		}
	}
	return nil
}

func traceAndCount(t *testing.T, c *Client, requests func() []map[string]any, key string, opts TraceOptions, body func(context.Context)) (map[string]int, map[string]any) {
	t.Helper()
	var traceID string
	_, err := c.Trace(context.Background(), key, func(ctx context.Context) (any, error) {
		traceID = GetCurrentSpan(ctx).TraceID()
		body(ctx)
		return nil, nil
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !c.FlushTraces(10 * time.Second) {
		t.Fatal("flush")
	}
	counts := map[string]int{}
	sent := requests()
	for _, request := range sent {
		if raw, ok := request["rawSpan"].(map[string]any); ok && raw["trace_id"] == traceID {
			name, _ := raw["span_data"].(map[string]any)["name"].(string)
			counts[name]++
		}
	}
	return counts, autoTraceTruncation(sent, traceID)
}

func callAuto(ctx context.Context, symbol string, times int) {
	for i := 0; i < times; i++ {
		autoTestCall(ctx, symbol, func() int { return i })
	}
}

func TestSubtree_DefaultMaxCapturedSubtreeSpansCapsDiscoveredSpans(t *testing.T) {
	c, requests := subtreeClient(t)
	counts, truncation := traceAndCount(t, c, requests, "discovered-default", TraceOptions{}, func(ctx context.Context) {
		callAuto(ctx, "discovered", 520)
	})
	if counts["discovered"] != 500 || counts["discovered-default"] != 1 {
		t.Fatalf("counts = %#v", counts)
	}
	if truncation["bitfab.truncated_by"] != "max_captured_subtree_spans" || truncation["bitfab.dropped_spans"] != float64(20) {
		t.Fatalf("truncation = %#v", truncation)
	}
}

func TestSubtree_DeclaredNodesRecordPastSubtreeLimitUpToMaxSpans(t *testing.T) {
	c, requests := subtreeClient(t)
	if err := c.Node("declared", NodeOptions{}); err != nil {
		t.Fatal(err)
	}
	counts, truncation := traceAndCount(t, c, requests, "declared-default", TraceOptions{}, func(ctx context.Context) {
		callAuto(ctx, "declared", 9000)
		callAuto(ctx, "discovered", 520)
		callAuto(ctx, "declared", 700)
	})
	if counts["discovered"] != 500 || counts["declared"] != 9499 || counts["declared-default"] != 1 || sumCounts(counts) != 10000 {
		t.Fatalf("counts = %#v", counts)
	}
	if truncation["bitfab.truncated_by"] != "max_spans,max_captured_subtree_spans" || truncation["bitfab.dropped_spans"] != float64(20+201) {
		t.Fatalf("truncation = %#v", truncation)
	}
}

func TestSubtree_ContentOffSpansCountTowardDefaultMaxSpansOnly(t *testing.T) {
	c, requests := subtreeClient(t)
	loadContentOffPlan(t, c, map[string][]string{"hidden-default": {"secret", "secret-declared"}})
	for _, symbol := range []string{"declared", "secret-declared"} {
		if err := c.Node(symbol, NodeOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	counts, _ := traceAndCount(t, c, requests, "hidden-default", TraceOptions{}, func(ctx context.Context) {
		callAuto(ctx, "secret", 100)
		callAuto(ctx, "discovered", 520)
		callAuto(ctx, "secret-declared", 50)
		callAuto(ctx, "declared", 9400)
		callAuto(ctx, "secret", 10)
		callAuto(ctx, "secret-declared", 10)
	})
	if counts["secret"] != 100 || counts["secret-declared"] != 50 || counts["discovered"] != 500 || counts["declared"] != 9349 || sumCounts(counts) != 10000 {
		t.Fatalf("counts = %#v", counts)
	}
}

func TestSubtree_MaxCapturedSubtreeSpansNeverExceedsMaxSpans(t *testing.T) {
	c, requests := subtreeClient(t)
	negative, zero, fifty, hundred := -1, 0, 50, 100
	if _, err := c.Trace(context.Background(), "negative", func(context.Context) (any, error) { t.Fatal("body ran"); return nil, nil }, TraceOptions{MaxCapturedSubtreeSpans: &negative}); err == nil {
		t.Fatal("negative MaxCapturedSubtreeSpans was accepted")
	}
	if _, err := c.Trace(context.Background(), "zero", func(context.Context) (any, error) { t.Fatal("body ran"); return nil, nil }, TraceOptions{MaxSpans: &zero}); err == nil {
		t.Fatal("MaxSpans of zero was accepted")
	}
	for _, opts := range []TraceOptions{{MaxSpans: &fifty}, {MaxSpans: &fifty, MaxCapturedSubtreeSpans: &hundred}} {
		counts, truncation := traceAndCount(t, c, requests, "capped", opts, func(ctx context.Context) {
			callAuto(ctx, "discovered", 60)
		})
		if counts["discovered"] != 49 || counts["capped"] != 1 || truncation["bitfab.truncated_by"] != "max_spans" || truncation["bitfab.dropped_spans"] != float64(11) {
			t.Fatalf("counts = %#v truncation = %#v", counts, truncation)
		}
	}
}

func sumCounts(counts map[string]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}

func TestSubtree_MaxSpansIsStrictTotalIncludingRoot(t *testing.T) {
	c, requests := subtreeClient(t)
	loadContentOffPlan(t, c, map[string][]string{"strict": {"secret", "secret-declared"}})
	for _, symbol := range []string{"declared", "secret-declared"} {
		if err := c.Node(symbol, NodeOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	limit := 10
	counts, truncation := traceAndCount(t, c, requests, "strict", TraceOptions{MaxSpans: &limit}, func(ctx context.Context) {
		var wg sync.WaitGroup
		for _, symbol := range []string{"declared", "discovered", "secret", "secret-declared"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				callAuto(ctx, symbol, 20)
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				_, _ = c.Trace(ctx, "nested", func(ctx context.Context) (any, error) { return i, nil }, TraceOptions{})
			}
		}()
		wg.Wait()
	})
	if total := sumCounts(counts); total != limit || counts["strict"] != 1 {
		t.Fatalf("sent %d spans for a root with MaxSpans %d: %#v", total, limit, counts)
	}
	if truncation["bitfab.truncated_by"] != "max_spans" || truncation["bitfab.dropped_spans"] != float64(4*20+5-(limit-1)) {
		t.Fatalf("truncation = %#v", truncation)
	}
}

func autoNestedCall(ctx context.Context, symbol string, fn func(context.Context)) {
	node := EnterAutoNode(ctx, symbol, symbol, nil, nil)
	if node != nil {
		defer node.End(nil)
		ctx = node.Context()
	}
	fn(ctx)
}

func TestSubtree_TruncationMetadataNamesLimitsAndDroppedCount(t *testing.T) {
	c, requests := subtreeClient(t)
	depth, spans, captured := 1, 6, 2
	counts, truncation := traceAndCount(t, c, requests, "truncated", TraceOptions{MaxDepth: &depth, MaxSpans: &spans, MaxCapturedSubtreeSpans: &captured}, func(ctx context.Context) {
		autoNestedCall(ctx, "outer", func(ctx context.Context) {
			callAuto(ctx, "too-deep", 3)
		})
		callAuto(ctx, "discovered", 4)
	})
	if counts["truncated"] != 1 || counts["outer"] != 1 || counts["discovered"] != 1 || counts["too-deep"] != 0 {
		t.Fatalf("counts = %#v", counts)
	}
	if truncation["bitfab.truncated_by"] != "max_depth,max_captured_subtree_spans" || truncation["bitfab.dropped_spans"] != float64(6) {
		t.Fatalf("truncation = %#v", truncation)
	}
}

func TestSubtree_UntruncatedTraceCarriesNoTruncationMetadata(t *testing.T) {
	c, requests := subtreeClient(t)
	var traceID string
	_, err := c.Trace(context.Background(), "whole", func(ctx context.Context) (any, error) {
		traceID = GetCurrentSpan(ctx).TraceID()
		GetCurrentTrace(ctx).SetMetadata(map[string]any{"ticket": "T-1"})
		callAuto(ctx, "child", 3)
		return nil, nil
	}, TraceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	found := false
	for _, request := range requests() {
		trace, _ := request["externalTrace"].(map[string]any)
		if trace == nil || trace["id"] != traceID {
			continue
		}
		found = true
		metadata, _ := trace["metadata"].(map[string]any)
		if metadata["ticket"] != "T-1" || len(metadata) != 1 {
			t.Fatalf("metadata = %#v", metadata)
		}
	}
	if !found {
		t.Fatal("trace completion was not sent")
	}
}

func TestSubtree_DroppedCallsPastLimitSkipTraceStateLockAndAllocations(t *testing.T) {
	c, _ := subtreeClient(t)
	limit := 1
	_, err := c.Trace(context.Background(), "cheap", func(ctx context.Context) (any, error) {
		scope := currentAutoScope(ctx)
		root := scope.frames[0].root
		parent := scope.frames[0].parent
		if node := EnterAutoNode(ctx, "dropped", "dropped", nil, nil); node != nil {
			t.Error("a call dropped past MaxSpans still built an invocation handle")
		}
		traceStateStore.Lock()
		done := make(chan float64)
		go func() {
			done <- testing.AllocsPerRun(1000, func() {
				if r, limited := root.add(parent, 1, "dropped", "function", "dropped", nil, false); r != nil || !limited {
					t.Error("call past MaxSpans was recorded")
				}
			})
		}()
		select {
		case allocs := <-done:
			traceStateStore.Unlock()
			if allocs != 0 {
				t.Errorf("dropped call allocated %v times", allocs)
			}
		case <-time.After(5 * time.Second):
			traceStateStore.Unlock()
			<-done
			t.Error("dropped call waited on the trace state lock")
		}
		if root.truncatedBy.Load() != truncatedByMaxSpans || root.droppedSpans.Load() < 1001 {
			t.Errorf("truncatedBy=%d dropped=%d", root.truncatedBy.Load(), root.droppedSpans.Load())
		}
		return nil, nil
	}, TraceOptions{MaxSpans: &limit})
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkSubtree_DroppedCallPastMaxSpans(b *testing.B) {
	c := newTestClient("http://127.0.0.1:1")
	defer c.Close(time.Second)
	limit := 1
	_, _ = c.Trace(context.Background(), "bench", func(ctx context.Context) (any, error) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			autoTestCall(ctx, "dropped", func() int { return 0 })
		}
		return nil, nil
	}, TraceOptions{MaxSpans: &limit})
}
