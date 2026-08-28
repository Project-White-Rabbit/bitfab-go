package bitfab

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func replayMockHandler(
	t *testing.T,
	state *replayTestServerState,
	treeChildren []map[string]any,
	lazyFetches *atomic.Int64,
) http.HandlerFunc {
	t.Helper()
	base := replayTestHandler(t, state, replayItems()[:1])
	return func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasPrefix(request.URL.Path, "/api/sdk/replay/spanTree/"):
			includeOutputs := request.URL.Query().Get("includeOutputs") != "false"
			children := make([]map[string]any, len(treeChildren))
			for index, child := range treeChildren {
				copy := make(map[string]any, len(child))
				for key, value := range child {
					if includeOutputs || (key != "output" && key != "outputMeta") {
						copy[key] = value
					}
				}
				children[index] = copy
			}
			writeReplayTestJSON(t, writer, map[string]any{"root": map[string]any{
				"sourceSpanId":     "source-span-1",
				"externalSpanId":   "source-span-1",
				"traceFunctionKey": "mock-root",
				"spanName":         "mock-root",
				"type":             "function",
				"children":         children,
			}})
		case request.URL.Path == "/api/sdk/externalSpans/recorded-child":
			lazyFetches.Add(1)
			writeReplayTestJSON(t, writer, map[string]any{
				"id":              "recorded-child",
				"externalTraceId": "external-source",
				"rawData": map[string]any{"span_data": map[string]any{
					"output": "recorded",
				}},
			})
		default:
			base.ServeHTTP(writer, request)
		}
	}
}

func recordedChildTree() []map[string]any {
	return []map[string]any{{
		"sourceSpanId":     "source-child",
		"externalSpanId":   "recorded-child",
		"traceFunctionKey": "child-workflow",
		"spanName":         "Child",
		"type":             "function",
		"output":           "recorded",
		"children":         []any{},
	}}
}

func TestReplayMarkedMockUsesRecordedOutput(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	var lazyFetches atomic.Int64
	server := newLegacyCarrierServer(t, replayMockHandler(t, state, recordedChildTree(), &lazyFetches))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var realCalls atomic.Int64

	result, err := client.Replay(
		context.Background(),
		"mock-root",
		func(ctx context.Context, _ string, _ int) (string, error) {
			value, err := client.Span(
				ctx,
				"child-workflow",
				func(context.Context) (any, error) {
					realCalls.Add(1)
					return "real", nil
				},
				WithName("Child"),
				WithType("function"),
				WithMockOnReplay(true),
			)
			if err != nil {
				return "", err
			}
			return value.(string), nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if result.Items[0].Result != "recorded" || realCalls.Load() != 0 {
		t.Fatalf("result=%#v real calls=%d", result.Items[0], realCalls.Load())
	}
	if lazyFetches.Load() != 1 {
		t.Fatalf("lazy output fetches = %d, want 1", lazyFetches.Load())
	}
	assertMockPayload(t, state, "recorded")
}

func TestReplayOverridePrecedesRegisteredOverrideAndMemoizesOutput(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	var lazyFetches atomic.Int64
	server := newLegacyCarrierServer(t, replayMockHandler(t, state, recordedChildTree(), &lazyFetches))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	matcher := func(node SpanNodeMeta) bool { return node.TraceFunctionKey == "child-workflow" }
	if err := client.RegisterMockOverride(MockOverride{Match: matcher, Value: "registered"}); err != nil {
		t.Fatalf("RegisterMockOverride: %v", err)
	}
	defer client.ClearMockOverrides()

	result, err := client.Replay(
		context.Background(),
		"mock-root",
		func(ctx context.Context, _ string, _ int) (string, error) {
			value, err := client.Span(
				ctx,
				"child-workflow",
				func(context.Context) (any, error) { return "real", nil },
				WithName("Child"),
				WithInput("live-input"),
			)
			if err != nil {
				return "", err
			}
			return value.(string), nil
		},
		&ReplayOptions{
			Mock: MockNone,
			MockOverrides: []MockOverride{{
				Match: matcher,
				Resolve: func(ctx MockOverrideContext) (any, error) {
					first, err := ctx.GetOriginalOutput()
					if err != nil {
						return nil, err
					}
					second, err := ctx.GetOriginalOutput()
					if err != nil {
						return nil, err
					}
					if len(ctx.Inputs) != 1 || ctx.Inputs[0] != "live-input" || first != second {
						return nil, errors.New("incorrect override context")
					}
					return fmt.Sprintf("%s-override", first), nil
				},
			}},
		},
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if result.Items[0].Result != "recorded-override" || lazyFetches.Load() != 1 {
		t.Fatalf("item=%#v lazy fetches=%d", result.Items[0], lazyFetches.Load())
	}
	assertMockPayload(t, state, "override")
}

func TestReplayMockAllFailsClosedWhenOccurrenceIsMissing(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	var lazyFetches atomic.Int64
	server := newLegacyCarrierServer(t, replayMockHandler(t, state, nil, &lazyFetches))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var realCalls atomic.Int64

	result, err := client.Replay(
		context.Background(),
		"mock-root",
		func(ctx context.Context, _ string, _ int) (string, error) {
			_, err := client.Span(ctx, "missing-child", func(context.Context) (any, error) {
				realCalls.Add(1)
				return "unsafe", nil
			}, WithName("Missing"))
			return "", err
		},
		&ReplayOptions{Mock: MockAll},
	)
	if err != nil {
		t.Fatalf("Replay returned whole-run error: %v", err)
	}
	item := result.Items[0]
	if item.TraceError == nil || item.Error == nil || !strings.Contains(*item.Error, "real span was not executed") {
		t.Fatalf("item = %#v", item)
	}
	if realCalls.Load() != 0 {
		t.Fatalf("real child executed %d time(s)", realCalls.Load())
	}
}

func TestReplayMockOutputDecodesIntoConcreteType(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	var lazyFetches atomic.Int64
	children := recordedChildTree()
	children[0]["output"] = map[string]any{"Label": "recorded", "Score": 7}
	server := newLegacyCarrierServer(t, replayMockHandler(t, state, children, &lazyFetches))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	type output struct {
		Label string
		Score int
	}

	result, err := client.Replay(
		context.Background(),
		"mock-root",
		func(ctx context.Context, _ string, _ int) (output, error) {
			value, err := client.Span(
				ctx,
				"child-workflow",
				func(context.Context) (any, error) { return output{Label: "real"}, nil },
				WithName("Child"),
				WithMockOutputType[output](),
			)
			if err != nil {
				return output{}, err
			}
			return value.(output), nil
		},
		&ReplayOptions{Mock: MockAll},
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if result.Items[0].Result != (output{Label: "recorded", Score: 7}) || lazyFetches.Load() != 0 {
		t.Fatalf("item=%#v lazy fetches=%d", result.Items[0], lazyFetches.Load())
	}
}

func TestReplayMocksRepeatedSpanOccurrencesInRecordedOrder(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	var lazyFetches atomic.Int64
	children := []map[string]any{
		{
			"sourceSpanId":     "source-child-1",
			"externalSpanId":   "recorded-child-1",
			"traceFunctionKey": "repeated-child",
			"spanName":         "Lookup",
			"type":             "function",
			"output":           "first-recorded",
			"children":         []any{},
		},
		{
			"sourceSpanId":     "source-child-2",
			"externalSpanId":   "recorded-child-2",
			"traceFunctionKey": "repeated-child",
			"spanName":         "Lookup",
			"type":             "function",
			"output":           "second-recorded",
			"children":         []any{},
		},
	}
	server := newLegacyCarrierServer(t, replayMockHandler(t, state, children, &lazyFetches))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var realCalls atomic.Int64

	result, err := client.Replay(
		context.Background(),
		"mock-root",
		func(ctx context.Context, _ string, _ int) ([]string, error) {
			outputs := make([]string, 0, 2)
			for range 2 {
				value, spanErr := client.Span(
					ctx,
					"repeated-child",
					func(context.Context) (any, error) {
						realCalls.Add(1)
						return "real", nil
					},
					WithName("Lookup"),
					WithMockOnReplay(true),
					WithMockOutputType[string](),
				)
				if spanErr != nil {
					return nil, spanErr
				}
				outputs = append(outputs, value.(string))
			}
			return outputs, nil
		},
		&ReplayOptions{Mock: MockAll},
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	want := []string{"first-recorded", "second-recorded"}
	if !reflect.DeepEqual(result.Items[0].Result, want) || realCalls.Load() != 0 {
		t.Fatalf("result=%#v error=%v real calls=%d", result.Items[0], result.Items[0].TraceError, realCalls.Load())
	}
	if lazyFetches.Load() != 0 {
		t.Fatalf("inline outputs triggered %d lazy fetches", lazyFetches.Load())
	}
}

func TestReplayRepeatedSpanFailsClosedAfterRecordedOccurrencesAreExhausted(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	var lazyFetches atomic.Int64
	server := newLegacyCarrierServer(t, replayMockHandler(t, state, recordedChildTree(), &lazyFetches))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var realCalls atomic.Int64

	result, err := client.Replay(
		context.Background(),
		"mock-root",
		func(ctx context.Context, _ string, _ int) error {
			for range 2 {
				_, spanErr := client.Span(
					ctx,
					"child-workflow",
					func(context.Context) (any, error) {
						realCalls.Add(1)
						return "real", nil
					},
					WithName("Child"),
					WithMockOnReplay(true),
				)
				if spanErr != nil {
					return spanErr
				}
			}
			return nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("Replay returned whole-run error: %v", err)
	}
	item := result.Items[0]
	if item.Error == nil || !strings.Contains(*item.Error, "recorded occurrence 2 is unavailable") {
		t.Fatalf("item = %#v", item)
	}
	if realCalls.Load() != 0 {
		t.Fatalf("real function ran %d times", realCalls.Load())
	}
}

func TestReplayMockMatcherPanicFailsItemClosed(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	var lazyFetches atomic.Int64
	server := newLegacyCarrierServer(t, replayMockHandler(t, state, recordedChildTree(), &lazyFetches))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var realCalls atomic.Int64

	result, err := client.Replay(
		context.Background(),
		"mock-root",
		func(ctx context.Context, _ string, _ int) error {
			_, spanErr := client.Span(ctx, "child-workflow", func(context.Context) (any, error) {
				realCalls.Add(1)
				return "real", nil
			}, WithName("Child"))
			return spanErr
		},
		&ReplayOptions{
			Mock: MockNone,
			MockOverrides: []MockOverride{{
				Match: func(SpanNodeMeta) bool { panic("broken matcher") },
			}},
		},
	)
	if err != nil {
		t.Fatalf("Replay returned whole-run error: %v", err)
	}
	item := result.Items[0]
	if item.Error == nil || !strings.Contains(*item.Error, "mock matcher panicked: broken matcher") {
		t.Fatalf("item = %#v", item)
	}
	if realCalls.Load() != 0 {
		t.Fatalf("real function ran %d times", realCalls.Load())
	}
}

func TestReplayMockResolverPanicFailsItemClosed(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	var lazyFetches atomic.Int64
	server := newLegacyCarrierServer(t, replayMockHandler(t, state, recordedChildTree(), &lazyFetches))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var realCalls atomic.Int64

	result, err := client.Replay(
		context.Background(),
		"mock-root",
		func(ctx context.Context, _ string, _ int) error {
			_, spanErr := client.Span(ctx, "child-workflow", func(context.Context) (any, error) {
				realCalls.Add(1)
				return "real", nil
			}, WithName("Child"))
			return spanErr
		},
		&ReplayOptions{
			Mock: MockNone,
			MockOverrides: []MockOverride{{
				Match: func(SpanNodeMeta) bool { return true },
				Resolve: func(MockOverrideContext) (any, error) {
					panic("broken resolver")
				},
			}},
		},
	)
	if err != nil {
		t.Fatalf("Replay returned whole-run error: %v", err)
	}
	item := result.Items[0]
	if item.Error == nil || !strings.Contains(*item.Error, "mock value panicked: broken resolver") {
		t.Fatalf("item = %#v", item)
	}
	if realCalls.Load() != 0 {
		t.Fatalf("real function ran %d times", realCalls.Load())
	}
}

func TestReplayOverrideOriginalOutputFailsWithoutRecordedOccurrence(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	var lazyFetches atomic.Int64
	server := newLegacyCarrierServer(t, replayMockHandler(t, state, nil, &lazyFetches))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var realCalls atomic.Int64

	result, err := client.Replay(
		context.Background(),
		"mock-root",
		func(ctx context.Context, _ string, _ int) error {
			_, spanErr := client.Span(ctx, "new-child", func(context.Context) (any, error) {
				realCalls.Add(1)
				return "real", nil
			}, WithName("NewChild"))
			return spanErr
		},
		&ReplayOptions{
			Mock: MockNone,
			MockOverrides: []MockOverride{{
				Match: func(SpanNodeMeta) bool { return true },
				Resolve: func(ctx MockOverrideContext) (any, error) {
					return ctx.GetOriginalOutput()
				},
			}},
		},
	)
	if err != nil {
		t.Fatalf("Replay returned whole-run error: %v", err)
	}
	item := result.Items[0]
	if item.Error == nil || !strings.Contains(*item.Error, "no recorded span to source output") {
		t.Fatalf("item = %#v", item)
	}
	if realCalls.Load() != 0 {
		t.Fatalf("real function ran %d times", realCalls.Load())
	}
}

func TestReplayFlatNilOverrideIsAValidMockValue(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	var lazyFetches atomic.Int64
	server := newLegacyCarrierServer(t, replayMockHandler(t, state, recordedChildTree(), &lazyFetches))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var realCalls atomic.Int64

	result, err := client.Replay(
		context.Background(),
		"mock-root",
		func(ctx context.Context, _ string, _ int) (bool, error) {
			value, spanErr := client.Span(ctx, "child-workflow", func(context.Context) (any, error) {
				realCalls.Add(1)
				return "real", nil
			}, WithName("Child"))
			return value == nil, spanErr
		},
		&ReplayOptions{
			Mock: MockNone,
			MockOverrides: []MockOverride{{
				Match: func(SpanNodeMeta) bool { return true },
				Value: nil,
			}},
		},
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if result.Items[0].Result != true || realCalls.Load() != 0 {
		t.Fatalf("item=%#v real calls=%d", result.Items[0], realCalls.Load())
	}
	assertMockPayload(t, state, "override")
}

func TestReplayMockNoneExecutesRealChildWithoutFetchingTree(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()[:1]))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var realCalls atomic.Int64

	result, err := client.Replay(
		context.Background(),
		"mock-root",
		func(ctx context.Context, _ string, _ int) (string, error) {
			value, spanErr := client.Span(ctx, "child-workflow", func(context.Context) (any, error) {
				realCalls.Add(1)
				return "real", nil
			}, WithName("Child"), WithMockOnReplay(true))
			if spanErr != nil {
				return "", spanErr
			}
			return value.(string), nil
		},
		&ReplayOptions{Mock: MockNone},
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if result.Items[0].Result != "real" || realCalls.Load() != 1 {
		t.Fatalf("item=%#v real calls=%d", result.Items[0], realCalls.Load())
	}
}

func TestReplayConcurrentMocksShareOneLazyOutputFetch(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	var lazyFetches atomic.Int64
	children := recordedChildTree()
	second := make(map[string]any, len(children[0]))
	for key, value := range children[0] {
		second[key] = value
	}
	second["sourceSpanId"] = "source-child-2"
	children = append(children, second)
	server := newLegacyCarrierServer(t, replayMockHandler(t, state, children, &lazyFetches))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	result, err := client.Replay(
		context.Background(),
		"mock-root",
		func(ctx context.Context, _ string, _ int) ([]string, error) {
			outputs := make([]string, 2)
			errorsByIndex := make([]error, 2)
			var workers sync.WaitGroup
			for index := range 2 {
				workers.Add(1)
				go func() {
					defer workers.Done()
					value, spanErr := client.Span(
						ctx,
						"child-workflow",
						func(context.Context) (any, error) { return "real", nil },
						WithName("Child"),
						WithMockOnReplay(true),
					)
					errorsByIndex[index] = spanErr
					if value != nil {
						outputs[index] = value.(string)
					}
				}()
			}
			workers.Wait()
			for _, spanErr := range errorsByIndex {
				if spanErr != nil {
					return nil, spanErr
				}
			}
			return outputs, nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if !reflect.DeepEqual(result.Items[0].Result, []string{"recorded", "recorded"}) {
		t.Fatalf("item = %#v", result.Items[0])
	}
	if lazyFetches.Load() != 1 {
		t.Fatalf("lazy output fetches = %d, want 1", lazyFetches.Load())
	}
}

func assertMockPayload(t *testing.T, state *replayTestServerState, source string) {
	t.Helper()
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, span := range state.spans {
		if span["traceFunctionKey"] == "child-workflow" {
			if span["mocked"] != true || span["mockTarget"] != "output" || span["mockSource"] != source {
				t.Fatalf("mock payload = %#v", span)
			}
			return
		}
	}
	t.Fatal("mocked child payload not found")
}
