package bitfab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type replayTestServerState struct {
	mu          sync.Mutex
	startBody   map[string]any
	spans       []map[string]any
	traces      []map[string]any
	statusCalls int
	completeErr bool
}

func replayTestHandler(t *testing.T, state *replayTestServerState, items []map[string]any) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/api/sdk/replay/start":
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatalf("decode start body: %v", err)
			}
			state.mu.Lock()
			state.startBody = body
			state.mu.Unlock()
			writeReplayTestJSON(t, w, map[string]any{
				"testRunId":  "run-1",
				"testRunUrl": "/experiments/run-1",
				"items":      items,
			})
		case strings.HasPrefix(request.URL.Path, "/api/sdk/externalSpans/"):
			spanID := strings.TrimPrefix(request.URL.Path, "/api/sdk/externalSpans/")
			input := map[string]any{
				"source-span-1": []any{"alpha", float64(2)},
				"source-span-2": []any{"beta", float64(3)},
			}[spanID]
			if input == nil {
				http.Error(w, "missing span", http.StatusNotFound)
				return
			}
			writeReplayTestJSON(t, w, map[string]any{
				"id":              spanID,
				"externalTraceId": "external-" + spanID,
				"rawData": map[string]any{"span_data": map[string]any{
					"input":  input,
					"output": "old-" + spanID,
				}},
			})
		case request.URL.Path == "/api/sdk/externalSpans":
			var payload map[string]any
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatalf("decode span payload: %v", err)
			}
			state.mu.Lock()
			state.spans = append(state.spans, payload)
			state.mu.Unlock()
		case request.URL.Path == "/api/sdk/externalTraces":
			var payload map[string]any
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatalf("decode trace payload: %v", err)
			}
			state.mu.Lock()
			state.traces = append(state.traces, payload)
			state.mu.Unlock()
		case request.URL.Path == "/api/sdk/replay/status":
			state.mu.Lock()
			state.statusCalls++
			state.mu.Unlock()
			writeReplayTestJSON(t, w, map[string]any{"traceIds": state.traceIDs()})
		case request.URL.Path == "/api/sdk/replay/complete":
			if state.completeErr {
				http.Error(w, "completion failed", http.StatusInternalServerError)
				return
			}
			traceIDs := state.traceIDs()
			tokens := map[string]any{}
			for _, traceID := range traceIDs {
				tokens[traceID] = map[string]any{"input": 4, "output": 2, "cached": 0, "total": 6}
			}
			writeReplayTestJSON(t, w, map[string]any{
				"id":         "run-1",
				"status":     "completed",
				"traceIds":   traceIDs,
				"tokens":     tokens,
				"traceCount": len(traceIDs),
			})
		default:
			http.NotFound(w, request)
		}
	}
}

func (state *replayTestServerState) traceIDs() map[string]string {
	state.mu.Lock()
	defer state.mu.Unlock()
	traceIDs := make(map[string]string, len(state.traces))
	for _, trace := range state.traces {
		localID, _ := trace["id"].(string)
		if localID != "" {
			traceIDs[localID] = "server-" + localID
		}
	}
	return traceIDs
}

func writeReplayTestJSON(t *testing.T, writer http.ResponseWriter, value any) {
	t.Helper()
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func replayItems() []map[string]any {
	return []map[string]any{
		{
			"originalTraceId": "original-trace-1",
			"originalSpanId":  "source-span-1",
			"durationMs":      12,
			"tokens":          map[string]any{"input": 1, "output": 2, "cached": 0, "total": 3},
			"model":           "old-model",
		},
		{
			"sourceTraceId": "original-trace-2",
			"sourceSpanId":  "source-span-2",
			"durationMs":    20,
		},
	}
}

func TestReplayRunsTypedFunctionAndPersistsResults(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()

	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var started []ReplayItemStartProgress
	var finished []ReplayItemFinishProgress
	var callbackMu sync.Mutex
	resultPath := filepath.Join(t.TempDir(), "replay", "result.json")
	t.Setenv("BITFAB_REPLAY_RESULT_PATH", resultPath)

	result, err := client.Replay(
		context.Background(),
		"typed-workflow",
		func(ctx context.Context, name string, count int) (string, error) {
			if GetCurrentTrace(ctx) == nil {
				return "", errors.New("missing replay trace context")
			}
			return fmt.Sprintf("%s:%d", name, count), nil
		},
		&ReplayOptions{
			Limit:          2,
			MaxConcurrency: 2,
			OnItemStart: func(progress ReplayItemStartProgress) {
				callbackMu.Lock()
				defer callbackMu.Unlock()
				started = append(started, progress)
			},
			OnItemFinish: func(progress ReplayItemFinishProgress) {
				callbackMu.Lock()
				defer callbackMu.Unlock()
				finished = append(finished, progress)
			},
		},
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if got, want := []any{result.Items[0].Result, result.Items[1].Result}, []any{"alpha:2", "beta:3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("results = %#v, want %#v", got, want)
	}
	if result.TestRunID != "run-1" || result.TestRunURL != server.URL+"/experiments/run-1" {
		t.Fatalf("result identity = %#v", result)
	}
	for _, item := range result.Items {
		if item.TraceID == nil || !strings.HasPrefix(*item.TraceID, "server-") {
			t.Errorf("trace ID = %v, want server mapping", item.TraceID)
		}
		if item.Tokens == nil || item.Tokens.Total == nil || *item.Tokens.Total != 6 {
			t.Errorf("tokens = %#v, want replay total 6", item.Tokens)
		}
	}
	if len(started) != 2 || len(finished) != 2 || finished[1].Completed != 2 {
		t.Fatalf("callbacks started=%d finished=%#v", len(started), finished)
	}
	for _, progress := range finished {
		if progress.Item.TraceID == nil || !strings.HasPrefix(*progress.Item.TraceID, "server-") {
			t.Errorf("finish callback trace ID = %v, want server mapping", progress.Item.TraceID)
		}
	}
	serialized, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatalf("read replay result file: %v", err)
	}
	if !strings.HasSuffix(string(serialized), "\n") || !strings.Contains(string(serialized), `"originalTraceId":"original-trace-1"`) {
		t.Fatalf("result file = %q", serialized)
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.startBody["traceFunctionKey"] != "typed-workflow" || state.startBody["limit"] != float64(2) {
		t.Errorf("start body = %#v", state.startBody)
	}
	if len(state.spans) != 2 || len(state.traces) != 2 {
		t.Fatalf("captured spans=%d traces=%d", len(state.spans), len(state.traces))
	}
	if state.statusCalls != 0 {
		t.Fatalf("status endpoint calls=%d, want acknowledgment-only persistence", state.statusCalls)
	}
	for _, span := range state.spans {
		if span["testRunId"] != "run-1" {
			t.Errorf("span omitted testRunId: %#v", span)
		}
		rawSpan := span["rawSpan"].(map[string]any)
		if rawSpan["input_source_span_id"] == nil {
			t.Errorf("span omitted input source: %#v", span)
		}
	}
	for _, trace := range state.traces {
		if trace["testRunId"] != "run-1" {
			t.Errorf("trace omitted testRunId: %#v", trace)
		}
	}
}

func TestReplayAdaptsInputsAndIsolatesFunctionErrors(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	type request struct {
		Name  string
		Count int
	}
	result, err := client.Replay(
		context.Background(),
		"adapted-workflow",
		func(input request) (string, error) {
			if input.Name == "beta" {
				return "", errors.New("beta is rejected")
			}
			return fmt.Sprintf("%s:%d", input.Name, input.Count), nil
		},
		&ReplayOptions{
			AdaptInputs: func(inputs []any, ctx AdaptContext) ([]any, error) {
				if ctx.OriginalTraceID == "" || len(inputs) != 2 {
					return nil, errors.New("missing adaptation context")
				}
				return []any{map[string]any{"Name": inputs[0], "Count": inputs[1]}}, nil
			},
		},
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if result.Items[0].Result != "alpha:2" || result.Items[0].Error != nil {
		t.Fatalf("successful item = %#v", result.Items[0])
	}
	if result.Items[1].TraceError == nil || result.Items[1].ReplayError != nil || result.Items[1].Error == nil {
		t.Fatalf("errored item = %#v", result.Items[1])
	}
	serialized, err := SerializeReplayResult(result)
	if err != nil {
		t.Fatalf("SerializeReplayResult: %v", err)
	}
	if !strings.Contains(serialized, `"traceError":{"message":"beta is rejected"`) || !strings.Contains(serialized, `"replayError":null`) {
		t.Fatalf("serialized errors = %s", serialized)
	}
}

func TestReplayPreservesItemsWhenCompletionFails(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{completeErr: true}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()[:1]))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	_, err := client.Replay(
		context.Background(),
		"completion-workflow",
		func(name string, count int) string { return fmt.Sprintf("%s:%d", name, count) },
		nil,
	)
	var replayErr *ReplayError
	if !errors.As(err, &replayErr) {
		t.Fatalf("error = %T %v, want *ReplayError", err, err)
	}
	if replayErr.TestRunID != "run-1" || len(replayErr.Items) != 1 || replayErr.Items[0].Result != "alpha:2" {
		t.Fatalf("ReplayError = %#v", replayErr)
	}
}

func TestNormalizeReplayOptions(t *testing.T) {
	resolved, err := normalizeReplayOptions(nil)
	if err != nil {
		t.Fatalf("normalize defaults: %v", err)
	}
	if resolved.Limit != defaultReplayLimit || resolved.MaxConcurrency != defaultReplayConcurrency {
		t.Fatalf("defaults = %#v", resolved)
	}
	if _, err := normalizeReplayOptions(&ReplayOptions{TraceIDs: []string{}}); err == nil {
		t.Fatal("empty trace IDs should fail")
	}
	if _, err := normalizeReplayOptions(&ReplayOptions{Limit: maxReplayLimit + 1}); err == nil {
		t.Fatal("oversized limit should fail")
	}
}

func TestReplayCallableInputShapes(t *testing.T) {
	tests := []struct {
		name      string
		fn        any
		raw       any
		want      []any
		wantError bool
	}{
		{
			name: "zero arguments with no recorded input",
			fn:   func() {},
			raw:  nil,
			want: []any{},
		},
		{
			name:      "zero arguments reject recorded input",
			fn:        func() {},
			raw:       "unexpected",
			wantError: true,
		},
		{
			name: "one scalar argument",
			fn:   func(string) {},
			raw:  "hello",
			want: []any{"hello"},
		},
		{
			name: "one slice argument preserves the whole value",
			fn:   func([]string) {},
			raw:  []any{"alpha", "beta"},
			want: []any{[]any{"alpha", "beta"}},
		},
		{
			name: "multiple arguments",
			fn:   func(string, int) {},
			raw:  []any{"hello", float64(2)},
			want: []any{"hello", float64(2)},
		},
		{
			name:      "multiple arguments reject scalar input",
			fn:        func(string, int) {},
			raw:       "hello",
			wantError: true,
		},
		{
			name: "variadic argument accepts scalar input",
			fn:   func(values ...string) {},
			raw:  "hello",
			want: []any{"hello"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			callable, err := prepareReplayCallable(test.fn)
			if err != nil {
				t.Fatalf("prepareReplayCallable: %v", err)
			}
			got, err := callable.inputs(test.raw)
			if test.wantError {
				if err == nil {
					t.Fatalf("inputs(%#v) succeeded, want error", test.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("inputs(%#v): %v", test.raw, err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("inputs(%#v) = %#v, want %#v", test.raw, got, test.want)
			}
		})
	}
}

func TestReplayCallableInvokeRecoversPanic(t *testing.T) {
	callable, err := prepareReplayCallable(func() string {
		panic("broken workflow")
	})
	if err != nil {
		t.Fatalf("prepareReplayCallable: %v", err)
	}
	result, err := callable.invoke(context.Background(), nil)
	if err == nil || err.Error() != "panic in replayed function: broken workflow" {
		t.Fatalf("invoke result=%#v error=%v", result, err)
	}
}

func TestPrepareReplayCallableRejectsNonFinalError(t *testing.T) {
	_, err := prepareReplayCallable(func() (error, string) { return nil, "result" })
	if err == nil {
		t.Fatal("non-final error return should fail")
	}
}

func TestReplaySendsExperimentOptionsAndExplicitTraceIDs(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, nil))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	description := "try the new prompt"

	_, err := client.Replay(
		context.Background(),
		"options-workflow",
		func() {},
		&ReplayOptions{
			TraceIDs:              []string{"trace-1", "trace-2"},
			Name:                  "candidate",
			CodeChangeDescription: &description,
			CodeChangeFiles: []CodeChangeFile{{
				Path:   "prompt.go",
				Before: "old",
				After:  "new",
			}},
			ExperimentGroupID: "group-1",
			DatasetID:         "dataset-1",
			GraderIDs:         []string{"grader-1"},
		},
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if _, present := state.startBody["limit"]; present {
		t.Errorf("start body should omit limit with trace IDs: %#v", state.startBody)
	}
	for key, want := range map[string]any{
		"name":                  "candidate",
		"codeChangeDescription": "try the new prompt",
		"experimentGroupId":     "group-1",
		"datasetId":             "dataset-1",
	} {
		if state.startBody[key] != want {
			t.Errorf("start body %s = %#v, want %#v", key, state.startBody[key], want)
		}
	}
	if got := state.startBody["traceIds"].([]any); len(got) != 2 {
		t.Errorf("trace IDs = %#v", got)
	}
	if got := state.startBody["graderIds"].([]any); len(got) != 1 {
		t.Errorf("grader IDs = %#v", got)
	}
	if got := state.startBody["codeChangeFiles"].([]any); len(got) != 1 {
		t.Errorf("code change files = %#v", got)
	}
}
