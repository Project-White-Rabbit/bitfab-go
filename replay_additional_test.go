package bitfab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFunctionReplayAndBindReplay(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, nil))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	function := client.GetFunction("bound-workflow")
	bound := function.BindReplay(func() string { return "ok" })
	if bound.traceFunctionKey != "bound-workflow" {
		t.Fatalf("bound key = %q", bound.traceFunctionKey)
	}
	result, err := function.Replay(context.Background(), bound.fn, nil)
	if err != nil {
		t.Fatalf("Function.Replay returned error: %v", err)
	}
	if result.TestRunID != "run-1" || len(result.Items) != 0 {
		t.Fatalf("result = %#v", result)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.startBody["traceFunctionKey"] != "bound-workflow" {
		t.Fatalf("start body = %#v", state.startBody)
	}
}

func TestNilFunctionReplayAndBindReplayAreSafe(t *testing.T) {
	var function *Function
	bound := function.BindReplay(func() {})
	if bound.traceFunctionKey != "" || bound.fn == nil {
		t.Fatalf("nil Function.BindReplay = %#v", bound)
	}
	if _, err := function.Replay(context.Background(), func() {}, nil); err == nil {
		t.Fatal("nil Function.Replay should fail")
	}
	withoutClient := &Function{traceFunctionKey: "workflow"}
	if _, err := withoutClient.Replay(context.Background(), func() {}, nil); err == nil {
		t.Fatal("Function.Replay without a client should fail")
	}
}

func TestReplayErrorMethodsAndStructuredSerialization(t *testing.T) {
	cause := errors.New("persistence unavailable")
	runErr := &ReplayError{Message: "replay did not finish", Cause: cause}
	if runErr.Error() != "replay did not finish" || !errors.Is(runErr, cause) {
		t.Fatalf("ReplayError = %#v", runErr)
	}

	transportCause := errors.New("connection reset")
	branchErr := &DBBranchReplayError{
		Code:            "lease_request_failed",
		Message:         "could not lease branch",
		OriginalTraceID: "trace-1",
		Cause:           transportCause,
	}
	if !errors.Is(branchErr, transportCause) {
		t.Fatal("DBBranchReplayError should unwrap its transport cause")
	}
	message := branchErr.Error()
	result := ReplayResult{Items: []ReplayItem{{
		OriginalTraceID: "trace-1",
		Error:           &message,
		ReplayError:     branchErr,
	}}}
	serialized, err := SerializeReplayResult(result)
	if err != nil {
		t.Fatalf("SerializeReplayResult: %v", err)
	}
	for _, expected := range []string{
		`"code":"lease_request_failed"`,
		`"originalTraceId":"trace-1"`,
		`"message":"connection reset"`,
	} {
		if !strings.Contains(serialized, expected) {
			t.Fatalf("serialized result omitted %s: %s", expected, serialized)
		}
	}
}

func TestReportReplayProgressWritesExactWireFormat(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	original := os.Stderr
	os.Stderr = writer
	t.Cleanup(func() { os.Stderr = original })

	progress := ReplayItemStartProgress{
		Type:      "started",
		TestRunID: "run-1",
		Started:   1,
		Total:     2,
		Item: AdaptContext{
			OriginalTraceID: "trace-1",
			OriginalSpanID:  "span-1",
			SourceTraceID:   "trace-1",
			SourceSpanID:    "span-1",
		},
	}
	ReportReplayProgress(progress)
	if err := writer.Close(); err != nil {
		t.Fatalf("close stderr pipe: %v", err)
	}
	os.Stderr = original
	wire, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read progress: %v", err)
	}
	const prefix = "@@bitfab:progress "
	if !strings.HasPrefix(string(wire), prefix) || !strings.HasSuffix(string(wire), "\n") {
		t.Fatalf("wire output = %q", wire)
	}
	var decoded ReplayItemStartProgress
	if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(string(wire), prefix))), &decoded); err != nil {
		t.Fatalf("decode progress JSON: %v", err)
	}
	if !reflect.DeepEqual(decoded, progress) {
		t.Fatalf("decoded progress = %#v, want %#v", decoded, progress)
	}
}

func TestReportReplayProgressIgnoresUnserializableValues(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	original := os.Stderr
	os.Stderr = writer
	t.Cleanup(func() { os.Stderr = original })

	ReportReplayProgress(make(chan int))
	if err := writer.Close(); err != nil {
		t.Fatalf("close stderr pipe: %v", err)
	}
	os.Stderr = original
	wire, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read progress: %v", err)
	}
	if len(wire) != 0 {
		t.Fatalf("unserializable progress wrote %q", wire)
	}
}

func TestReplayCallbacksRecoverPanics(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var starts atomic.Int64
	var finishes atomic.Int64

	result, err := client.Replay(
		context.Background(),
		"callback-workflow",
		func(name string, count int) string { return fmt.Sprintf("%s:%d", name, count) },
		&ReplayOptions{
			MaxConcurrency: 2,
			OnItemStart: func(ReplayItemStartProgress) {
				starts.Add(1)
				panic("start callback")
			},
			OnItemFinish: func(ReplayItemFinishProgress) {
				finishes.Add(1)
				panic("finish callback")
			},
		},
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if len(result.Items) != 2 || starts.Load() != 2 || finishes.Load() != 2 {
		t.Fatalf("items=%d starts=%d finishes=%d", len(result.Items), starts.Load(), finishes.Load())
	}
}

func TestReplaySetupFailuresRemainItemScoped(t *testing.T) {
	tests := []struct {
		name    string
		fn      any
		options *ReplayOptions
		want    string
	}{
		{
			name: "adapter error",
			fn:   func(string, int) {},
			options: &ReplayOptions{AdaptInputs: func([]any, AdaptContext) ([]any, error) {
				return nil, errors.New("adapter rejected input")
			}},
			want: "adapter rejected input",
		},
		{
			name: "adapted arity mismatch",
			fn:   func(string, int) {},
			options: &ReplayOptions{AdaptInputs: func([]any, AdaptContext) ([]any, error) {
				return []any{"only-one"}, nil
			}},
			want: "adapted input has 1 values",
		},
		{
			name:    "recorded arity mismatch",
			fn:      func(string, int, bool) {},
			options: nil,
			want:    "recorded input has 2 values",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
			state := &replayTestServerState{}
			server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()[:1]))
			defer server.Close()
			client := newTestClient(server.URL)
			defer client.Close(5 * time.Second)

			result, err := client.Replay(context.Background(), "setup-workflow", test.fn, test.options)
			if err != nil {
				t.Fatalf("Replay returned whole-run error: %v", err)
			}
			item := result.Items[0]
			if item.ReplayError == nil || item.TraceError != nil || item.Error == nil || !strings.Contains(*item.Error, test.want) {
				t.Fatalf("item = %#v", item)
			}
			if item.TraceID != nil || item.DurationMS != nil {
				t.Fatalf("setup failure unexpectedly executed: %#v", item)
			}
		})
	}
}

func TestWaitForReplayPersistencePollsUntilComplete(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/sdk/replay/status" {
			http.NotFound(writer, request)
			return
		}
		if calls.Add(1) == 1 {
			writeReplayTestJSON(t, writer, map[string]any{"traceIds": map[string]string{}})
			return
		}
		writeReplayTestJSON(t, writer, map[string]any{
			"traceIds": map[string]string{"local-1": "server-1"},
		})
	}))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	client.httpClient.trackTraceDeliveries([]string{"local-1"})
	client.httpClient.recordSubmittedCarrier(&carrierRef{traceID: "local-1", spanID: "span-1"})
	client.httpClient.recordSubmittedCarrier(&carrierRef{traceID: "local-1", spanID: "span-2"})
	client.httpClient.recordSubmittedCarrier(&carrierRef{traceID: "local-1"})

	persisted, err := client.waitForReplayPersistence(context.Background(), "run-1", []string{"local-1"})
	if err != nil {
		t.Fatalf("waitForReplayPersistence: %v", err)
	}
	if calls.Load() != 2 || len(persisted) != 0 {
		t.Fatalf("calls=%d persisted=%#v", calls.Load(), persisted)
	}
}

func TestWaitForReplayPersistenceUsesCarrierAcknowledgementsWithoutPolling(t *testing.T) {
	var statusCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		statusCalls.Add(1)
		http.Error(writer, "status polling should not be needed", http.StatusInternalServerError)
	}))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	client.httpClient.trackTraceDeliveries([]string{"local-1"})
	spanRef := &carrierRef{traceID: "local-1", spanID: "span-1"}
	closingRef := &carrierRef{traceID: "local-1"}
	client.httpClient.recordSubmittedCarrier(spanRef)
	client.httpClient.recordSubmittedCarrier(closingRef)
	client.httpClient.recordDeliveredCarriers([]carrierRef{*spanRef, *closingRef})
	client.httpClient.recordServerTraceIDs(map[string]any{
		"traceIds": map[string]any{"local-1": "server-1"},
	})

	persisted, err := client.waitForReplayPersistence(context.Background(), "run-1", []string{"local-1"})
	if err != nil {
		t.Fatalf("waitForReplayPersistence: %v", err)
	}
	if statusCalls.Load() != 0 || persisted["local-1"] != "server-1" {
		t.Fatalf("status calls=%d persisted=%#v", statusCalls.Load(), persisted)
	}
}

func TestWaitForReplayPersistenceHonorsContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "not ready", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	client.httpClient.trackTraceDeliveries([]string{"local-1"})
	client.httpClient.recordSubmittedCarrier(&carrierRef{traceID: "local-1", spanID: "span-1"})
	client.httpClient.recordSubmittedCarrier(&carrierRef{traceID: "local-1"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.waitForReplayPersistence(ctx, "run-1", []string{"local-1"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestWaitForReplayPersistenceSkipsFlushWhenNoTraceClosed(t *testing.T) {
	client := newTestClient("http://127.0.0.1:1")
	defer client.Close(time.Second)
	client.httpClient.trackTraceDeliveries([]string{"local-1"})
	persisted, err := client.waitForReplayPersistence(context.Background(), "run-1", []string{"local-1"})
	if err != nil || len(persisted) != 0 {
		t.Fatalf("persisted=%#v error=%v", persisted, err)
	}
}

func TestNormalizeReplayOptionsRejectsInvalidValues(t *testing.T) {
	tooManyTraceIDs := make([]string, maxReplayTraceIDs+1)
	tests := []ReplayOptions{
		{Limit: -1},
		{Limit: maxReplayLimit + 1},
		{MaxConcurrency: -1},
		{Mock: MockStrategy("sometimes")},
		{MockOverrides: []MockOverride{{}}},
		{TraceIDs: []string{}},
		{TraceIDs: tooManyTraceIDs},
	}
	for _, options := range tests {
		if _, err := normalizeReplayOptions(&options); err == nil {
			t.Fatalf("options %#v should fail", options)
		}
	}
}

func TestReplayCallableTypedNilVariadicAndMultipleReturns(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}
	callable, err := prepareReplayCallable(func(ctx context.Context, input *payload, prefix string, values ...int) (string, int, error) {
		if ctx == nil || input != nil {
			return "", 0, errors.New("unexpected context or pointer")
		}
		return prefix, values[0] + values[1], nil
	})
	if err != nil {
		t.Fatalf("prepareReplayCallable: %v", err)
	}
	inputs := []any{nil, "sum", float64(2), float64(3)}
	if err := callable.validateInputs(inputs); err != nil {
		t.Fatalf("validateInputs: %v", err)
	}
	result, err := callable.invoke(context.Background(), inputs)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !reflect.DeepEqual(result, []any{"sum", 5}) {
		t.Fatalf("result = %#v", result)
	}
}

func TestReplayRejectsInvalidClientFunctionAndKey(t *testing.T) {
	client := NewClient("", WithEnabled(false))
	defer client.Close(time.Second)
	tests := []struct {
		name string
		key  string
		fn   any
	}{
		{name: "nil function", key: "workflow", fn: nil},
		{name: "non function", key: "workflow", fn: "not-a-function"},
		{name: "disabled client", key: "workflow", fn: func() {}},
		{name: "blank key", key: "", fn: func() {}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := client
			if test.name == "blank key" {
				candidate = NewClient("key", WithServiceURL("http://127.0.0.1:1"))
				defer candidate.Close(time.Second)
			}
			if _, err := candidate.Replay(context.Background(), test.key, test.fn, nil); err == nil {
				t.Fatal("Replay should fail")
			}
		})
	}
}

func TestSerializeReplayResultRejectsUnsupportedResult(t *testing.T) {
	_, err := SerializeReplayResult(ReplayResult{Items: []ReplayItem{{Result: make(chan int)}}})
	if err == nil || !strings.Contains(err.Error(), "serialize replay result") {
		t.Fatalf("error = %v", err)
	}
}

func TestWriteReplayResultFileReportsDirectoryFailure(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, []byte("occupied\n"), 0o600); err != nil {
		t.Fatalf("write parent file: %v", err)
	}
	t.Setenv("BITFAB_REPLAY_RESULT_PATH", filepath.Join(parent, "result.json"))
	err := writeReplayResultFile(ReplayResult{})
	if err == nil || !strings.Contains(err.Error(), "create replay result directory") {
		t.Fatalf("error = %v", err)
	}
}
