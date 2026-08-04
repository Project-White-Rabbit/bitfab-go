package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type recordedRequest struct {
	endpoint string
	payload  map[string]any
}

type fakeSender struct {
	mu        sync.Mutex
	requests  []recordedRequest
	inFlight  int
	maxFlight int
	respond   func(index int, payload map[string]any) (map[string]any, error)
	delay     time.Duration
}

func (f *fakeSender) send(endpoint string, payload map[string]any, _ time.Duration) (map[string]any, error) {
	f.mu.Lock()
	index := len(f.requests)
	f.requests = append(f.requests, recordedRequest{endpoint: endpoint, payload: payload})
	f.inFlight++
	if f.inFlight > f.maxFlight {
		f.maxFlight = f.inFlight
	}
	respond := f.respond
	delay := f.delay
	f.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	f.mu.Lock()
	f.inFlight--
	f.mu.Unlock()

	if respond == nil {
		return map[string]any{}, nil
	}
	return respond(index, payload)
}

func (f *fakeSender) recorded() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedRequest{}, f.requests...)
}

func (f *fakeSender) peakConcurrency() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxFlight
}

func newTestTransport(t *testing.T, sender *fakeSender, mutate func(*otelTransportConfig)) *otelTransport {
	t.Helper()
	cfg := otelTransportConfig{
		apiKey:              func() string { return "test-key" },
		directSender:        sender.send,
		maxRequestBytes:     otelMaxRequestBytes,
		maxRequestBatchSize: otelDirectMaxRequestBatchSize,
		exportConcurrency:   otelDefaultExportConcurrency,
		maxQueueSize:        otelMaxQueueSize,
		scheduleDelay:       time.Hour,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	transport := newOtelTransport(cfg)
	t.Cleanup(func() { transport.shutdown(5 * time.Second) })
	return transport
}

// carriersIn pulls every carrier out of the recorded OTLP requests.
func carriersIn(t *testing.T, requests []recordedRequest) []capturedCarrier {
	t.Helper()
	var carriers []capturedCarrier
	for _, request := range requests {
		encoded, err := json.Marshal(request.payload)
		if err != nil {
			t.Fatalf("re-encoding request: %v", err)
		}
		carriers = append(carriers, decodeOtlpCarriers(t, encoded)...)
	}
	return carriers
}

func TestOtel_CarrierHoldsOperationAndPayload(t *testing.T) {
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, nil)

	transport.submit(operationExternalSpan, map[string]any{
		"traceFunctionKey": "generateAnswer",
		"rawSpan": map[string]any{
			"started_at": "2026-01-02T03:04:05.000Z",
			"ended_at":   "2026-01-02T03:04:06.000Z",
			"span_data":  map[string]any{"name": "callModel"},
		},
	}, nil)
	if !transport.flush(5 * time.Second) {
		t.Fatal("flush reported failure")
	}

	requests := sender.recorded()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	if requests[0].endpoint != otelTracesEndpoint {
		t.Errorf("endpoint = %q, want %q", requests[0].endpoint, otelTracesEndpoint)
	}

	carriers := carriersIn(t, requests)
	if len(carriers) != 1 {
		t.Fatalf("carriers = %d, want 1", len(carriers))
	}
	carrier := carriers[0]
	if carrier.operation != string(operationExternalSpan) {
		t.Errorf("operation = %q, want external_span", carrier.operation)
	}
	if carrier.name != "callModel" {
		t.Errorf("carrier name = %q, want callModel (the span's own name)", carrier.name)
	}
	if carrier.payload["traceFunctionKey"] != "generateAnswer" {
		t.Errorf("payload = %#v, want the submitted Bitfab payload", carrier.payload)
	}

	started, err := strconv.ParseInt(carrier.startTimeUnixNano, 10, 64)
	if err != nil {
		t.Fatalf("parsing startTimeUnixNano: %v", err)
	}
	want := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).UnixNano()
	if started != want {
		t.Errorf("startTimeUnixNano = %d, want %d (the payload's started_at)", started, want)
	}
}

func TestOtel_CarrierNameFallbacks(t *testing.T) {
	cases := []struct {
		name      string
		operation traceOperation
		payload   map[string]any
		want      string
	}{
		{
			name:      "trace completion uses the function key",
			operation: operationExternalTrace,
			payload:   map[string]any{"traceFunctionKey": "generateAnswer"},
			want:      "generateAnswer",
		},
		{
			name:      "span without span_data name falls back to the function key",
			operation: operationExternalSpan,
			payload:   map[string]any{"traceFunctionKey": "generateAnswer"},
			want:      "generateAnswer",
		},
		{
			name:      "no key at all falls back to the operation",
			operation: operationExternalTrace,
			payload:   map[string]any{},
			want:      "bitfab.external_trace",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := otelSpanName(testCase.operation, testCase.payload); got != testCase.want {
				t.Errorf("otelSpanName = %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestOtel_CarrierStatusReflectsPayloadError(t *testing.T) {
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, nil)

	transport.submit(operationExternalSpan, map[string]any{
		"rawSpan": map[string]any{"span_data": map[string]any{"name": "ok", "error": nil}},
	}, nil)
	transport.submit(operationExternalSpan, map[string]any{
		"rawSpan": map[string]any{"span_data": map[string]any{"name": "boom", "error": "exploded"}},
	}, nil)
	transport.flush(5 * time.Second)

	byName := map[string]int{}
	for _, carrier := range carriersIn(t, sender.recorded()) {
		byName[carrier.name] = carrier.statusCode
	}
	if byName["ok"] != 0 {
		t.Errorf("clean span status = %d, want 0 (unset)", byName["ok"])
	}
	if byName["boom"] != 2 {
		t.Errorf("errored span status = %d, want 2 (OTLP error)", byName["boom"])
	}
}

func TestOtel_StatusCodeMappingIsNotIdentity(t *testing.T) {
	if got := otlpStatus(sdktrace.Status{Code: codes.Ok})["code"]; got != 1 {
		t.Errorf("codes.Ok maps to %v, want OTLP 1", got)
	}
	if got := otlpStatus(sdktrace.Status{Code: codes.Error})["code"]; got != 2 {
		t.Errorf("codes.Error maps to %v, want OTLP 2", got)
	}
	if got := otlpStatus(sdktrace.Status{Code: codes.Unset})["code"]; got != 0 {
		t.Errorf("codes.Unset maps to %v, want OTLP 0", got)
	}
	status := otlpStatus(sdktrace.Status{Code: codes.Error, Description: "why"})
	if status["message"] != "why" {
		t.Errorf("message = %v, want why", status["message"])
	}
	if _, ok := otlpStatus(sdktrace.Status{Code: codes.Unset})["message"]; ok {
		t.Error("an empty description must be omitted rather than sent as an empty string")
	}
}

func TestOtel_DirectRequestsAreCountBounded(t *testing.T) {
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, func(cfg *otelTransportConfig) {
		cfg.maxExportBatchSize = 512
	})

	for i := range 20 {
		transport.submit(operationExternalSpan, map[string]any{"index": i}, nil)
	}
	transport.flush(10 * time.Second)

	requests := sender.recorded()
	if len(requests) != 3 {
		t.Fatalf("requests = %d, want 3 (20 carriers at 8 per request)", len(requests))
	}
	for _, request := range requests {
		carriers := carriersIn(t, []recordedRequest{request})
		if len(carriers) > otelDirectMaxRequestBatchSize {
			t.Errorf("request held %d carriers, want at most %d", len(carriers), otelDirectMaxRequestBatchSize)
		}
	}
	if total := len(carriersIn(t, requests)); total != 20 {
		t.Errorf("delivered %d carriers, want all 20", total)
	}
}

func TestOtel_DirectRequestsAreByteBounded(t *testing.T) {
	sender := &fakeSender{}
	const limit = 20_000
	transport := newTestTransport(t, sender, func(cfg *otelTransportConfig) {
		cfg.maxRequestBytes = limit
		cfg.maxExportBatchSize = 512
	})

	for i := range 6 {
		transport.submit(operationExternalSpan, map[string]any{
			"index": i,
			"blob":  strings.Repeat("x", 6_000),
		}, nil)
	}
	transport.flush(10 * time.Second)

	requests := sender.recorded()
	if len(requests) < 2 {
		t.Fatalf("requests = %d, want the batch split by size", len(requests))
	}
	for _, request := range requests {
		if size := encodedSize(request.payload); size > limit {
			t.Errorf("request encoded to %d bytes, want at most %d", size, limit)
		}
	}
	if total := len(carriersIn(t, requests)); total != 6 {
		t.Errorf("delivered %d carriers, want all 6", total)
	}
}

func TestOtel_OversizedSingleCarrierIsRejected(t *testing.T) {
	resetWarnOnce()
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, func(cfg *otelTransportConfig) {
		cfg.maxRequestBytes = 2_000
	})

	transport.submit(operationExternalSpan, map[string]any{"blob": strings.Repeat("x", 10_000)}, nil)
	if transport.flush(5 * time.Second) {
		t.Error("flush must report failure when a carrier cannot be exported")
	}
	if len(sender.recorded()) != 0 {
		t.Error("an oversized carrier must be rejected before it is sent")
	}
}

func TestOtel_ExportConcurrencyIsBounded(t *testing.T) {
	sender := &fakeSender{delay: 50 * time.Millisecond}
	transport := newTestTransport(t, sender, func(cfg *otelTransportConfig) {
		cfg.exportConcurrency = 2
		cfg.maxExportBatchSize = 512
	})

	for i := range 40 {
		transport.submit(operationExternalSpan, map[string]any{"index": i}, nil)
	}
	transport.flush(30 * time.Second)

	if len(sender.recorded()) != 5 {
		t.Fatalf("requests = %d, want 5", len(sender.recorded()))
	}
	if peak := sender.peakConcurrency(); peak > 2 {
		t.Errorf("peak concurrent requests = %d, want at most 2", peak)
	}
}

// TestOtel_PanickingSenderDoesNotHangExport guards the dispatch loop: recovery
// is per request, so a sender that panics on every call must still let every
// worker keep draining. Recovering per worker instead would strand the
// dispatcher on the unbuffered channel and hang flush past its deadline.
func TestOtel_PanickingSenderDoesNotHangExport(t *testing.T) {
	resetWarnOnce()
	sender := &fakeSender{
		respond: func(int, map[string]any) (map[string]any, error) {
			panic("sender exploded")
		},
	}
	transport := newTestTransport(t, sender, func(cfg *otelTransportConfig) {
		cfg.exportConcurrency = 2
		cfg.maxExportBatchSize = 512
	})

	for i := range 40 {
		transport.submit(operationExternalSpan, map[string]any{"index": i}, nil)
	}

	done := make(chan bool, 1)
	go func() { done <- transport.flush(20 * time.Second) }()

	select {
	case flushed := <-done:
		if flushed {
			t.Error("flush must report failure when every export panicked")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("flush hung: a panicking sender starved the export dispatch loop")
	}

	if got := len(sender.recorded()); got != 5 {
		t.Errorf("requests attempted = %d, want all 5 (workers must survive a panic)", got)
	}
}

// TestOtel_ExportStopsWhenContextIsCancelled verifies the dispatch loop honors
// the export deadline instead of queueing every remaining request.
func TestOtel_ExportStopsWhenContextIsCancelled(t *testing.T) {
	sender := &fakeSender{delay: 200 * time.Millisecond}
	exporter := &bitfabSpanExporter{
		directSender:        sender.send,
		maxRequestBytes:     otelMaxRequestBytes,
		maxRequestBatchSize: otelDirectMaxRequestBatchSize,
		exportConcurrency:   1,
	}

	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer provider.Shutdown(context.Background())
	tracer := provider.Tracer("test")

	var spans []sdktrace.ReadOnlySpan
	recorder := tracetest.NewSpanRecorder()
	recorded := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)
	for i := range 40 {
		_, span := recorded.Tracer("test").Start(context.Background(), fmt.Sprintf("carrier-%d", i))
		span.End()
	}
	_ = tracer
	spans = recorder.Ended()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := exporter.ExportSpans(ctx, spans)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("a cancelled export must report failure, not silent success")
	}
	if elapsed > 5*time.Second {
		t.Errorf("export took %v; it must abandon dispatch when the deadline expires", elapsed)
	}
	if got := len(sender.recorded()); got >= 5 {
		t.Errorf("requests sent = %d; dispatch should have stopped early", got)
	}
}

func TestOtel_RetriesTransientFailures(t *testing.T) {
	var attempts atomic.Int32
	sender := &fakeSender{
		respond: func(int, map[string]any) (map[string]any, error) {
			if attempts.Add(1) < 3 {
				return nil, &httpStatusError{StatusCode: 503, Body: "unavailable"}
			}
			return map[string]any{}, nil
		},
	}
	transport := newTestTransport(t, sender, nil)

	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil)
	if !transport.flush(10 * time.Second) {
		t.Error("flush must succeed once a retry lands")
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}

func TestOtel_DoesNotRetryPermanentFailures(t *testing.T) {
	var attempts atomic.Int32
	sender := &fakeSender{
		respond: func(int, map[string]any) (map[string]any, error) {
			attempts.Add(1)
			return nil, &httpStatusError{StatusCode: 400, Body: "bad request"}
		},
	}
	transport := newTestTransport(t, sender, nil)

	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil)
	if transport.flush(10 * time.Second) {
		t.Error("flush must report failure on a permanent error")
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (no retry on 400)", got)
	}
}

func TestOtel_DoesNotRetryPayloadTooLarge(t *testing.T) {
	resetWarnOnce()
	var attempts atomic.Int32
	sender := &fakeSender{
		respond: func(int, map[string]any) (map[string]any, error) {
			attempts.Add(1)
			return nil, &httpStatusError{StatusCode: 413, Body: "too large"}
		},
	}
	transport := newTestTransport(t, sender, nil)

	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil)
	if transport.flush(10 * time.Second) {
		t.Error("flush must report failure on 413")
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (no retry on 413)", got)
	}
}

func TestOtel_PartialSuccessFailsTheExport(t *testing.T) {
	resetWarnOnce()
	sender := &fakeSender{
		respond: func(int, map[string]any) (map[string]any, error) {
			return map[string]any{
				"partialSuccess": map[string]any{
					"rejectedSpans": "2",
					"errorMessage":  "invalid payload",
				},
			}, nil
		},
	}
	transport := newTestTransport(t, sender, nil)

	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil)
	if transport.flush(10 * time.Second) {
		t.Error("flush must report failure when ingestion rejects carriers")
	}
	if len(sender.recorded()) != 1 {
		t.Errorf("requests = %d, want 1 (partial success is not retried)", len(sender.recorded()))
	}
}

func TestOtel_ZeroRejectedSpansIsSuccess(t *testing.T) {
	sender := &fakeSender{
		respond: func(int, map[string]any) (map[string]any, error) {
			return map[string]any{
				"partialSuccess": map[string]any{"rejectedSpans": "0"},
			}, nil
		},
	}
	transport := newTestTransport(t, sender, nil)

	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil)
	if !transport.flush(10 * time.Second) {
		t.Error("a partialSuccess reporting zero rejections is a success")
	}
}

func TestOtel_SubmitAfterShutdownIsDropped(t *testing.T) {
	resetWarnOnce()
	sender := &fakeSender{}
	cfg := otelTransportConfig{
		apiKey:              func() string { return "test-key" },
		directSender:        sender.send,
		maxRequestBytes:     otelMaxRequestBytes,
		maxRequestBatchSize: otelDirectMaxRequestBatchSize,
		exportConcurrency:   otelDefaultExportConcurrency,
		maxQueueSize:        otelMaxQueueSize,
		scheduleDelay:       time.Hour,
	}
	transport := newOtelTransport(cfg)

	transport.submit(operationExternalSpan, map[string]any{"before": true}, nil)
	if !transport.shutdown(5 * time.Second) {
		t.Fatal("shutdown reported failure")
	}
	transport.submit(operationExternalSpan, map[string]any{"after": true}, nil)

	carriers := carriersIn(t, sender.recorded())
	if len(carriers) != 1 {
		t.Fatalf("carriers = %d, want 1", len(carriers))
	}
	if carriers[0].payload["before"] != true {
		t.Errorf("payload = %#v, want the pre-shutdown submission", carriers[0].payload)
	}
}

func TestOtel_ShutdownIsIdempotent(t *testing.T) {
	sender := &fakeSender{}
	transport := newOtelTransport(otelTransportConfig{
		apiKey:              func() string { return "test-key" },
		directSender:        sender.send,
		maxRequestBytes:     otelMaxRequestBytes,
		maxRequestBatchSize: otelDirectMaxRequestBatchSize,
		exportConcurrency:   otelDefaultExportConcurrency,
		maxQueueSize:        otelMaxQueueSize,
		scheduleDelay:       time.Hour,
	})

	if !transport.shutdown(5 * time.Second) {
		t.Fatal("first shutdown reported failure")
	}
	if !transport.shutdown(5 * time.Second) {
		t.Fatal("second shutdown must be a no-op success")
	}

	liveOtelTransportsMu.Lock()
	_, stillLive := liveOtelTransports[transport]
	liveOtelTransportsMu.Unlock()
	if stillLive {
		t.Error("a shut-down transport must be dropped from the live set")
	}
}

func TestOtel_MaxRequestBytesFromEnv(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  int
	}{
		{"a smaller target is honored", "500000", 500_000},
		{"the ceiling cannot be raised", "9000000", otelMaxRequestBytes},
		{"zero falls back", "0", otelMaxRequestBytes},
		{"a negative value falls back", "-1", otelMaxRequestBytes},
		{"garbage falls back", "lots", otelMaxRequestBytes},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			resetWarnOnce()
			t.Setenv(otelMaxRequestBytesEnv, testCase.value)
			if got := otelMaxRequestBytesFromEnv(); got != testCase.want {
				t.Errorf("otelMaxRequestBytesFromEnv = %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestOtel_ExportConcurrencyFromEnv(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  int
	}{
		{"a valid value is honored", "8", 8},
		{"the ceiling is enforced", "128", otelDefaultExportConcurrency},
		{"zero falls back", "0", otelDefaultExportConcurrency},
		{"garbage falls back", "many", otelDefaultExportConcurrency},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			resetWarnOnce()
			t.Setenv(otelExportConcurrencyEnv, testCase.value)
			if got := otelExportConcurrencyFromEnv(); got != testCase.want {
				t.Errorf("otelExportConcurrencyFromEnv = %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestOtel_CollectorModeReplacesDirectDelivery(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var auths []string
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		auths = append(auths, r.Header.Get("Authorization"))
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer collector.Close()

	sender := &fakeSender{}
	transport := newTestTransport(t, sender, func(cfg *otelTransportConfig) {
		cfg.collectorEndpoint = collector.URL
	})

	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil)
	transport.flush(10 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(paths) == 0 {
		t.Fatal("the collector received nothing")
	}
	if paths[0] != "/v1/traces" {
		t.Errorf("collector path = %q, want /v1/traces appended", paths[0])
	}
	if auths[0] != "Bearer test-key" {
		t.Errorf("Authorization = %q, want Bearer test-key", auths[0])
	}
	if len(sender.recorded()) != 0 {
		t.Error("a configured collector must replace direct delivery, not duplicate it")
	}
}

func TestOtel_CollectorEndpointKeepsAnExplicitTracesPath(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer collector.Close()

	sender := &fakeSender{}
	transport := newTestTransport(t, sender, func(cfg *otelTransportConfig) {
		cfg.collectorEndpoint = collector.URL + "/v1/traces"
	})

	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil)
	transport.flush(10 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(paths) == 0 || paths[0] != "/v1/traces" {
		t.Errorf("collector paths = %#v, want a single /v1/traces", paths)
	}
}

func TestOtel_CollectorFailureFallsBackToDirect(t *testing.T) {
	resetWarnOnce()
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, func(cfg *otelTransportConfig) {
		cfg.collectorEndpoint = "://not-a-url"
	})

	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil)
	transport.flush(10 * time.Second)

	if len(sender.recorded()) != 1 {
		t.Fatalf(
			"direct requests = %d, want 1: an unusable collector endpoint must not silently drop spans",
			len(sender.recorded()),
		)
	}
}

func TestOtel_PayloadHasError(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]any
		want    bool
	}{
		{"clean", map[string]any{}, false},
		{"nil errors", map[string]any{"errors": nil}, false},
		{"empty errors list", map[string]any{"errors": []any{}}, false},
		{"populated errors list", map[string]any{"errors": []any{"boom"}}, true},
		{"empty errors string", map[string]any{"errors": ""}, false},
		{
			"span data error",
			map[string]any{"rawSpan": map[string]any{"span_data": map[string]any{"error": "boom"}}},
			true,
		},
		{
			"nil span data error",
			map[string]any{"rawSpan": map[string]any{"span_data": map[string]any{"error": nil}}},
			false,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := payloadHasError(testCase.payload); got != testCase.want {
				t.Errorf("payloadHasError = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestOtel_FlushWithNothingQueuedSucceeds(t *testing.T) {
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, nil)
	if !transport.flush(5 * time.Second) {
		t.Error("flushing an idle transport must succeed")
	}
	if len(sender.recorded()) != 0 {
		t.Error("an idle flush must not send a request")
	}
}

// TestOtel_SpanPathEncodesExactlyOnce guards the seam between the encode-once
// send path and the OTel carrier: the carrier attribute must reuse the body the
// send path already built, not encode the payload a second time.
func TestOtel_SpanPathEncodesExactlyOnce(t *testing.T) {
	sink := &carrierSink{}
	server := newCarrierCaptureServer(t, sink)
	defer server.Close()

	encodes := 0
	hc := newHTTPClient("test-key", server.URL)
	hc.sendExternalSpan(map[string]any{
		"traceFunctionKey": "generateAnswer",
		"rawSpan": map[string]any{
			"span_data": map[string]any{"input": countingMarshaler{&encodes}},
		},
	})
	if !hc.flush(5 * time.Second) {
		t.Fatal("flush reported failure")
	}
	t.Cleanup(func() { hc.close(time.Second) })

	if encodes != 1 {
		t.Errorf("span payload encoded %d times, want exactly 1", encodes)
	}

	payloads := sink.spanPayloads()
	if len(payloads) != 1 {
		t.Fatalf("span payloads = %d, want 1", len(payloads))
	}
	rawSpan, _ := payloads[0]["rawSpan"].(map[string]any)
	spanData, _ := rawSpan["span_data"].(map[string]any)
	if spanData["input"] != "captured" {
		t.Errorf("carrier input = %v, want the single encode's output", spanData["input"])
	}
}

func TestOtel_UnserializablePayloadStillShips(t *testing.T) {
	resetWarnOnce()
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, nil)

	transport.submit(operationExternalSpan, map[string]any{
		"traceFunctionKey": "generateAnswer",
		"stray":            make(chan int),
	}, nil)
	transport.flush(5 * time.Second)

	carriers := carriersIn(t, sender.recorded())
	if len(carriers) != 1 {
		t.Fatalf("carriers = %d, want 1: a stray value must not drop the span", len(carriers))
	}
	if carriers[0].payload["traceFunctionKey"] != "generateAnswer" {
		t.Errorf("payload = %#v, want the serializable fields preserved", carriers[0].payload)
	}
}
