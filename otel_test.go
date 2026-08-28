package bitfab

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
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
	body     []byte
	payload  map[string]any
	prepared preparedRequest
}

type fakeSender struct {
	mu        sync.Mutex
	requests  []recordedRequest
	inFlight  int
	maxFlight int
	respond   func(index int, payload map[string]any) (map[string]any, error)
	delay     time.Duration
}

func (f *fakeSender) send(endpoint string, prepared preparedRequest, _ time.Duration) (map[string]any, error) {
	// Decoding here is an assertion in itself: the exporter assembles the
	// request by concatenating pre-encoded spans, so every test that sends
	// doubles as a check that what it assembled is still valid JSON.
	body := prepared.body
	if prepared.contentEncoding == "gzip" {
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			panic(fmt.Sprintf("exporter produced invalid gzip: %v", err))
		}
		body, err = io.ReadAll(reader)
		if err != nil {
			panic(fmt.Sprintf("reading exporter gzip failed: %v", err))
		}
		if err := reader.Close(); err != nil {
			panic(fmt.Sprintf("closing exporter gzip failed: %v", err))
		}
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		panic(fmt.Sprintf("exporter produced invalid JSON: %v", err))
	}

	f.mu.Lock()
	index := len(f.requests)
	f.requests = append(f.requests, recordedRequest{endpoint: endpoint, body: body, payload: payload, prepared: prepared})
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

// A payload the SDK degraded on its own (a budget trim, a stubbed value) is an
// incomplete capture, not a failed operation. Marking those carriers errored
// would turn every oversized span into an error in the user's dashboards, which
// is exactly the traffic the budget exists to keep shipping.
func TestOtel_SdkDegradationDoesNotMarkTheCarrierErrored(t *testing.T) {
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, nil)

	transport.submit(operationExternalSpan, map[string]any{
		"rawSpan": map[string]any{"span_data": map[string]any{"name": "trimmed"}},
		"errors": []any{
			map[string]any{"source": "sdk", "step": payloadBudgetStep, "error": "trimmed a field"},
		},
	}, nil)
	transport.submit(operationExternalSpan, map[string]any{
		"rawSpan": map[string]any{"span_data": map[string]any{"name": "mixed"}},
		"errors": []any{
			map[string]any{"source": "sdk", "step": payloadBudgetStep, "error": "trimmed a field"},
			map[string]any{"source": "user", "error": "a real failure"},
		},
	}, nil)
	transport.flush(5 * time.Second)

	byName := map[string]int{}
	for _, carrier := range carriersIn(t, sender.recorded()) {
		byName[carrier.name] = carrier.statusCode
	}
	if byName["trimmed"] != 0 {
		t.Errorf("budget-trimmed span status = %d, want 0 (unset)", byName["trimmed"])
	}
	// A genuine error alongside the SDK's own note still marks the span.
	if byName["mixed"] != 2 {
		t.Errorf("span with a real error status = %d, want 2 (OTLP error)", byName["mixed"])
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
		if size := len(request.body); size > limit {
			t.Errorf("request encoded to %d bytes, want at most %d", size, limit)
		}
	}
	if total := len(carriersIn(t, requests)); total != 6 {
		t.Errorf("delivered %d carriers, want all 6", total)
	}
}

// The envelope is cut from a marshalled empty request rather than written by
// hand, so this pins that the cut kept the resource and scope intact.
func TestOtel_RequestCarriesResourceAndScope(t *testing.T) {
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, nil)

	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil)
	transport.flush(10 * time.Second)

	requests := sender.recorded()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	resourceSpans := requests[0].payload["resourceSpans"].([]any)[0].(map[string]any)
	attributes := map[string]string{}
	for _, raw := range resourceSpans["resource"].(map[string]any)["attributes"].([]any) {
		attribute := raw.(map[string]any)
		value := attribute["value"].(map[string]any)
		attributes[attribute["key"].(string)] = value["stringValue"].(string)
	}
	if attributes["service.name"] != "bitfab-go-sdk" || attributes["service.version"] != Version {
		t.Errorf("resource attributes = %#v, want the SDK name and version", attributes)
	}

	scope := resourceSpans["scopeSpans"].([]any)[0].(map[string]any)["scope"].(map[string]any)
	if scope["name"] != "bitfab" || scope["version"] != Version {
		t.Errorf("scope = %#v, want bitfab/%s", scope, Version)
	}
}

// The exporter packs batches against a running byte count built from per-span
// encodes, then assembles the body by concatenation. If the two ever disagree
// the byte bound stops meaning anything and oversized requests reach ingestion,
// so the assembled body must equal marshalling the whole request.
func TestOtel_AssembledBodyMatchesMarshallingTheRequest(t *testing.T) {
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, nil)

	for i := range 3 {
		transport.submit(operationExternalSpan, map[string]any{
			"index": i,
			"note":  "unicode ✓",
		}, nil)
	}
	transport.flush(10 * time.Second)

	requests := sender.recorded()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	want, err := json.Marshal(requests[0].payload)
	if err != nil {
		t.Fatalf("re-marshalling the decoded request failed: %v", err)
	}
	if string(requests[0].body) != string(want) {
		t.Errorf("assembled body diverged from marshalling the request object")
	}
}

func TestOtel_OversizedSingleCarrierIsRejected(t *testing.T) {
	resetWarnOnce()
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, func(cfg *otelTransportConfig) {
		cfg.maxRequestBytes = 200
	})

	transport.submit(operationExternalSpan, map[string]any{"blob": strings.Repeat("x", 10_000)}, nil)
	if transport.flush(5 * time.Second) {
		t.Error("flush must report failure when a carrier cannot be exported")
	}
	if len(sender.recorded()) != 0 {
		t.Error("an oversized carrier must be rejected before it is sent")
	}
}

func TestOtel_RawOversizedCarrierIsTrimmedBeforePreparation(t *testing.T) {
	sender := &fakeSender{}
	exporter := &bitfabSpanExporter{
		directSender:        sender.send,
		maxRequestBytes:     otelMaxRequestBytes,
		maxRequestBatchSize: otelDirectMaxRequestBatchSize,
		exportConcurrency:   1,
	}
	payload, err := json.Marshal(map[string]any{
		"rawSpan": map[string]any{
			"span_data": map[string]any{
				"name":  "draft",
				"type":  "llm",
				"input": strings.Repeat("x", 8_100_000),
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	carrier, err := json.Marshal(map[string]any{
		"attributes": []any{
			map[string]any{
				"key":   otelPayloadAttribute,
				"value": map[string]any{"stringValue": string(payload)},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal carrier: %v", err)
	}
	envelope := otlpEnvelope{
		head: []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[`),
		tail: []byte(`]}]}]}`),
	}
	envelope.size = len(envelope.head) + len(envelope.tail)
	span := encodedSpan{body: carrier, size: len(carrier)}

	if err := exporter.send(
		context.Background(),
		envelope,
		requestBatch{spans: []encodedSpan{span}, size: envelope.size + span.size},
	); err != nil {
		t.Fatalf("send raw-oversized carrier: %v", err)
	}
	requests := sender.recorded()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	if requests[0].prepared.rawBytes >= otelMaxDecompressedBytes {
		t.Errorf("prepared raw bytes = %d, want trimmed below %d", requests[0].prepared.rawBytes, otelMaxDecompressedBytes)
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

func TestOtel_RetryWaitPrefersTheServersInstruction(t *testing.T) {
	budget := otelExportTimeout
	for attempt := range 3 {
		wait, keepTrying := retryWait(&httpStatusError{StatusCode: 503}, attempt, budget)
		if !keepTrying {
			t.Fatalf("attempt %d: must keep trying without an instruction", attempt)
		}
		ceiling := min(otelRetryBaseDelay<<attempt, otelRetryBackoffCeiling)
		if wait < ceiling/2 || wait >= ceiling {
			t.Errorf("attempt %d: wait = %s, want jittered within [%s, %s)", attempt, wait, ceiling/2, ceiling)
		}
	}

	asked := &httpStatusError{StatusCode: 429, RetryAfter: 2 * time.Second}
	if wait, keepTrying := retryWait(asked, 0, budget); !keepTrying || wait != 2*time.Second {
		t.Errorf("retryWait honoring Retry-After = %s/%v, want 2s/true", wait, keepTrying)
	}

	// Past a fixed ceiling but servable, so it is honored: OTLP says to honor
	// Retry-After and calls data dropped while throttled the outcome to avoid.
	servable := &httpStatusError{StatusCode: 429, RetryAfter: 8 * time.Second}
	if wait, keepTrying := retryWait(servable, 0, budget); !keepTrying || wait != 8*time.Second {
		t.Errorf("retryWait honoring a servable 8s wait = %s/%v, want 8s/true", wait, keepTrying)
	}

	// Refused only for outlasting what the budget can serve, since the processor
	// kills an export that outlives it.
	if _, keepTrying := retryWait(servable, 0, 4*time.Second); keepTrying {
		t.Error("a wait the budget cannot serve must stop the retries, not shorten them")
	}
}

// The refused request is one of several in a window. Delaying only that one
// leaves the rest hitting a server that just asked for room.
func TestOtel_ThrottleHoldsSiblingRequests(t *testing.T) {
	var attempts atomic.Int32
	sender := &fakeSender{
		respond: func(int, map[string]any) (map[string]any, error) {
			if attempts.Add(1) == 1 {
				return nil, &httpStatusError{StatusCode: 429, RetryAfter: 30 * time.Second}
			}
			return map[string]any{}, nil
		},
	}
	transport := newTestTransport(t, sender, nil)

	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil)
	transport.submit(operationExternalSpan, map[string]any{"index": 2}, nil)
	if transport.flush(10 * time.Second) {
		t.Error("flush must report failure while a throttle is active")
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (the throttle must hold the sibling back)", got)
	}
}

// The other SDKs bail before the wait once attempts are exhausted; sleeping
// after the final attempt only delays the failure reaching flush callers.
//
// The final attempt is refused with a Retry-After far longer than the jittered
// backoffs between attempts, so a trailing wait is unmistakable in the elapsed
// time rather than lost in the noise of the earlier two.
func TestOtel_DoesNotWaitAfterTheFinalAttempt(t *testing.T) {
	var attempts atomic.Int32
	sender := &fakeSender{
		respond: func(int, map[string]any) (map[string]any, error) {
			if attempts.Add(1) == otelMaxAttempts {
				return nil, &httpStatusError{StatusCode: 429, RetryAfter: 3 * time.Second}
			}
			return nil, &httpStatusError{StatusCode: 503, Body: "unavailable"}
		},
	}
	transport := newTestTransport(t, sender, nil)

	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil)
	start := time.Now()
	if transport.flush(10 * time.Second) {
		t.Error("flush must report failure once the attempts are exhausted")
	}
	elapsed := time.Since(start)
	if got := attempts.Load(); got != otelMaxAttempts {
		t.Errorf("attempts = %d, want %d", got, otelMaxAttempts)
	}
	if elapsed >= time.Second {
		t.Errorf("elapsed = %s, want well under the final attempt's 3s Retry-After", elapsed)
	}
}

func TestParseRetryAfter(t *testing.T) {
	if got := parseRetryAfter("2"); got != 2*time.Second {
		t.Errorf("parseRetryAfter(\"2\") = %s, want 2s", got)
	}
	if got := parseRetryAfter(""); got != 0 {
		t.Errorf("parseRetryAfter(\"\") = %s, want 0", got)
	}
	if got := parseRetryAfter("not-a-wait"); got != 0 {
		t.Errorf("parseRetryAfter of nonsense = %s, want 0", got)
	}
	// An HTTP-date already in the past is not a wait we can act on.
	if got := parseRetryAfter(time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)); got != 0 {
		t.Errorf("parseRetryAfter of a past date = %s, want 0", got)
	}
	future := parseRetryAfter(time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat))
	if future <= 0 || future > 30*time.Second {
		t.Errorf("parseRetryAfter of a future date = %s, want within 30s", future)
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

func TestOtel_DeliveryListenerReceivesOnlySuccessfulCarrierRefs(t *testing.T) {
	sender := &fakeSender{}
	var delivered []carrierRef
	transport := newTestTransport(t, sender, func(config *otelTransportConfig) {
		config.onDelivered = func(refs []carrierRef) {
			delivered = append(delivered, refs...)
		}
	})
	spanRef := carrierRef{traceID: "trace-1", spanID: "span-1"}
	closingRef := carrierRef{traceID: "trace-1"}
	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil, carrierMeta{ref: &spanRef})
	transport.submit(operationExternalTrace, map[string]any{"index": 2}, nil, carrierMeta{ref: &closingRef})
	if !transport.flush(10 * time.Second) {
		t.Fatal("flush reported failure")
	}
	if !reflect.DeepEqual(delivered, []carrierRef{spanRef, closingRef}) {
		t.Fatalf("delivered refs = %#v", delivered)
	}

	failing := &fakeSender{respond: func(int, map[string]any) (map[string]any, error) {
		return nil, &httpStatusError{StatusCode: http.StatusBadRequest}
	}}
	transport = newTestTransport(t, failing, func(config *otelTransportConfig) {
		config.onDelivered = func(refs []carrierRef) {
			delivered = append(delivered, refs...)
		}
	})
	failedRef := carrierRef{traceID: "trace-2", spanID: "span-2"}
	transport.submit(operationExternalSpan, map[string]any{"index": 3}, nil, carrierMeta{ref: &failedRef})
	if transport.flush(10 * time.Second) {
		t.Fatal("failed request should fail the flush")
	}
	if !reflect.DeepEqual(delivered, []carrierRef{spanRef, closingRef}) {
		t.Fatalf("failed carrier was acknowledged: %#v", delivered)
	}
}

func TestOtel_DeliveryListenerPanicDoesNotFailExport(t *testing.T) {
	resetWarnOnce()
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, func(config *otelTransportConfig) {
		config.onDelivered = func([]carrierRef) { panic("listener crashed") }
	})
	ref := carrierRef{traceID: "trace-1", spanID: "span-1"}
	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil, carrierMeta{ref: &ref})
	if !transport.flush(10 * time.Second) {
		t.Fatal("delivery listener panic must not fail an accepted export")
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
	var delivered []carrierRef
	transport := newTestTransport(t, sender, func(config *otelTransportConfig) {
		config.onDelivered = func(refs []carrierRef) { delivered = append(delivered, refs...) }
	})
	ref := carrierRef{traceID: "trace-1", spanID: "span-1"}

	transport.submit(operationExternalSpan, map[string]any{"index": 1}, nil, carrierMeta{ref: &ref})
	if transport.flush(10 * time.Second) {
		t.Error("flush must report failure when ingestion rejects carriers")
	}
	if len(sender.recorded()) != 1 {
		t.Errorf("requests = %d, want 1 (partial success is not retried)", len(sender.recorded()))
	}
	if len(delivered) != 0 {
		t.Fatalf("partially rejected carrier was acknowledged: %#v", delivered)
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

func TestOtel_DeliversCompressibleSpanAboveRawTargetIntact(t *testing.T) {
	sender := &fakeSender{}
	transport := newTestTransport(t, sender, nil)

	payload := map[string]any{
		"id": "huge",
		"rawSpan": map[string]any{
			"id": "huge",
			"span_data": map[string]any{
				"name":   "huge",
				"type":   "function",
				"input":  strings.Repeat("x", 4_000_000),
				"output": "ok",
			},
		},
	}
	transport.submit(operationExternalSpan, payload, nil)
	transport.flush(5 * time.Second)

	carriers := carriersIn(t, sender.recorded())
	if len(carriers) != 1 {
		t.Fatalf("delivered %d carriers, want 1 (0 means the exporter dropped it)", len(carriers))
	}
	request := sender.recorded()[0]
	if request.prepared.rawBytes <= otelMaxRequestBytes || request.prepared.wireBytes > otelMaxRequestBytes {
		t.Fatalf("prepared sizes = raw %d, wire %d", request.prepared.rawBytes, request.prepared.wireBytes)
	}
	input := carriers[0].payload["rawSpan"].(map[string]any)["span_data"].(map[string]any)["input"].(string)
	if len(input) != 4_000_000 {
		t.Fatalf("input length = %d, want 4000000", len(input))
	}
}

func TestOtel_TrimsOversizedSpanWhenCompressionIsDisabled(t *testing.T) {
	t.Setenv(disableCompressionEnv, "1")
	sender := &fakeSender{}
	var delivered []carrierRef
	transport := newTestTransport(t, sender, func(config *otelTransportConfig) {
		config.onDelivered = func(refs []carrierRef) { delivered = append(delivered, refs...) }
	})
	ref := carrierRef{traceID: "trace-1", spanID: "span-1"}
	transport.submit(operationExternalSpan, map[string]any{
		"rawSpan": map[string]any{
			"span_data": map[string]any{
				"name":  "huge",
				"input": strings.Repeat("x", 4_000_000),
			},
		},
	}, nil, carrierMeta{ref: &ref})
	transport.flush(5 * time.Second)

	carriers := carriersIn(t, sender.recorded())
	input := carriers[0].payload["rawSpan"].(map[string]any)["span_data"].(map[string]any)["input"].(string)
	if !strings.Contains(input, "too_large_") {
		t.Fatalf("input = %q, want size placeholder", input)
	}
	if !reflect.DeepEqual(delivered, []carrierRef{ref}) {
		t.Fatalf("trimmed carrier refs = %#v, want %#v", delivered, []carrierRef{ref})
	}
}
