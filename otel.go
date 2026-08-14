package bitfab

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	otelOperationAttribute = "bitfab.operation"
	otelPayloadAttribute   = "bitfab.payload"
	otelTracesEndpoint     = "/api/sdk/otel/v1/traces"

	otelMaxRequestBytesEnv   = "BITFAB_OTEL_MAX_REQUEST_BYTES"
	otelExportConcurrencyEnv = "BITFAB_OTEL_EXPORT_CONCURRENCY"

	otelMaxRequestBytes           = 3_000_000
	otelMaxDecompressedBytes      = 8_000_000
	otelMaxQueueSize              = 8_192
	otelDirectMaxExportBatch      = 512
	otelDirectMaxRequestBatchSize = 8
	otelDefaultExportConcurrency  = 32
	otelMaxExportConcurrency      = 64

	otelScheduleDelay = 5 * time.Second
	otelExportTimeout = 30 * time.Second
	otelRetryDelay    = 100 * time.Millisecond
	otelMaxAttempts   = 3
)

var (
	liveOtelTransportsMu sync.Mutex
	liveOtelTransports   = map[*otelTransport]struct{}{}
)

func otelMaxRequestBytesFromEnv() int {
	raw, ok := os.LookupEnv(otelMaxRequestBytesEnv)
	if !ok {
		return otelMaxRequestBytes
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err == nil && value > 0 && value <= otelMaxRequestBytes {
		return value
	}
	warnOnce(
		"otel-max-request-bytes-invalid",
		fmt.Sprintf(
			"%s must be a positive integer no greater than %d; using %d",
			otelMaxRequestBytesEnv, otelMaxRequestBytes, otelMaxRequestBytes,
		),
	)
	return otelMaxRequestBytes
}

func otelExportConcurrencyFromEnv() int {
	raw, ok := os.LookupEnv(otelExportConcurrencyEnv)
	if !ok {
		return otelDefaultExportConcurrency
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err == nil && value > 0 && value <= otelMaxExportConcurrency {
		return value
	}
	warnOnce(
		"otel-export-concurrency-invalid",
		fmt.Sprintf(
			"%s must be a positive integer no greater than %d; using %d",
			otelExportConcurrencyEnv, otelMaxExportConcurrency, otelDefaultExportConcurrency,
		),
	)
	return otelDefaultExportConcurrency
}

type otelTransport struct {
	mu       sync.Mutex
	closed   bool
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
	tracker  *deliveryTrackingExporter
}

func createOtelTransport(directSender directBatchSender) *otelTransport {
	return newOtelTransport(otelTransportConfig{
		directSender:        directSender,
		maxRequestBytes:     otelMaxRequestBytesFromEnv(),
		maxRequestBatchSize: otelDirectMaxRequestBatchSize,
		exportConcurrency:   otelExportConcurrencyFromEnv(),
		maxQueueSize:        otelMaxQueueSize,
		scheduleDelay:       otelScheduleDelay,
	})
}

type otelTransportConfig struct {
	directSender        directBatchSender
	maxRequestBytes     int
	maxRequestBatchSize int
	exportConcurrency   int
	maxQueueSize        int
	maxExportBatchSize  int
	scheduleDelay       time.Duration
}

func newOtelTransport(cfg otelTransportConfig) *otelTransport {
	maxExportBatchSize := cfg.maxExportBatchSize
	if maxExportBatchSize <= 0 {
		maxExportBatchSize = otelDirectMaxExportBatch
	}

	tracker := &deliveryTrackingExporter{exporter: newOtelExporter(cfg)}

	// Every provider option is supplied explicitly. Left to its defaults the
	// provider would read OTEL_* environment variables, so a host application's
	// global sampler or attribute-length limit could silently drop or truncate
	// Bitfab payloads.
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resource.NewSchemaless(
			attribute.String("service.name", "bitfab-go-sdk"),
			attribute.String("service.version", Version),
		)),
		sdktrace.WithSpanLimits(sdktrace.SpanLimits{
			AttributeValueLengthLimit:   -1,
			AttributeCountLimit:         2,
			EventCountLimit:             0,
			LinkCountLimit:              0,
			AttributePerEventCountLimit: 0,
			AttributePerLinkCountLimit:  0,
		}),
		sdktrace.WithBatcher(
			tracker,
			sdktrace.WithMaxQueueSize(cfg.maxQueueSize),
			sdktrace.WithBatchTimeout(cfg.scheduleDelay),
			sdktrace.WithMaxExportBatchSize(maxExportBatchSize),
			sdktrace.WithExportTimeout(otelExportTimeout),
		),
	)

	transport := &otelTransport{
		provider: provider,
		tracer:   provider.Tracer("bitfab", trace.WithInstrumentationVersion(Version)),
		tracker:  tracker,
	}

	liveOtelTransportsMu.Lock()
	liveOtelTransports[transport] = struct{}{}
	liveOtelTransportsMu.Unlock()

	return transport
}

func newOtelExporter(cfg otelTransportConfig) sdktrace.SpanExporter {
	return &bitfabSpanExporter{
		directSender:        cfg.directSender,
		maxRequestBytes:     cfg.maxRequestBytes,
		maxRequestBatchSize: cfg.maxRequestBatchSize,
		exportConcurrency:   cfg.exportConcurrency,
	}
}

func (t *otelTransport) submit(operation traceOperation, payload map[string]any, encoded []byte) {
	defer func() {
		if r := recover(); r != nil {
			warnOnce("otel-submit-panic", fmt.Sprintf("queueing a span panicked and was recovered: %v", r))
		}
	}()

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		warnOnce("otel-submit-after-shutdown", "OpenTelemetry transport is shut down; dropping spans")
		return
	}
	tracer := t.tracer
	t.mu.Unlock()

	if tracer == nil {
		return
	}

	if encoded == nil {
		var dropped []string
		encoded, dropped = marshalPayloadSafe(payload)
		warnForStubbedBody(dropped)
	}

	// context.Background rather than a caller context: a carrier is transport
	// bookkeeping, so a cancelled or already-traced host context must not
	// decide whether it is recorded or where it is parented.
	_, span := tracer.Start(
		context.Background(),
		otelSpanName(operation, payload),
		trace.WithTimestamp(otelTimestamp(payload, "started_at")),
		trace.WithAttributes(
			attribute.String(otelOperationAttribute, string(operation)),
			attribute.String(otelPayloadAttribute, string(encoded)),
		),
	)
	if payloadHasError(payload) {
		span.SetStatus(codes.Error, "")
	}
	span.End(trace.WithTimestamp(otelTimestamp(payload, "ended_at")))
}

func (t *otelTransport) flush(timeout time.Duration) bool {
	t.mu.Lock()
	provider := t.provider
	tracker := t.tracker
	t.mu.Unlock()

	if provider == nil {
		return true
	}
	if timeout < 0 {
		timeout = 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	flushed := provider.ForceFlush(ctx) == nil
	failed := 0
	if tracker != nil {
		failed = tracker.takeFailedExports()
	}
	return flushed && failed == 0
}

func (t *otelTransport) shutdown(timeout time.Duration) bool {
	if timeout < 0 {
		timeout = 0
	}
	deadline := time.Now().Add(timeout)

	t.mu.Lock()
	alreadyClosed := t.closed
	t.closed = true
	provider := t.provider
	t.mu.Unlock()

	if alreadyClosed || provider == nil {
		t.forget()
		return true
	}

	flushed := t.flush(time.Until(deadline))

	ctx, cancel := context.WithTimeout(context.Background(), max(0, time.Until(deadline)))
	defer cancel()
	stopped := provider.Shutdown(ctx) == nil

	t.mu.Lock()
	t.provider = nil
	t.tracer = nil
	t.tracker = nil
	t.mu.Unlock()
	t.forget()

	return flushed && stopped
}

func (t *otelTransport) forget() {
	liveOtelTransportsMu.Lock()
	delete(liveOtelTransports, t)
	liveOtelTransportsMu.Unlock()
}

func currentOtelTransports() []*otelTransport {
	liveOtelTransportsMu.Lock()
	defer liveOtelTransportsMu.Unlock()
	transports := make([]*otelTransport, 0, len(liveOtelTransports))
	for transport := range liveOtelTransports {
		transports = append(transports, transport)
	}
	return transports
}

func flushOtelTransports(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	succeeded := true
	for _, transport := range currentOtelTransports() {
		succeeded = transport.flush(max(0, time.Until(deadline))) && succeeded
	}
	return succeeded
}

func shutdownOtelTransports(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	succeeded := true
	for _, transport := range currentOtelTransports() {
		succeeded = transport.shutdown(max(0, time.Until(deadline))) && succeeded
	}
	return succeeded
}

type deliveryTrackingExporter struct {
	exporter      sdktrace.SpanExporter
	mu            sync.Mutex
	failedExports int
}

func (d *deliveryTrackingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	err := d.exporter.ExportSpans(ctx, spans)
	if err != nil {
		d.mu.Lock()
		d.failedExports++
		d.mu.Unlock()
	}
	return err
}

func (d *deliveryTrackingExporter) Shutdown(ctx context.Context) error {
	return d.exporter.Shutdown(ctx)
}

func (d *deliveryTrackingExporter) takeFailedExports() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	failed := d.failedExports
	d.failedExports = 0
	return failed
}

type bitfabSpanExporter struct {
	directSender        directBatchSender
	maxRequestBytes     int
	maxRequestBatchSize int
	exportConcurrency   int
}

func (e *bitfabSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}

	encoded := make([]encodedSpan, 0, len(spans))
	for _, span := range spans {
		span, err := encodeSpan(span)
		if err != nil {
			warnOnce("otel-encode-failed", fmt.Sprintf("failed to encode an OpenTelemetry span batch (%v)", err))
			return err
		}
		encoded = append(encoded, span)
	}
	envelope, err := requestEnvelope(spans[0])
	if err != nil {
		warnOnce("otel-encode-failed", fmt.Sprintf("failed to encode an OpenTelemetry span batch (%v)", err))
		return err
	}

	batches := e.buildRequestBatches(envelope, encoded)
	results := make([]error, len(batches))

	workers := e.exportConcurrency
	if workers > len(batches) {
		workers = len(batches)
	}
	if workers < 1 {
		workers = 1
	}

	// Recovery is per request, not per worker: a worker that unwound on panic
	// would stop draining next, and if every worker died the dispatch loop
	// below would block on the unbuffered channel forever, hanging flush and
	// close past their deadlines.
	next := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range next {
				results[index] = e.sendRecovered(ctx, envelope, batches[index])
			}
		}()
	}

	dispatched := 0
dispatch:
	for index := range batches {
		select {
		case next <- index:
			dispatched++
		case <-ctx.Done():
			break dispatch
		}
	}
	close(next)
	wg.Wait()

	for _, err := range results {
		if err != nil {
			return err
		}
	}
	if dispatched < len(batches) {
		return fmt.Errorf(
			"bitfab: export deadline expired with %d of %d requests unsent: %w",
			len(batches)-dispatched, len(batches), ctx.Err(),
		)
	}
	return nil
}

func (e *bitfabSpanExporter) sendRecovered(
	ctx context.Context,
	envelope otlpEnvelope,
	batch requestBatch,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			warnOnce("otel-export-panic", fmt.Sprintf("an OpenTelemetry export panicked and was recovered: %v", r))
			err = fmt.Errorf("bitfab: export panicked: %v", r)
		}
	}()
	return e.send(ctx, envelope, batch)
}

func (e *bitfabSpanExporter) Shutdown(context.Context) error {
	return nil
}

func (e *bitfabSpanExporter) buildRequestBatches(
	envelope otlpEnvelope,
	spans []encodedSpan,
) []requestBatch {
	var batches []requestBatch
	var current []encodedSpan
	size := envelope.size

	for _, span := range spans {
		addition := span.size
		if len(current) > 0 {
			addition += spanSeparatorBytes
		}
		if len(current) > 0 && (len(current) >= e.maxRequestBatchSize || size+addition > e.maxRequestBytes) {
			batches = append(batches, requestBatch{spans: current, size: size})
			current = nil
			size = envelope.size
			addition = span.size
		}
		current = append(current, span)
		size += addition
	}

	if len(current) > 0 {
		batches = append(batches, requestBatch{spans: current, size: size})
	}
	return batches
}

func (e *bitfabSpanExporter) send(
	ctx context.Context,
	envelope otlpEnvelope,
	batch requestBatch,
) error {
	requestSpans := batch.spans
	requestRawBytes := batch.size
	alreadyTrimmed := false
	for {
		if requestRawBytes <= otelMaxDecompressedBytes {
			prepared := prepareRequestBody(encodeRequest(envelope, requestSpans))
			if prepared.wireBytes <= e.maxRequestBytes {
				return e.sendWithRetries(ctx, prepared, len(batch.spans))
			}
		}

		if len(batch.spans) != 1 {
			return fmt.Errorf("bitfab: span batch exceeds the configured request-size target")
		}
		if alreadyTrimmed {
			return fmt.Errorf("bitfab: span exceeds the configured request-size target after trimming")
		}
		trimmed, ok := trimEncodedSpan(batch.spans[0])
		if !ok {
			return fmt.Errorf("bitfab: span exceeds the configured request-size target and could not be trimmed")
		}
		requestSpans = []encodedSpan{trimmed}
		requestRawBytes = envelope.size + trimmed.size
		alreadyTrimmed = true
	}
}

func (e *bitfabSpanExporter) sendWithRetries(
	ctx context.Context,
	request preparedRequest,
	spanCount int,
) error {
	var lastErr error
	for attempt := range otelMaxAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(otelRetryDelay):
			}
		}

		response, err := e.directSender(otelTracesEndpoint, request, otelExportTimeout)
		if err == nil {
			if rejected, message, ok := otlpPartialSuccess(response); ok {
				warnOnce(
					"otel-partial-success",
					fmt.Sprintf("OTLP ingestion rejected %d span(s): %s", rejected, message),
				)
				return fmt.Errorf("bitfab: OTLP ingestion rejected %d span(s)", rejected)
			}
			return nil
		}

		lastErr = err
		if statusCode(err) == http.StatusRequestEntityTooLarge {
			if spanCount == 1 {
				warnOnce(
					"otel-span-too-large-ingress",
					"a single span exceeded the ingestion request limit and could not be exported",
				)
			} else {
				warnOnce(
					"otel-batch-too-large-ingress",
					"a span batch exceeded the ingestion request limit and could not be exported",
				)
			}
			return err
		}
		if !isRetryableStatus(err) {
			return err
		}
	}
	return lastErr
}

func otlpPartialSuccess(response map[string]any) (rejected int64, message string, ok bool) {
	partial, isMap := response["partialSuccess"].(map[string]any)
	if !isMap {
		return 0, "", false
	}

	switch raw := partial["rejectedSpans"].(type) {
	case float64:
		rejected = int64(raw)
	case string:
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, "", false
		}
		rejected = parsed
	default:
		return 0, "", false
	}

	if rejected == 0 {
		return 0, "", false
	}

	message = "no reason provided"
	if text, isString := partial["errorMessage"].(string); isString && text != "" {
		message = text
	}
	return rejected, message, true
}

func buildOtlpRequest(first sdktrace.ReadOnlySpan, spans []map[string]any) map[string]any {
	scope := first.InstrumentationScope()
	return map[string]any{
		"resourceSpans": []any{
			map[string]any{
				"resource": map[string]any{
					"attributes": otlpAttributes(first.Resource().Attributes()),
				},
				"scopeSpans": []any{
					map[string]any{
						"scope": map[string]any{
							"name":    scope.Name,
							"version": scope.Version,
						},
						"spans": spans,
					},
				},
			},
		},
	}
}

// encodedSpan is a span encoded exactly as it goes on the wire, carrying its
// byte count. Encoding once and remembering the size is what keeps request
// packing linear: sizing a candidate batch by re-encoding the whole request
// re-escapes every carrier's bitfab.payload string on every span considered.
type encodedSpan struct {
	body []byte
	size int
}

// otlpEnvelope is the invariant head and tail of an OTLP request for one export
// window, so a body can be assembled by concatenating pre-encoded spans.
type otlpEnvelope struct {
	head []byte
	tail []byte
	size int
}

type requestBatch struct {
	spans []encodedSpan
	size  int
}

// spanSeparatorBytes is the comma joining adjacent spans in the span list.
const spanSeparatorBytes = 1

func encodeSpan(span sdktrace.ReadOnlySpan) (encodedSpan, error) {
	body, err := json.Marshal(spanToOtlp(span))
	if err != nil {
		return encodedSpan{}, err
	}
	return encodedSpan{body: body, size: len(body)}, nil
}

func trimEncodedSpan(span encodedSpan) (encodedSpan, bool) {
	var carrier map[string]any
	if err := json.Unmarshal(span.body, &carrier); err != nil {
		return encodedSpan{}, false
	}
	attributes, ok := carrier["attributes"].([]any)
	if !ok {
		return encodedSpan{}, false
	}
	for _, rawAttribute := range attributes {
		attribute, ok := rawAttribute.(map[string]any)
		if !ok || attribute["key"] != otelPayloadAttribute {
			continue
		}
		value, ok := attribute["value"].(map[string]any)
		if !ok {
			return encodedSpan{}, false
		}
		payloadBody, ok := value["stringValue"].(string)
		if !ok {
			return encodedSpan{}, false
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(payloadBody), &payload); err != nil {
			return encodedSpan{}, false
		}
		_, trimmedBody, _ := marshalSpanBody(payload)
		value["stringValue"] = string(trimmedBody)
		body, err := json.Marshal(carrier)
		if err != nil {
			return encodedSpan{}, false
		}
		return encodedSpan{body: body, size: len(body)}, true
	}
	return encodedSpan{}, false
}

// requestEnvelope splits an encoded request around its empty span list, so the
// head and tail are byte-identical to what marshalling the assembled request
// would produce without depending on Go's map key ordering.
func requestEnvelope(first sdktrace.ReadOnlySpan) (otlpEnvelope, error) {
	encoded, err := json.Marshal(buildOtlpRequest(first, []map[string]any{}))
	if err != nil {
		return otlpEnvelope{}, err
	}
	marker := []byte(`"spans":[]`)
	index := bytes.Index(encoded, marker)
	if index < 0 {
		return otlpEnvelope{}, fmt.Errorf("bitfab: no span list in the encoded OTLP request")
	}
	cut := index + len(marker) - 1
	head := encoded[:cut]
	tail := encoded[cut:]
	return otlpEnvelope{head: head, tail: tail, size: len(head) + len(tail)}, nil
}

func encodeRequest(envelope otlpEnvelope, spans []encodedSpan) []byte {
	size := envelope.size
	for index, span := range spans {
		size += span.size
		if index > 0 {
			size += spanSeparatorBytes
		}
	}
	body := make([]byte, 0, size)
	body = append(body, envelope.head...)
	for index, span := range spans {
		if index > 0 {
			body = append(body, ',')
		}
		body = append(body, span.body...)
	}
	return append(body, envelope.tail...)
}

func spanToOtlp(span sdktrace.ReadOnlySpan) map[string]any {
	spanContext := span.SpanContext()
	result := map[string]any{
		"traceId":                spanContext.TraceID().String(),
		"spanId":                 spanContext.SpanID().String(),
		"name":                   span.Name(),
		"kind":                   int(span.SpanKind()),
		"startTimeUnixNano":      strconv.FormatInt(span.StartTime().UnixNano(), 10),
		"endTimeUnixNano":        strconv.FormatInt(span.EndTime().UnixNano(), 10),
		"attributes":             otlpAttributes(span.Attributes()),
		"droppedAttributesCount": span.DroppedAttributes(),
		"droppedEventsCount":     span.DroppedEvents(),
		"droppedLinksCount":      span.DroppedLinks(),
		"status":                 otlpStatus(span.Status()),
		"flags":                  int(spanContext.TraceFlags()),
	}
	if parent := span.Parent(); parent.SpanID().IsValid() {
		result["parentSpanId"] = parent.SpanID().String()
	}
	if state := spanContext.TraceState().String(); state != "" {
		result["traceState"] = state
	}
	return result
}

// otlpStatus maps otel/codes onto the OTLP status enum. The two do not line up:
// otel/codes is Unset/Error/Ok (0/1/2) while OTLP is UNSET/OK/ERROR (0/1/2), so
// Error and Ok are transposed.
func otlpStatus(status sdktrace.Status) map[string]any {
	code := 0
	switch status.Code {
	case codes.Ok:
		code = 1
	case codes.Error:
		code = 2
	}
	result := map[string]any{"code": code}
	if status.Description != "" {
		result["message"] = status.Description
	}
	return result
}

func otlpAttributes(attributes []attribute.KeyValue) []any {
	encoded := make([]any, 0, len(attributes))
	for _, attr := range attributes {
		encoded = append(encoded, map[string]any{
			"key":   string(attr.Key),
			"value": otlpValue(attr.Value),
		})
	}
	return encoded
}

func otlpValue(value attribute.Value) map[string]any {
	switch value.Type() {
	case attribute.BOOL:
		return map[string]any{"boolValue": value.AsBool()}
	case attribute.INT64:
		return map[string]any{"intValue": strconv.FormatInt(value.AsInt64(), 10)}
	case attribute.FLOAT64:
		return map[string]any{"doubleValue": value.AsFloat64()}
	case attribute.STRING:
		return map[string]any{"stringValue": value.AsString()}
	case attribute.BOOLSLICE:
		items := value.AsBoolSlice()
		values := make([]any, 0, len(items))
		for _, item := range items {
			values = append(values, map[string]any{"boolValue": item})
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}
	case attribute.INT64SLICE:
		items := value.AsInt64Slice()
		values := make([]any, 0, len(items))
		for _, item := range items {
			values = append(values, map[string]any{"intValue": strconv.FormatInt(item, 10)})
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}
	case attribute.FLOAT64SLICE:
		items := value.AsFloat64Slice()
		values := make([]any, 0, len(items))
		for _, item := range items {
			values = append(values, map[string]any{"doubleValue": item})
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}
	case attribute.STRINGSLICE:
		items := value.AsStringSlice()
		values := make([]any, 0, len(items))
		for _, item := range items {
			values = append(values, map[string]any{"stringValue": item})
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}
	default:
		return map[string]any{"stringValue": value.Emit()}
	}
}

func otelSpanName(operation traceOperation, payload map[string]any) string {
	if operation == operationExternalSpan {
		if rawSpan, ok := payload["rawSpan"].(map[string]any); ok {
			if spanData, ok := rawSpan["span_data"].(map[string]any); ok {
				if name, ok := spanData["name"].(string); ok && name != "" {
					return name
				}
			}
		}
	}
	if key, ok := payload["traceFunctionKey"].(string); ok && key != "" {
		return key
	}
	return "bitfab." + string(operation)
}

func otelTimestamp(payload map[string]any, field string) time.Time {
	var raw any
	if rawSpan, ok := payload["rawSpan"].(map[string]any); ok {
		raw = rawSpan[field]
	}
	if raw == nil {
		container, ok := payload["externalTrace"].(map[string]any)
		if !ok {
			container, _ = payload["rawTrace"].(map[string]any)
		}
		if container != nil {
			raw = container[field]
		}
	}
	if text, ok := raw.(string); ok {
		if parsed, err := time.Parse(time.RFC3339, text); err == nil {
			return parsed
		}
	}
	return time.Now()
}

func payloadHasError(payload map[string]any) bool {
	if rawSpan, ok := payload["rawSpan"].(map[string]any); ok {
		if spanData, ok := rawSpan["span_data"].(map[string]any); ok {
			if spanData["error"] != nil {
				return true
			}
		}
	}
	switch errors := payload["errors"].(type) {
	case nil:
		return false
	case []any:
		// Entries the SDK wrote about itself (a budget trim, a stubbed value)
		// report an incomplete capture, not a failed operation, so they must not
		// mark the carrier errored: a large payload is normal traffic, and
		// flagging it would turn every oversized span into an error in the
		// user's dashboards. The payload still carries the entry, which is what
		// tells the server the trace is incomplete.
		for _, entry := range errors {
			if m, ok := entry.(map[string]any); ok && m["source"] == "sdk" {
				continue
			}
			return true
		}
		return false
	case string:
		return errors != ""
	case bool:
		return errors
	default:
		return true
	}
}
