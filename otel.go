package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	otelOperationAttribute = "bitfab.operation"
	otelPayloadAttribute   = "bitfab.payload"
	otelTracesEndpoint     = "/api/sdk/otel/v1/traces"

	otelCollectorEndpointEnv = "BITFAB_OTEL_EXPORTER_ENDPOINT"
	otelMaxRequestBytesEnv   = "BITFAB_OTEL_MAX_REQUEST_BYTES"
	otelExportConcurrencyEnv = "BITFAB_OTEL_EXPORT_CONCURRENCY"

	otelMaxRequestBytes           = 3_000_000
	otelMaxQueueSize              = 8_192
	otelDirectMaxExportBatch      = 512
	otelCollectorMaxExportBatch   = 32
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

func createOtelTransport(apiKey apiKeyResolver, directSender directBatchSender) *otelTransport {
	return newOtelTransport(otelTransportConfig{
		apiKey:              apiKey,
		directSender:        directSender,
		collectorEndpoint:   strings.TrimSpace(os.Getenv(otelCollectorEndpointEnv)),
		maxRequestBytes:     otelMaxRequestBytesFromEnv(),
		maxRequestBatchSize: otelDirectMaxRequestBatchSize,
		exportConcurrency:   otelExportConcurrencyFromEnv(),
		maxQueueSize:        otelMaxQueueSize,
		scheduleDelay:       otelScheduleDelay,
	})
}

type otelTransportConfig struct {
	apiKey              apiKeyResolver
	directSender        directBatchSender
	collectorEndpoint   string
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
		if cfg.collectorEndpoint == "" {
			maxExportBatchSize = otelDirectMaxExportBatch
		} else {
			maxExportBatchSize = otelCollectorMaxExportBatch
		}
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
	if cfg.collectorEndpoint == "" {
		return &bitfabSpanExporter{
			directSender:        cfg.directSender,
			maxRequestBytes:     cfg.maxRequestBytes,
			maxRequestBatchSize: cfg.maxRequestBatchSize,
			exportConcurrency:   cfg.exportConcurrency,
		}
	}

	endpoint := strings.TrimRight(cfg.collectorEndpoint, "/")
	if !strings.HasSuffix(endpoint, "/v1/traces") {
		endpoint += "/v1/traces"
	}

	directExporter := &bitfabSpanExporter{
		directSender:        cfg.directSender,
		maxRequestBytes:     cfg.maxRequestBytes,
		maxRequestBatchSize: cfg.maxRequestBatchSize,
		exportConcurrency:   cfg.exportConcurrency,
	}

	// otlptracehttp.New only logs an unparseable endpoint and hands back an
	// exporter aimed at its own default host, so an endpoint typo would send
	// every span into the void. Reject it here and keep delivering directly.
	if err := validateCollectorEndpoint(endpoint); err != nil {
		warnOnce(
			"otel-collector-endpoint-invalid",
			fmt.Sprintf(
				"%s is not a usable OTLP/HTTP endpoint (%v); falling back to direct delivery",
				otelCollectorEndpointEnv, err,
			),
		)
		return directExporter
	}

	exporter, err := otlptracehttp.New(
		context.Background(),
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithTimeout(otelExportTimeout),
		otlptracehttp.WithMaxRequestSize(cfg.maxRequestBytes),
		otlptracehttp.WithHTTPClient(&http.Client{
			Timeout:   otelExportTimeout,
			Transport: &dynamicAuthTransport{apiKey: cfg.apiKey},
		}),
	)
	if err != nil {
		warnOnce(
			"otel-collector-exporter-failed",
			fmt.Sprintf("failed to build the OpenTelemetry collector exporter (%v); falling back to direct delivery", err),
		)
		return directExporter
	}

	return &sizeLimitedExporter{exporter: exporter, maxRequestBytes: cfg.maxRequestBytes}
}

func validateCollectorEndpoint(endpoint string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https, got %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return fmt.Errorf("missing host")
	}
	return nil
}

// dynamicAuthTransport keeps the API key late-bound. The official exporter
// takes static headers at construction, so the key is stamped per request here
// instead of being frozen when the client's transport is first built.
type dynamicAuthTransport struct {
	apiKey apiKeyResolver
	base   http.RoundTripper
}

func (t *dynamicAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	key := ""
	if t.apiKey != nil {
		key = t.apiKey()
	}
	cloned := req.Clone(req.Context())
	cloned.Header.Set("Authorization", "Bearer "+key)
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(cloned)
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

// sizeLimitedExporter partitions a collector export so no single protobuf
// request exceeds the byte target. Protobuf is strictly smaller than the
// equivalent JSON for these payloads, so each carrier's encoded JSON size is a
// conservative bound. The official exporter's protobuf transformer is internal
// to that module and cannot be reached to measure the real encoded size.
type sizeLimitedExporter struct {
	exporter        sdktrace.SpanExporter
	maxRequestBytes int
}

func (s *sizeLimitedExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}

	var batches [][]sdktrace.ReadOnlySpan
	var current []sdktrace.ReadOnlySpan
	currentSize := 0
	for _, span := range spans {
		size := encodedSpanSize(span)
		if len(current) > 0 && currentSize+size > s.maxRequestBytes {
			batches = append(batches, current)
			current = nil
			currentSize = 0
		}
		current = append(current, span)
		currentSize += size
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}

	var firstErr error
	for _, batch := range batches {
		if err := s.exporter.ExportSpans(ctx, batch); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *sizeLimitedExporter) Shutdown(ctx context.Context) error {
	return s.exporter.Shutdown(ctx)
}

func encodedSpanSize(span sdktrace.ReadOnlySpan) int {
	encoded, err := json.Marshal(spanToOtlp(span))
	if err != nil {
		return 0
	}
	return len(encoded)
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

	encoded := make([]map[string]any, 0, len(spans))
	for _, span := range spans {
		encoded = append(encoded, spanToOtlp(span))
	}

	batches := e.buildRequestBatches(spans[0], encoded)
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
				results[index] = e.sendRecovered(ctx, spans[0], batches[index])
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
	first sdktrace.ReadOnlySpan,
	spans []map[string]any,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			warnOnce("otel-export-panic", fmt.Sprintf("an OpenTelemetry export panicked and was recovered: %v", r))
			err = fmt.Errorf("bitfab: export panicked: %v", r)
		}
	}()
	return e.send(ctx, first, spans)
}

func (e *bitfabSpanExporter) Shutdown(context.Context) error {
	return nil
}

func (e *bitfabSpanExporter) buildRequestBatches(
	first sdktrace.ReadOnlySpan,
	spans []map[string]any,
) [][]map[string]any {
	var batches [][]map[string]any
	var current []map[string]any

	for _, span := range spans {
		if len(current) >= e.maxRequestBatchSize {
			batches = append(batches, current)
			current = nil
		}

		candidate := append(append([]map[string]any{}, current...), span)
		if len(current) > 0 && encodedRequestSize(first, candidate) > e.maxRequestBytes {
			batches = append(batches, current)
			current = []map[string]any{span}
		} else {
			current = candidate
		}
	}

	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}

func (e *bitfabSpanExporter) send(
	ctx context.Context,
	first sdktrace.ReadOnlySpan,
	spans []map[string]any,
) error {
	payload := buildOtlpRequest(first, spans)
	if encodedSize(payload) > e.maxRequestBytes {
		warnOnce(
			"otel-span-too-large",
			"a single span exceeded the configured request-size target and could not be exported",
		)
		return fmt.Errorf("bitfab: span exceeds the configured request-size target")
	}
	return e.sendWithRetries(ctx, payload, len(spans))
}

func (e *bitfabSpanExporter) sendWithRetries(
	ctx context.Context,
	payload map[string]any,
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

		response, err := e.directSender(otelTracesEndpoint, payload, otelExportTimeout)
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

func encodedRequestSize(first sdktrace.ReadOnlySpan, spans []map[string]any) int {
	return encodedSize(buildOtlpRequest(first, spans))
}

func encodedSize(value any) int {
	encoded, err := json.Marshal(value)
	if err != nil {
		return 0
	}
	return len(encoded)
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
		return len(errors) > 0
	case string:
		return errors != ""
	case bool:
		return errors
	default:
		return true
	}
}
