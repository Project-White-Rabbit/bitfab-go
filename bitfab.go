// Package bitfab provides span tracing for Go applications.
//
// It sends trace data to the Bitfab API for visualization and analysis.
// Spans are sent asynchronously in background goroutines.
//
// Two tracing styles are supported:
//
// Closure style (wraps a function inline):
//
//	result, err := client.Span(ctx, "my-service", func(ctx context.Context) (any, error) {
//	    return doWork(ctx)
//	}, bitfab.WithName("ProcessOrder"), bitfab.WithType("function"))
//
// Start/End style (instrument an existing function):
//
//	func processOrder(ctx context.Context, orderID string) (Order, error) {
//	    ctx, span := client.Start(ctx, "order-service", "ProcessOrder", bitfab.WithType("function"))
//	    defer span.End()
//	    span.SetInput(orderID)
//	    order, err := doWork(ctx, orderID)
//	    if err != nil {
//	        span.SetError(err)
//	        return Order{}, err
//	    }
//	    span.SetOutput(order)
//	    return order, nil
//	}
package bitfab

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SpanOccurrence selects one match when multiple spans share a name.
// The zero value defaults to the last matching span.
type SpanOccurrence string

const (
	// FirstSpanOccurrence selects the earliest matching span.
	FirstSpanOccurrence SpanOccurrence = "first"
	// LastSpanOccurrence selects the latest matching span.
	LastSpanOccurrence SpanOccurrence = "last"
)

// CaptureWhen controls whether a span may become the root of a new trace.
type CaptureWhen string

const (
	// CaptureWhenAlways captures the span even when it starts a new trace.
	CaptureWhenAlways CaptureWhen = "always"
	// CaptureWhenNested captures the span only when another Bitfab span is active.
	CaptureWhenNested CaptureWhen = "nested"
)

// SpanOccurrenceAt selects a zero-based match in chronological order.
func SpanOccurrenceAt(index int) SpanOccurrence {
	return SpanOccurrence(strconv.Itoa(index))
}

// SpanLookup identifies a span by its Bitfab ID or name.
// Set exactly one of ID or Name.
type SpanLookup struct {
	ID         string
	Name       string
	Occurrence SpanOccurrence
}

// CapturedSpan is one persisted span returned by GetTraceSpan.
type CapturedSpan struct {
	ID           string         `json:"id"`
	TraceID      string         `json:"traceId"`
	ParentSpanID *string        `json:"parentSpanId"`
	Name         *string        `json:"name"`
	Type         string         `json:"type"`
	Input        any            `json:"input"`
	Output       any            `json:"output"`
	Contexts     []ContextEntry `json:"contexts"`
	Prompt       *string        `json:"prompt"`
	Metadata     map[string]any `json:"metadata"`
	Metrics      map[string]any `json:"metrics"`
	Errors       any            `json:"errors"`
	StartedAt    *time.Time     `json:"startedAt"`
	EndedAt      *time.Time     `json:"endedAt"`
}

var traceIDPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

var lastTimestampMicros atomic.Int64

func nowISOTimestamp() string {
	candidate := time.Now().UTC().UnixMicro()
	for {
		previous := lastTimestampMicros.Load()
		if candidate <= previous {
			candidate = previous + 1
		}
		if lastTimestampMicros.CompareAndSwap(previous, candidate) {
			return time.UnixMicro(candidate).UTC().Format("2006-01-02T15:04:05.000000Z")
		}
	}
}

// Client is the main entry point for creating spans.
type Client struct {
	dbSnapshot             *DBSnapshotConfig
	bamlExecutor           BAMLExecutor
	bamlEnv                map[string]string
	apiKey                 string
	apiKeyFunc             func() string
	apiKeyMu               sync.Mutex
	resolvedAPIKey         string
	apiKeyWarned           bool
	captureDisabled        bool
	simulationPlanDisabled bool
	requestTimeout         time.Duration
	serviceURL             string
	enabled                bool
	strict                 bool
	httpClient             *httpClient
	mockOverridesMu        sync.RWMutex
	mockOverrides          []MockOverride
	autoMu                 sync.RWMutex
	autoNodes              map[string]NodeOptions
	autoPending            int
	autoPendingDone        chan struct{}
	closeMu                sync.Mutex
	closeDone              chan struct{}
	closeResult            bool

	// Datasets creates, reads, and modifies the organization's datasets.
	Datasets *DatasetsClient
	// AssertionCategories manages organization-scoped assertion groupings.
	AssertionCategories *AssertionCategoriesClient
	// Traces searches traces and reads or writes their assertions.
	Traces *TracesClient
	// Labels reads and writes trace and assertion verdicts.
	Labels *LabelsClient
	// Graders reads the individual verdicts recorded by automated graders.
	Graders *GradersClient
}

const captureEnabledEnv = "BITFAB_CAPTURE_ENABLED"

var trueEnvValues = map[string]bool{"1": true, "true": true, "yes": true}
var falseEnvValues = map[string]bool{"0": true, "false": true, "no": true}

func readBooleanEnv(name string) (bool, bool) {
	raw, present := os.LookupEnv(name)
	if !present {
		return false, false
	}
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return false, false
	}
	if trueEnvValues[value] {
		return true, true
	}
	if falseEnvValues[value] {
		return false, true
	}
	warnOnce(
		"unrecognized-boolean-env:"+name,
		fmt.Sprintf("%s=%q is not 0 or 1; ignoring it.", name, raw),
	)
	return false, false
}

// Option configures a Client.
type Option func(*Client)

// WithServiceURL sets a custom Bitfab API base URL.
func WithServiceURL(url string) Option {
	return func(c *Client) { c.serviceURL = url }
}

// WithEnabled is a deprecated alias for WithCaptureEnabled. Replay and seed
// execution still record when ordinary capture is disabled.
func WithEnabled(enabled bool) Option {
	return func(c *Client) {
		warnOnce("enabled-deprecated", "WithEnabled is deprecated; use WithCaptureEnabled")
		c.enabled = enabled
	}
}

// WithCaptureEnabled controls ordinary tracing while preserving replay and seed capture.
func WithCaptureEnabled(enabled bool) Option {
	return func(c *Client) { c.captureDisabled = !enabled }
}

// WithAPIKeyFunc resolves credentials at first use, after application setup.
func WithAPIKeyFunc(resolve func() string) Option {
	return func(c *Client) { c.apiKeyFunc = resolve; c.apiKey = "" }
}

// WithSimulationPlan controls background simulation-plan content policy reads.
func WithSimulationPlan(enabled bool) Option {
	return func(c *Client) { c.simulationPlanDisabled = !enabled }
}

// WithTimeout sets the default HTTP request timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) { c.requestTimeout = timeout }
}

// WithAPIKey sets the API key. Equivalent to the apiKey argument of NewClient,
// useful when constructing purely via options; whichever is set last wins.
func WithAPIKey(apiKey string) Option {
	return func(c *Client) { c.apiKey = apiKey; c.apiKeyFunc = nil }
}

// WithStrict makes a missing API key panic at first use. Client construction
// remains lazy so environment loading can follow package initialization.
func WithStrict(strict bool) Option {
	return func(c *Client) { c.strict = strict }
}

// NewClient creates a new Bitfab client.
//
// Credentials are resolved at first use and a successful resolution is cached.
func NewClient(apiKey string, opts ...Option) *Client {
	enabled := true
	if fromEnv, ok := readBooleanEnv(captureEnabledEnv); ok {
		enabled = fromEnv
	}
	c := &Client{
		apiKey:     apiKey,
		serviceURL: DefaultServiceURL,
		enabled:    enabled,
	}
	for _, opt := range opts {
		opt(c)
	}
	c.httpClient = newHTTPClient(c.apiKey, c.serviceURL)
	c.httpClient.apiKeyFunc = c.resolveAPIKey
	c.httpClient.simulationPlan = newSimulationPlan(c.httpClient.getSimulationPlan, !c.simulationPlanDisabled)
	if c.requestTimeout > 0 {
		c.httpClient.client.Timeout = c.requestTimeout
	}
	c.Datasets = &DatasetsClient{httpClient: c.httpClient}
	c.AssertionCategories = &AssertionCategoriesClient{httpClient: c.httpClient}
	c.Traces = &TracesClient{httpClient: c.httpClient}
	c.Labels = &LabelsClient{httpClient: c.httpClient}
	c.Graders = &GradersClient{httpClient: c.httpClient}
	startCommitRefResolution()
	return c
}

// GetTraceSpan fetches one persisted span without loading the full trace.
// Name lookups default to the last matching span.
func (c *Client) GetTraceSpan(ctx context.Context, traceID string, lookup SpanLookup) (*CapturedSpan, error) {
	if !traceIDPattern.MatchString(traceID) {
		return nil, fmt.Errorf("bitfab: invalid trace ID")
	}
	if (lookup.ID == "") == (lookup.Name == "") {
		return nil, fmt.Errorf("bitfab: provide exactly one of ID or Name")
	}

	query := url.Values{}
	if lookup.ID != "" {
		if !traceIDPattern.MatchString(lookup.ID) {
			return nil, fmt.Errorf("bitfab: invalid span ID")
		}
		query.Set("id", lookup.ID)
	} else {
		occurrence := lookup.Occurrence
		if occurrence == "" {
			occurrence = LastSpanOccurrence
		}
		if occurrence != FirstSpanOccurrence && occurrence != LastSpanOccurrence {
			index, err := strconv.Atoi(string(occurrence))
			if err != nil || index < 0 {
				return nil, fmt.Errorf("bitfab: occurrence must be first, last, or a non-negative index")
			}
		}
		query.Set("name", lookup.Name)
		query.Set("occurrence", string(occurrence))
	}

	var response struct {
		Span *CapturedSpan `json:"span"`
	}
	endpoint := "/api/sdk/traces/" + url.PathEscape(traceID) + "/span?" + query.Encode()
	if err := c.httpClient.get(ctx, endpoint, &response); err != nil {
		return nil, err
	}
	return response.Span, nil
}

// SpanFunc is the function signature for code executed inside a span.
type SpanFunc func(ctx context.Context) (any, error)

// SpanOption configures a single span.
type SpanOption func(*spanConfig)

type spanConfig struct {
	finalize       SpanFinalizer
	testRunID      string
	name           string
	spanType       string
	functionName   string
	input          any
	captureWhen    CaptureWhen
	mockOnReplay   bool
	mockOutputType reflect.Type
}

// WithName sets an explicit span name. Defaults to the traceFunctionKey if not set.
func WithName(name string) SpanOption {
	return func(c *spanConfig) { c.name = name }
}

// WithType sets the span type. Must be one of: llm, agent, function, guardrail, handoff, custom.
// Defaults to "custom".
func WithType(spanType string) SpanOption {
	return func(c *spanConfig) { c.spanType = spanType }
}

// WithFunctionName sets the function name recorded in span data.
func WithFunctionName(name string) SpanOption {
	return func(c *spanConfig) { c.functionName = name }
}

// WithInput sets the input data recorded in span data for the closure-style Span API.
// Pass one or more arguments. A single argument is stored directly; multiple arguments
// are stored as a slice.
func WithInput(args ...any) SpanOption {
	return func(c *spanConfig) {
		if len(args) == 1 {
			c.input = args[0]
		} else {
			c.input = args
		}
	}
}

// WithFinalize records a serializable view of the returned value asynchronously.
// The original value returns unchanged. FlushTraces and Close await finalization.
func WithFinalize(finalize SpanFinalizer) SpanOption {
	return func(c *spanConfig) { c.finalize = finalize }
}

// WithTestRunID attributes a span and the trace it starts to a test run.
// Replay attribution takes precedence so items remain in their experiment.
func WithTestRunID(testRunID string) SpanOption {
	return func(c *spanConfig) { c.testRunID = testRunID }
}

// WithCaptureWhen controls whether a span may start a new trace.
// CaptureWhenNested is useful for helpers that should only appear inside an
// existing Bitfab trace. The function still runs normally when no parent exists.
// Unknown values warn once and default to CaptureWhenAlways.
func WithCaptureWhen(captureWhen CaptureWhen) SpanOption {
	return func(c *spanConfig) { c.captureWhen = captureWhen }
}

// WithMockOnReplay marks a closure-style child Span for recorded-output
// substitution under the default MockMarked replay strategy.
func WithMockOnReplay(mock bool) SpanOption {
	return func(c *spanConfig) { c.mockOnReplay = mock }
}

// WithMockOutputType decodes a recorded or overridden JSON output into T before
// returning it from a mocked closure-style Span.
func WithMockOutputType[T any]() SpanOption {
	return func(c *spanConfig) {
		c.mockOutputType = reflect.TypeOf((*T)(nil)).Elem()
	}
}

func normalizeCaptureWhen(captureWhen CaptureWhen, traceFunctionKey string) CaptureWhen {
	switch captureWhen {
	case CaptureWhenAlways, CaptureWhenNested:
		return captureWhen
	default:
		warnOnce(
			"invalid-capture-when:"+traceFunctionKey,
			fmt.Sprintf(
				"unknown captureWhen value %q; defaulting to %q. Valid values: %q, %q.",
				captureWhen,
				CaptureWhenAlways,
				CaptureWhenAlways,
				CaptureWhenNested,
			),
		)
		return CaptureWhenAlways
	}
}

// Span executes fn inside a traced span. The span is sent to the Bitfab API
// in the background after fn completes. Nested spans are automatically tracked
// through the context.
//
// The return value of fn is automatically captured as the span output.
// Use WithInput to capture input data.
// If fn returns an error, it is captured in the span data and returned to the caller.
func (c *Client) Span(ctx context.Context, traceFunctionKey string, fn SpanFunc, opts ...SpanOption) (any, error) {
	if !c.shouldRecord(ctx) {
		return fn(ctx)
	}

	if err := checkMixedAutoSpan(ctx); err != nil {
		return nil, err
	}
	cfg := spanConfig{
		name:        traceFunctionKey,
		spanType:    "custom",
		captureWhen: CaptureWhenAlways,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.captureWhen = normalizeCaptureWhen(cfg.captureWhen, traceFunctionKey)
	if cfg.captureWhen == CaptureWhenNested && currentSpan(ctx) == nil {
		return fn(ctx)
	}

	// An invalid span type must not fail the user's call. Degrade to "custom"
	// (warn once) so fn still runs and the span still ships, rather than
	// returning a Bitfab error in place of the user's real result.
	if !validSpanTypes[cfg.spanType] {
		warnOnce(
			"invalid-span-type",
			fmt.Sprintf("an invalid span type was used; defaulting to %q. Valid types: llm, agent, function, guardrail, handoff, custom.", "custom"),
		)
		cfg.spanType = "custom"
	}

	// Compute span identity and register trace state. If this instrumentation
	// prologue fails for any reason, run fn untraced rather than crashing the
	// host or skipping the user's call.
	id, ok := c.beginSpan(ctx)
	if !ok {
		return fn(ctx)
	}

	replayAtStart := currentReplayContext(ctx)
	if id.isRootSpan && cfg.testRunID != "" && (replayAtStart == nil || replayAtStart.testRunID == "") {
		state := getTraceState(id.traceID)
		state.mu.Lock()
		state.TestRunID = cfg.testRunID
		state.mu.Unlock()
	}
	prepareManagedAutoSpan(ctx, id)
	startedAt := nowISOTimestamp()

	// Execute fn with the new span pushed onto the context stack, unless replay
	// selected this child span for recorded or overridden output substitution.
	childCtx := withSpanContext(ctx, id.traceID, id.spanID)
	childCtx, enrichment := withSpanEnrichment(childCtx, id.spanID)
	result, intercepted, mockSource, mockErr := c.resolveReplayMock(
		ctx,
		traceFunctionKey,
		cfg,
		id.isRootSpan,
	)
	mocked := intercepted && mockErr == nil
	fnErr := mockErr
	var fnPanic any
	if !intercepted {
		func() {
			defer c.beginExplicitAutoScope(ctx)()
			defer func() { fnPanic = recover() }()
			result, fnErr = fn(childCtx)
		}()
	}
	if fnPanic != nil {
		fnErr = fmt.Errorf("panic: %v", fnPanic)
	}

	// Build and send span data - wrapped in a closure so a panic here
	// never crashes the host app. The user's result/error is always returned.
	c.finalizeSpanOutput(id.traceID, cfg.finalize, result, fnErr, intercepted, func(recordedResult any, recordedErr error) {
		defer c.httpClient.traceCompletion.end(id.traceID, id.spanID)
		finishManagedAutoSpan(ctx, id.spanID, func() {
			endedAt := nowISOTimestamp()
			defer func() { recover() }()

			spanData := map[string]any{
				"name": cfg.name,
				"type": cfg.spanType,
			}
			if cfg.functionName != "" {
				spanData["function_name"] = cfg.functionName
			}
			var dropped []string
			if cfg.input != nil {
				v, d := capValueReport(cfg.input)
				spanData["input"] = v
				dropped = append(dropped, d...)
			}
			if recordedResult != nil {
				v, d := capValueReport(recordedResult)
				spanData["output"] = v
				dropped = append(dropped, d...)
			}
			if recordedErr != nil {
				spanData["error"] = recordedErr.Error()
				spanData["error_source"] = "code"
			}

			enrichment.apply(spanData)
			rawSpan := map[string]any{
				"id":         id.spanID,
				"trace_id":   id.traceID,
				"started_at": startedAt,
				"ended_at":   endedAt,
				"span_data":  spanData,
			}
			if id.parentSpanID != "" {
				rawSpan["parent_id"] = id.parentSpanID
			}
			replay := currentReplayContext(ctx)
			if replay != nil && replay.inputSourceSpanID != "" {
				rawSpan["input_source_span_id"] = replay.inputSourceSpanID
			}

			// If drop() was called on this trace, suppress the span PAYLOAD upload
			// for every span that completes after the flag was set. The trace
			// completion still rides out with dropped: true (see
			// sendTraceCompletion), so the server scrubs any sibling spans that
			// already raced out before the flag was set.
			if ts := getTraceState(id.traceID); ts == nil || !ts.isDropped() {
				payload := map[string]any{
					"id":               id.spanID,
					"traceId":          id.traceID,
					"type":             "sdk-function",
					"source":           "go-sdk-function",
					"sourceTraceId":    id.traceID,
					"traceFunctionKey": traceFunctionKey,
					"rawSpan":          rawSpan,
				}
				if replay != nil && replay.testRunID != "" {
					payload["testRunId"] = replay.testRunID
				}
				if cfg.testRunID != "" && (replay == nil || replay.testRunID == "") {
					payload["testRunId"] = cfg.testRunID
				}
				if mocked {
					payload["mocked"] = true
					payload["mockTarget"] = "output"
					payload["mockSource"] = string(mockSource)
				}
				stampManagedAutoSpan(ctx, rawSpan, payload)
				c.httpClient.sendExternalSpan(payload, dropped...)
			}

			if id.isRootSpan {
				c.sendTraceCompletion(traceFunctionKey, id.traceID, startedAt, endedAt)
			}
		})

	})

	if fnPanic != nil {
		panic(fnPanic)
	}
	return result, fnErr
}

// spanIdentity is the per-span state produced by the instrumentation prologue.
type spanIdentity struct {
	traceID      string
	spanID       string
	parentSpanID string
	isRootSpan   bool
}

// beginSpan computes the trace/span ids and registers root trace state. It runs
// on the user's synchronous call path, so it is fully guarded: a panic here
// (id generation, map writes) can never crash the host. On failure it cleans up
// any partially registered trace state, warns once, and returns ok=false so the
// caller runs the user's function untraced.
func (c *Client) beginSpan(ctx context.Context) (id spanIdentity, ok bool) {
	var registered string
	defer func() {
		if r := recover(); r != nil {
			if registered != "" {
				deleteTraceState(registered)
			}
			warnOnce(
				"span-setup",
				"span setup failed; this call runs untraced. Your function still executes and returns normally.",
			)
			id = spanIdentity{}
			ok = false
		}
	}()

	parent := currentSpan(ctx)
	traceID := randomUUID()
	if parent != nil {
		traceID = parent.traceID
	} else if seed := seedFromContext(ctx); seed != nil {
		traceID = seed.traceID
	} else if replay := currentReplayContext(ctx); replay != nil && replay.traceID != "" {
		traceID = replay.traceID
	}
	spanID := randomUUID()

	isRootSpan := parent == nil
	var parentSpanID string
	if parent != nil {
		parentSpanID = parent.spanID
	}

	if isRootSpan && getTraceState(traceID) == nil {
		state := createTraceState(traceID)
		state.DBSnapshotRef = c.buildDBSnapshotRef(state.StartedAt)
		if replay := currentReplayContext(ctx); replay != nil {
			state.TestRunID = replay.testRunID
			state.InputSourceTraceID = replay.inputSourceTraceID
			state.replay = replay
		}
		registered = traceID
	}

	c.httpClient.traceCompletion.start(traceID, spanID)
	return spanIdentity{
		traceID:      traceID,
		spanID:       spanID,
		parentSpanID: parentSpanID,
		isRootSpan:   isRootSpan,
	}, true
}

// Start begins a new span and returns the updated context and an ActiveSpan handle.
// Use defer span.End() to complete the span. Use SetInput, SetOutput, and SetError
// to record data on the span.
//
// This is the recommended way to instrument existing functions without restructuring them.
func (c *Client) Start(ctx context.Context, traceFunctionKey string, spanName string, opts ...SpanOption) (context.Context, *ActiveSpan) {
	if err := checkMixedAutoSpan(ctx); err != nil {
		panic(err)
	}
	if !c.shouldRecord(ctx) {
		return ctx, &ActiveSpan{}
	}

	cfg := spanConfig{
		name:        spanName,
		spanType:    "custom",
		captureWhen: CaptureWhenAlways,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.captureWhen = normalizeCaptureWhen(cfg.captureWhen, traceFunctionKey)
	if cfg.captureWhen == CaptureWhenNested && currentSpan(ctx) == nil {
		return ctx, &ActiveSpan{}
	}

	if !validSpanTypes[cfg.spanType] {
		warnOnce(
			"invalid-span-type",
			fmt.Sprintf("an invalid span type was used; defaulting to %q. Valid types: llm, agent, function, guardrail, handoff, custom.", "custom"),
		)
		cfg.spanType = "custom"
	}

	// If the instrumentation prologue fails, hand back the original context and
	// a no-op span (its methods all guard a nil client) so the caller keeps
	// running normally, untraced.
	id, ok := c.beginSpan(ctx)
	if !ok {
		return ctx, &ActiveSpan{}
	}

	childCtx := withSpanContext(ctx, id.traceID, id.spanID)
	childCtx, enrichment := withSpanEnrichment(childCtx, id.spanID)

	span := &ActiveSpan{
		enrichment:       enrichment,
		client:           c,
		traceFunctionKey: traceFunctionKey,
		traceID:          id.traceID,
		spanID:           id.spanID,
		parentSpanID:     id.parentSpanID,
		startedAt:        nowISOTimestamp(),
		cfg:              cfg,
		isRootSpan:       id.isRootSpan,
	}
	if replay := currentReplayContext(ctx); replay != nil {
		span.testRunID = replay.testRunID
		span.inputSourceSpanID = replay.inputSourceSpanID
	}

	replayAtStart := currentReplayContext(ctx)
	if cfg.testRunID != "" && (replayAtStart == nil || replayAtStart.testRunID == "") {
		span.testRunID = cfg.testRunID
		if id.isRootSpan {
			state := getTraceState(id.traceID)
			state.mu.Lock()
			state.TestRunID = cfg.testRunID
			state.mu.Unlock()
		}
	}
	span.autoRestore = c.beginExplicitAutoScope(ctx)
	return childCtx, span
}

// FlushTraces drains this client's pending span deliveries within timeout.
// It reports false when an export failed or the deadline expired.
func (c *Client) FlushTraces(timeout time.Duration) bool {
	started := time.Now()
	settled := c.waitAutoFinalizers(timeout)
	flushed := c.httpClient.flush(max(timeout-time.Since(started), 0))
	return settled && flushed
}

// Close flushes this client's pending spans and permanently shuts down its
// OpenTelemetry worker. It is idempotent, and reports false when an export
// failed or the deadline expired. A closed client no longer records spans.
func (c *Client) Close(timeout time.Duration) bool {
	started := time.Now()
	c.closeMu.Lock()
	if c.closeDone != nil {
		done := c.closeDone
		c.closeMu.Unlock()
		timer := time.NewTimer(max(timeout, 0))
		defer timer.Stop()
		select {
		case <-done:
			c.closeMu.Lock()
			result := c.closeResult
			c.closeMu.Unlock()
			return result
		default:
		}
		select {
		case <-done:
			c.closeMu.Lock()
			result := c.closeResult
			c.closeMu.Unlock()
			return result
		case <-timer.C:
			return false
		}
	}
	c.closeDone = make(chan struct{})
	c.closeMu.Unlock()
	settled := c.waitAutoFinalizers(timeout)
	closed := c.httpClient.close(max(timeout-time.Since(started), 0))
	c.closeMu.Lock()
	c.closeResult = settled && closed
	close(c.closeDone)
	c.closeMu.Unlock()
	return settled && closed
}

// GetFunction returns a Function bound to the given traceFunctionKey.
// This provides a fluent API for creating multiple spans under the same key.
func (c *Client) GetFunction(traceFunctionKey string) *Function {
	c.httpClient.simulationPlan.refresh()
	return &Function{
		client:           c,
		traceFunctionKey: traceFunctionKey,
	}
}

// RegisterMockOverride appends a client-scoped replay override.
// Per-call ReplayOptions.MockOverrides take precedence over registered values.
func (c *Client) RegisterMockOverride(override MockOverride) error {
	if override.Match == nil {
		return fmt.Errorf("bitfab: replay mock override requires a matcher")
	}
	c.mockOverridesMu.Lock()
	defer c.mockOverridesMu.Unlock()
	c.mockOverrides = append(c.mockOverrides, override)
	return nil
}

// ClearMockOverrides removes every override registered on this client.
func (c *Client) ClearMockOverrides() {
	c.mockOverridesMu.Lock()
	defer c.mockOverridesMu.Unlock()
	c.mockOverrides = nil
}

func (c *Client) registeredMockOverrides() []MockOverride {
	c.mockOverridesMu.RLock()
	defer c.mockOverridesMu.RUnlock()
	return append([]MockOverride(nil), c.mockOverrides...)
}

// Function is a helper that binds a traceFunctionKey for repeated span creation.
type Function struct {
	client           *Client
	traceFunctionKey string
}

// Span executes fn inside a traced span using this Function's traceFunctionKey.
func (f *Function) Span(ctx context.Context, fn SpanFunc, opts ...SpanOption) (any, error) {
	return f.client.Span(ctx, f.traceFunctionKey, fn, opts...)
}

// Start begins a new span using this Function's traceFunctionKey.
func (f *Function) Start(ctx context.Context, spanName string, opts ...SpanOption) (context.Context, *ActiveSpan) {
	return f.client.Start(ctx, f.traceFunctionKey, spanName, opts...)
}

// ActiveSpan represents an in-progress span created by Start.
// Call End() to complete the span and send it to the API.
type ActiveSpan struct {
	enrichment        *spanEnrichment
	autoRestore       func()
	client            *Client
	traceFunctionKey  string
	traceID           string
	spanID            string
	parentSpanID      string
	startedAt         string
	cfg               spanConfig
	input             any
	output            any
	spanErr           error
	contexts          []ContextEntry
	prompt            string
	isRootSpan        bool
	testRunID         string
	inputSourceSpanID string
	once              sync.Once
}

// SetInput records the span's input data. Pass one or more arguments.
// A single argument is stored directly; multiple arguments are stored as a slice.
// Safe to call on nil receiver (no-op).
func (s *ActiveSpan) SetInput(args ...any) {
	defer func() { recover() }()
	if s == nil {
		return
	}
	if len(args) == 1 {
		s.input = args[0]
	} else {
		s.input = args
	}
}

// SetOutput records the span's output data.
// Safe to call on nil receiver (no-op).
func (s *ActiveSpan) SetOutput(output any) {
	defer func() { recover() }()
	if s == nil {
		return
	}
	s.output = output
}

// SetError records an error on the span.
// Safe to call on nil receiver (no-op).
func (s *ActiveSpan) SetError(err error) {
	defer func() { recover() }()
	if s == nil {
		return
	}
	s.spanErr = err
}

// AddContext adds a context entry to the span.
// The entire map is pushed as a single entry in the contexts array.
// Context entries are accumulated - multiple calls add to the list.
// Safe to call on nil receiver (no-op).
func (s *ActiveSpan) AddContext(context map[string]any) {
	defer func() { recover() }()
	if s == nil || context == nil {
		return
	}
	s.contexts = append(s.contexts, context)
}

// SetPrompt sets the prompt string on the span.
// The prompt is stored in span_data.prompt. Calling multiple times
// overwrites the previous value.
// Safe to call on nil receiver (no-op).
func (s *ActiveSpan) SetPrompt(prompt string) {
	defer func() { recover() }()
	if s == nil || prompt == "" {
		return
	}
	s.prompt = prompt
}

// End completes the span and sends it to the API in the background.
// End is idempotent - calling it multiple times has no effect after the first call.
func (s *ActiveSpan) End() {
	defer func() { recover() }() // Never crash the host app (catches nil receiver)
	if s.client == nil {
		return
	}
	s.once.Do(func() {
		if s.autoRestore != nil {
			defer s.autoRestore()
		}
		defer func() { recover() }() // Never crash the host app
		s.client.finalizeSpanOutput(s.traceID, s.cfg.finalize, s.output, s.spanErr, false, func(recordedOutput any, recordedErr error) {
			defer s.client.httpClient.traceCompletion.end(s.traceID, s.spanID)
			defer func() { recover() }()
			endedAt := nowISOTimestamp()

			spanData := map[string]any{
				"name": s.cfg.name,
				"type": s.cfg.spanType,
			}
			if s.cfg.functionName != "" {
				spanData["function_name"] = s.cfg.functionName
			}
			var dropped []string
			if s.input != nil {
				v, d := capValueReport(s.input)
				spanData["input"] = v
				dropped = append(dropped, d...)
			}
			if recordedOutput != nil {
				v, d := capValueReport(recordedOutput)
				spanData["output"] = v
				dropped = append(dropped, d...)
			}
			if recordedErr != nil {
				spanData["error"] = recordedErr.Error()
				spanData["error_source"] = "code"
			}
			if len(s.contexts) > 0 {
				spanData["contexts"] = s.contexts
			}
			if s.prompt != "" {
				spanData["prompt"] = s.prompt
			}

			s.enrichment.apply(spanData)
			rawSpan := map[string]any{
				"id":         s.spanID,
				"trace_id":   s.traceID,
				"started_at": s.startedAt,
				"ended_at":   endedAt,
				"span_data":  spanData,
			}
			if s.parentSpanID != "" {
				rawSpan["parent_id"] = s.parentSpanID
			}
			if s.inputSourceSpanID != "" {
				rawSpan["input_source_span_id"] = s.inputSourceSpanID
			}

			// If drop() was called on this trace, suppress the span PAYLOAD upload
			// for every span that completes after the flag was set. The trace
			// completion still rides out with dropped: true (see
			// sendTraceCompletion), so the server scrubs any sibling spans that
			// already raced out before the flag was set.
			if ts := getTraceState(s.traceID); ts == nil || !ts.isDropped() {
				payload := map[string]any{
					"id":               s.spanID,
					"traceId":          s.traceID,
					"type":             "sdk-function",
					"source":           "go-sdk-function",
					"sourceTraceId":    s.traceID,
					"traceFunctionKey": s.traceFunctionKey,
					"rawSpan":          rawSpan,
				}
				if s.testRunID != "" {
					payload["testRunId"] = s.testRunID
				}
				s.client.httpClient.sendExternalSpan(payload, dropped...)
			}

			if s.isRootSpan {
				s.client.sendTraceCompletion(s.traceFunctionKey, s.traceID, s.startedAt, endedAt)
			}
		})
	})
}

// sendTraceCompletion sends trace completion data to the API.
func (c *Client) sendTraceCompletion(traceFunctionKey, traceID, startedAt, endedAt string) {
	defer func() { recover() }() // Never crash the host app

	ts := getTraceState(traceID)
	if ts != nil {
		ts.mu.Lock()
		if ts.pendingFinalizers > 0 {
			ts.completion = func() { c.sendTraceCompletion(traceFunctionKey, traceID, startedAt, nowISOTimestamp()) }
			ts.mu.Unlock()
			return
		}
		ts.mu.Unlock()
	}
	traceStartedAt := startedAt
	if ts != nil && ts.StartedAt != "" {
		traceStartedAt = ts.StartedAt
	}

	rawTrace := map[string]any{
		"id":         traceID,
		"started_at": traceStartedAt,
		"ended_at":   endedAt,
	}

	if ts != nil {
		if ts.IngestionType != "" {
			rawTrace["ingestion_type"] = ts.IngestionType
		}
		if ts.Name != "" {
			rawTrace["name"] = ts.Name
		}
		if ts.Metadata != nil {
			rawTrace["metadata"] = ts.Metadata
		}
		if len(ts.Contexts) > 0 {
			rawTrace["contexts"] = ts.Contexts
		}
		if ts.replay != nil {
			rawTrace["replay_attempt"] = ts.replay.attempt
		}
		if ts.InputSourceTraceID != "" {
			rawTrace["input_source_trace_id"] = ts.InputSourceTraceID
		}
		if ts.DBSnapshotRef != nil {
			rawTrace["db_snapshot_ref"] = ts.DBSnapshotRef
		}
		if usage := dbSnapshotUsage(ts.replay); usage != nil {
			rawTrace["db_snapshot_usage"] = usage
		}
	}
	if ref := currentCommitRef(); ref != nil {
		rawTrace["commit_ref"] = ref
	}

	payload := map[string]any{
		"id":               traceID,
		"type":             "sdk-function",
		"source":           "go-sdk-function",
		"traceFunctionKey": traceFunctionKey,
		"externalTrace":    rawTrace,
		"completed":        true,
	}

	if ts != nil && ts.SessionID != "" {
		payload["sessionId"] = ts.SessionID
	}
	if ts != nil && ts.TestRunID != "" {
		payload["testRunId"] = ts.TestRunID
	}

	if ts != nil && ts.isDropped() {
		payload["dropped"] = true
	}

	c.httpClient.sendExternalTrace(payload)

	// Clean up trace state
	deleteTraceState(traceID)
}
