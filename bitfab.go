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
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Client is the main entry point for creating spans.
type Client struct {
	apiKey       string
	serviceURL   string
	enabled      bool
	httpClient   *httpClient
	pendingSpans map[string][]<-chan struct{}
	pendingMu    sync.Mutex
}

// Option configures a Client.
type Option func(*Client)

// WithServiceURL sets a custom Bitfab API base URL.
func WithServiceURL(url string) Option {
	return func(c *Client) { c.serviceURL = url }
}

// WithEnabled controls whether the client sends spans. Defaults to true.
// When disabled, Span still executes the callback and Start returns a no-op ActiveSpan,
// but no data is sent to the API.
func WithEnabled(enabled bool) Option {
	return func(c *Client) { c.enabled = enabled }
}

// NewClient creates a new Bitfab client.
func NewClient(apiKey string, opts ...Option) *Client {
	c := &Client{
		apiKey:       apiKey,
		serviceURL:   DefaultServiceURL,
		enabled:      true,
		pendingSpans: make(map[string][]<-chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.enabled && strings.TrimSpace(c.apiKey) == "" {
		log.Println("Bitfab: apiKey is empty — tracing is disabled. Provide a valid API key to enable tracing.")
		c.enabled = false
	}
	c.httpClient = newHTTPClient(c.apiKey, c.serviceURL)
	return c
}

// SpanFunc is the function signature for code executed inside a span.
type SpanFunc func(ctx context.Context) (any, error)

// SpanOption configures a single span.
type SpanOption func(*spanConfig)

type spanConfig struct {
	name         string
	spanType     string
	functionName string
	input        any
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

// Span executes fn inside a traced span. The span is sent to the Bitfab API
// in the background after fn completes. Nested spans are automatically tracked
// through the context.
//
// The return value of fn is automatically captured as the span output.
// Use WithInput to capture input data.
// If fn returns an error, it is captured in the span data and returned to the caller.
func (c *Client) Span(ctx context.Context, traceFunctionKey string, fn SpanFunc, opts ...SpanOption) (any, error) {
	if !c.enabled {
		return fn(ctx)
	}

	cfg := spanConfig{
		name:     traceFunctionKey,
		spanType: "custom",
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if !validSpanTypes[cfg.spanType] {
		return nil, fmt.Errorf("bitfab: invalid span type %q, must be one of: llm, agent, function, guardrail, handoff, custom", cfg.spanType)
	}

	parent := currentSpan(ctx)
	traceID := uuid.New().String()
	if parent != nil {
		traceID = parent.traceID
	}
	spanID := uuid.New().String()

	var parentSpanID string
	isRootSpan := parent == nil
	if parent != nil {
		parentSpanID = parent.spanID
	}

	// Register trace state for root spans
	if isRootSpan && getTraceState(traceID) == nil {
		createTraceState(traceID)
		c.pendingMu.Lock()
		c.pendingSpans[traceID] = []<-chan struct{}{}
		c.pendingMu.Unlock()
	}

	startedAt := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")

	// Execute fn with the new span pushed onto the context stack
	childCtx := withSpanContext(ctx, traceID, spanID)
	result, fnErr := fn(childCtx)

	endedAt := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")

	// Build and send span data — wrapped in a closure so a panic here
	// never crashes the host app. The user's result/error is always returned.
	func() {
		defer func() { recover() }()

		spanData := map[string]any{
			"name": cfg.name,
			"type": cfg.spanType,
		}
		if cfg.functionName != "" {
			spanData["function_name"] = cfg.functionName
		}
		if cfg.input != nil {
			spanData["input"] = cfg.input
		}
		if result != nil {
			spanData["output"] = result
		}
		if fnErr != nil {
			spanData["error"] = fnErr.Error()
			spanData["error_source"] = "code"
		}

		rawSpan := map[string]any{
			"id":         spanID,
			"trace_id":   traceID,
			"started_at": startedAt,
			"ended_at":   endedAt,
			"span_data":  spanData,
		}
		if parentSpanID != "" {
			rawSpan["parent_id"] = parentSpanID
		}

		done := c.httpClient.sendExternalSpan(map[string]any{
			"type":             "sdk-function",
			"source":           "go-sdk-function",
			"sourceTraceId":    traceID,
			"traceFunctionKey": traceFunctionKey,
			"rawSpan":          rawSpan,
		})

		if isRootSpan {
			c.pendingMu.Lock()
			pending := c.pendingSpans[traceID]
			delete(c.pendingSpans, traceID)
			c.pendingMu.Unlock()

			for _, ch := range pending {
				select {
				case <-ch:
				case <-time.After(5 * time.Second):
				}
			}

			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}

			c.sendTraceCompletion(traceFunctionKey, traceID, startedAt, endedAt)
		} else {
			c.pendingMu.Lock()
			c.pendingSpans[traceID] = append(c.pendingSpans[traceID], done)
			c.pendingMu.Unlock()
		}
	}()

	return result, fnErr
}

// Start begins a new span and returns the updated context and an ActiveSpan handle.
// Use defer span.End() to complete the span. Use SetInput, SetOutput, and SetError
// to record data on the span.
//
// This is the recommended way to instrument existing functions without restructuring them.
func (c *Client) Start(ctx context.Context, traceFunctionKey string, spanName string, opts ...SpanOption) (context.Context, *ActiveSpan) {
	if !c.enabled {
		return ctx, &ActiveSpan{}
	}

	cfg := spanConfig{
		name:     spanName,
		spanType: "custom",
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	parent := currentSpan(ctx)
	traceID := uuid.New().String()
	if parent != nil {
		traceID = parent.traceID
	}
	spanID := uuid.New().String()

	var parentSpanID string
	isRootSpan := parent == nil
	if parent != nil {
		parentSpanID = parent.spanID
	}

	// Register trace state for root spans
	if isRootSpan && getTraceState(traceID) == nil {
		createTraceState(traceID)
		c.pendingMu.Lock()
		c.pendingSpans[traceID] = []<-chan struct{}{}
		c.pendingMu.Unlock()
	}

	childCtx := withSpanContext(ctx, traceID, spanID)

	span := &ActiveSpan{
		client:           c,
		traceFunctionKey: traceFunctionKey,
		traceID:          traceID,
		spanID:           spanID,
		parentSpanID:     parentSpanID,
		startedAt:        time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		cfg:              cfg,
		isRootSpan:       isRootSpan,
	}

	return childCtx, span
}

// FlushTraces waits for all pending background span deliveries to complete,
// up to the given timeout.
func (c *Client) FlushTraces(timeout time.Duration) {
	c.httpClient.flush(timeout)
}

// GetFunction returns a Function bound to the given traceFunctionKey.
// This provides a fluent API for creating multiple spans under the same key.
func (c *Client) GetFunction(traceFunctionKey string) *Function {
	return &Function{
		client:           c,
		traceFunctionKey: traceFunctionKey,
	}
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
	client           *Client
	traceFunctionKey string
	traceID          string
	spanID           string
	parentSpanID     string
	startedAt        string
	cfg              spanConfig
	input            any
	output           any
	spanErr          error
	contexts         []ContextEntry
	prompt           string
	isRootSpan       bool
	once             sync.Once
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
// End is idempotent — calling it multiple times has no effect after the first call.
func (s *ActiveSpan) End() {
	defer func() { recover() }() // Never crash the host app (catches nil receiver)
	if s.client == nil {
		return
	}
	s.once.Do(func() {
		defer func() { recover() }() // Never crash the host app

		endedAt := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")

		spanData := map[string]any{
			"name": s.cfg.name,
			"type": s.cfg.spanType,
		}
		if s.cfg.functionName != "" {
			spanData["function_name"] = s.cfg.functionName
		}
		if s.input != nil {
			spanData["input"] = s.input
		}
		if s.output != nil {
			spanData["output"] = s.output
		}
		if s.spanErr != nil {
			spanData["error"] = s.spanErr.Error()
			spanData["error_source"] = "code"
		}
		if len(s.contexts) > 0 {
			spanData["contexts"] = s.contexts
		}
		if s.prompt != "" {
			spanData["prompt"] = s.prompt
		}

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

		done := s.client.httpClient.sendExternalSpan(map[string]any{
			"type":             "sdk-function",
			"source":           "go-sdk-function",
			"sourceTraceId":    s.traceID,
			"traceFunctionKey": s.traceFunctionKey,
			"rawSpan":          rawSpan,
		})

		if s.isRootSpan {
			s.client.pendingMu.Lock()
			pending := s.client.pendingSpans[s.traceID]
			delete(s.client.pendingSpans, s.traceID)
			s.client.pendingMu.Unlock()

			for _, ch := range pending {
				select {
				case <-ch:
				case <-time.After(5 * time.Second):
				}
			}

			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}

			s.client.sendTraceCompletion(s.traceFunctionKey, s.traceID, s.startedAt, endedAt)
		} else {
			s.client.pendingMu.Lock()
			s.client.pendingSpans[s.traceID] = append(s.client.pendingSpans[s.traceID], done)
			s.client.pendingMu.Unlock()
		}
	})
}

// sendTraceCompletion sends trace completion data to the API.
func (c *Client) sendTraceCompletion(traceFunctionKey, traceID, startedAt, endedAt string) {
	defer func() { recover() }() // Never crash the host app

	ts := getTraceState(traceID)
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
		if ts.Metadata != nil {
			rawTrace["metadata"] = ts.Metadata
		}
		if len(ts.Contexts) > 0 {
			rawTrace["contexts"] = ts.Contexts
		}
	}

	payload := map[string]any{
		"type":             "sdk-function",
		"source":           "go-sdk-function",
		"traceFunctionKey": traceFunctionKey,
		"externalTrace":    rawTrace,
		"completed":        true,
	}

	if ts != nil && ts.SessionID != "" {
		payload["sessionId"] = ts.SessionID
	}

	c.httpClient.sendExternalTrace(payload)

	// Clean up trace state
	deleteTraceState(traceID)
}
