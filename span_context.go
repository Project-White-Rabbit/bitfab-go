package bitfab

import (
	"context"
	"sync"
	"time"
)

// spanStackKey is the context key for the span stack.
// Using a private struct type ensures no collisions with other packages.
type spanStackKey struct{}

// spanEntry represents a single entry in the span stack.
type spanEntry struct {
	traceID string
	spanID  string
}

// currentSpan returns the top of the span stack from the context, or nil if empty.
func currentSpan(ctx context.Context) *spanEntry {
	stack, _ := ctx.Value(spanStackKey{}).([]spanEntry)
	if len(stack) == 0 {
		return nil
	}
	top := stack[len(stack)-1]
	return &top
}

// withSpanContext pushes a new span entry onto the context's span stack.
func withSpanContext(ctx context.Context, traceID, spanID string) context.Context {
	stack, _ := ctx.Value(spanStackKey{}).([]spanEntry)
	newStack := make([]spanEntry, len(stack)+1)
	copy(newStack, stack)
	newStack[len(stack)] = spanEntry{traceID: traceID, spanID: spanID}
	return context.WithValue(ctx, spanStackKey{}, newStack)
}

// ContextEntry represents a single context entry containing multiple key-value pairs.
type ContextEntry = map[string]any

// TraceState holds trace-level state that is sent when the trace completes.
type TraceState struct {
	TraceID   string
	SessionID string
	Metadata  map[string]any
	Contexts  []ContextEntry
	StartedAt string
	Dropped   bool
	mu        sync.Mutex
}

// traceStateStore is the global store for active trace states.
var traceStateStore = struct {
	sync.RWMutex
	states map[string]*TraceState
}{
	states: make(map[string]*TraceState),
}

// getTraceState retrieves the trace state for a given trace ID.
func getTraceState(traceID string) *TraceState {
	traceStateStore.RLock()
	defer traceStateStore.RUnlock()
	return traceStateStore.states[traceID]
}

// isDropped reports whether Drop() has flagged this trace. Reading Dropped is
// synchronized with Drop()'s write via ts.mu, so a span finalizing on one
// goroutine can safely check the flag while another goroutine calls Drop().
func (ts *TraceState) isDropped() bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.Dropped
}

// createTraceState creates or retrieves the trace state for a given trace ID.
func createTraceState(traceID string) *TraceState {
	traceStateStore.Lock()
	defer traceStateStore.Unlock()
	if ts, ok := traceStateStore.states[traceID]; ok {
		return ts
	}
	ts := &TraceState{
		TraceID:   traceID,
		StartedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}
	traceStateStore.states[traceID] = ts
	return ts
}

// deleteTraceState removes the trace state for a given trace ID.
func deleteTraceState(traceID string) {
	traceStateStore.Lock()
	defer traceStateStore.Unlock()
	delete(traceStateStore.states, traceID)
}

// clearAllTraceStates clears all trace states (for testing).
func clearAllTraceStates() {
	traceStateStore.Lock()
	defer traceStateStore.Unlock()
	traceStateStore.states = make(map[string]*TraceState)
}

// CurrentSpan identifies the current active span.
type CurrentSpan struct {
	id      string
	traceID string
}

// ID returns the canonical Bitfab span ID, or an empty string outside a span.
// Safe to call on a nil receiver.
func (cs *CurrentSpan) ID() string {
	if cs == nil {
		return ""
	}
	return cs.id
}

// TraceID returns the canonical Bitfab trace ID, or an empty string outside a span.
// Safe to call on a nil receiver.
func (cs *CurrentSpan) TraceID() string {
	if cs == nil {
		return ""
	}
	return cs.traceID
}

// GetCurrentSpan returns the current active span from the context.
// Returns nil if not inside a span context.
func GetCurrentSpan(ctx context.Context) *CurrentSpan {
	entry := currentSpan(ctx)
	if entry == nil {
		return nil
	}
	return &CurrentSpan{id: entry.spanID, traceID: entry.traceID}
}

// CurrentTrace provides a handle to the current active trace for setting trace-level context.
type CurrentTrace struct {
	traceID string
}

// TraceID returns the canonical Bitfab trace ID, or an empty string outside a span.
// Safe to call on a nil receiver.
func (ct *CurrentTrace) TraceID() string {
	if ct == nil {
		return ""
	}
	return ct.traceID
}

// SetSessionID sets the session ID for this trace.
// Session ID is used to group traces from the same user session.
// This is stored as a database column.
// Safe to call on nil receiver (no-op).
func (ct *CurrentTrace) SetSessionID(sessionID string) {
	defer func() { recover() }() // Never crash the host app
	if ct == nil || ct.traceID == "" {
		return
	}
	ts := getTraceState(ct.traceID)
	if ts == nil {
		ts = createTraceState(ct.traceID)
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.SessionID = sessionID
}

// SetMetadata sets metadata for this trace.
// Metadata is stored in the raw trace data. Subsequent calls merge with
// existing metadata, with later values taking precedence.
// Safe to call on nil receiver (no-op).
func (ct *CurrentTrace) SetMetadata(metadata map[string]any) {
	defer func() { recover() }() // Never crash the host app
	if ct == nil || ct.traceID == "" || metadata == nil {
		return
	}
	ts := getTraceState(ct.traceID)
	if ts == nil {
		ts = createTraceState(ct.traceID)
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.Metadata == nil {
		ts.Metadata = make(map[string]any)
	}
	for k, v := range metadata {
		ts.Metadata[k] = v
	}
}

// AddContext adds a context entry to this trace.
// The entire map is pushed as a single entry in the contexts array.
// Context entries are accumulated - multiple calls add to the list.
// Safe to call on nil receiver (no-op).
func (ct *CurrentTrace) AddContext(context map[string]any) {
	defer func() { recover() }() // Never crash the host app
	if ct == nil || ct.traceID == "" || context == nil {
		return
	}
	ts := getTraceState(ct.traceID)
	if ts == nil {
		ts = createTraceState(ct.traceID)
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.Contexts = append(ts.Contexts, context)
}

// Drop flags the current in-flight trace to be dropped.
// When the trace completes, the completion payload carries a top-level
// dropped: true, and the server scrubs and marks the trace dropped.
// Safe to call on nil receiver (no-op).
func (ct *CurrentTrace) Drop() {
	defer func() { recover() }() // Never crash the host app
	if ct == nil || ct.traceID == "" {
		return
	}
	ts := getTraceState(ct.traceID)
	if ts == nil {
		ts = createTraceState(ct.traceID)
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.Dropped = true
}

// GetCurrentTrace returns a handle to the current active trace from the context.
// Returns nil if not inside a span context.
func GetCurrentTrace(ctx context.Context) *CurrentTrace {
	entry := currentSpan(ctx)
	if entry == nil {
		return nil
	}
	return &CurrentTrace{traceID: entry.traceID}
}
