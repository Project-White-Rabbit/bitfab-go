package bitfab

import (
	"errors"
	"fmt"
)

// StreamFinalizationError preserves partial stream output alongside its error.
// Ordinary finalizer errors omit the output and receive a "finalize failed" prefix.
type StreamFinalizationError struct {
	Output any
	Err    error
}

func (e *StreamFinalizationError) Error() string {
	if e.Err == nil {
		return "stream finalization failed"
	}
	return e.Err.Error()
}

func (e *StreamFinalizationError) Unwrap() error { return e.Err }

func runSpanFinalizer(output any, finalize SpanFinalizer) (value any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			value = nil
			err = fmt.Errorf("finalize failed: %v", recovered)
		}
	}()
	value, err = finalize(output)
	if err != nil {
		var partial *StreamFinalizationError
		if errors.As(err, &partial) {
			return partial.Output, partial
		}
		return nil, fmt.Errorf("finalize failed: %w", err)
	}
	return value, nil
}

func (c *Client) finalizeSpanOutput(traceID string, finalize SpanFinalizer, output any, err error, mocked bool, send func(any, error)) {
	if finalize == nil || err != nil || mocked {
		send(output, err)
		return
	}
	done := c.beginSpanFinalizer(traceID)
	go func() { defer done(); value, finalErr := runSpanFinalizer(output, finalize); send(value, finalErr) }()
}

func (c *Client) beginSpanFinalizer(traceID string) func() {
	clientDone := c.beginAutoFinalizer()
	state := getTraceState(traceID)
	if state != nil {
		state.mu.Lock()
		state.pendingFinalizers++
		state.mu.Unlock()
	}
	return func() {
		defer clientDone()
		var complete func()
		if state != nil {
			state.mu.Lock()
			state.pendingFinalizers--
			if state.pendingFinalizers == 0 {
				complete = state.completion
				state.completion = nil
			}
			state.mu.Unlock()
		}
		if complete != nil {
			complete()
		}
	}
}
