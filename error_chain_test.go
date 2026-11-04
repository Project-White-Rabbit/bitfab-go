package bitfab

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

// queryError mimics a driver error type that reports only its own message and
// keeps the connection failure as its cause.
type queryError struct {
	query string
	err   error
}

func (e *queryError) Error() string { return "failed query: " + e.query }
func (e *queryError) Unwrap() error { return e.err }

type pgTimeout struct{}

func (pgTimeout) Error() string    { return "Connection terminated due to connection timeout" }
func (pgTimeout) SQLState() string { return "08006" }

func TestDescribeErrorAppendsTheDeepestCause(t *testing.T) {
	err := &queryError{
		query: `select "assertions"`,
		err:   &queryError{query: "inner", err: pgTimeout{}},
	}
	want := `failed query: select "assertions" (caused by: Connection terminated due to connection timeout)`
	if got := describeError(err); got != want {
		t.Fatalf("describeError = %q, want %q", got, want)
	}
}

func TestDescribeErrorLeavesAWrappedMessageAsItIs(t *testing.T) {
	err := fmt.Errorf("request failed: %w", errors.New("socket hang up"))
	if got := describeError(err); got != "request failed: socket hang up" {
		t.Fatalf("describeError = %q", got)
	}
	if got := describeError(errors.New("boom")); got != "boom" {
		t.Fatalf("describeError = %q", got)
	}
}

func TestReplayErrorJSONCarriesTheDriverCodeAndStopsAtTheDepthLimit(t *testing.T) {
	raw, err := json.Marshal(replayErrorJSON(&queryError{query: "select 1", err: pgTimeout{}}))
	if err != nil {
		t.Fatal(err)
	}
	var serialized map[string]any
	if err := json.Unmarshal(raw, &serialized); err != nil {
		t.Fatal(err)
	}
	cause, _ := serialized["cause"].(map[string]any)
	if cause["code"] != "08006" || cause["message"] != "Connection terminated due to connection timeout" {
		t.Fatalf("serialized = %s", raw)
	}

	var deep error = errors.New("level 0")
	for level := 1; level < maxErrorCauseDepth+5; level++ {
		deep = &queryError{query: fmt.Sprint(level), err: deep}
	}
	depth := 0
	for node, _ := replayErrorJSON(deep).(map[string]any); node != nil; node, _ = node["cause"].(map[string]any) {
		depth++
	}
	if depth != maxErrorCauseDepth {
		t.Fatalf("depth = %d, want %d", depth, maxErrorCauseDepth)
	}
}
