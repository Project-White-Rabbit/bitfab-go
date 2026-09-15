package bitfab

import (
	"bytes"
	"strings"
	"testing"
)

func TestReplaySummaryDistinguishesSeededExpectedFromCapturedOutputs(t *testing.T) {
	seeded := "seeded"
	failure := "failed"
	result := ReplayResult{Attempts: 2, TestRunURL: "/run", Items: []ReplayItem{
		{IngestionType: &seeded, Result: 6, OriginalOutput: float64(6)},
		{IngestionType: &seeded, Result: 7, OriginalOutput: 6},
		{Result: map[string]any{"a": 1}, OriginalOutput: map[string]any{"a": float64(1)}},
		{Result: 2, OriginalOutput: 1},
		{IngestionType: &seeded, Error: &failure},
	}}
	var output bytes.Buffer
	renderReplaySummary("pipeline", result, &output)
	for _, line := range []string{"Attempts: 2", "Same:     1", "Changed:  1", "Matched expected: 1", "Missed expected:  1", "Errors:   1"} {
		if !strings.Contains(output.String(), line) {
			t.Fatalf("missing %q: %s", line, output.String())
		}
	}
	output.Reset()
	renderReplaySummary("pipeline", ReplayResult{Items: result.Items[:2]}, &output)
	if strings.Contains(output.String(), "Same:") || strings.Contains(output.String(), "Changed:") {
		t.Fatal("expected-only summary uses captured terms")
	}
}
