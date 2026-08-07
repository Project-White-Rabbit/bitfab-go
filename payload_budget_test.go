package bitfab

import (
	"encoding/json"
	"strings"
	"testing"
)

// exporterRequestCap mirrors otelMaxRequestBytes: past this, the carrier is
// dropped rather than trimmed.
const exporterRequestCap = 3_000_000

// actualCarrierBytes is what the exporter actually weighs, computed
// independently of the code under test: the body re-escaped into the OTLP
// attribute. Asserting with the SDK's own fitsCarrierBudget would be
// tautological, and passed happily against the body-length rule that shipped
// the bug this guards.
func actualCarrierBytes(t *testing.T, body []byte) int {
	t.Helper()
	encoded, err := json.Marshal(string(body))
	if err != nil {
		t.Fatalf("re-encoding the body failed: %v", err)
	}
	return len(encoded)
}

// The budget exists because the exporter drops a carrier that cannot fit a
// request on its own, so a span the SDK accepts must always encode to a
// deliverable size.

func blob(size int) string {
	return strings.Repeat("x", size)
}

func budgetSpanPayload(spanData map[string]any) map[string]any {
	data := map[string]any{"name": "redact", "type": "function"}
	for k, v := range spanData {
		data[k] = v
	}
	return map[string]any{
		"id":               "span-1",
		"traceId":          "trace-1",
		"sourceTraceId":    "trace-1",
		"traceFunctionKey": "redaction-pipeline",
		"rawSpan": map[string]any{
			"id":         "span-1",
			"trace_id":   "trace-1",
			"started_at": "2026-08-06T00:00:00.000Z",
			"span_data":  data,
		},
	}
}

func spanDataOf(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	rawSpan, ok := decoded["rawSpan"].(map[string]any)
	if !ok {
		t.Fatalf("body has no rawSpan")
	}
	spanData, ok := rawSpan["span_data"].(map[string]any)
	if !ok {
		t.Fatalf("body has no span_data")
	}
	return spanData
}

func TestPayloadBudgetLeavesPayloadWithinBudgetUntouched(t *testing.T) {
	payload := budgetSpanPayload(map[string]any{
		"input":  map[string]any{"doc": blob(1_000_000)},
		"output": "ok",
	})
	_, body, dropped := marshalSpanBody(payload)

	if len(dropped) != 0 {
		t.Fatalf("expected no drops, got %v", dropped)
	}
	if got := spanDataOf(t, body)["output"]; got != "ok" {
		t.Fatalf("output = %v, want ok", got)
	}
}

func TestPayloadBudgetKeepsSingleLargeValueThatFits(t *testing.T) {
	// One ~2.1MB document input, small output. Every byte fits the span budget,
	// so nothing should be stubbed. The old 512KB per-value cap stubbed this.
	document := map[string]any{"pages": blob(2_100_000)}
	_, body, dropped := marshalSpanBody(budgetSpanPayload(map[string]any{
		"input":  document,
		"output": map[string]any{"redacted": true},
	}))

	if len(dropped) != 0 {
		t.Fatalf("expected no drops, got %v", dropped)
	}
	input, ok := spanDataOf(t, body)["input"].(map[string]any)
	if !ok {
		t.Fatalf("input was replaced, want it intact")
	}
	if pages, _ := input["pages"].(string); len(pages) != 2_100_000 {
		t.Fatalf("input pages length = %d, want 2100000", len(pages))
	}
}

func TestPayloadBudgetTrimsLargestFieldWhenOverBudget(t *testing.T) {
	_, body, dropped := marshalSpanBody(budgetSpanPayload(map[string]any{
		"input":  map[string]any{"doc": blob(2_000_000)},
		"output": map[string]any{"doc": blob(2_000_000)},
	}))

	if got := actualCarrierBytes(t, body); got > exporterRequestCap {
		t.Fatalf("carrier is %d bytes, want <= %d", got, exporterRequestCap)
	}
	// A trim is a size decision, not an encoding failure: it must not be
	// reported as a non-serializable value (that drives a separate, wrong
	// warning). The payload_budget errors entry is the durable signal.
	if len(dropped) != 0 {
		t.Fatalf("trim reported as dropped: %v", dropped)
	}
	spanData := spanDataOf(t, body)
	stubbed := 0
	for _, key := range []string{"input", "output"} {
		if s, ok := spanData[key].(string); ok && strings.Contains(s, "too_large_") {
			stubbed++
		}
	}
	// One trim is enough to fit; the other field survives intact.
	if stubbed != 1 {
		t.Fatalf("stubbed %d fields, want 1", stubbed)
	}
}

func TestPayloadBudgetNeverTrimsSpanIdentity(t *testing.T) {
	_, body, _ := marshalSpanBody(budgetSpanPayload(map[string]any{
		"input":  map[string]any{"doc": blob(2_000_000)},
		"output": map[string]any{"doc": blob(2_000_000)},
	}))
	spanData := spanDataOf(t, body)

	if spanData["name"] != "redact" {
		t.Fatalf("name = %v, want redact", spanData["name"])
	}
	if spanData["type"] != "function" {
		t.Fatalf("type = %v, want function", spanData["type"])
	}
}

func TestPayloadBudgetRecordsTrimInErrors(t *testing.T) {
	_, body, _ := marshalSpanBody(budgetSpanPayload(map[string]any{
		"input":  map[string]any{"doc": blob(2_000_000)},
		"output": map[string]any{"doc": blob(2_000_000)},
	}))

	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	errors, _ := decoded["errors"].([]any)
	for _, entry := range errors {
		if m, ok := entry.(map[string]any); ok && m["step"] == payloadBudgetStep {
			return
		}
	}
	t.Fatalf("no %s error recorded, got %v", payloadBudgetStep, errors)
}

func TestPayloadBudgetStubsEveryOversizedFieldWhenOneTrimIsNotEnough(t *testing.T) {
	_, body, _ := marshalSpanBody(budgetSpanPayload(map[string]any{
		"input":    map[string]any{"doc": blob(2_300_000)},
		"output":   map[string]any{"doc": blob(2_300_000)},
		"contexts": []any{map[string]any{"key": "k", "value": blob(2_300_000)}},
	}))

	if got := actualCarrierBytes(t, body); got > exporterRequestCap {
		t.Fatalf("carrier is %d bytes, want <= %d", got, exporterRequestCap)
	}
}

func TestPayloadBudgetDoesNotMutateCallersPayload(t *testing.T) {
	payload := budgetSpanPayload(map[string]any{
		"input":  map[string]any{"doc": blob(2_000_000)},
		"output": map[string]any{"doc": blob(2_000_000)},
	})
	marshalSpanBody(payload)

	spanData := payload["rawSpan"].(map[string]any)["span_data"].(map[string]any)
	for _, key := range []string{"input", "output"} {
		field, ok := spanData[key].(map[string]any)
		if !ok {
			t.Fatalf("caller's %s was mutated to %T", key, spanData[key])
		}
		if doc, _ := field["doc"].(string); len(doc) != 2_000_000 {
			t.Fatalf("caller's %s doc length = %d, want 2000000", key, len(doc))
		}
	}
}

func TestPayloadBudgetTrimsPayloadWithNoSpanData(t *testing.T) {
	_, body, _ := marshalSpanBody(map[string]any{
		"id":        "trace-1",
		"completed": true,
		"metadata":  map[string]any{"doc": blob(3_000_000)},
	})

	if got := actualCarrierBytes(t, body); got > exporterRequestCap {
		t.Fatalf("carrier is %d bytes, want <= %d", got, exporterRequestCap)
	}
}

// Every test above builds its payload from strings.Repeat("x", n), the cheapest
// possible content to escape. This covers the content that is not: a body sized
// to the budget can still double when re-escaped into the OTLP attribute, and
// bounding the body rather than the carrier let exactly that reach the exporter
// and be dropped.
func TestPayloadBudgetKeepsCarrierWithinBudgetForEscapeHeavyContent(t *testing.T) {
	for _, tc := range []struct{ name, unit string }{
		{"prose", "The quick brown fox. "},
		{"quotes", `a "b" c `},
		{"backslashes", `\`},
		{"mixed", `C:\path\to "file" `},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Swept rather than sized to one value on purpose. A single
			// oversized payload proves nothing: it gets trimmed under any
			// threshold. The failure only shows where a payload sits just under
			// the limit and is shipped, so walk the whole danger zone.
			for chars := 1_000_000; chars <= 3_000_000; chars += 250_000 {
				doc := strings.Repeat(tc.unit, chars/len(tc.unit)+1)[:chars]
				_, body, _ := marshalSpanBody(budgetSpanPayload(map[string]any{
					"input":  map[string]any{"doc": doc},
					"output": "ok",
				}))
				if got := actualCarrierBytes(t, body); got > exporterRequestCap {
					t.Fatalf("chars=%d carrier is %d bytes, want <= %d", chars, got, exporterRequestCap)
				}
				if got, want := carrierByteLength(body), actualCarrierBytes(t, body); got != want {
					t.Fatalf("chars=%d carrierByteLength=%d, want %d", chars, got, want)
				}
			}
		})
	}
}

// The sweep above always leaves a tiny remainder after the first trim, so it
// never exercises the loop's own stopping condition. This does: two large
// escape-heavy fields, where stubbing one leaves a remainder that passes a raw
// byte check but whose carrier is still over budget.
func TestPayloadBudgetTrimLoopStopsOnCarrierSize(t *testing.T) {
	heavy := strings.Repeat(`\`, 1_300_000)
	_, body, _ := marshalSpanBody(budgetSpanPayload(map[string]any{
		"input":  map[string]any{"doc": heavy},
		"output": map[string]any{"doc": heavy},
	}))
	if got := actualCarrierBytes(t, body); got > exporterRequestCap {
		t.Fatalf("carrier is %d bytes, want <= %d", got, exporterRequestCap)
	}
}
