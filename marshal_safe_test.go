package bitfab

import (
	"encoding/json"
	"strings"
	"testing"
)

// A span/trace payload must always reach the wire, even if a stray value that
// json.Marshal can't encode (a channel, func, etc.) slipped into it. Otherwise
// Marshal fails, the request is dropped, and the trace is silently left
// incomplete or non-replayable.

func TestMarshalPayloadSafe_CleanPayload(t *testing.T) {
	body, dropped := marshalPayloadSafe(map[string]any{"a": 1, "b": []any{"x", "y"}})
	if len(dropped) != 0 {
		t.Fatalf("expected no drops, got %v", dropped)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
}

func TestMarshalPayloadSafe_StubsStrayValueAndPreservesSiblings(t *testing.T) {
	// A channel cannot be JSON-encoded. The span must still send, with the
	// channel stubbed and the sibling field preserved.
	payload := map[string]any{
		"keep": "yes",
		"rawSpan": map[string]any{
			"span_data": map[string]any{
				"input": map[string]any{"bad": make(chan int)},
			},
		},
	}

	body, dropped := marshalPayloadSafe(payload)
	if len(dropped) == 0 {
		t.Fatal("expected the channel to be reported as dropped")
	}

	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if out["keep"] != "yes" {
		t.Fatalf("sibling field not preserved: %v", out["keep"])
	}
	input := out["rawSpan"].(map[string]any)["span_data"].(map[string]any)["input"].(map[string]any)
	bad, ok := input["bad"].(string)
	if !ok || !strings.Contains(bad, "unserializable") {
		t.Fatalf("stray value not stubbed: %#v", input["bad"])
	}
}

func TestMarshalPayloadSafe_NeverPanicsOnCycle(t *testing.T) {
	// A self-referential map would make json.Marshal error; the depth guard
	// must keep marshalPayloadSafe from spinning or panicking.
	m := map[string]any{"name": "root"}
	m["self"] = m

	body, dropped := marshalPayloadSafe(map[string]any{"input": m})
	if len(dropped) == 0 {
		t.Fatal("expected the cyclic value to be reported as dropped")
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
}

func TestMarshalPayloadSafe_TypedSlicePreservesSiblings(t *testing.T) {
	// A typed slice ([]map[string]any, the shape of span contexts) with one bad
	// element value must stub only that value, not collapse the whole slice.
	payload := map[string]any{
		"contexts": []map[string]any{
			{"good": "keep", "bad": make(chan int)},
		},
	}

	body, dropped := marshalPayloadSafe(payload)
	if len(dropped) == 0 {
		t.Fatal("expected the channel to be reported as dropped")
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	ctx := out["contexts"].([]any)[0].(map[string]any)
	if ctx["good"] != "keep" {
		t.Fatalf("sibling key not preserved: %#v", ctx["good"])
	}
	if s, ok := ctx["bad"].(string); !ok || !strings.Contains(s, "unserializable") {
		t.Fatalf("stray value not stubbed: %#v", ctx["bad"])
	}
}

func TestMarshalPayloadSafe_StructPreservesSiblings(t *testing.T) {
	// A struct with one non-encodable field must stub only that field and keep
	// its serializable siblings, honoring the field's json tag.
	type input struct {
		Name string   `json:"name"`
		Bad  chan int `json:"bad"`
	}
	payload := map[string]any{"input": input{Name: "keep", Bad: make(chan int)}}

	body, dropped := marshalPayloadSafe(payload)
	if len(dropped) == 0 {
		t.Fatal("expected the channel field to be reported as dropped")
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	in := out["input"].(map[string]any)
	if in["name"] != "keep" {
		t.Fatalf("sibling field not preserved (or json tag ignored): %#v", in)
	}
	if s, ok := in["bad"].(string); !ok || !strings.Contains(s, "unserializable") {
		t.Fatalf("stray field not stubbed: %#v", in["bad"])
	}
}
