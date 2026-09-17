package bitfab

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestBAMLPrepareCanonicalInputsAndCredentials(t *testing.T) {
	source := `function Run(text: string, count: int, ok: bool, rows: map<string,int>, values: int[]) -> string {}`
	inputs := map[string]any{"text": "123", "count": "42", "ok": "FALSE", "rows": `{"a":1}`, "values": "[1,2]", "unknown": "42"}
	request, err := PrepareBAML(source, inputs, nil, map[string]string{"OPENAI_API_KEY": "llm-key", "DATABASE_URL": "private"})
	if err != nil {
		t.Fatal(err)
	}
	if request.Inputs["text"] != "123" || request.Inputs["count"] != int64(42) || request.Inputs["ok"] != false || request.Inputs["unknown"] != "42" {
		t.Fatalf("wrong coercion: %#v", request.Inputs)
	}
	if !reflect.DeepEqual(request.EnvVars, map[string]string{"OPENAI_API_KEY": "llm-key"}) {
		t.Fatalf("unfiltered env: %#v", request.EnvVars)
	}
	if inputs["count"] != "42" {
		t.Fatal("mutated caller inputs")
	}
}

func TestBAMLCallExecutesAndSubmitsThroughTransport(t *testing.T) {
	sink := &carrierSink{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/sdk/functions/lookup" {
			json.NewEncoder(w).Encode(map[string]any{"id": "fn-id", "prompt": "function Run(count: int) -> bool {}", "providers": []any{}})
			return
		}
		if r.URL.Path == otelTracesEndpoint {
			body, _ := io.ReadAll(r.Body)
			sink.add(decodeOtlpCarriers(t, body))
			w.Write([]byte(`{}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client := NewClient("test", WithServiceURL(server.URL), WithSimulationPlan(false), WithBAMLExecutor(func(ctx context.Context, request BAMLRequest) (BAMLResult, error) {
		if request.FunctionName != "Run" || request.Inputs["count"] != int64(2) {
			t.Fatalf("bad request: %#v", request)
		}
		return BAMLResult{Result: false, RawCollector: map[string]any{"usage": map[string]any{"input_tokens": 3}}}, nil
	}))
	defer client.Close(time.Second)
	result, err := client.Call(context.Background(), "run", map[string]any{"count": "2"})
	if err != nil {
		t.Fatal(err)
	}
	if result != false {
		t.Fatalf("changed return: %#v", result)
	}
	if !client.FlushTraces(time.Second) {
		t.Fatal("flush failed")
	}
	received := sink.byOperation(operationInternalTrace)
	if len(received) != 1 || received[0]["functionId"] != "fn-id" || received[0]["result"] != "false" {
		t.Fatalf("wrong carrier: %#v", received)
	}
}

func TestCurrentSpanEnrichmentMatchesActiveSpan(t *testing.T) {
	ctx, state := withSpanEnrichment(withSpanContext(context.Background(), "trace", "span"), "span")
	handle := GetCurrentSpan(ctx)
	handle.SetPrompt("prompt")
	handle.AddContext(map[string]any{"tokens": 2})
	nested := withSpanContext(ctx, "trace", "nested")
	GetCurrentSpan(nested).SetPrompt("wrong")
	result := map[string]any{}
	state.apply(result, false)
	if result["prompt"] != "prompt" || len(result["contexts"].([]ContextEntry)) != 1 {
		t.Fatalf("wrong enrichment: %#v", result)
	}
	withheld := map[string]any{}
	state.apply(withheld, true)
	if _, ok := withheld["prompt"]; ok || len(withheld["contexts"].([]ContextEntry)) != 1 {
		t.Fatalf("content-off enrichment kept prompt or lost contexts: %#v", withheld)
	}
}
