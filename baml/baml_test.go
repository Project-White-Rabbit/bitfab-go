package baml

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	bitfab "github.com/Project-White-Rabbit/bitfab-go"
	native "github.com/boundaryml/baml/engine/language_client_go/pkg"
)

func localBAML(t *testing.T) (string, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request["model"] != "local-test" {
			t.Errorf("wrong model: %#v", request)
		}
		response := "42"
		if encoded, _ := json.Marshal(request); strings.Contains(string(encoded), "answer: int") {
			response = `{"answer":42}`
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"id": "chat-test", "object": "chat.completion", "model": "local-test", "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": response}, "finish_reason": "stop"}}, "usage": map[string]any{"prompt_tokens": 5, "completion_tokens": 4, "total_tokens": 9}})
	}))
	source := fmt.Sprintf(`client<llm> Local {
 provider openai
 options {
  model "local-test"
  base_url %q
  api_key "local-only"
 }
}
function Run(count: int) -> int {
 client Local
 prompt #"Return {{ count }} as an integer."#
}`, server.URL+"/v1")
	return source, server
}

func TestNativeExecute(t *testing.T) {
	source, server := localBAML(t)
	defer server.Close()
	request, err := bitfab.PrepareBAML(source, map[string]any{"count": "42"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(result.Result) != "42" {
		t.Fatalf("wrong parsed result: %#v", result.Result)
	}
	last, _ := result.RawCollector["last"].(map[string]any)
	calls, _ := last["calls"].([]any)
	if len(calls) != 1 {
		t.Fatalf("missing native collector: %#v", result.RawCollector)
	}
}

func TestNativeCollectorWrapper(t *testing.T) {
	source, server := localBAML(t)
	defer server.Close()
	runtime, err := native.CreateRuntime("/tmp/bitfab-test", map[string]string{"source.baml": source}, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	var callback native.Collector
	wrapper := WrapBAML(runtime.NewCollector, func(ctx context.Context, input int, collector native.Collector) (any, error) {
		args := native.BamlFunctionArguments{Kwargs: map[string]any{"count": input}, Collectors: []native.Collector{collector}}
		encoded, err := args.Encode()
		if err != nil {
			return nil, err
		}
		result, err := runtime.CallFunction(ctx, "Run", encoded, nil)
		if err != nil {
			return nil, err
		}
		return result.Data, result.Error
	}, func(c native.Collector) { callback = c })
	result, err := wrapper.Call(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(result) != "42" || wrapper.Collector() == nil || !reflect.DeepEqual(wrapper.Collector(), callback) {
		t.Fatalf("incorrect result/collector: %#v", result)
	}
	data := collectorMap(callback)
	log, _ := data["last"].(map[string]any)
	if len(log["calls"].([]any)) != 1 {
		t.Fatal("missing wrapped invocation")
	}
}

func TestNativeStructuredOutput(t *testing.T) {
	source, server := localBAML(t)
	defer server.Close()
	source = "class Answer {\n answer int\n}\n" + strings.Replace(source, "-> int", "-> Answer", 1)
	source = strings.Replace(source, "as an integer.", "as JSON. {{ ctx.output_format }}", 1)
	request, err := bitfab.PrepareBAML(source, map[string]any{"count": 42}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	object, ok := result.Result.(map[string]any)
	if !ok || fmt.Sprint(object["answer"]) != "42" {
		t.Fatalf("wrong structured result: %#v", result.Result)
	}
}

func TestNativeUnionOutput(t *testing.T) {
	source, server := localBAML(t)
	defer server.Close()
	source = strings.Replace(source, "-> int", "-> int | string", 1)
	request, err := bitfab.PrepareBAML(source, map[string]any{"count": 42}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(result.Result) != "42" {
		t.Fatalf("wrong union result: %#v", result.Result)
	}
}
