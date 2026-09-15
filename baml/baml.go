// Package baml adapts the optional native BAML runtime to Bitfab local execution and collector wrappers.
package baml

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sync"

	bitfab "github.com/Project-White-Rabbit/bitfab-go"
	"github.com/boundaryml/baml/engine/language_client_go/baml_go/serde"
	native "github.com/boundaryml/baml/engine/language_client_go/pkg"
)

// Execute runs a prepared BAML function using the native runtime.
func Execute(ctx context.Context, request bitfab.BAMLRequest) (bitfab.BAMLResult, error) {
	runtime, err := native.CreateRuntime("/tmp/bitfab_baml_runtime", map[string]string{"source.baml": request.Source}, request.EnvVars)
	if err != nil {
		return bitfab.BAMLResult{}, err
	}
	collector, err := runtime.NewCollector("bitfab-collector")
	if err != nil {
		return bitfab.BAMLResult{}, err
	}
	args := native.BamlFunctionArguments{Kwargs: request.Inputs, Env: request.EnvVars, Collectors: []native.Collector{collector}}
	encoded, err := args.Encode()
	if err != nil {
		return bitfab.BAMLResult{}, err
	}
	result, err := runtime.CallFunction(ctx, request.FunctionName, encoded, nil)
	if err != nil {
		return bitfab.BAMLResult{}, err
	}
	if result.Error != nil {
		return bitfab.BAMLResult{}, result.Error
	}
	if !result.HasData {
		return bitfab.BAMLResult{}, fmt.Errorf("BAML returned no parsed result")
	}
	return bitfab.BAMLResult{Result: normalizeDynamic(result.Data), RawCollector: collectorMap(collector)}, nil
}

// CollectorFactory is implemented by a generated client's NewCollector function.
type CollectorFactory func(string) (native.Collector, error)

// Wrapper preserves typed arguments and output while enriching the currently active span.
type Wrapper[Input, Output any] struct {
	factory     CollectorFactory
	fn          func(context.Context, Input, native.Collector) (Output, error)
	onCollector func(native.Collector)
	mu          sync.Mutex
	collector   native.Collector
}

// WrapBAML wraps a typed invocation. Pass its collector through the generated client's WithCollector option.
func WrapBAML[Input, Output any](factory CollectorFactory, fn func(context.Context, Input, native.Collector) (Output, error), onCollector func(native.Collector)) *Wrapper[Input, Output] {
	return &Wrapper[Input, Output]{factory: factory, fn: fn, onCollector: onCollector}
}

// Collector returns the most recently completed invocation's native collector.
func (w *Wrapper[Input, Output]) Collector() native.Collector {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.collector
}

// Call invokes the generated function and attaches rendered prompt and usage metadata.
func (w *Wrapper[Input, Output]) Call(ctx context.Context, input Input) (Output, error) {
	var zero Output
	if w.factory == nil || w.fn == nil {
		return zero, fmt.Errorf("BAML wrapper requires a collector factory and callable")
	}
	collector, err := w.factory("bitfab-baml-tracing")
	if err != nil {
		return zero, err
	}
	result, err := w.fn(ctx, input, collector)
	if err != nil {
		return result, err
	}
	w.mu.Lock()
	w.collector = collector
	w.mu.Unlock()
	enrich(ctx, collectorMap(collector))
	if w.onCollector != nil {
		func() { defer func() { recover() }(); w.onCollector(collector) }()
	}
	return result, nil
}

var collectorFields = map[string]string{
	"Logs": "logs", "Last": "last", "Usage": "usage", "FunctionName": "function_name", "LogType": "log_type",
	"Timing": "timing", "Calls": "calls", "SelectedCall": "selected_call", "Tags": "tags", "Selected": "selected",
	"HttpRequest": "http_request", "HttpResponse": "http_response", "Provider": "provider", "ClientName": "client_name",
	"InputTokens": "input_tokens", "OutputTokens": "output_tokens", "CachedInputTokens": "cached_input_tokens",
	"StartTimeUtcMs": "start_time_utc_ms", "DurationMs": "duration_ms", "Url": "url", "Method": "method", "Headers": "headers", "Body": "body", "Status": "status",
}

func collectorMap(collector native.Collector) map[string]any {
	value, _ := serialize(reflect.ValueOf(collector), 0).(map[string]any)
	return value
}

func serialize(value reflect.Value, depth int) (result any) {
	defer func() {
		if recover() != nil {
			result = nil
		}
	}()
	if depth > 8 || !value.IsValid() {
		return nil
	}
	if (value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer) && value.IsNil() {
		return nil
	}
	if method := value.MethodByName("JSON"); method.IsValid() {
		result := method.Call(nil)
		if len(result) == 2 && result[1].IsNil() {
			return serialize(result[0], depth+1)
		}
	}
	output := map[string]any{}
	for method, key := range collectorFields {
		fn := value.MethodByName(method)
		if !fn.IsValid() || fn.Type().NumIn() != 0 || fn.Type().NumOut() != 2 {
			continue
		}
		values := fn.Call(nil)
		if values[1].IsNil() {
			output[key] = serialize(values[0], depth+1)
		}
	}
	if len(output) > 0 {
		return output
	}
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		return serialize(value.Elem(), depth+1)
	case reflect.Slice, reflect.Array:
		items := make([]any, value.Len())
		for i := range items {
			items[i] = serialize(value.Index(i), depth+1)
		}
		return items
	case reflect.Map:
		mapped := map[string]any{}
		iter := value.MapRange()
		for iter.Next() {
			mapped[fmt.Sprint(iter.Key().Interface())] = serialize(iter.Value(), depth+1)
		}
		return mapped
	case reflect.String:
		return value.String()
	case reflect.Bool:
		return value.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.Uint()
	case reflect.Float32, reflect.Float64:
		return value.Float()
	}
	return nil
}

var modelURL = regexp.MustCompile(`/models/([^/:]+)`)

func enrich(ctx context.Context, collector map[string]any) {
	defer func() { recover() }()
	log, _ := collector["last"].(map[string]any)
	calls, _ := log["calls"].([]any)
	var selected map[string]any
	for _, item := range calls {
		call, _ := item.(map[string]any)
		if selected == nil {
			selected = call
		}
		if call["selected"] == true {
			selected = call
			break
		}
	}
	request, _ := selected["http_request"].(map[string]any)
	body, _ := request["body"].(map[string]any)
	if messages, ok := body["messages"].([]any); ok {
		rendered := []map[string]any{}
		for _, item := range messages {
			message, ok := item.(map[string]any)
			if !ok {
				continue
			}
			role, ok := message["role"].(string)
			if !ok {
				continue
			}
			content, ok := message["content"].(string)
			if !ok {
				encoded, _ := json.Marshal(message["content"])
				content = string(encoded)
			}
			rendered = append(rendered, map[string]any{"role": role, "content": content})
		}
		if len(rendered) > 0 {
			encoded, _ := json.Marshal(rendered)
			bitfab.GetCurrentSpan(ctx).SetPrompt(string(encoded))
		}
	}
	metadata := map[string]any{}
	if provider, ok := selected["provider"].(string); ok {
		metadata["provider"] = provider
	}
	if model, ok := body["model"].(string); ok {
		metadata["model"] = model
	} else if url, ok := request["url"].(string); ok {
		if match := modelURL.FindStringSubmatch(url); match != nil {
			metadata["model"] = match[1]
		}
	}
	usage, _ := collector["usage"].(map[string]any)
	fallback, _ := selected["usage"].(map[string]any)
	for key, target := range map[string]string{"input_tokens": "inputTokens", "output_tokens": "outputTokens"} {
		value := usage[key]
		if value == nil {
			value = fallback[key]
		}
		if value != nil {
			metadata[target] = value
		}
	}
	timing, _ := log["timing"].(map[string]any)
	if value := timing["duration_ms"]; value != nil {
		metadata["durationMs"] = value
	}
	if len(metadata) > 0 {
		bitfab.GetCurrentSpan(ctx).AddContext(metadata)
	}
}

func normalizeDynamic(value any) any {
	switch v := value.(type) {
	case serde.DynamicClass:
		return normalizeDynamic(v.Fields)
	case *serde.DynamicClass:
		return normalizeDynamic(v.Fields)
	case serde.DynamicEnum:
		return v.Value
	case *serde.DynamicEnum:
		return v.Value
	case serde.DynamicUnion:
		return normalizeDynamic(v.Value)
	case *serde.DynamicUnion:
		return normalizeDynamic(v.Value)
	}
	raw := reflect.ValueOf(value)
	if !raw.IsValid() {
		return nil
	}
	switch raw.Kind() {
	case reflect.Map:
		result := map[string]any{}
		iter := raw.MapRange()
		for iter.Next() {
			result[fmt.Sprint(iter.Key().Interface())] = normalizeDynamic(iter.Value().Interface())
		}
		return result
	case reflect.Slice, reflect.Array:
		result := make([]any, raw.Len())
		for i := range result {
			result[i] = normalizeDynamic(raw.Index(i).Interface())
		}
		return result
	}
	return value
}
