package bitfab

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// MarshalSpanPayload serializes a span payload to JSON bytes, matching what
// the HTTP client does before sending to the API.
func MarshalSpanPayload(payload map[string]any) ([]byte, error) {
	return json.Marshal(payload)
}

// maxSanitizeDepth guards against cyclic structures (e.g. a map that contains
// itself) when sanitizing a payload that json.Marshal rejected.
const maxSanitizeDepth = 32

// marshalPayloadSafe JSON-encodes a request body without ever failing on a
// stray value that json.Marshal can't encode (a channel, func, complex, a
// non-string-keyed map, or a cyclic structure).
//
// Upstream span construction should already supply JSON-safe data. This is the
// boundary backstop: if anything non-encodable still slips through, only the
// offending leaves are stubbed instead of letting Marshal fail and silently
// drop the whole span/trace. Returns the encoded body and the type names that
// had to be stubbed, so the caller can warn loudly rather than ship a degraded
// payload in silence.
func marshalPayloadSafe(payload map[string]any) ([]byte, []string) {
	if body, err := json.Marshal(payload); err == nil {
		return body, nil
	}

	var dropped []string
	sanitized := sanitizeValue(payload, &dropped, 0)
	body, err := json.Marshal(sanitized)
	if err != nil {
		// Truly pathological. Still never drop silently: send a marker body.
		marker, _ := json.Marshal(map[string]any{"error": "payload_serialize_failed"})
		return marker, dropped
	}
	return body, dropped
}

// sanitizeValue returns a JSON-encodable copy of v, replacing any value that
// json.Marshal can't encode with a stub string and recording its type name in
// dropped. Composite values (maps, slices/arrays, and structs, of any element
// type) are walked via reflection so only the offending leaf is stubbed and
// serializable siblings are preserved.
func sanitizeValue(v any, dropped *[]string, depth int) any {
	if depth > maxSanitizeDepth {
		*dropped = append(*dropped, "max_depth")
		return "<unserializable: max_depth>"
	}
	if _, err := json.Marshal(v); err == nil {
		return v
	}
	if v == nil {
		return nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		return sanitizeValue(rv.Elem().Interface(), dropped, depth+1)
	case reflect.Map:
		// JSON object keys must be strings; fmt.Sprint coerces other key types.
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			key := fmt.Sprint(iter.Key().Interface())
			out[key] = sanitizeValue(iter.Value().Interface(), dropped, depth+1)
		}
		return out
	case reflect.Slice, reflect.Array:
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = sanitizeValue(rv.Index(i).Interface(), dropped, depth+1)
		}
		return out
	case reflect.Struct:
		out := make(map[string]any)
		t := rv.Type()
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" {
				continue // unexported field; encoding/json skips it too
			}
			name, skip := jsonFieldName(field)
			if skip {
				continue
			}
			out[name] = sanitizeValue(rv.Field(i).Interface(), dropped, depth+1)
		}
		return out
	default:
		name := fmt.Sprintf("%T", v)
		*dropped = append(*dropped, name)
		return fmt.Sprintf("<unserializable: %s>", name)
	}
}

// jsonFieldName resolves the JSON key for a struct field the way encoding/json
// would: an explicit `json:"name"` tag wins, `json:"-"` omits the field, and an
// empty tag name falls back to the Go field name.
func jsonFieldName(field reflect.StructField) (name string, skip bool) {
	tag := field.Tag.Get("json")
	if tag == "" {
		return field.Name, false
	}
	tagName, _, _ := strings.Cut(tag, ",")
	if tagName == "-" {
		return "", true
	}
	if tagName == "" {
		return field.Name, false
	}
	return tagName, false
}

// uniqueStrings returns the distinct values of in, preserving first-seen order.
func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// UnmarshalSpanPayload deserializes JSON bytes back into the target type T.
// This proves that serialized span data can be restored to its original Go type.
func UnmarshalSpanPayload[T any](data []byte) (T, error) {
	var result T
	err := json.Unmarshal(data, &result)
	return result, err
}
