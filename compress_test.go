package bitfab

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func largeBody(t *testing.T) []byte {
	t.Helper()
	spans := make([]map[string]string, 800)
	for i := range spans {
		spans[i] = map[string]string{"name": "span"}
	}
	body, err := json.Marshal(map[string]any{"spans": spans})
	if err != nil {
		t.Fatalf("failed to build fixture: %v", err)
	}
	return body
}

func gunzip(t *testing.T, body []byte) string {
	t.Helper()
	reader, err := gzip.NewReader(strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("body was not gzip framed: %v", err)
	}
	defer reader.Close()
	inflated, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to inflate: %v", err)
	}
	return string(inflated)
}

func TestEncodeRequestBodyLeavesSmallBodyUncompressed(t *testing.T) {
	body, encoding := encodeRequestBody([]byte(`{"a":1}`))

	if string(body) != `{"a":1}` {
		t.Errorf("body = %q, want the original", body)
	}
	if encoding != "" {
		t.Errorf("encoding = %q, want empty", encoding)
	}
}

func TestEncodeRequestBodyGzipsPastThreshold(t *testing.T) {
	original := largeBody(t)

	body, encoding := encodeRequestBody(original)

	if encoding != "gzip" {
		t.Fatalf("encoding = %q, want gzip", encoding)
	}
	if gunzip(t, body) != string(original) {
		t.Error("round trip did not reproduce the original body")
	}
}

func TestEncodeRequestBodyShrinksWhatItCompresses(t *testing.T) {
	original := largeBody(t)

	body, _ := encodeRequestBody(original)

	if len(body) >= len(original) {
		t.Errorf("compressed length %d, want less than %d", len(body), len(original))
	}
}

func TestEncodeRequestBodySkipsCompressionWhenDisabled(t *testing.T) {
	t.Setenv(disableCompressionEnv, "1")
	original := largeBody(t)

	body, encoding := encodeRequestBody(original)

	if string(body) != string(original) {
		t.Error("body was modified while compression was disabled")
	}
	if encoding != "" {
		t.Errorf("encoding = %q, want empty", encoding)
	}
}

func TestHTTPClientGzipsLargeBodyAndDeclaresEncoding(t *testing.T) {
	spans := make([]string, 2_000)
	for i := range spans {
		spans[i] = "span"
	}
	payload := map[string]any{"spans": spans}

	var received map[string]any
	var encoding string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encoding = r.Header.Get("Content-Encoding")
		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("request body was not gzip framed: %v", err)
		} else {
			defer reader.Close()
			if err := json.NewDecoder(reader).Decode(&received); err != nil {
				t.Errorf("failed to decode inflated body: %v", err)
			}
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	hc := newHTTPClient("test-key", server.URL)
	if _, err := hc.request(context.Background(), "/api/test", payload, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if encoding != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip", encoding)
	}
	inflated, ok := received["spans"].([]any)
	if !ok {
		t.Fatalf("server received %#v, want a spans list", received)
	}
	if len(inflated) != len(spans) {
		t.Errorf("server received %d spans, want %d", len(inflated), len(spans))
	}
}

func TestHTTPClientSendsSmallBodyUncompressed(t *testing.T) {
	var encoding string
	var received map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encoding = r.Header.Get("Content-Encoding")
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("failed to decode body: %v", err)
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	hc := newHTTPClient("test-key", server.URL)
	if _, err := hc.request(context.Background(), "/api/test", map[string]any{"data": "test"}, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if encoding != "" {
		t.Errorf("Content-Encoding = %q, want empty", encoding)
	}
	if received["data"] != "test" {
		t.Errorf("received = %#v, want data test", received)
	}
}
