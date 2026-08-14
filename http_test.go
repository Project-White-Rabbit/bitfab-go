package bitfab

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPClient_Request_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", r.Header.Get("Content-Type"))
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("Authorization = %q, want Bearer test-key", r.Header.Get("Authorization"))
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"success": true})
	}))
	defer server.Close()

	hc := newHTTPClient("test-key", server.URL)
	result, err := hc.request(context.Background(), "/api/test", map[string]any{"data": "test"}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["success"] != true {
		t.Errorf("result = %#v, want success true", result)
	}
}

func TestHTTPClient_Request_ErrorResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{"error": "Bad request"})
	}))
	defer server.Close()

	hc := newHTTPClient("test-key", server.URL)
	_, err := hc.request(context.Background(), "/api/test", map[string]any{}, 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "Bad request" {
		t.Errorf("error = %q, want %q", err.Error(), "Bad request")
	}
	if isRetryableStatus(err) {
		t.Error("an API error response must not be retryable")
	}
}

func TestHTTPClient_Request_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("Internal Server Error"))
	}))
	defer server.Close()

	hc := newHTTPClient("test-key", server.URL)
	_, err := hc.request(context.Background(), "/api/test", map[string]any{}, 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != 500 {
		t.Fatalf("error = %#v, want httpStatusError with status 500", err)
	}
	if !isRetryableStatus(err) {
		t.Error("a 500 must be retryable")
	}
}

func TestIsRetryableStatus(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{errors.New("dial tcp: connection refused"), true},
		{&httpStatusError{StatusCode: 429}, true},
		{&httpStatusError{StatusCode: 502}, true},
		{&httpStatusError{StatusCode: 503}, true},
		{&httpStatusError{StatusCode: 504}, true},
		// Ours rather than OTLP's: Bitfab ingestion answers every unhandled
		// error with 500, so a connection blip is indistinguishable here from a
		// permanent fault.
		{&httpStatusError{StatusCode: 500}, true},
		{&httpStatusError{StatusCode: 501}, false},
		{&httpStatusError{StatusCode: 408}, false},
		{&httpStatusError{StatusCode: 425}, false},
		{&httpStatusError{StatusCode: 400}, false},
		{&httpStatusError{StatusCode: 401}, false},
		{&httpStatusError{StatusCode: 413}, false},
		{&apiResponseError{Message: "rejected"}, false},
	}
	for _, testCase := range cases {
		if got := isRetryableStatus(testCase.err); got != testCase.want {
			t.Errorf("isRetryableStatus(%v) = %v, want %v", testCase.err, got, testCase.want)
		}
	}
}

func TestHTTPClient_SendExternalSpan_GoesThroughOtel(t *testing.T) {
	sink := &carrierSink{}
	server := newCarrierCaptureServer(t, sink)
	defer server.Close()

	hc := newHTTPClient("test-key", server.URL)
	hc.sendExternalSpan(map[string]any{"test": true})
	if !hc.flush(5 * time.Second) {
		t.Fatal("flush reported failure")
	}

	payloads := sink.spanPayloads()
	if len(payloads) != 1 {
		t.Fatalf("span payloads = %d, want 1", len(payloads))
	}
	if payloads[0]["sdkVersion"] != Version {
		t.Errorf("sdkVersion = %v, want %v", payloads[0]["sdkVersion"], Version)
	}
	if payloads[0]["test"] != true {
		t.Errorf("payload = %#v, want test true", payloads[0])
	}
}

func TestHTTPClient_LazyTransport(t *testing.T) {
	hc := newHTTPClient("test-key", "https://example.invalid")
	hc.transportMu.Lock()
	transport := hc.transport
	hc.transportMu.Unlock()
	if transport != nil {
		t.Fatal("a client that never sent a span must not build a transport")
	}

	hc.sendExternalSpan(map[string]any{"test": true})

	hc.transportMu.Lock()
	transport = hc.transport
	hc.transportMu.Unlock()
	if transport == nil {
		t.Fatal("submitting a span must build the transport")
	}
	hc.close(time.Second)
}

func TestHTTPClient_Flush_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Second)
	}))
	defer server.Close()

	hc := newHTTPClient("test-key", server.URL)
	hc.sendExternalSpan(map[string]any{"test": true})

	start := time.Now()
	flushed := hc.flush(100 * time.Millisecond)
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Errorf("flush took %v, expected < 2s", elapsed)
	}
	if flushed {
		t.Error("flush must report failure when the deadline expires")
	}
}

func TestHTTPClient_Close_IsIdempotentAndStopsSubmissions(t *testing.T) {
	sink := &carrierSink{}
	server := newCarrierCaptureServer(t, sink)
	defer server.Close()

	hc := newHTTPClient("test-key", server.URL)
	hc.sendExternalSpan(map[string]any{"first": true})

	if !hc.close(5 * time.Second) {
		t.Fatal("close reported failure")
	}
	if !hc.close(5 * time.Second) {
		t.Fatal("close must be idempotent")
	}

	hc.sendExternalSpan(map[string]any{"second": true})
	hc.flush(time.Second)

	payloads := sink.spanPayloads()
	if len(payloads) != 1 {
		t.Fatalf("span payloads = %d, want 1 (the post-close submit must be dropped)", len(payloads))
	}
	if payloads[0]["first"] != true {
		t.Errorf("payload = %#v, want the pre-close span", payloads[0])
	}
}
