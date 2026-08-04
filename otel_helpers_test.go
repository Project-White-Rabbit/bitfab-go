package bitfab

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type capturedCarrier struct {
	operation         string
	payload           map[string]any
	name              string
	statusCode        int
	startTimeUnixNano string
	endTimeUnixNano   string
	traceID           string
	spanID            string
}

// decodeOtlpCarriers unwraps an OTLP/JSON export request into the Bitfab
// payloads its carrier spans hold.
func decodeOtlpCarriers(t *testing.T, body []byte) []capturedCarrier {
	t.Helper()

	var request struct {
		ResourceSpans []struct {
			ScopeSpans []struct {
				Spans []struct {
					TraceID           string `json:"traceId"`
					SpanID            string `json:"spanId"`
					Name              string `json:"name"`
					StartTimeUnixNano string `json:"startTimeUnixNano"`
					EndTimeUnixNano   string `json:"endTimeUnixNano"`
					Status            struct {
						Code int `json:"code"`
					} `json:"status"`
					Attributes []struct {
						Key   string `json:"key"`
						Value struct {
							StringValue *string `json:"stringValue"`
						} `json:"value"`
					} `json:"attributes"`
				} `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatalf("decoding OTLP request: %v", err)
	}

	var carriers []capturedCarrier
	for _, resourceSpan := range request.ResourceSpans {
		for _, scopeSpan := range resourceSpan.ScopeSpans {
			for _, span := range scopeSpan.Spans {
				carrier := capturedCarrier{
					name:              span.Name,
					statusCode:        span.Status.Code,
					startTimeUnixNano: span.StartTimeUnixNano,
					endTimeUnixNano:   span.EndTimeUnixNano,
					traceID:           span.TraceID,
					spanID:            span.SpanID,
				}
				for _, attribute := range span.Attributes {
					if attribute.Value.StringValue == nil {
						continue
					}
					switch attribute.Key {
					case otelOperationAttribute:
						carrier.operation = *attribute.Value.StringValue
					case otelPayloadAttribute:
						payload := map[string]any{}
						if err := json.Unmarshal([]byte(*attribute.Value.StringValue), &payload); err != nil {
							t.Fatalf("decoding carrier payload: %v", err)
						}
						carrier.payload = payload
					}
				}
				carriers = append(carriers, carrier)
			}
		}
	}
	return carriers
}

// carrierSink collects the Bitfab payloads a test server received, split by
// carrier operation.
type carrierSink struct {
	mu       sync.Mutex
	carriers []capturedCarrier
	requests int
}

func (s *carrierSink) add(carriers []capturedCarrier) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.carriers = append(s.carriers, carriers...)
	s.requests++
}

func (s *carrierSink) byOperation(operation traceOperation) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var payloads []map[string]any
	for _, carrier := range s.carriers {
		if carrier.operation == string(operation) {
			payloads = append(payloads, carrier.payload)
		}
	}
	return payloads
}

func (s *carrierSink) spanPayloads() []map[string]any {
	return s.byOperation(operationExternalSpan)
}

func (s *carrierSink) tracePayloads() []map[string]any {
	return s.byOperation(operationExternalTrace)
}

func (s *carrierSink) lastSpanPayload() map[string]any {
	payloads := s.spanPayloads()
	if len(payloads) == 0 {
		return nil
	}
	return payloads[len(payloads)-1]
}

func (s *carrierSink) lastTracePayload() map[string]any {
	payloads := s.tracePayloads()
	if len(payloads) == 0 {
		return nil
	}
	return payloads[len(payloads)-1]
}

func (s *carrierSink) all() []capturedCarrier {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]capturedCarrier{}, s.carriers...)
}

func (s *carrierSink) requestCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests
}

// newCarrierCaptureServer serves the OTLP ingestion endpoint and records every
// carrier payload it receives.
func newCarrierCaptureServer(t *testing.T, sink *carrierSink) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == otelTracesEndpoint {
			body, _ := io.ReadAll(r.Body)
			sink.add(decodeOtlpCarriers(t, body))
		}
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{})
	}))
}

func legacyEndpointFor(operation string) string {
	switch operation {
	case string(operationExternalSpan):
		return "/api/sdk/externalSpans"
	case string(operationExternalTrace):
		return "/api/sdk/externalTraces"
	default:
		return "/api/sdk/internalTraces"
	}
}

// newLegacyCarrierServer adapts a handler written against the pre-OTel
// per-endpoint wire shape. Each carrier in an OTLP request is replayed through
// handle with the path and body that operation used to send on its own, so
// assertions stay expressed in Bitfab payload terms rather than OTLP envelopes.
// Non-OTLP requests reach handle untouched.
func newLegacyCarrierServer(t *testing.T, handle http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != otelTracesEndpoint {
			handle(w, r)
			return
		}

		body, _ := io.ReadAll(r.Body)
		for _, carrier := range decodeOtlpCarriers(t, body) {
			encoded, err := json.Marshal(carrier.payload)
			if err != nil {
				t.Fatalf("re-encoding carrier payload: %v", err)
			}
			replay := r.Clone(r.Context())
			replay.URL.Path = legacyEndpointFor(carrier.operation)
			replay.Body = io.NopCloser(bytes.NewReader(encoded))
			handle(httptest.NewRecorder(), replay)
		}

		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]any{})
	}))
}
