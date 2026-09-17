package bitfab

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func newConnCountingServer(t *testing.T, handler http.Handler) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var newConns atomic.Int64
	server := httptest.NewUnstartedServer(handler)
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConns.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	return server, &newConns
}

func TestHTTPClient_UsesSharedTransport(t *testing.T) {
	first := newHTTPClient("key", "https://example.invalid")
	second := newHTTPClient("other", "https://example.invalid")
	if first.client.Transport == nil || first.client.Transport == http.DefaultTransport {
		t.Fatalf("client transport = %v, want the SDK transport", first.client.Transport)
	}
	if first.client.Transport != second.client.Transport {
		t.Error("clients must share one transport")
	}
	transport := sharedTransport()
	if first.client.Transport != transport {
		t.Error("client transport is not the shared SDK transport")
	}
	if transport.MaxIdleConnsPerHost < otelMaxExportConcurrency {
		t.Errorf("MaxIdleConnsPerHost = %d, want at least %d", transport.MaxIdleConnsPerHost, otelMaxExportConcurrency)
	}
	if transport.MaxIdleConns < otelMaxExportConcurrency {
		t.Errorf("MaxIdleConns = %d, want at least %d", transport.MaxIdleConns, otelMaxExportConcurrency)
	}
	if transport.IdleConnTimeout != 90*time.Second {
		t.Errorf("IdleConnTimeout = %v, want 90s", transport.IdleConnTimeout)
	}
	if first.client.Timeout != 120*time.Second {
		t.Errorf("default client timeout = %v, want 120s", first.client.Timeout)
	}

	c := NewClient("key", WithServiceURL("https://example.invalid"), WithTimeout(5*time.Second), WithSimulationPlan(false))
	defer c.Close(time.Second)
	if c.httpClient.client.Transport != transport {
		t.Error("NewClient must use the shared SDK transport")
	}
	if c.httpClient.client.Timeout != 5*time.Second {
		t.Errorf("client timeout = %v, want the configured request timeout", c.httpClient.client.Timeout)
	}
}

func TestHTTPClient_TransportFallsBackWhenDefaultReplaced(t *testing.T) {
	replaced := roundTripperFunc(func(*http.Request) (*http.Response, error) { return nil, nil })
	transport := newSDKTransport(replaced)
	if transport.Proxy == nil || transport.DialContext == nil || !transport.ForceAttemptHTTP2 {
		t.Error("fallback transport must keep proxy, dialer, and HTTP/2 defaults")
	}
	if transport.MaxIdleConnsPerHost < otelMaxExportConcurrency {
		t.Errorf("MaxIdleConnsPerHost = %d, want at least %d", transport.MaxIdleConnsPerHost, otelMaxExportConcurrency)
	}

	base := &http.Transport{MaxIdleConns: 7}
	cloned := newSDKTransport(base)
	if cloned == base {
		t.Error("the default transport must be cloned, not mutated")
	}
	if base.MaxIdleConnsPerHost != 0 {
		t.Error("cloning must leave the host transport untouched")
	}
}

func TestHTTPClient_ParallelSpanUploadsReuseConnectionsAcrossBursts(t *testing.T) {
	const concurrency = otelDefaultExportConcurrency
	var inFlight sync.WaitGroup
	var gate atomic.Pointer[chan struct{}]
	server, newConns := newConnCountingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inFlight.Done()
		<-*gate.Load()
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	hc := newHTTPClient("test-key", server.URL)

	burst := func() {
		release := make(chan struct{})
		gate.Store(&release)
		inFlight.Add(concurrency)
		var done sync.WaitGroup
		errs := make(chan error, concurrency)
		for range concurrency {
			done.Add(1)
			go func() {
				defer done.Done()
				_, err := hc.sendTransportRequest(otelTracesEndpoint, prepareRequestBody([]byte(`{}`)), otelExportTimeout)
				errs <- err
			}()
		}
		arrived := make(chan struct{})
		go func() {
			inFlight.Wait()
			close(arrived)
		}()
		select {
		case <-arrived:
		case <-time.After(10 * time.Second):
			t.Fatal("uploads did not run in parallel")
		}
		close(release)
		done.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("upload failed: %v", err)
			}
		}
	}

	burst()
	afterFirst := newConns.Load()
	if afterFirst > concurrency {
		t.Fatalf("first burst opened %d connections, want at most %d", afterFirst, concurrency)
	}
	time.Sleep(200 * time.Millisecond)
	burst()
	if total := newConns.Load(); total > concurrency {
		t.Errorf("two bursts opened %d connections, want at most %d (connections were not reused)", total, concurrency)
	}
}

func TestHTTPClient_SimulationPlanReadsReuseOneConnection(t *testing.T) {
	server, newConns := newConnCountingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"nodes":[]}` + "\n\n\n"))
	}))
	hc := newHTTPClient("test-key", server.URL)
	for range 5 {
		if _, err := hc.getSimulationPlan(); err != nil {
			t.Fatalf("sim plan read failed: %v", err)
		}
	}
	if got := newConns.Load(); got != 1 {
		t.Errorf("five sim plan reads opened %d connections, want 1", got)
	}
}

func TestHTTPClient_ErrorResponsesReuseConnection(t *testing.T) {
	server, newConns := newConnCountingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"nope"}`))
	}))
	hc := newHTTPClient("test-key", server.URL)
	for range 3 {
		var out map[string]any
		if err := hc.get(context.Background(), "/api/test", &out); err == nil {
			t.Fatal("expected GET error")
		}
		if _, err := hc.send(context.Background(), "/api/test", []byte(`{}`), time.Second); err == nil {
			t.Fatal("expected POST error")
		}
	}
	if got := newConns.Load(); got != 1 {
		t.Errorf("error responses opened %d connections, want 1", got)
	}
}

func TestHTTPClient_PerCallTimeoutSemantics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	hc := newHTTPClient("test-key", server.URL)
	if _, err := hc.send(context.Background(), "/api/test", []byte(`{}`), 20*time.Millisecond); err == nil {
		t.Error("a per-call timeout shorter than the response must fail")
	}

	hc.client.Timeout = 20 * time.Millisecond
	if _, err := hc.send(context.Background(), "/api/test", []byte(`{}`), 2*time.Second); err != nil {
		t.Errorf("a per-call timeout must replace the client default: %v", err)
	}
	if _, err := hc.send(context.Background(), "/api/test", []byte(`{}`), 0); err == nil {
		t.Error("without a per-call timeout the client default must apply")
	}
}
