package bitfab

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type failingCompletionTransport struct {
	traceTransport
	calls int
}

func (transport *failingCompletionTransport) submit(traceOperation, map[string]any, []byte, ...carrierMeta) {
	transport.calls++
	panic("transport failed")
}

func TestTraceCompletionDeferredTransportFailure(t *testing.T) {
	transport := &failingCompletionTransport{}
	client := newTestClient("https://bitfab.ai")
	client.httpClient.transport = transport
	client.httpClient.traceCompletion.start("trace", "child")
	client.httpClient.sendExternalTrace(map[string]any{
		"externalTrace": map[string]any{"id": "trace"},
		"completed":     true,
	})
	if transport.calls != 0 {
		t.Fatal("completion sent before child ended")
	}
	client.httpClient.traceCompletion.end("trace", "child")
	if transport.calls != 1 {
		t.Fatalf("completion calls = %d, want 1", transport.calls)
	}
}

func TestTraceCompletionChildOutlivesRoot(t *testing.T) {
	sink := &carrierSink{}
	server := newCarrierCaptureServer(t, sink)
	defer server.Close()
	client := newTestClient(server.URL)
	ctx, root := client.Start(context.Background(), "root", "root")
	childCtx, child := client.Start(ctx, "child", "child")
	root.End()
	if !client.httpClient.flush(5 * time.Second) {
		t.Fatal("flush failed")
	}
	for _, carrier := range sink.all() {
		if carrier.operation == string(operationExternalTrace) {
			t.Fatal("completion sent before child ended")
		}
	}
	_, grandchild := client.Start(childCtx, "grandchild", "grandchild")
	grandchild.End()
	child.End()
	child.End()
	if !client.httpClient.close(5 * time.Second) {
		t.Fatal("close failed")
	}
	completions := 0
	for _, carrier := range sink.all() {
		if carrier.operation == string(operationExternalTrace) {
			completions++
			if carrier.payload["expectedSpanCount"] != float64(3) {
				t.Fatalf("expected count 3, got %v", carrier.payload["expectedSpanCount"])
			}
		}
	}
	if completions != 1 {
		t.Fatalf("completion count = %d, want 1", completions)
	}
}

func TestTraceCompletionConcurrentFinalization(t *testing.T) {
	var tracker traceCompletion
	const spans = 20
	for i := range spans {
		tracker.start("trace", string(rune('a'+i)))
	}
	counts := make(chan int, 2)
	tracker.close("trace", func(count int) { counts <- count })
	var wg sync.WaitGroup
	for i := range spans {
		wg.Add(1)
		go func() {
			defer wg.Done()
			spanID := string(rune('a' + i))
			tracker.record("trace", spanID)
			tracker.end("trace", spanID)
		}()
	}
	wg.Wait()
	tracker.close("trace", func(count int) { counts <- count })
	for range 2 {
		if count := <-counts; count != spans {
			t.Fatalf("count = %d, want %d", count, spans)
		}
	}
}

func TestTraceCompletionEvictedDuplicateDoesNotInventZeroCount(t *testing.T) {
	var tracker traceCompletion
	tracker.open("old")
	tracker.record("old", "span")
	tracker.close("old", func(int) {})
	tracker.start("old", "late")
	for i := range 1024 {
		traceID := fmt.Sprintf("later-%d", i)
		tracker.open(traceID)
		tracker.close(traceID, func(int) {})
	}
	called := false
	tracker.close("old", func(int) { called = true })
	if called {
		t.Fatal("evicted duplicate completion was called with an invented count")
	}
}

func TestTraceCompletionRegisteredEmptyTrace(t *testing.T) {
	var tracker traceCompletion
	tracker.open("empty")
	count := -1
	tracker.close("empty", func(value int) { count = value })
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}

func TestTraceCompletionAbortedSpanIsNotCounted(t *testing.T) {
	var tracker traceCompletion
	tracker.open("trace")
	tracker.start("trace", "sent")
	tracker.start("trace", "panicked")
	tracker.record("trace", "sent")
	tracker.end("trace", "sent")
	tracker.abort("trace", "panicked")
	count := -1
	tracker.close("trace", func(value int) { count = value })
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}
