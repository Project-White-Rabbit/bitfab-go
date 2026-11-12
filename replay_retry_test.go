package bitfab

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeReplayRetry struct {
	mu      sync.Mutex
	current time.Time
	delays  []time.Duration
	onSleep func(attempt int)
}

func newFakeReplayRetry() *fakeReplayRetry {
	return &fakeReplayRetry{current: time.Unix(1_700_000_000, 0)}
}

func (f *fakeReplayRetry) policy() replayRetryPolicy {
	return replayRetryPolicy{
		now: func() time.Time {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.current
		},
		sleep: func(ctx context.Context, delay time.Duration) error {
			f.mu.Lock()
			f.delays = append(f.delays, delay)
			f.current = f.current.Add(delay)
			count := len(f.delays)
			onSleep := f.onSleep
			f.mu.Unlock()
			if onSleep != nil {
				onSleep(count)
			}
			return ctx.Err()
		},
		jitter: func() float64 { return 1 },
	}
}

func (f *fakeReplayRetry) sleeps() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.delays...)
}

func newRetryTestHTTPClient(serverURL string, fake *fakeReplayRetry) *httpClient {
	client := newHTTPClient("test-key", serverURL)
	client.replayRetry = fake.policy()
	return client
}

func getWithReplayRetry(ctx context.Context, client *httpClient) (map[string]any, error) {
	return retryReplayRequest(ctx, client.replayRetry, func() (map[string]any, error) {
		result := map[string]any{}
		err := client.get(ctx, "/thing", &result)
		return result, err
	})
}

func TestReplayRetrySucceedsAfterServerErrors(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			http.Error(w, "stalled", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	fake := newFakeReplayRetry()

	result, err := getWithReplayRetry(context.Background(), newRetryTestHTTPClient(server.URL, fake))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if result["ok"] != true || calls.Load() != 3 {
		t.Fatalf("result = %#v after %d calls, want ok after 3", result, calls.Load())
	}
	if got := fake.sleeps(); len(got) != 2 || got[0] != time.Second || got[1] != 2*time.Second {
		t.Fatalf("sleeps = %v, want [1s 2s]", got)
	}
}

func TestReplayRetryDoesNotRetryClientErrors(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusTooManyRequests} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				http.Error(w, "nope", status)
			}))
			defer server.Close()

			_, err := getWithReplayRetry(context.Background(), newRetryTestHTTPClient(server.URL, newFakeReplayRetry()))
			if statusCode(err) != status || calls.Load() != 1 {
				t.Fatalf("err = %v after %d calls, want HTTP %d after 1", err, calls.Load(), status)
			}
		})
	}
}

func TestReplayRetryDoesNotRetryAnErrorInTheResponseBody(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"error":"experiment not found"}`))
	}))
	defer server.Close()
	client := newRetryTestHTTPClient(server.URL, newFakeReplayRetry())

	_, err := retryReplayRequest(context.Background(), client.replayRetry, func() (map[string]any, error) {
		return client.request(context.Background(), "/api/sdk/replay/complete", map[string]any{}, time.Second)
	})
	var apiErr *apiResponseError
	if !errors.As(err, &apiErr) || calls.Load() != 1 {
		t.Fatalf("err = %v after %d calls, want one apiResponseError", err, calls.Load())
	}
}

func TestReplayRetryRetriesTimeouts(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	defer close(release)
	client := newRetryTestHTTPClient(server.URL, newFakeReplayRetry())

	result, err := retryReplayRequest(context.Background(), client.replayRetry, func() (map[string]any, error) {
		return client.request(context.Background(), "/api/sdk/replay/complete", map[string]any{}, 50*time.Millisecond)
	})
	if err != nil || result["ok"] != true || calls.Load() != 2 {
		t.Fatalf("result = %#v, err = %v after %d calls, want ok after 2", result, err, calls.Load())
	}
}

func TestReplayRetryRetriesConnectionErrors(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	serverURL := server.URL
	server.Close()
	fake := newFakeReplayRetry()
	var calls int

	_, err := retryReplayRequest(context.Background(), fake.policy(), func() (map[string]any, error) {
		calls++
		result := map[string]any{}
		err := newHTTPClient("test-key", serverURL).get(context.Background(), "/thing", &result)
		if calls == 3 {
			return result, nil
		}
		return result, err
	})
	if err != nil || calls != 3 {
		t.Fatalf("err = %v after %d calls, want success after 3", err, calls)
	}
}

func TestReplayRetryGivesUpAtTheDeadlineWithTheLastError(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, fmt.Sprintf("stalled %d", calls.Add(1)), http.StatusServiceUnavailable)
	}))
	defer server.Close()
	fake := newFakeReplayRetry()

	_, err := getWithReplayRetry(context.Background(), newRetryTestHTTPClient(server.URL, fake))
	if statusCode(err) != http.StatusServiceUnavailable {
		t.Fatalf("err = %v, want HTTP 503", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("stalled %d", calls.Load())) {
		t.Fatalf("err = %v, want the error from attempt %d", err, calls.Load())
	}
	var total time.Duration
	for _, delay := range fake.sleeps() {
		total += delay
	}
	if total > replayRetryBudget || total < replayRetryBudget-replayRetryMaxDelay {
		t.Fatalf("slept %v in total, want close to but not past %v", total, replayRetryBudget)
	}
	if got := fake.sleeps(); got[len(got)-1] != replayRetryMaxDelay {
		t.Fatalf("last sleep = %v, want the %v cap", got[len(got)-1], replayRetryMaxDelay)
	}
	if int(calls.Load()) != len(fake.sleeps())+1 {
		t.Fatalf("%d calls for %d sleeps", calls.Load(), len(fake.sleeps()))
	}
}

func TestReplayRetryDelayIsCappedAndJittered(t *testing.T) {
	cases := []struct {
		retry  int
		jitter float64
		want   time.Duration
	}{
		{1, 1, time.Second},
		{3, 1, 4 * time.Second},
		{5, 1, 16 * time.Second},
		{6, 1, 30 * time.Second},
		{40, 1, 30 * time.Second},
		{2, 0.5, time.Second},
	}
	for _, c := range cases {
		if got := replayRetryDelay(c.retry, c.jitter); got != c.want {
			t.Errorf("replayRetryDelay(%d, %v) = %v, want %v", c.retry, c.jitter, got, c.want)
		}
	}
	policy := replayRetryPolicy{}.withDefaults()
	for range 100 {
		if jitter := policy.jitter(); jitter < 0.5 || jitter > 1 {
			t.Fatalf("jitter = %v, want within [0.5, 1]", jitter)
		}
	}
}

func TestReplayRetryStopsWhenTheContextIsCancelled(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "stalled", http.StatusInternalServerError)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := newFakeReplayRetry()
	fake.onSleep = func(attempt int) {
		if attempt == 2 {
			cancel()
		}
	}

	_, err := getWithReplayRetry(ctx, newRetryTestHTTPClient(server.URL, fake))
	if statusCode(err) != http.StatusInternalServerError || calls.Load() != 2 {
		t.Fatalf("err = %v after %d calls, want HTTP 500 after 2", err, calls.Load())
	}
}

func TestReplayRetryRealSleepHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if err := sleepWithContext(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("sleep did not return when the context was cancelled")
	}
}

func TestReplayRetriesSpanFetchAndCompletionThroughServerErrors(t *testing.T) {
	state := &replayTestServerState{}
	var spanCalls, completeCalls atomic.Int32
	handler := replayTestHandler(t, state, replayItems()[:1])
	server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/sdk/spans/") && spanCalls.Add(1) <= 2 {
			http.Error(w, "stalled", http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/api/sdk/replay/complete" && completeCalls.Add(1) <= 2 {
			http.Error(w, "stalled", http.StatusInternalServerError)
			return
		}
		handler(w, r)
	})
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	result, err := client.Replay(
		context.Background(),
		"retry-workflow",
		func(name string, count int) string { return fmt.Sprintf("%s:%d", name, count) },
		&ReplayOptions{CodeChange: CodeChangeSkip},
	)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].Error != nil || result.Items[0].Result != "alpha:2" {
		t.Fatalf("items = %#v", result.Items)
	}
	if spanCalls.Load() != 3 || completeCalls.Load() != 3 {
		t.Fatalf("span calls = %d, complete calls = %d, want 3 each", spanCalls.Load(), completeCalls.Load())
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.completeBody["experimentId"] != "run-1" {
		t.Fatalf("complete body = %#v", state.completeBody)
	}
}

func TestReplayReportsCompletionThatKeepsFailing(t *testing.T) {
	state := &replayTestServerState{completeErr: true}
	var completeCalls atomic.Int32
	handler := replayTestHandler(t, state, replayItems()[:1])
	server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/sdk/replay/complete" {
			completeCalls.Add(1)
		}
		handler(w, r)
	})
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	_, err := client.Replay(
		context.Background(),
		"retry-workflow",
		func(name string, count int) string { return fmt.Sprintf("%s:%d", name, count) },
		&ReplayOptions{CodeChange: CodeChangeSkip},
	)
	var replayErr *ReplayError
	if !errors.As(err, &replayErr) || replayErr.ExperimentID != "run-1" {
		t.Fatalf("err = %T %v, want *ReplayError for run-1", err, err)
	}
	if statusCode(err) != http.StatusInternalServerError || !strings.Contains(err.Error(), "complete replay") {
		t.Fatalf("err = %v, want the completion HTTP 500", err)
	}
	if completeCalls.Load() < 2 {
		t.Fatalf("complete calls = %d, want retries", completeCalls.Load())
	}
}

func TestReplayRetryWaitsAsLongAsRetryAfterAsks(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.Header().Set("Retry-After", "7")
			http.Error(w, "busy", http.StatusServiceUnavailable)
		case 2:
			http.Error(w, "stalled", http.StatusInternalServerError)
		default:
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	}))
	defer server.Close()
	fake := newFakeReplayRetry()

	if _, err := getWithReplayRetry(context.Background(), newRetryTestHTTPClient(server.URL, fake)); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := fake.sleeps(); len(got) != 2 || got[0] != 7*time.Second || got[1] != 2*time.Second {
		t.Fatalf("sleeps = %v, want [7s 2s]", got)
	}
}

func TestReplayRetryGivesUpWhenRetryAfterPassesTheDeadline(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "600")
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	fake := newFakeReplayRetry()

	_, err := getWithReplayRetry(context.Background(), newRetryTestHTTPClient(server.URL, fake))
	if statusCode(err) != http.StatusServiceUnavailable || calls.Load() != 1 {
		t.Fatalf("err = %v after %d calls, want HTTP 503 after 1", err, calls.Load())
	}
	if got := fake.sleeps(); len(got) != 0 {
		t.Fatalf("sleeps = %v, want none", got)
	}
}

func TestReplayRetryNeverRetriesAClientErrorWithRetryAfter(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	defer server.Close()

	_, err := getWithReplayRetry(context.Background(), newRetryTestHTTPClient(server.URL, newFakeReplayRetry()))
	if statusCode(err) != http.StatusTooManyRequests || calls.Load() != 1 {
		t.Fatalf("err = %v after %d calls, want HTTP 429 after 1", err, calls.Load())
	}
}
