package bitfab

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"time"
)

const (
	replayRetryBudget    = 5 * time.Minute
	replayRetryBaseDelay = time.Second
	replayRetryMaxDelay  = 30 * time.Second
)

type replayRetryPolicy struct {
	now    func() time.Time
	sleep  func(ctx context.Context, delay time.Duration) error
	jitter func() float64
}

func (p replayRetryPolicy) withDefaults() replayRetryPolicy {
	if p.now == nil {
		p.now = time.Now
	}
	if p.sleep == nil {
		p.sleep = sleepWithContext
	}
	if p.jitter == nil {
		p.jitter = func() float64 { return 0.5 + rand.Float64()*0.5 }
	}
	return p
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func replayRetryDelay(retry int, jitter float64) time.Duration {
	backoff := replayRetryMaxDelay
	if retry <= 6 {
		backoff = min(replayRetryBaseDelay<<(retry-1), replayRetryMaxDelay)
	}
	return time.Duration(float64(backoff) * jitter)
}

func isRetryableReplayError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	var apiErr *apiResponseError
	if errors.As(err, &apiErr) {
		return false
	}
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode >= http.StatusInternalServerError
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.EOF)
}

func retryReplayRequest[T any](ctx context.Context, policy replayRetryPolicy, call func() (T, error)) (T, error) {
	policy = policy.withDefaults()
	deadline := policy.now().Add(replayRetryBudget)
	for retry := 1; ; retry++ {
		result, err := call()
		if err == nil || !isRetryableReplayError(ctx, err) {
			return result, err
		}
		delay := replayRetryDelay(retry, policy.jitter())
		var statusErr *httpStatusError
		if errors.As(err, &statusErr) && statusErr.RetryAfter > 0 {
			delay = statusErr.RetryAfter
		}
		if policy.now().Add(delay).After(deadline) {
			return result, err
		}
		if policy.sleep(ctx, delay) != nil {
			return result, err
		}
	}
}
