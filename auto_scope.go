package bitfab

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

type autoScopeContextKey struct{}
type managedTraceRootKey struct{}

type managedTraceRoot struct {
	mu     sync.Mutex
	root   *autoRoot
	spanID string
	data   map[string]any
	output any
	error  error
}

func withManagedTraceRoot(ctx context.Context) context.Context {
	return context.WithValue(ctx, managedTraceRootKey{}, &managedTraceRoot{})
}

type autoFrame struct {
	root   *autoRoot
	parent *autoRecord
	depth  int
}

type autoScope struct {
	client   *Client
	frames   []autoFrame
	explicit bool
	suppress bool
	skipName string
}

var autoScopes = struct {
	sync.RWMutex
	values map[uint64]*autoScope
}{values: map[uint64]*autoScope{}}

var autoActiveRoots atomic.Int64
var autoActiveExplicit atomic.Int64
var autoConfiguredNodes atomic.Int64

// AutoCaptureActive is the generated instrumentation's allocation-free fast path.
// Application code should enter capture through Client.Trace.
func AutoCaptureActive() bool {
	return autoActiveRoots.Load() > 0 || (autoConfiguredNodes.Load() > 0 && autoActiveExplicit.Load() > 0)
}

func autoGoroutineID() uint64 {
	var buffer [64]byte
	n := runtime.Stack(buffer[:], false)
	prefix := "goroutine "
	if n <= len(prefix) || string(buffer[:len(prefix)]) != prefix {
		return 0
	}
	var id uint64
	for _, b := range buffer[len(prefix):n] {
		if b < '0' || b > '9' {
			break
		}
		id = id*10 + uint64(b-'0')
	}
	return id
}

func currentAutoScope(ctx context.Context) *autoScope {
	scope, _ := lookupAutoScope(ctx)
	return scope
}

func lookupAutoScope(ctx context.Context) (scope *autoScope, onGoroutine bool) {
	id := autoGoroutineID()
	autoScopes.RLock()
	scope = autoScopes.values[id]
	autoScopes.RUnlock()
	if scope != nil {
		return scope, true
	}
	if ctx != nil {
		scope, _ = ctx.Value(autoScopeContextKey{}).(*autoScope)
	}
	return scope, false
}

func pushAutoScope(scope *autoScope) func() {
	id := autoGoroutineID()
	if id == 0 {
		warnOnce("auto-goroutine-id", "automatic subtree tracing could not identify the current goroutine; its calls run untraced")
		return func() {}
	}
	autoScopes.Lock()
	previous := autoScopes.values[id]
	autoScopes.values[id] = scope
	autoScopes.Unlock()
	return func() {
		autoScopes.Lock()
		if autoScopes.values[id] == scope {
			if previous == nil {
				delete(autoScopes.values, id)
			} else {
				autoScopes.values[id] = previous
			}
		}
		autoScopes.Unlock()
	}
}

// AutoContextSnapshot carries an invocation's subtree context into a generated
// goroutine launch. Its fields are intentionally private.
type AutoContextSnapshot struct {
	scope *autoScope
}

// CaptureAutoContext snapshots parentage when a generated goroutine is submitted.
func CaptureAutoContext() AutoContextSnapshot {
	if !AutoCaptureActive() {
		return AutoContextSnapshot{}
	}
	return AutoContextSnapshot{scope: currentAutoScope(nil)}
}

// RunAutoContext restores a generated goroutine's submission-time parentage.
func RunAutoContext(snapshot AutoContextSnapshot, fn func()) {
	if snapshot.scope == nil {
		fn()
		return
	}
	defer pushAutoScope(snapshot.scope)()
	fn()
}

func (c *Client) beginExplicitAutoScope(ctx context.Context) func() {
	if managed, _ := ctx.Value(managedTraceRootKey{}).(*managedTraceRoot); managed != nil && currentSpan(ctx) == nil {
		return func() {}
	}
	autoActiveExplicit.Add(1)
	restore := pushAutoScope(&autoScope{explicit: true, client: c})
	return func() {
		restore()
		autoActiveExplicit.Add(-1)
	}
}

func checkMixedAutoSpan(ctx context.Context) error {
	if autoActiveRoots.Load() == 0 {
		return nil
	}
	scope := currentAutoScope(ctx)
	if scope != nil && !scope.suppress && len(scope.frames) > 0 {
		return &MixedTracingError{Entered: "span", Active: "trace"}
	}
	return nil
}
