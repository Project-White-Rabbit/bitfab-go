package bitfab

import (
	"context"
	"runtime"
	"strconv"
	"strings"
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
	// Go exposes no goroutine-local storage API. Use only the public stack
	// formatter, with a checked prefix and a fail-open fallback, never runtime
	// memory layouts or linkname. The transform's inactive path never reads it.
	var buffer [64]byte
	n := runtime.Stack(buffer[:], false)
	line := string(buffer[:n])
	if !strings.HasPrefix(line, "goroutine ") {
		return 0
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	id, _ := strconv.ParseUint(fields[1], 10, 64)
	return id
}

func currentAutoScope(ctx context.Context) *autoScope {
	id := autoGoroutineID()
	autoScopes.RLock()
	scope := autoScopes.values[id]
	autoScopes.RUnlock()
	if scope == nil && ctx != nil {
		scope, _ = ctx.Value(autoScopeContextKey{}).(*autoScope)
	}
	return scope
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
