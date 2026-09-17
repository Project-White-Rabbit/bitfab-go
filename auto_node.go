package bitfab

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"sync"
)

// AutoFunctionSymbol resolves the actual compiled symbol for generated code.
// Package main has a different runtime path under go test than under go run.
func AutoFunctionSymbol(fallback string) string {
	pc, _, _, ok := runtime.Caller(1)
	if ok {
		if fn := runtime.FuncForPC(pc); fn != nil {
			return normalizeAutoSymbol(fn.Name())
		}
	}
	return normalizeAutoSymbol(fallback)
}

// AutoNode is an invocation handle used by generated instrumentation.
type AutoNode struct {
	records  []*autoRecord
	ctx      context.Context
	restore  func()
	outputs  []any
	finalize SpanFinalizer
	mocked   bool
	value    any
	err      error
	once     sync.Once
}

// EnterAutoNode is called by generated first-party function prologues. Application
// code configures these functions with Client.Node and enters with Client.Trace.
func EnterAutoNode(ctx context.Context, symbol, name string, inputs, outputs []any, wrapper ...bool) *AutoNode {
	name = normalizeAutoSymbol(name)
	if !AutoCaptureActive() {
		return nil
	}
	scope, onGoroutine := lookupAutoScope(ctx)
	if scope == nil || scope.suppress {
		return nil
	}
	if scope.explicit {
		if _, configured := scope.client.nodeOptions(symbol); configured {
			panic(&MixedTracingError{Entered: "node", Active: "span"})
		}
		return nil
	}
	if len(scope.frames) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = scope.frames[len(scope.frames)-1].root.ctx
	}
	if normalizeAutoSymbol(symbol) == scope.skipName {
		next := *scope
		next.skipName = ""
		return &AutoNode{ctx: ctx, restore: pushAutoScope(&next), outputs: outputs}
	}
	var records []*autoRecord
	var finalize SpanFinalizer
	frames := scope.frames
	copied := false
	droppedByLimit, droppedForGood := true, true
	for i, frame := range scope.frames {
		opts, configured := frame.root.client.nodeOptions(symbol)
		if len(wrapper) > 0 && wrapper[0] && !configured && !frame.root.opts.IncludeWrappers {
			droppedByLimit = false
			continue
		}
		if opts.Capture != nil && !*opts.Capture {
			droppedByLimit = false
			continue
		}
		nodeName, kind := name, "function"
		if opts.Name != "" {
			nodeName = opts.Name
		}
		if opts.Type != "" {
			kind = opts.Type
		}
		r, limited := frame.root.add(frame.parent, frame.depth+1, nodeName, kind, symbol, inputs, configured)
		if r == nil {
			droppedByLimit = droppedByLimit && limited
			droppedForGood = droppedForGood && (frame.depth+1 > frame.root.maxDepth || frame.root.spansFull.Load())
			continue
		}
		records = append(records, r)
		r.testRunID = opts.TestRunID
		finalize = opts.Finalize
		if !copied {
			frames = append([]autoFrame(nil), scope.frames...)
			copied = true
		}
		frames[i] = autoFrame{root: frame.root, parent: r, depth: frame.depth + 1}
	}
	if len(records) == 0 && droppedByLimit && onGoroutine && (scope.skipName == "" || droppedForGood) {
		return nil
	}
	if !copied {
		frames = append([]autoFrame(nil), scope.frames...)
	}
	node := &AutoNode{outputs: outputs, ctx: ctx, records: records, finalize: finalize}
	if len(node.records) > 0 {
		r := node.records[len(node.records)-1]
		opts, _ := r.root.client.nodeOptions(symbol)
		marked := r.root.opts.MockOnReplayDefault
		if opts.MockOnReplay != nil {
			marked = *opts.MockOnReplay
		}
		mockCtx := withSpanContext(context.WithValue(ctx, spanStackKey{}, []spanEntry(nil)), r.root.traceID, r.parentID)
		var source MockSource
		node.value, node.mocked, source, node.err = r.root.client.resolveReplayMock(mockCtx, r.root.key, spanConfig{name: r.name, spanType: r.kind, input: inputs, mockOnReplay: marked}, false)
		for _, record := range node.records {
			record.mocked = node.mocked && node.err == nil
			record.mockSource = source
		}
	}
	frame := frames[len(frames)-1]
	node.ctx = withSpanContext(context.WithValue(ctx, spanStackKey{}, []spanEntry(nil)), frame.root.traceID, frame.parent.id)
	if len(node.records) > 0 {
		var enrichment *spanEnrichment
		node.ctx, enrichment = withSpanEnrichment(node.ctx, frame.parent.id)
		for _, record := range node.records {
			record.enrichment = enrichment
		}
	} else if frame.parent.enrichment != nil {
		node.ctx = context.WithValue(node.ctx, spanEnrichmentKey{}, frame.parent.enrichment)
	}
	next := &autoScope{frames: frames}
	node.ctx = context.WithValue(node.ctx, autoScopeContextKey{}, next)
	node.restore = pushAutoScope(next)
	return node
}

// Context returns the context containing the automatically captured current node.
func (n *AutoNode) Context() context.Context { return n.ctx }

// Mocked reports whether replay selected this call for output substitution.
func (n *AutoNode) Mocked() bool { return n.mocked }

var autoErrorType = reflect.TypeFor[error]()

// AssignMock writes a replacement into generated named return variables.
func (n *AutoNode) AssignMock() {
	values := make([]reflect.Value, len(n.outputs))
	errorIndex := -1
	for i, p := range n.outputs {
		values[i] = reflect.ValueOf(p).Elem()
		if i == len(values)-1 && values[i].Type().Implements(autoErrorType) {
			errorIndex = i
		}
	}
	count := len(values)
	if errorIndex >= 0 {
		count--
	}
	if n.err == nil {
		items := []any{n.value}
		if count > 1 {
			var ok bool
			items, ok = n.value.([]any)
			if !ok || len(items) != count {
				n.err = fmt.Errorf("bitfab: mocked output must contain %d return values", count)
			}
		}
		if n.err == nil {
			for i := 0; i < count; i++ {
				v, err := decodeReplayValue(items[i], values[i].Type())
				if err != nil {
					n.err = err
					break
				}
				values[i].Set(v)
			}
		}
	}
	if n.err != nil {
		if errorIndex < 0 || !reflect.TypeOf(n.err).AssignableTo(values[errorIndex].Type()) {
			panic(n.err)
		}
		values[errorIndex].Set(reflect.ValueOf(n.err))
	}
}

// End captures named return values after user defers have run. The generated
// defer rethrows panicValue so instrumentation preserves application panics.
func (n *AutoNode) End(panicValue any) {
	n.once.Do(func() {
		if n.restore != nil {
			n.restore()
		}
		var values []any
		err := n.err
		for i, p := range n.outputs {
			v := reflect.ValueOf(p).Elem()
			if i == len(n.outputs)-1 && v.Type().Implements(autoErrorType) {
				if (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface || v.Kind() == reflect.Map || v.Kind() == reflect.Slice || v.Kind() == reflect.Func || v.Kind() == reflect.Chan) && v.IsNil() {
					continue
				}
				if e, ok := v.Interface().(error); ok {
					err = e
				}
				continue
			}
			values = append(values, v.Interface())
		}
		var output any
		if len(values) == 1 {
			output = values[0]
		} else if len(values) > 1 {
			output = values
		}
		if panicValue != nil {
			err = fmt.Errorf("panic: %v", panicValue)
		}
		finalize := n.finalize
		if n.mocked {
			finalize = nil
		}
		finishAutoRecords(n.records, output, err, finalize)
	})
}
