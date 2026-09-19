package bitfab

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SpanFinalizer changes the recorded output without changing the returned value.
// It runs in the background; FlushTraces and Close wait for it within their timeout.
type SpanFinalizer func(any) (any, error)

// TraceOptions configures a subtree owned by one trace invocation.
type TraceOptions struct {
	Name, Type, ExperimentID string
	// Deprecated: Use ExperimentID instead.
	TestRunID               string
	Input                   []any
	MaxDepth                *int
	MaxCapturedSubtreeSpans *int
	Exclude                 []string
	CaptureWhen             CaptureWhen
	MockOnReplayDefault     bool
	IncludeWrappers         bool
	Finalize                SpanFinalizer
}

// NodeOptions customizes an automatically discovered function. Nil booleans inherit
// the trace's defaults. Capture=false omits this node and reparents its descendants.
type NodeOptions struct {
	Name, Type, ExperimentID string
	// Deprecated: Use ExperimentID instead.
	TestRunID             string
	Capture, MockOnReplay *bool
	Finalize              SpanFinalizer
}

// MixedTracingError prevents combining opt-in spans and automatic subtrees.
type MixedTracingError struct{ Entered, Active string }

func (e *MixedTracingError) Error() string {
	return fmt.Sprintf("bitfab: cannot enter %s instrumentation inside %s instrumentation", e.Entered, e.Active)
}

func normalizeAutoSymbol(s string) string {
	s = strings.TrimSuffix(s, "-fm")
	if strings.IndexByte(s, '[') < 0 && strings.IndexByte(s, ']') < 0 {
		return s
	}
	var b strings.Builder
	depth := 0
	for _, r := range s {
		if r == '[' {
			depth++
			continue
		}
		if r == ']' {
			depth--
			continue
		}
		if depth == 0 {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func autoSymbol(fn any) (string, error) {
	if symbol, ok := fn.(string); ok && symbol != "" {
		return normalizeAutoSymbol(symbol), nil
	}
	v := reflect.ValueOf(fn)
	if !v.IsValid() || v.Kind() != reflect.Func || v.IsNil() {
		return "", fmt.Errorf("bitfab: Node requires a function or qualified function name")
	}
	f := runtime.FuncForPC(v.Pointer())
	if f == nil {
		return "", fmt.Errorf("bitfab: could not identify node function")
	}
	return normalizeAutoSymbol(f.Name()), nil
}

func autoRootName(symbol, key string) string {
	name := symbol[strings.LastIndex(symbol, "/")+1:]
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		name = name[dot+1:]
	}
	if name == "" || strings.Contains(name, ".func") {
		return key
	}
	return name
}

// Node registers policy for a function discovered by bitfab-instrument. It does
// not wrap the function or create spans outside a Trace invocation.
func (c *Client) Node(fn any, opts NodeOptions) error {
	if opts.Capture != nil && !*opts.Capture && opts.MockOnReplay != nil && *opts.MockOnReplay {
		return fmt.Errorf("bitfab: a node with Capture=false cannot enable MockOnReplay")
	}
	experimentID, err := resolveExperimentID(opts.ExperimentID, opts.TestRunID)
	if err != nil {
		return err
	}
	opts.ExperimentID, opts.TestRunID = experimentID, experimentID
	if opts.Type == "" {
		opts.Type = "custom"
	}
	symbol, err := autoSymbol(fn)
	if err != nil {
		return err
	}
	if opts.Type != "" && !validSpanTypes[opts.Type] {
		return fmt.Errorf("bitfab: invalid node type %q", opts.Type)
	}
	c.autoMu.Lock()
	defer c.autoMu.Unlock()
	if c.autoNodes == nil {
		c.autoNodes = make(map[string]NodeOptions)
	}
	if _, exists := c.autoNodes[symbol]; !exists {
		autoConfiguredNodes.Add(1)
	}
	c.autoNodes[symbol] = opts
	return nil
}

func (c *Client) nodeOptions(symbol string) (NodeOptions, bool) {
	c.autoMu.RLock()
	defer c.autoMu.RUnlock()
	opts, ok := c.autoNodes[normalizeAutoSymbol(symbol)]
	return opts, ok
}

type autoRoot struct {
	client                            *Client
	key, traceID                      string
	opts                              TraceOptions
	ctx                               context.Context
	mu                                sync.Mutex
	active, closing                   bool
	capturedSubtreeCount              int
	maxDepth, maxCapturedSubtreeSpans int
	records                           map[*autoRecord]bool
	rootRecord                        *autoRecord
	managed                           *managedTraceRoot
	complete                          func()
	completeOnce                      sync.Once
	closed                            atomic.Bool
	truncatedBy                       atomic.Uint32
	droppedSpans                      atomic.Int64
}

const (
	truncatedByMaxDepth uint32 = 1 << iota
	truncatedByMaxCapturedSubtreeSpans
)

var truncationLimitNames = []struct {
	flag uint32
	name string
}{
	{truncatedByMaxDepth, "max_depth"},
	{truncatedByMaxCapturedSubtreeSpans, "max_captured_subtree_spans"},
}

type autoRecord struct {
	root                                              *autoRoot
	id, parentID, name, kind, functionName, startedAt string
	input                                             []any
	links                                             map[string]any
	once                                              sync.Once
	bodyDone                                          bool
	mocked                                            bool
	mockSource                                        MockSource
	experimentID                                      string
	enrichment                                        *spanEnrichment
	planPolicy                                        simulationPlanPolicy
}

func (root *autoRoot) excluded(symbol, name string) bool {
	for _, pattern := range root.opts.Exclude {
		if matched, _ := path.Match(pattern, symbol); matched || pattern == symbol || pattern == name || pattern == symbol[strings.LastIndex(symbol, ".")+1:] {
			return true
		}
	}
	return false
}

func (root *autoRoot) drop(limit uint32) {
	if root.closed.Load() {
		return
	}
	root.truncatedBy.Or(limit)
	root.droppedSpans.Add(1)
}

func (root *autoRoot) add(parent *autoRecord, depth int, name, kind, symbol string, input []any, planPolicy simulationPlanPolicy) (*autoRecord, bool) {
	if root.closed.Load() || root.excluded(symbol, name) {
		return nil, false
	}
	if depth > root.maxDepth {
		root.drop(truncatedByMaxDepth)
		return nil, true
	}
	declared := planPolicy != simulationPlanApplies
	contentOff := !declared && root.client.httpClient.simulationPlan.isContentOff(root.key, name)
	root.mu.Lock()
	defer root.mu.Unlock()
	if !root.active {
		return nil, false
	}
	captured := !contentOff && !declared
	if captured && root.capturedSubtreeCount >= root.maxCapturedSubtreeSpans {
		root.drop(truncatedByMaxCapturedSubtreeSpans)
		return nil, true
	}
	r := &autoRecord{root: root, id: randomUUID(), name: name, kind: kind, functionName: symbol, startedAt: nowISOTimestamp(), input: input, planPolicy: planPolicy}
	if parent != nil {
		r.parentID = parent.id
	}
	if captured {
		root.capturedSubtreeCount++
	}
	root.records[r] = true
	return r, false
}

func (root *autoRoot) recordTruncation() {
	limits := root.truncatedBy.Load()
	if limits == 0 {
		return
	}
	state := getTraceState(root.traceID)
	if state == nil {
		return
	}
	names := make([]string, 0, len(truncationLimitNames))
	for _, limit := range truncationLimitNames {
		if limits&limit.flag != 0 {
			names = append(names, limit.name)
		}
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	state.Metadata["bitfab.truncated_by"] = strings.Join(names, ",")
	state.Metadata["bitfab.dropped_spans"] = int(root.droppedSpans.Load())
}

func (r *autoRecord) finish(output any, err error) {
	r.once.Do(func() {
		defer func() {
			r.root.mu.Lock()
			delete(r.root.records, r)
			r.root.mu.Unlock()
			r.root.tryComplete()
		}()
		defer func() {
			if recover() != nil {
				warnOnce("auto-span-send", "automatic span serialization failed; application results are unchanged")
			}
		}()
		data := map[string]any{"name": r.name, "type": r.kind, "function_name": r.functionName}
		var dropped []string
		contentOff := r.planPolicy != simulationPlanIgnored && r.root.client.httpClient.simulationPlan.withholdsContent(r.root.key, r.name, r.parentID == "", "trace")
		if contentOff {
			data["content_off_by_simulation_plan"] = true
		}
		if r.input != nil && !contentOff {
			value, fields := capValueReport(r.input)
			data["input"] = value
			dropped = append(dropped, fields...)
		}
		if output != nil && !contentOff {
			value, fields := capValueReport(output)
			data["output"] = value
			dropped = append(dropped, fields...)
		}
		if err != nil {
			data["error"] = err.Error()
			data["error_source"] = "code"
		}
		for k, v := range r.links {
			data[k] = v
		}
		r.enrichment.apply(data, contentOff)
		root := r.root
		if root.managed != nil && root.rootRecord == r {
			root.managed.mu.Lock()
			root.managed.data = data
			root.managed.mu.Unlock()
		} else if state := getTraceState(root.traceID); state == nil || !state.isDropped() {
			raw := map[string]any{"id": r.id, "trace_id": root.traceID, "started_at": r.startedAt, "ended_at": nowISOTimestamp(), "span_data": data, "span_origin": MakeSpanOrigin("trace")}
			if r.parentID != "" {
				raw["parent_id"] = r.parentID
			}
			payload := map[string]any{"id": r.id, "traceId": root.traceID, "type": "sdk-function", "source": "go-sdk-function", "sourceTraceId": root.traceID, "traceFunctionKey": root.key, "rootTraceFunctionKey": root.key, "rawSpan": raw}
			if replay := currentReplayContext(root.ctx); replay != nil && replay.inputSourceSpanID != "" {
				raw["input_source_span_id"] = replay.inputSourceSpanID
			}
			if state := getTraceState(root.traceID); state != nil && state.ExperimentID != "" {
				setExperimentIDKeys(payload, state.ExperimentID)
			}
			replay := currentReplayContext(root.ctx)
			if replay != nil && replay.experimentID != "" {
				setExperimentIDKeys(payload, replay.experimentID)
			}
			if r.experimentID != "" && (replay == nil || replay.experimentID == "") {
				setExperimentIDKeys(payload, r.experimentID)
			}
			if r.mocked {
				payload["mocked"] = true
				payload["mockTarget"] = "output"
				payload["mockSource"] = string(r.mockSource)
			}
			root.client.httpClient.sendExternalSpanWithPlanPolicy(payload, r.planPolicy, dropped...)
		}
	})
}

func (root *autoRoot) tryComplete() {
	root.mu.Lock()
	ready := root.closing && len(root.records) == 0 && (root.managed == nil || root.complete != nil)
	complete := root.complete
	root.mu.Unlock()
	if ready {
		root.completeOnce.Do(func() {
			root.recordTruncation()
			if complete != nil {
				complete()
			} else {
				root.client.sendTraceCompletion(root.key, root.traceID, root.rootRecord.startedAt, nowISOTimestamp())
			}
		})
	}
}

func (root *autoRoot) close() {
	root.mu.Lock()
	root.active = false
	root.closed.Store(true)
	root.closing = true
	var unfinished []*autoRecord
	for r := range root.records {
		if !r.bodyDone {
			r.bodyDone = true
			unfinished = append(unfinished, r)
		}
	}
	root.mu.Unlock()
	for _, r := range unfinished {
		r.finish(nil, fmt.Errorf("trace ended before this function completed"))
	}
	root.tryComplete()
}

func finishAutoRecords(records []*autoRecord, output any, err error, finalize SpanFinalizer) {
	live := make([]*autoRecord, 0, len(records))
	for _, r := range records {
		r.root.mu.Lock()
		if !r.bodyDone {
			r.bodyDone = true
			live = append(live, r)
		}
		r.root.mu.Unlock()
	}
	records = live
	if len(records) == 0 {
		return
	}
	finish := func() {
		value, finalErr := output, err
		if finalize != nil && err == nil {
			value, finalErr = runSpanFinalizer(output, finalize)
		}
		for _, r := range records {
			r.finish(value, finalErr)
		}
	}
	if finalize == nil || err != nil || len(records) == 0 {
		finish()
		return
	}
	clients := map[*Client]func(){}
	for _, r := range records {
		if _, ok := clients[r.root.client]; !ok {
			clients[r.root.client] = r.root.client.beginAutoFinalizer()
		}
	}
	go func() {
		defer func() {
			for _, done := range clients {
				done()
			}
		}()
		finish()
	}()
}

func (c *Client) beginAutoFinalizer() func() {
	c.autoMu.Lock()
	if c.autoPending == 0 {
		c.autoPendingDone = make(chan struct{})
	}
	c.autoPending++
	c.autoMu.Unlock()
	return func() {
		c.autoMu.Lock()
		c.autoPending--
		if c.autoPending == 0 {
			close(c.autoPendingDone)
		}
		c.autoMu.Unlock()
	}
}

func (c *Client) waitAutoFinalizers(timeout time.Duration) bool {
	c.autoMu.RLock()
	pending, done := c.autoPending, c.autoPendingDone
	c.autoMu.RUnlock()
	if pending == 0 {
		return true
	}
	if timeout <= 0 {
		return false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// Trace captures calls to first-party functions compiled with bitfab-instrument.
// Nested Trace invocations create linked independent roots except during replay
// and seed, where they remain in the item-owned trace.
func (c *Client) Trace(ctx context.Context, key string, fn SpanFunc, opts TraceOptions) (result any, err error) {
	if fn == nil {
		return nil, fmt.Errorf("bitfab: Trace requires a function")
	}
	if opts.MaxDepth != nil && *opts.MaxDepth < 0 || opts.MaxCapturedSubtreeSpans != nil && *opts.MaxCapturedSubtreeSpans < 0 {
		return nil, fmt.Errorf("bitfab: trace limits must be non-negative")
	}
	experimentID, err := resolveExperimentID(opts.ExperimentID, opts.TestRunID)
	if err != nil {
		return nil, err
	}
	opts.ExperimentID, opts.TestRunID = experimentID, experimentID
	if !c.shouldRecord(ctx) {
		return fn(ctx)
	}
	previous := currentAutoScope(ctx)
	managed, _ := ctx.Value(managedTraceRootKey{}).(*managedTraceRoot)
	managedParent := false
	if managed != nil {
		managed.mu.Lock()
		managedParent = currentSpan(ctx) != nil && currentSpan(ctx).spanID == managed.spanID
		managed.mu.Unlock()
	}
	if previous != nil && previous.explicit || currentSpan(ctx) != nil && (previous == nil || len(previous.frames) == 0) && !managedParent {
		return nil, &MixedTracingError{Entered: "trace", Active: "span"}
	}
	if opts.CaptureWhen == CaptureWhenNested && (previous == nil || len(previous.frames) == 0) {
		return fn(ctx)
	}
	symbol, _ := autoSymbol(fn)
	if opts.Name == "" {
		opts.Name = autoRootName(symbol, key)
	}
	if opts.Type == "" {
		opts.Type = "custom"
	}
	if !validSpanTypes[opts.Type] {
		opts.Type = "custom"
	}
	if previous == nil || len(previous.frames) == 0 {
		c.httpClient.simulationPlan.refresh()
		c.httpClient.simulationPlan.awaitFirstRead(ctx)
	}
	maxDepth, maxCapturedSubtreeSpans := 30, 500
	if opts.MaxDepth != nil {
		maxDepth = *opts.MaxDepth
	}
	if opts.MaxCapturedSubtreeSpans != nil {
		maxCapturedSubtreeSpans = *opts.MaxCapturedSubtreeSpans
	}
	var frames []autoFrame
	var records []*autoRecord
	if previous != nil {
		frames = append(frames, previous.frames...)
	}
	for i, frame := range frames {
		r, _ := frame.root.add(frame.parent, frame.depth+1, opts.Name, "function", symbol, opts.Input, simulationPlanIgnored)
		if r != nil {
			records = append(records, r)
			frames[i] = autoFrame{root: frame.root, parent: r, depth: frame.depth + 1}
		}
	}
	var root *autoRoot
	if len(frames) == 0 || currentReplayContext(ctx) == nil && seedFromContext(ctx) == nil {
		root = &autoRoot{client: c, key: key, traceID: randomUUID(), opts: opts, maxDepth: maxDepth, maxCapturedSubtreeSpans: maxCapturedSubtreeSpans, ctx: ctx, active: true, records: map[*autoRecord]bool{}}
		if len(frames) == 0 && managed != nil && currentSpan(ctx) != nil {
			root.traceID = currentSpan(ctx).traceID
			root.managed = managed
		}
		state := createTraceState(root.traceID)
		if state.DBSnapshotRef == nil && seedFromContext(ctx) == nil {
			state.DBSnapshotRef = c.buildDBSnapshotRef(state.StartedAt)
		}
		replay := currentReplayContext(ctx)
		if replay != nil {
			state.ExperimentID = replay.experimentID
			state.replay = replay
			state.InputSourceTraceID = replay.inputSourceTraceID
		}
		if opts.ExperimentID != "" && (replay == nil || replay.experimentID == "") {
			state.ExperimentID = opts.ExperimentID
		}
		r := &autoRecord{root: root, id: randomUUID(), name: opts.Name, kind: opts.Type, functionName: symbol, startedAt: nowISOTimestamp(), input: opts.Input, links: map[string]any{}}
		if root.managed != nil {
			r.id = currentSpan(ctx).spanID
			managed.mu.Lock()
			managed.root = root
			managed.mu.Unlock()
		}
		root.rootRecord = r
		root.records[r] = true
		if len(frames) > 0 {
			outer := frames[len(frames)-1].parent
			r.links["enclosing_trace_id"] = outer.root.traceID
			r.links["enclosing_span_id"] = outer.id
			r.links["enclosing_trace_function_key"] = outer.root.key
			for _, copy := range records {
				copy.links = map[string]any{"nested_trace_id": root.traceID, "nested_trace_function_key": key, "nested_root_span_id": r.id}
			}
		}
		records = append(records, r)
		frames = append(frames, autoFrame{root: root, parent: r})
	}
	scope := &autoScope{frames: frames, skipName: symbol}
	childCtx := context.WithValue(ctx, autoScopeContextKey{}, scope)
	if len(frames) > 0 {
		frame := frames[len(frames)-1]
		childCtx = withSpanContext(context.WithValue(childCtx, spanStackKey{}, []spanEntry(nil)), frame.root.traceID, frame.parent.id)
		var enrichment *spanEnrichment
		if existing, ok := childCtx.Value(spanEnrichmentKey{}).(*spanEnrichment); ok && existing.id == frame.parent.id {
			enrichment = existing
		} else {
			childCtx, enrichment = withSpanEnrichment(childCtx, frame.parent.id)
		}
		for _, record := range records {
			record.enrichment = enrichment
		}
	}
	autoActiveRoots.Add(1)
	restore := pushAutoScope(scope)
	finalize := opts.Finalize
	defer func() {
		p := recover()
		restore()
		autoActiveRoots.Add(-1)
		if p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
		finishAutoRecords(records, result, err, finalize)
		if root != nil {
			root.close()
		}
		if p != nil {
			panic(p)
		}
	}()
	if root == nil && len(records) > 0 && currentReplayContext(ctx) != nil {
		record := records[len(records)-1]
		value, mocked, source, mockErr := record.root.client.resolveReplayMock(ctx, record.root.key, spanConfig{name: record.name, spanType: "function", input: opts.Input}, false)
		if mocked {
			finalize = nil
			for _, record := range records {
				record.mocked = mockErr == nil
				record.mockSource = source
			}
			return value, mockErr
		}
	}
	return fn(childCtx)
}

func prepareManagedAutoSpan(ctx context.Context, id spanIdentity) {
	managed, _ := ctx.Value(managedTraceRootKey{}).(*managedTraceRoot)
	if managed != nil && id.isRootSpan {
		managed.mu.Lock()
		managed.spanID = id.spanID
		managed.mu.Unlock()
	}
}

func finishManagedAutoSpan(ctx context.Context, spanID string, send func()) {
	managed, _ := ctx.Value(managedTraceRootKey{}).(*managedTraceRoot)
	if managed == nil {
		send()
		return
	}
	managed.mu.Lock()
	root := managed.root
	isRoot := managed.spanID == spanID
	managed.mu.Unlock()
	if root == nil || !isRoot {
		send()
		return
	}
	root.mu.Lock()
	root.complete = send
	root.mu.Unlock()
	root.tryComplete()
}

func managedAutoSpanDropped(ctx context.Context, plan *simulationPlan, spanID, key, name string, root bool) bool {
	if managed, _ := ctx.Value(managedTraceRootKey{}).(*managedTraceRoot); managed != nil {
		managed.mu.Lock()
		if managed.root != nil && managed.spanID == spanID {
			key = managed.root.key
			name, _ = managed.data["name"].(string)
		}
		managed.mu.Unlock()
	}
	return plan.dropsSpan(key, name, root)
}

func managedAutoSpanContentOff(ctx context.Context, plan *simulationPlan, spanID, key, name string, root bool) bool {
	instrumentation := "span"
	if managed, _ := ctx.Value(managedTraceRootKey{}).(*managedTraceRoot); managed != nil {
		managed.mu.Lock()
		if managed.root != nil && managed.spanID == spanID {
			key = managed.root.key
			name, _ = managed.data["name"].(string)
			instrumentation = "trace"
		}
		managed.mu.Unlock()
	}
	return plan.withholdsContent(key, name, root, instrumentation)
}

func stampManagedAutoSpan(ctx context.Context, raw, payload map[string]any) {
	managed, _ := ctx.Value(managedTraceRootKey{}).(*managedTraceRoot)
	if managed == nil {
		return
	}
	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.root == nil {
		return
	}
	if raw["id"] != managed.spanID {
		return
	}
	data := make(map[string]any, len(managed.data)+1)
	for key, value := range managed.data {
		data[key] = value
	}
	if _, hasInput := data["input"]; !hasInput && data["content_off_by_simulation_plan"] != true {
		if original, ok := raw["span_data"].(map[string]any); ok {
			if input, present := original["input"]; present {
				data["input"] = input
			}
		}
	}
	raw["span_data"] = data
	raw["span_origin"] = MakeSpanOrigin("trace")
	payload["rootTraceFunctionKey"] = managed.root.key
}
