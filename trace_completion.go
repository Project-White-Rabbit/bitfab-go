package bitfab

import "sync"

type completionState struct {
	spanIDs  map[string]struct{}
	active   map[string]struct{}
	complete func(int)
}

type traceCompletion struct {
	mu          sync.Mutex
	traces      map[string]*completionState
	closed      map[string]int
	closedOrder []string
}

func (t *traceCompletion) state(traceID string) *completionState {
	if t.traces == nil {
		t.traces = make(map[string]*completionState)
	}
	state := t.traces[traceID]
	if state == nil {
		state = &completionState{spanIDs: make(map[string]struct{}), active: make(map[string]struct{})}
		t.traces[traceID] = state
	}
	return state
}

func (t *traceCompletion) start(traceID, spanID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, closed := t.closed[traceID]; closed {
		return
	}
	state := t.state(traceID)
	state.spanIDs[spanID] = struct{}{}
	state.active[spanID] = struct{}{}
}

func (t *traceCompletion) open(traceID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, closed := t.closed[traceID]; !closed {
		t.state(traceID)
	}
}

func (t *traceCompletion) record(traceID, spanID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, closed := t.closed[traceID]; !closed {
		t.state(traceID).spanIDs[spanID] = struct{}{}
	}
}

func (t *traceCompletion) end(traceID, spanID string) {
	t.mu.Lock()
	state := t.traces[traceID]
	if state == nil {
		t.mu.Unlock()
		return
	}
	delete(state.active, spanID)
	complete, count := t.drain(traceID, state)
	t.mu.Unlock()
	if complete != nil {
		complete(count)
	}
}

func (t *traceCompletion) abort(traceID, spanID string) {
	t.mu.Lock()
	state := t.traces[traceID]
	if state == nil {
		t.mu.Unlock()
		return
	}
	delete(state.active, spanID)
	delete(state.spanIDs, spanID)
	if len(state.active) == 0 && len(state.spanIDs) == 0 && state.complete == nil {
		delete(t.traces, traceID)
		t.mu.Unlock()
		return
	}
	complete, count := t.drain(traceID, state)
	t.mu.Unlock()
	if complete != nil {
		complete(count)
	}
}

func (t *traceCompletion) close(traceID string, complete func(int), dropped ...bool) {
	t.mu.Lock()
	if count, closed := t.closed[traceID]; closed {
		t.mu.Unlock()
		complete(count)
		return
	}
	state := t.traces[traceID]
	if state == nil {
		t.mu.Unlock()
		return
	}
	state.complete = complete
	if len(dropped) > 0 && dropped[0] {
		clear(state.active)
	}
	ready, count := t.drain(traceID, state)
	t.mu.Unlock()
	if ready != nil {
		ready(count)
	}
}

func (t *traceCompletion) drain(traceID string, state *completionState) (func(int), int) {
	if len(state.active) != 0 || state.complete == nil {
		return nil, 0
	}
	delete(t.traces, traceID)
	count := len(state.spanIDs)
	if t.closed == nil {
		t.closed = make(map[string]int)
	}
	t.closed[traceID] = count
	t.closedOrder = append(t.closedOrder, traceID)
	if len(t.closedOrder) > 1024 {
		delete(t.closed, t.closedOrder[0])
		t.closedOrder = t.closedOrder[1:]
	}
	return state.complete, count
}
