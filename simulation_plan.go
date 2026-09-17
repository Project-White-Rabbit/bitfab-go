package bitfab

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const simulationPlanReadTimeout = 5 * time.Second
const simulationPlanRefreshInterval = 60 * time.Second
const simulationPlanRetryInterval = 10 * time.Second
const simulationPlanMaxHeld = 1000

type simulationPlanPolicy uint8

const (
	simulationPlanApplies simulationPlanPolicy = iota
	simulationPlanIgnored
)

type heldPlanRecord struct {
	payload map[string]any
	key     string
	policy  simulationPlanPolicy
	submit  func(map[string]any)
	discard func()
}

type simulationPlan struct {
	mu           sync.Mutex
	read         func() (map[string]any, error)
	disabled     bool
	contentOff   map[string]map[string]bool
	unavailable  bool
	closing      bool
	drainMu      sync.Mutex
	refreshAfter time.Time
	held         []heldPlanRecord
	draining     map[string]bool
	reading      chan struct{}
	stopCh       chan struct{}
	stopped      bool

	firstReadWait     time.Duration
	firstReadDeadline time.Time
	firstReadDone     chan struct{}
	firstReadOnce     sync.Once
}

func newSimulationPlan(read func() (map[string]any, error), enabled bool) *simulationPlan {
	return &simulationPlan{read: read, disabled: !enabled, stopCh: make(chan struct{}), firstReadWait: simulationPlanReadTimeout, firstReadDone: make(chan struct{})}
}

func (p *simulationPlan) finishFirstRead() {
	p.firstReadOnce.Do(func() { close(p.firstReadDone) })
}

func (p *simulationPlan) awaitFirstRead(ctx context.Context) {
	if p == nil || p.isDisabled() {
		return
	}
	p.mu.Lock()
	started := !p.firstReadDeadline.IsZero()
	remaining := time.Until(p.firstReadDeadline)
	pending := started && !p.stopped && p.contentOff == nil && remaining > 0
	p.mu.Unlock()
	if !pending {
		return
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-p.firstReadDone:
	case <-timer.C:
	case <-ctx.Done():
	case <-p.stopCh:
	}
}

func (p *simulationPlan) isDisabled() bool {
	return p.disabled || strings.TrimSpace(os.Getenv("BITFAB_DISABLE_SIM_PLAN")) != ""
}

func (p *simulationPlan) refresh() {
	if p == nil || p.isDisabled() {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.stopped && p.reading == nil && !time.Now().Before(p.refreshAfter) {
		p.startReader(0)
	}
}

func (p *simulationPlan) startReader(delay time.Duration) {
	p.reading = make(chan struct{})
	if p.firstReadDeadline.IsZero() {
		p.firstReadDeadline = time.Now().Add(delay + p.firstReadWait)
	}
	go p.readPlan(delay, p.reading)
}

func (p *simulationPlan) pause(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-p.stopCh:
		return false
	}
}

func parseSimulationPlan(body map[string]any) (map[string]map[string]bool, error) {
	nodes, ok := body["nodes"].([]any)
	if !ok {
		return nil, fmt.Errorf("simulation plan response must contain a nodes array")
	}
	names := make(map[string]map[string]bool)
	for _, value := range nodes {
		node, ok := value.(map[string]any)
		if !ok {
			continue
		}
		key, keyOK := node["traceFunctionKey"].(string)
		name, nameOK := node["name"].(string)
		capture, captureOK := node["captureContent"].(bool)
		if keyOK && nameOK && captureOK && !capture {
			if names[key] == nil {
				names[key] = make(map[string]bool)
			}
			names[key][name] = true
		}
	}
	return names, nil
}

func (p *simulationPlan) readOnce() (result map[string]map[string]bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			warnOnce("sim-plan-unavailable", fmt.Sprintf("simulation plan read failed: %v", recovered))
			result = nil
		}
	}()
	body, err := p.read()
	if statusCode(err) == 404 {
		return map[string]map[string]bool{}
	}
	if err != nil {
		warnOnce("sim-plan-unavailable", fmt.Sprintf("could not read simulation plan: %v", err))
		return nil
	}
	parsed, err := parseSimulationPlan(body)
	if err != nil {
		warnOnce("sim-plan-unreadable", err.Error())
		return nil
	}
	return parsed
}

func (p *simulationPlan) readPlan(delay time.Duration, done chan struct{}) {
	defer func() {
		p.mu.Lock()
		p.draining = nil
		p.reading = nil
		close(done)
		p.mu.Unlock()
		p.finishFirstRead()
	}()
	if delay > 0 && !p.pause(delay) {
		return
	}
	p.mu.Lock()
	skipped := p.stopped || p.isDisabled()
	p.mu.Unlock()
	var parsed map[string]map[string]bool
	if !skipped {
		parsed = p.readOnce()
	}
	p.drainMu.Lock()
	defer p.drainMu.Unlock()
	p.mu.Lock()
	if parsed != nil {
		p.contentOff = parsed
		p.unavailable = false
		p.refreshAfter = time.Now().Add(simulationPlanRefreshInterval)
	} else {
		p.refreshAfter = time.Now().Add(simulationPlanRetryInterval)
		if !skipped && p.contentOff == nil {
			p.unavailable = true
		}
	}
	entries := p.takeHeld()
	p.mu.Unlock()
	p.finishFirstRead()
	p.drain(entries)
}

func (p *simulationPlan) takeHeld() []heldPlanRecord {
	entries := p.held
	p.held = nil
	if len(entries) > 0 {
		p.draining = make(map[string]bool)
		for _, entry := range entries {
			if entry.key != "" {
				p.draining[planTraceID(entry.payload)] = true
			}
		}
	}
	return entries
}

func (p *simulationPlan) drain(entries []heldPlanRecord) {
	for len(entries) > 0 {
		for _, entry := range entries {
			p.submit(entry)
		}
		p.mu.Lock()
		entries, p.held = p.held, nil
		if len(entries) == 0 {
			p.draining = nil
		}
		p.mu.Unlock()
	}
}

func (p *simulationPlan) submit(entry heldPlanRecord) {
	defer func() {
		if value := recover(); value != nil {
			warnOnce("sim-plan-held-span-dropped", fmt.Sprintf("held record could not be sent: %v", value))
		}
	}()
	if entry.key == "" {
		entry.submit(entry.payload)
	} else {
		p.deliver(entry)
	}
}

func (p *simulationPlan) deliver(entry heldPlanRecord) {
	if entry.policy == simulationPlanIgnored {
		entry.submit(entry.payload)
		return
	}
	if payload, send := p.prepare(entry.payload, entry.key); send {
		entry.submit(payload)
	} else {
		entry.discard()
	}
}

func (p *simulationPlan) submitWithoutPlan(entry heldPlanRecord) {
	defer func() {
		if value := recover(); value != nil {
			warnOnce("sim-plan-held-span-dropped", fmt.Sprintf("held record could not be sent: %v", value))
		}
	}()
	if entry.key == "" || entry.policy == simulationPlanIgnored {
		entry.submit(entry.payload)
	} else {
		entry.submit(withoutPlanContent(entry.payload))
	}
}

func (p *simulationPlan) prepare(payload map[string]any, key string) (map[string]any, bool) {
	p.mu.Lock()
	loaded, withheld := p.contentOff != nil, p.unavailable || p.closing || p.stopped
	p.mu.Unlock()
	if loaded {
		return p.apply(payload, key)
	}
	if withheld {
		return withoutPlanContent(payload), true
	}
	return payload, true
}

func planTraceID(payload map[string]any) string {
	for _, key := range []string{"traceId", "sourceTraceId", "id"} {
		if id, ok := payload[key].(string); ok {
			return id
		}
	}
	return ""
}

func recordedByFramework(payload map[string]any) bool {
	raw, _ := payload["rawSpan"].(map[string]any)
	origin, _ := raw["span_origin"].(map[string]any)
	instrumentation, _ := origin["instrumentation"].(map[string]any)
	return frameworkInstrumentation(instrumentation["name"])
}

func frameworkInstrumentation(name any) bool {
	switch name {
	case "openai-agents", "langgraph", "claude-agent-sdk", "vercel-ai":
		return true
	}
	return false
}

// MakeSpanOrigin identifies which SDK and instrumentation produced a span.
func MakeSpanOrigin(instrumentation string) map[string]any {
	return map[string]any{"name": "bitfab.sdk.go", "version": Version, "instrumentation": map[string]any{"name": instrumentation}}
}

func (p *simulationPlan) hold(entry heldPlanRecord) (overflow heldPlanRecord, overflowed bool) {
	p.held = append(p.held, entry)
	if len(p.held) > simulationPlanMaxHeld {
		overflow, overflowed = p.held[0], true
		p.held[0] = heldPlanRecord{}
		p.held = p.held[1:]
		warnOnce("sim-plan-held-overflow", "simulation plan has not loaded; sending the oldest records beyond 1000 without inputs and outputs")
	}
	if p.reading == nil && !p.stopped && !p.isDisabled() {
		p.startReader(max(time.Until(p.refreshAfter), 0))
	}
	return overflow, overflowed
}

func (p *simulationPlan) holdAndRelease(entry heldPlanRecord) {
	overflow, overflowed := p.hold(entry)
	p.mu.Unlock()
	if overflowed {
		p.submitWithoutPlan(overflow)
	}
}

func (p *simulationPlan) sendSpan(payload map[string]any, policy simulationPlanPolicy, submit func(map[string]any), discard func()) {
	if p == nil || p.isDisabled() {
		submit(payload)
		return
	}
	p.refresh()
	key, _ := payload["rootTraceFunctionKey"].(string)
	if key == "" {
		key, _ = payload["traceFunctionKey"].(string)
	}
	raw, _ := payload["rawSpan"].(map[string]any)
	data, _ := raw["span_data"].(map[string]any)
	if key == "" || data["name"] == nil || recordedByFramework(payload) {
		submit(payload)
		return
	}
	p.mu.Lock()
	entry := heldPlanRecord{payload, key, policy, submit, discard}
	if !p.stopped && p.contentOff == nil && !p.unavailable && !p.closing {
		p.holdAndRelease(entry)
		return
	}
	p.mu.Unlock()
	p.deliver(entry)
}

func (p *simulationPlan) sendTrace(payload map[string]any, submit func(map[string]any)) {
	if p == nil || p.isDisabled() {
		submit(payload)
		return
	}
	p.refresh()
	id := planTraceID(payload)
	p.mu.Lock()
	held := p.draining[id]
	for _, entry := range p.held {
		if entry.key != "" && planTraceID(entry.payload) == id {
			held = true
			break
		}
	}
	if !p.stopped && id != "" && held {
		p.holdAndRelease(heldPlanRecord{payload, "", simulationPlanApplies, submit, nil})
		return
	}
	p.mu.Unlock()
	submit(payload)
}

func (p *simulationPlan) isContentOff(key, name string) bool {
	if p == nil || p.isDisabled() || key == "" || name == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.stopped && p.contentOff[key][name]
}

func (p *simulationPlan) withholdsContent(key, name string, root bool, instrumentation string) bool {
	if p == nil || p.isDisabled() || key == "" || name == "" || frameworkInstrumentation(instrumentation) {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return false
	}
	if p.contentOff != nil {
		return p.contentOff[key][name]
	}
	return p.unavailable && !root
}

func (p *simulationPlan) apply(payload map[string]any, key string) (map[string]any, bool) {
	if recordedByFramework(payload) {
		return payload, true
	}
	raw, _ := payload["rawSpan"].(map[string]any)
	data, _ := raw["span_data"].(map[string]any)
	name, _ := data["name"].(string)
	p.mu.Lock()
	off := p.contentOff[key][name]
	p.mu.Unlock()
	if !off {
		return payload, true
	}
	if parent, _ := raw["parent_id"].(string); parent != "" && data["error"] == nil {
		return nil, false
	}
	return stripPlanContent(payload), true
}

func withoutPlanContent(payload map[string]any) map[string]any {
	raw, _ := payload["rawSpan"].(map[string]any)
	if parent, _ := raw["parent_id"].(string); parent == "" || recordedByFramework(payload) {
		return payload
	}
	warnOnce("sim-plan-unavailable-content-withheld", "simulation plan could not be read; sending spans without inputs and outputs until it loads")
	return stripPlanContent(payload)
}

func stripPlanContent(payload map[string]any) map[string]any {
	raw, _ := payload["rawSpan"].(map[string]any)
	data, _ := raw["span_data"].(map[string]any)
	kept := make(map[string]any, len(data))
	for field, value := range data {
		switch field {
		case "input", "input_meta", "output", "output_meta", "input_serialized", "output_serialized", "prompt":
			continue
		}
		kept[field] = value
	}
	kept["content_off_by_simulation_plan"] = true
	clonedRaw := make(map[string]any, len(raw))
	for field, value := range raw {
		clonedRaw[field] = value
	}
	clonedRaw["span_data"] = kept
	cloned := make(map[string]any, len(payload))
	for field, value := range payload {
		cloned[field] = value
	}
	cloned["rawSpan"] = clonedRaw
	return cloned
}

func (p *simulationPlan) release(timeout time.Duration, final bool) bool {
	if p == nil {
		return true
	}
	p.refresh()
	p.mu.Lock()
	done := p.reading
	waiting := len(p.held) > 0 || len(p.draining) > 0
	p.mu.Unlock()
	if done != nil && waiting {
		timer := time.NewTimer(max(timeout, 0))
		select {
		case <-done:
		case <-timer.C:
		}
		timer.Stop()
	}
	if final {
		p.flushHeld()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.held) == 0 && len(p.draining) == 0
}

func (p *simulationPlan) flushHeld() {
	p.drainMu.Lock()
	defer p.drainMu.Unlock()
	p.mu.Lock()
	p.closing = true
	entries := p.takeHeld()
	p.mu.Unlock()
	p.drain(entries)
}

func (p *simulationPlan) stop() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.stopped {
		p.stopped = true
		close(p.stopCh)
	}
	p.mu.Unlock()
	p.flushHeld()
}

func (h *httpClient) getSimulationPlan() (map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), simulationPlanReadTimeout)
	defer cancel()
	var body map[string]any
	err := h.get(ctx, "/api/sdk/sim-plan", &body)
	return body, err
}
