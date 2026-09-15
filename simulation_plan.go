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

type heldPlanRecord struct {
	payload map[string]any
	key     string
	submit  func(map[string]any)
}

type simulationPlan struct {
	mu           sync.Mutex
	read         func() (map[string]any, error)
	disabled     bool
	contentOff   map[string]map[string]bool
	refreshAfter time.Time
	held         []heldPlanRecord
	draining     map[string]bool
	reading      chan struct{}
	stopCh       chan struct{}
	stopped      bool
}

func newSimulationPlan(read func() (map[string]any, error), enabled bool) *simulationPlan {
	return &simulationPlan{read: read, disabled: !enabled, stopCh: make(chan struct{})}
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
	go p.readUntilLoaded(delay, p.reading)
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
			warnOnce("sim-plan-unavailable", fmt.Sprintf("simulation plan read failed: %v; holding content until it loads", recovered))
			result = nil
		}
	}()
	body, err := p.read()
	if statusCode(err) == 404 {
		return map[string]map[string]bool{}
	}
	if err != nil {
		warnOnce("sim-plan-unavailable", fmt.Sprintf("could not read simulation plan: %v; holding content until it loads", err))
		return nil
	}
	parsed, err := parseSimulationPlan(body)
	if err != nil {
		warnOnce("sim-plan-unreadable", err.Error())
		return nil
	}
	return parsed
}

func (p *simulationPlan) readUntilLoaded(delay time.Duration, done chan struct{}) {
	defer func() {
		p.mu.Lock()
		p.draining = nil
		p.reading = nil
		close(done)
		p.mu.Unlock()
	}()
	if delay > 0 && !p.pause(delay) {
		return
	}
	for {
		p.mu.Lock()
		stopped := p.stopped
		p.mu.Unlock()
		var parsed map[string]map[string]bool
		if !stopped && !p.isDisabled() {
			parsed = p.readOnce()
		}
		p.mu.Lock()
		var entries []heldPlanRecord
		finished := false
		if parsed != nil {
			p.contentOff = parsed
			p.refreshAfter = time.Now().Add(simulationPlanRefreshInterval)
			entries, p.held = p.held, nil
			p.draining = make(map[string]bool)
			for _, entry := range entries {
				if entry.key != "" {
					p.draining[planTraceID(entry.payload)] = true
				}
			}
			finished = true
		} else if p.contentOff != nil || p.stopped || p.isDisabled() || len(p.held) == 0 {
			p.refreshAfter = time.Now().Add(simulationPlanRetryInterval)
			finished = true
		}
		p.mu.Unlock()
		if finished {
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
			return
		}
		if !p.pause(simulationPlanRetryInterval) {
			return
		}
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
		entry.submit(p.apply(entry.payload, entry.key))
	}
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
	switch instrumentation["name"] {
	case "openai-agents", "langgraph", "claude-agent-sdk", "vercel-ai":
		return true
	}
	return false
}

// MakeSpanOrigin identifies which SDK and instrumentation produced a span.
func MakeSpanOrigin(instrumentation string) map[string]any {
	return map[string]any{"name": "bitfab.sdk.go", "version": Version, "instrumentation": map[string]any{"name": instrumentation}}
}

func (p *simulationPlan) hold(entry heldPlanRecord) {
	p.held = append(p.held, entry)
	if len(p.held) > simulationPlanMaxHeld {
		p.held[0] = heldPlanRecord{}
		p.held = p.held[1:]
		warnOnce("sim-plan-held-overflow", "simulation plan has not loaded; dropping oldest records beyond 1000")
	}
	if p.reading == nil && !p.stopped && !p.isDisabled() {
		p.startReader(max(time.Until(p.refreshAfter), 0))
	}
}

func (p *simulationPlan) sendSpan(payload map[string]any, submit func(map[string]any)) {
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
	if p.stopped {
		p.mu.Unlock()
		submit(payload)
		return
	}
	if p.contentOff == nil {
		p.hold(heldPlanRecord{payload, key, submit})
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	submit(p.apply(payload, key))
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
		p.hold(heldPlanRecord{payload, "", submit})
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	submit(payload)
}

func (p *simulationPlan) apply(payload map[string]any, key string) map[string]any {
	if recordedByFramework(payload) {
		return payload
	}
	raw, _ := payload["rawSpan"].(map[string]any)
	data, _ := raw["span_data"].(map[string]any)
	name, _ := data["name"].(string)
	p.mu.Lock()
	off := p.contentOff[key][name]
	p.mu.Unlock()
	if !off {
		return payload
	}
	kept := make(map[string]any, len(data))
	for field, value := range data {
		switch field {
		case "input", "input_meta", "output", "output_meta", "input_serialized", "output_serialized":
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

func (p *simulationPlan) release(timeout time.Duration) bool {
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.contentOff == nil && len(p.held) > 0 {
		warnOnce("sim-plan-never-loaded", "records remain held because simulation plan has not loaded")
	}
	return len(p.held) == 0 && len(p.draining) == 0
}

func (p *simulationPlan) stop() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.stopped {
		p.stopped = true
		close(p.stopCh)
		p.held = nil
	}
}

func (h *httpClient) getSimulationPlan() (map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), simulationPlanReadTimeout)
	defer cancel()
	var body map[string]any
	err := h.getWithConnectionClose(ctx, "/api/sdk/sim-plan", &body, true)
	return body, err
}
