package bitfab

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func planTestSpan() map[string]any {
	return map[string]any{"traceId": "t", "traceFunctionKey": "child", "rootTraceFunctionKey": "root", "rawSpan": map[string]any{"span_origin": MakeSpanOrigin("trace"), "span_data": map[string]any{"name": "secret", "input": "private", "input_meta": "private", "output": "private", "output_meta": "private", "input_serialized": "private", "output_serialized": "private", "error": "failure"}}}
}
func planTestPolicy() map[string]any {
	return map[string]any{"nodes": []any{map[string]any{"traceFunctionKey": "root", "name": "secret", "captureContent": false}}}
}

func TestSimulationPlan_HoldStripAndDrain(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	entered, release := make(chan struct{}), make(chan struct{})
	plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return planTestPolicy(), nil }, true)
	defer plan.stop()
	var mu sync.Mutex
	var sent []map[string]any
	submit := func(record map[string]any) { mu.Lock(); defer mu.Unlock(); sent = append(sent, record) }
	original := planTestSpan()
	plan.sendSpan(original, submit)
	<-entered
	plan.sendTrace(map[string]any{"traceId": "t", "completed": true}, submit)
	plan.sendTrace(map[string]any{"traceId": "unrelated", "completed": true}, submit)
	mu.Lock()
	if len(sent) != 1 || sent[0]["traceId"] != "unrelated" {
		t.Errorf("before load: %#v", sent)
	}
	mu.Unlock()
	close(release)
	if !plan.release(time.Second) {
		t.Fatal("plan did not drain")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 3 || sent[2]["completed"] != true {
		t.Fatalf("records: %#v", sent)
	}
	data := sent[1]["rawSpan"].(map[string]any)["span_data"].(map[string]any)
	for _, key := range []string{"input", "output", "input_meta", "output_meta", "input_serialized", "output_serialized"} {
		if _, ok := data[key]; ok {
			t.Errorf("leaked %s", key)
		}
	}
	if data["error"] != "failure" || data["content_off_by_simulation_plan"] != true {
		t.Errorf("lost shape: %#v", data)
	}
	if original["rawSpan"].(map[string]any)["span_data"].(map[string]any)["input"] != "private" {
		t.Error("mutated input")
	}
}

func TestSimulationPlan_CompletionWaitsForSubmission(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	entered, release, draining, submitted := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return planTestPolicy(), nil }, true)
	defer plan.stop()
	completed := make(chan struct{})
	plan.sendSpan(planTestSpan(), func(map[string]any) { close(draining); <-submitted })
	<-entered
	close(release)
	<-draining
	plan.sendTrace(map[string]any{"traceId": "t", "completed": true}, func(map[string]any) { close(completed) })
	if plan.release(0) {
		t.Error("flush completed before submit")
	}
	select {
	case <-completed:
		t.Error("completion overtook span")
	default:
	}
	close(submitted)
	if !plan.release(time.Second) {
		t.Fatal("did not drain")
	}
	select {
	case <-completed:
	default:
		t.Error("missing completion")
	}
}

func TestSimulationPlan_FrameworkAndMissingEndpoint(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	plan := newSimulationPlan(func() (map[string]any, error) { return nil, &httpStatusError{StatusCode: 404} }, true)
	defer plan.stop()
	sent := make(chan map[string]any, 2)
	span := planTestSpan()
	span["rawSpan"].(map[string]any)["span_origin"] = MakeSpanOrigin("langgraph")
	plan.sendSpan(span, func(p map[string]any) { sent <- p })
	select {
	case got := <-sent:
		if got["rawSpan"].(map[string]any)["span_data"].(map[string]any)["input"] != "private" {
			t.Error("framework stripped")
		}
	default:
		t.Fatal("framework held")
	}
	plan.sendSpan(planTestSpan(), func(p map[string]any) { sent <- p })
	if !plan.release(time.Second) {
		t.Fatal("404 failed to release")
	}
	select {
	case <-sent:
	default:
		t.Fatal("404 held content")
	}
}

func TestSimulationPlan_FailedReadBoundAndDisabled(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	entered, release := make(chan struct{}), make(chan struct{})
	plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return nil, errors.New("offline") }, true)
	defer plan.stop()
	sent := make(chan struct{}, 1)
	for i := 0; i < simulationPlanMaxHeld+1; i++ {
		plan.sendSpan(planTestSpan(), func(map[string]any) { sent <- struct{}{} })
	}
	<-entered
	plan.mu.Lock()
	n := len(plan.held)
	plan.mu.Unlock()
	if n != simulationPlanMaxHeld {
		t.Errorf("held %d", n)
	}
	if plan.release(0) {
		t.Error("failed read considered flushed")
	}
	select {
	case <-sent:
		t.Error("leaked unread content")
	default:
	}
	close(release)
	disabled := newSimulationPlan(func() (map[string]any, error) { t.Error("disabled read"); return nil, nil }, false)
	defer disabled.stop()
	disabled.sendSpan(planTestSpan(), func(map[string]any) { sent <- struct{}{} })
	select {
	case <-sent:
	default:
		t.Error("disabled held content")
	}
}

func TestSimulationPlan_ClientFlushReportsUnsubmittedHeldRecords(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	entered, release := make(chan struct{}), make(chan struct{})
	plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return map[string]any{}, nil }, true)
	defer close(release)
	client := NewClient("key", WithSimulationPlan(false))
	client.httpClient.simulationPlan = plan
	defer client.Close(0)
	plan.sendSpan(planTestSpan(), func(map[string]any) {})
	<-entered
	if client.FlushTraces(0) {
		t.Fatal("flush reported delivery while records remain held")
	}
	if client.Close(0) {
		t.Fatal("close reported delivery after dropping held records")
	}
	if client.Close(0) {
		t.Fatal("repeated close forgot the dropped held records")
	}
}
