package bitfab

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func planTestSpan() map[string]any {
	return map[string]any{"traceId": "t", "traceFunctionKey": "child", "rootTraceFunctionKey": "root", "rawSpan": map[string]any{"span_origin": MakeSpanOrigin("trace"), "span_data": map[string]any{"name": "secret", "input": "private", "input_meta": "private", "output": "private", "output_meta": "private", "input_serialized": "private", "output_serialized": "private", "prompt": "private", "replay_recording": "private", "error": "failure"}}}
}

func planTestChildSpan(name string) map[string]any {
	span := planTestSpan()
	raw := span["rawSpan"].(map[string]any)
	raw["parent_id"] = "parent"
	raw["span_data"].(map[string]any)["name"] = name
	return span
}

func planTestPolicy() map[string]any {
	return map[string]any{"nodes": []any{map[string]any{"traceFunctionKey": "root", "name": "secret", "captureContent": false}}}
}

func planSpanData(record map[string]any) map[string]any {
	return record["rawSpan"].(map[string]any)["span_data"].(map[string]any)
}

func assertWithoutContent(t *testing.T, label string, data map[string]any) {
	t.Helper()
	for _, key := range []string{"input", "output", "input_meta", "output_meta", "input_serialized", "output_serialized", "replay_recording", "prompt"} {
		if _, ok := data[key]; ok {
			t.Errorf("%s leaked %s", label, key)
		}
	}
	if data["error"] != "failure" || data["content_off_by_simulation_plan"] != true {
		t.Errorf("%s lost shape: %#v", label, data)
	}
}

type recordedSubmissions struct {
	mu   sync.Mutex
	sent []map[string]any
}

func (r *recordedSubmissions) submit(record map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, record)
}

func (r *recordedSubmissions) snapshot() []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]map[string]any(nil), r.sent...)
}

func TestSimulationPlan_HoldStripAndDrain(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	entered, release := make(chan struct{}), make(chan struct{})
	plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return planTestPolicy(), nil }, true)
	defer plan.stop()
	var recorded recordedSubmissions
	original := planTestSpan()
	plan.sendSpan(original, simulationPlanApplies, recorded.submit, func() {})
	<-entered
	plan.sendTrace(map[string]any{"traceId": "t", "completed": true}, recorded.submit)
	plan.sendTrace(map[string]any{"traceId": "unrelated", "completed": true}, recorded.submit)
	if sent := recorded.snapshot(); len(sent) != 1 || sent[0]["traceId"] != "unrelated" {
		t.Errorf("before load: %#v", sent)
	}
	close(release)
	if !plan.release(time.Second, false) {
		t.Fatal("plan did not drain")
	}
	sent := recorded.snapshot()
	if len(sent) != 3 || sent[2]["completed"] != true {
		t.Fatalf("records: %#v", sent)
	}
	assertWithoutContent(t, "held span", planSpanData(sent[1]))
	if planSpanData(original)["input"] != "private" {
		t.Error("mutated input")
	}
}

func TestSimulationPlan_CompletionWaitsForSubmission(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	entered, release, draining, submitted := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return planTestPolicy(), nil }, true)
	defer plan.stop()
	completed := make(chan struct{})
	plan.sendSpan(planTestSpan(), simulationPlanApplies, func(map[string]any) { close(draining); <-submitted }, func() {})
	<-entered
	close(release)
	<-draining
	plan.sendTrace(map[string]any{"traceId": "t", "completed": true}, func(map[string]any) { close(completed) })
	if plan.release(0, false) {
		t.Error("flush completed before submit")
	}
	select {
	case <-completed:
		t.Error("completion overtook span")
	default:
	}
	close(submitted)
	if !plan.release(time.Second, false) {
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
	span := planTestChildSpan("secret")
	span["rawSpan"].(map[string]any)["span_origin"] = MakeSpanOrigin("langgraph")
	plan.sendSpan(span, simulationPlanApplies, func(p map[string]any) { sent <- p }, func() {})
	select {
	case got := <-sent:
		if planSpanData(got)["input"] != "private" {
			t.Error("framework stripped")
		}
	default:
		t.Fatal("framework held")
	}
	plan.sendSpan(planTestChildSpan("secret"), simulationPlanApplies, func(p map[string]any) { sent <- p }, func() {})
	if !plan.release(time.Second, false) {
		t.Fatal("404 failed to release")
	}
	select {
	case got := <-sent:
		if planSpanData(got)["input"] != "private" || planSpanData(got)["content_off_by_simulation_plan"] != nil {
			t.Errorf("404 stripped content: %#v", planSpanData(got))
		}
	default:
		t.Fatal("404 held content")
	}
}

func TestSimulationPlan_FailedFirstReadSendsWithoutContent(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	entered, release := make(chan struct{}), make(chan struct{})
	plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return nil, errors.New("offline") }, true)
	defer plan.stop()
	var recorded recordedSubmissions
	for i := 0; i < simulationPlanMaxHeld+1; i++ {
		plan.sendSpan(planTestChildSpan("public"), simulationPlanApplies, recorded.submit, func() {})
	}
	<-entered
	framework := planTestChildSpan("public")
	framework["rawSpan"].(map[string]any)["span_origin"] = MakeSpanOrigin("langgraph")
	plan.sendSpan(planTestSpan(), simulationPlanApplies, recorded.submit, func() {})
	plan.sendSpan(framework, simulationPlanApplies, recorded.submit, func() {})
	plan.mu.Lock()
	held := len(plan.held)
	plan.mu.Unlock()
	if held != simulationPlanMaxHeld {
		t.Errorf("held %d", held)
	}
	before := recorded.snapshot()
	if len(before) != 3 {
		t.Fatalf("overflow and framework records sent before the read finished = %d", len(before))
	}
	assertWithoutContent(t, "overflow", planSpanData(before[0]))
	assertWithoutContent(t, "overflow", planSpanData(before[1]))
	if planSpanData(before[2])["input"] != "private" {
		t.Error("framework span lost content")
	}
	if plan.release(0, false) {
		t.Error("flush reported delivery while the first read was running")
	}
	close(release)
	if !plan.release(time.Second, false) {
		t.Fatal("failed read did not release held records")
	}
	plan.sendSpan(planTestChildSpan("public"), simulationPlanApplies, recorded.submit, func() {})
	sent := recorded.snapshot()
	if len(sent) != simulationPlanMaxHeld+4 {
		t.Fatalf("sent %d records", len(sent))
	}
	roots := 0
	for i, record := range sent {
		raw := record["rawSpan"].(map[string]any)
		switch {
		case raw["parent_id"] == nil:
			roots++
			if planSpanData(record)["input"] != "private" || planSpanData(record)["content_off_by_simulation_plan"] != nil {
				t.Errorf("root span lost content: %#v", planSpanData(record))
			}
		case recordedByFramework(record):
		default:
			assertWithoutContent(t, fmt.Sprintf("record %d", i), planSpanData(record))
		}
	}
	if roots != 1 {
		t.Errorf("root spans = %d", roots)
	}
	disabled := newSimulationPlan(func() (map[string]any, error) { t.Error("disabled read"); return nil, nil }, false)
	defer disabled.stop()
	got := make(chan map[string]any, 1)
	disabled.sendSpan(planTestChildSpan("public"), simulationPlanApplies, func(p map[string]any) { got <- p }, func() {})
	select {
	case record := <-got:
		if planSpanData(record)["input"] != "private" {
			t.Error("disabled plan stripped content")
		}
	default:
		t.Error("disabled held content")
	}
}

func TestSimulationPlan_LaterReadAppliesPlanAndFailedRefreshKeepsIt(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	var succeed atomic.Bool
	plan := newSimulationPlan(func() (map[string]any, error) {
		if succeed.Load() {
			return planTestPolicy(), nil
		}
		return nil, errors.New("offline")
	}, true)
	defer plan.stop()
	readNow := func() {
		plan.mu.Lock()
		plan.refreshAfter = time.Time{}
		plan.mu.Unlock()
		plan.refresh()
		plan.mu.Lock()
		done := plan.reading
		plan.mu.Unlock()
		if done != nil {
			<-done
		}
	}
	send := func(name string) map[string]any {
		var got map[string]any
		plan.sendSpan(planTestChildSpan(name), simulationPlanApplies, func(record map[string]any) { got = record }, func() {})
		if got == nil {
			t.Fatalf("%s was held", name)
		}
		return planSpanData(got)
	}
	readNow()
	assertWithoutContent(t, "public while unreadable", send("public"))
	succeed.Store(true)
	readNow()
	if public := send("public"); public["input"] != "private" || public["content_off_by_simulation_plan"] != nil {
		t.Errorf("public after load: %#v", public)
	}
	assertWithoutContent(t, "secret after load", send("secret"))
	succeed.Store(false)
	readNow()
	if public := send("public"); public["input"] != "private" || public["content_off_by_simulation_plan"] != nil {
		t.Errorf("public after failed refresh: %#v", public)
	}
	assertWithoutContent(t, "secret after failed refresh", send("secret"))
}

func TestSimulationPlan_CloseSendsHeldRecordsWithoutContent(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	entered, release := make(chan struct{}), make(chan struct{})
	plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return map[string]any{}, nil }, true)
	defer close(release)
	client := NewClient("key", WithSimulationPlan(false))
	client.httpClient.simulationPlan = plan
	defer client.Close(0)
	var recorded recordedSubmissions
	plan.sendSpan(planTestChildSpan("public"), simulationPlanApplies, recorded.submit, func() {})
	<-entered
	if client.FlushTraces(0) {
		t.Fatal("flush reported delivery while records remain held")
	}
	if len(recorded.snapshot()) != 0 {
		t.Fatal("flush released a held record before the first read finished")
	}
	if !client.Close(0) {
		t.Fatal("close did not send held records")
	}
	if !client.Close(0) {
		t.Fatal("repeated close changed its result")
	}
	sent := recorded.snapshot()
	if len(sent) != 1 {
		t.Fatalf("sent %d records", len(sent))
	}
	assertWithoutContent(t, "held at close", planSpanData(sent[0]))
}

func TestSimulationPlan_UnreadablePlanSkipsContentAndKeepsLimits(t *testing.T) {
	c, requests := subtreeClient(t)
	plan := newSimulationPlan(func() (map[string]any, error) { return nil, errors.New("offline") }, true)
	installPlan(t, c, plan)
	plan.refresh()
	<-plan.firstReadDone
	two := 2
	var rootCalls, childCalls atomic.Int32
	_, err := c.Trace(context.Background(), "unreadable", func(ctx context.Context) (any, error) {
		for i := 0; i < 5; i++ {
			_, _ = planTestNode(ctx, "child", &childCalls)
		}
		return planMarshalCounter{&rootCalls}, nil
	}, TraceOptions{MaxCapturedSubtreeSpans: &two, Input: []any{planMarshalCounter{&rootCalls}}})
	if err != nil {
		t.Fatal(err)
	}
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	if childCalls.Load() != 0 {
		t.Errorf("child content was built %d times while the plan was unreadable", childCalls.Load())
	}
	if rootCalls.Load() == 0 {
		t.Error("root content was never built")
	}
	byName := sentSpansByName(requests())
	if len(byName["child"]) != two {
		t.Fatalf("child spans = %d, want %d under MaxCapturedSubtreeSpans %d", len(byName["child"]), two, two)
	}
	for _, data := range byName["child"] {
		if _, ok := data["input"]; ok || data["content_off_by_simulation_plan"] != true || data["error"] != "child failed" {
			t.Errorf("child span = %#v", data)
		}
	}
	if roots := byName["unreadable"]; len(roots) != 1 || roots[0]["input"] == nil || roots[0]["content_off_by_simulation_plan"] != nil {
		t.Fatalf("root spans = %#v", roots)
	}
}

func installPlan(t *testing.T, c *Client, plan *simulationPlan) {
	t.Helper()
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	c.httpClient.simulationPlan.stop()
	c.httpClient.simulationPlan = plan
	t.Cleanup(plan.stop)
}

func blockedPlanReader(entered, release chan struct{}) func() (map[string]any, error) {
	var once sync.Once
	return func() (map[string]any, error) {
		once.Do(func() { close(entered) })
		<-release
		return map[string]any{"nodes": []any{}}, nil
	}
}

func timedTrace(t *testing.T, c *Client, ctx context.Context, key string) time.Duration {
	t.Helper()
	start := time.Now()
	if _, err := c.Trace(ctx, key, func(context.Context) (any, error) { return nil, nil }, TraceOptions{}); err != nil {
		t.Fatal(err)
	}
	return time.Since(start)
}

func TestSimulationPlan_ClientCreationStartsRead(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	requested := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/sdk/sim-plan" {
			select {
			case requested <- struct{}{}:
			default:
			}
		}
		_, _ = w.Write([]byte(`{"nodes":[]}`))
	}))
	defer server.Close()
	c := NewClient("key", WithServiceURL(server.URL))
	defer c.Close(time.Second)
	select {
	case <-requested:
	case <-time.After(2 * time.Second):
		t.Fatal("client creation did not start the simulation plan read")
	}
}

func TestSimulationPlan_RootWaitsForFirstReadAndAppliesPlan(t *testing.T) {
	c, requests := subtreeClient(t)
	entered, release := make(chan struct{}), make(chan struct{})
	body := map[string]any{"nodes": []any{map[string]any{"traceFunctionKey": "planned", "name": "secret", "captureContent": false}}}
	plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return body, nil }, true)
	installPlan(t, c, plan)
	plan.refresh()
	<-entered
	var calls atomic.Int32
	started, done := make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := c.Trace(context.Background(), "planned", func(ctx context.Context) (any, error) {
			close(started)
			_, _ = planTestNode(ctx, "secret", &calls)
			return nil, nil
		}, TraceOptions{})
		done <- err
	}()
	select {
	case <-started:
		t.Fatal("root started before the first plan read finished")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	if calls.Load() != 0 {
		t.Errorf("capture-off content was built %d times", calls.Load())
	}
	secret := sentSpansByName(requests())["secret"]
	if len(secret) != 1 || secret[0]["content_off_by_simulation_plan"] != true {
		t.Fatalf("secret spans = %#v", secret)
	}
}

func TestSimulationPlan_RootAfterFailedFirstReadDoesNotWait(t *testing.T) {
	c, _ := subtreeClient(t)
	var reads atomic.Int32
	release := make(chan struct{})
	defer close(release)
	plan := newSimulationPlan(func() (map[string]any, error) {
		if reads.Add(1) > 1 {
			<-release
		}
		return nil, errors.New("offline")
	}, true)
	installPlan(t, c, plan)
	plan.refresh()
	<-plan.firstReadDone
	deadline := time.Now().Add(2 * time.Second)
	for reads.Load() < 2 && time.Now().Before(deadline) {
		plan.mu.Lock()
		plan.refreshAfter = time.Time{}
		plan.mu.Unlock()
		plan.refresh()
		time.Sleep(time.Millisecond)
	}
	if reads.Load() < 2 {
		t.Fatal("second read never started")
	}
	if elapsed := timedTrace(t, c, context.Background(), "root"); elapsed > time.Second {
		t.Fatalf("root waited %v after the first read failed", elapsed)
	}
}

func TestSimulationPlan_RootWaitNeverExceedsCap(t *testing.T) {
	c, _ := subtreeClient(t)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	plan := newSimulationPlan(blockedPlanReader(entered, release), true)
	plan.firstReadWait = 200 * time.Millisecond
	installPlan(t, c, plan)
	plan.refresh()
	<-entered
	if elapsed := timedTrace(t, c, context.Background(), "first"); elapsed < 50*time.Millisecond || elapsed > time.Second {
		t.Fatalf("first root waited %v with a 200ms cap", elapsed)
	}
	if elapsed := timedTrace(t, c, context.Background(), "second"); elapsed > 50*time.Millisecond {
		t.Fatalf("second root waited %v after the cap passed", elapsed)
	}
}

func TestSimulationPlan_CancelledContextEndsRootWait(t *testing.T) {
	c, _ := subtreeClient(t)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	plan := newSimulationPlan(blockedPlanReader(entered, release), true)
	installPlan(t, c, plan)
	plan.refresh()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	if elapsed := timedTrace(t, c, ctx, "root"); elapsed > time.Second {
		t.Fatalf("root waited %v after its context was cancelled", elapsed)
	}
}

func TestSimulationPlan_NestedRootNeverWaits(t *testing.T) {
	c, _ := subtreeClient(t)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	plan := newSimulationPlan(blockedPlanReader(entered, release), true)
	installPlan(t, c, plan)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	var nested time.Duration
	_, err := c.Trace(cancelled, "outer", func(ctx context.Context) (any, error) {
		<-entered
		nested = timedTrace(t, c, context.WithoutCancel(ctx), "nested")
		return nil, nil
	}, TraceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if nested > time.Second {
		t.Fatalf("nested root waited %v", nested)
	}
}

func TestSimulationPlan_DisabledOrStoppedPlanNeverWaits(t *testing.T) {
	c, _ := subtreeClient(t)
	release := make(chan struct{})
	defer close(release)
	disabled := newSimulationPlan(func() (map[string]any, error) { t.Error("disabled plan was read"); <-release; return nil, nil }, false)
	installPlan(t, c, disabled)
	if elapsed := timedTrace(t, c, context.Background(), "disabled"); elapsed > time.Second {
		t.Fatalf("disabled plan waited %v", elapsed)
	}
	entered := make(chan struct{})
	stopped := newSimulationPlan(blockedPlanReader(entered, release), true)
	installPlan(t, c, stopped)
	stopped.refresh()
	<-entered
	stopped.stop()
	if elapsed := timedTrace(t, c, context.Background(), "stopped"); elapsed > time.Second {
		t.Fatalf("stopped plan waited %v", elapsed)
	}
}

func TestSimulationPlan_FrameworkSpansKeepContentAtCaptureTime(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	loaded := newSimulationPlan(nil, true)
	parsed, err := parseSimulationPlan(planTestPolicy())
	if err != nil {
		t.Fatal(err)
	}
	loaded.contentOff = parsed
	defer loaded.stop()
	failed := newSimulationPlan(nil, true)
	failed.unavailable = true
	defer failed.stop()
	for label, plan := range map[string]*simulationPlan{"capture off": loaded, "failed read": failed} {
		for _, instrumentation := range []string{"span", "trace"} {
			if !plan.withholdsContent("root", "secret", false, instrumentation) {
				t.Errorf("%s: %s span kept content", label, instrumentation)
			}
		}
		for _, instrumentation := range []string{"openai-agents", "langgraph", "claude-agent-sdk", "vercel-ai"} {
			if plan.withholdsContent("root", "secret", false, instrumentation) {
				t.Errorf("%s: %s span lost content at capture time", label, instrumentation)
			}
		}
	}
}

func planTestTraceSpan(traceID, name string, parent bool) map[string]any {
	span := planTestChildSpan(name)
	span["traceId"] = traceID
	raw := span["rawSpan"].(map[string]any)
	raw["trace_id"] = traceID
	if !parent {
		delete(raw, "parent_id")
	}
	return span
}

func deliveredOnce(t *testing.T, sent []map[string]any, want []string) map[string]map[string]any {
	t.Helper()
	byLabel := map[string]map[string]any{}
	var order []string
	for _, record := range sent {
		label := "completion:" + planTraceID(record)
		if raw, ok := record["rawSpan"].(map[string]any); ok {
			label = planSpanData(record)["name"].(string)
			if raw["parent_id"] == nil {
				label = "root:" + label
			}
		}
		if _, seen := byLabel[label]; seen {
			t.Errorf("%s delivered twice", label)
		}
		byLabel[label] = record
		order = append(order, label)
	}
	if len(order) != len(want) {
		t.Fatalf("delivered %v, want %v", order, want)
	}
	for i, label := range want {
		if order[i] != label {
			t.Fatalf("delivered %v, want %v", order, want)
		}
	}
	return byLabel
}

func TestSimulationPlan_CloseNeverDropsHeldRecords(t *testing.T) {
	t.Run("loaded plan still draining", func(t *testing.T) {
		t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
		entered, release := make(chan struct{}), make(chan struct{})
		plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return planTestPolicy(), nil }, true)
		client := NewClient("key", WithSimulationPlan(false))
		client.httpClient.simulationPlan = plan
		var recorded recordedSubmissions
		draining, unblock := make(chan struct{}), make(chan struct{})
		var once sync.Once
		blocking := func(record map[string]any) {
			once.Do(func() { close(draining); <-unblock })
			recorded.submit(record)
		}
		plan.sendSpan(planTestTraceSpan("t", "secret", true), simulationPlanApplies, blocking, func() {})
		plan.sendSpan(planTestTraceSpan("t", "public", true), simulationPlanApplies, recorded.submit, func() {})
		<-entered
		plan.sendTrace(map[string]any{"traceId": "t", "completed": true}, recorded.submit)
		close(release)
		<-draining
		plan.sendTrace(map[string]any{"traceId": "t", "completed": true, "second": true}, recorded.submit)
		closed := make(chan bool, 1)
		go func() { closed <- client.Close(0) }()
		select {
		case <-closed:
			t.Fatal("close finished while a held record was still being submitted")
		case <-time.After(50 * time.Millisecond):
		}
		close(unblock)
		if !<-closed {
			t.Fatal("close reported undelivered records")
		}
		sent := recorded.snapshot()
		if len(sent) != 4 || sent[3]["second"] != true {
			t.Fatalf("records = %#v", sent)
		}
		byLabel := deliveredOnce(t, sent[:3], []string{"secret", "public", "completion:t"})
		assertWithoutContent(t, "secret", planSpanData(byLabel["secret"]))
		if planSpanData(byLabel["public"])["input"] != "private" {
			t.Errorf("public lost content: %#v", planSpanData(byLabel["public"]))
		}
	})
	t.Run("first read in flight", func(t *testing.T) {
		t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
		entered, release := make(chan struct{}), make(chan struct{})
		plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return planTestPolicy(), nil }, true)
		client := NewClient("key", WithSimulationPlan(false))
		client.httpClient.simulationPlan = plan
		var recorded recordedSubmissions
		plan.sendSpan(planTestTraceSpan("t", "public", true), simulationPlanApplies, recorded.submit, func() {})
		plan.sendSpan(planTestTraceSpan("t", "answer", false), simulationPlanApplies, recorded.submit, func() {})
		<-entered
		plan.sendTrace(map[string]any{"traceId": "t", "completed": true}, recorded.submit)
		if !client.Close(0) {
			t.Fatal("close reported undelivered records")
		}
		plan.mu.Lock()
		reading := plan.reading
		plan.mu.Unlock()
		close(release)
		if reading != nil {
			<-reading
		}
		byLabel := deliveredOnce(t, recorded.snapshot(), []string{"public", "root:answer", "completion:t"})
		assertWithoutContent(t, "public", planSpanData(byLabel["public"]))
		if root := planSpanData(byLabel["root:answer"]); root["input"] != "private" || root["content_off_by_simulation_plan"] != nil {
			t.Errorf("root lost content: %#v", root)
		}
	})
}

func promptTestNode(ctx context.Context, name string) (result int) {
	node := EnterAutoNode(ctx, name, name, []any{name}, []any{&result})
	if node != nil {
		defer node.End(nil)
		GetCurrentSpan(node.Context()).SetPrompt(name + " prompt")
		GetCurrentSpan(node.Context()).AddContext(map[string]any{"step": name})
	}
	return 1
}

func assertPromptWithheld(t *testing.T, byName map[string][]map[string]any, name string) {
	t.Helper()
	if len(byName[name]) != 1 {
		t.Fatalf("%s spans = %#v", name, byName[name])
	}
	data := byName[name][0]
	if _, ok := data["prompt"]; ok || data["content_off_by_simulation_plan"] != true {
		t.Errorf("%s leaked its prompt: %#v", name, data)
	}
	if contexts, _ := data["contexts"].([]any); len(contexts) != 1 {
		t.Errorf("%s lost its contexts: %#v", name, data)
	}
}

func assertPromptKept(t *testing.T, byName map[string][]map[string]any, name string) {
	t.Helper()
	if len(byName[name]) != 1 || byName[name][0]["prompt"] != name+" prompt" {
		t.Fatalf("%s spans = %#v", name, byName[name])
	}
}

func TestSimulationPlan_ContentOffWithholdsPrompt(t *testing.T) {
	c, requests := subtreeClient(t)
	loadContentOffPlan(t, c, map[string][]string{"prompted": {"secret"}, "optin": {"secret-span", "secret-start"}})
	_, err := c.Trace(context.Background(), "prompted", func(ctx context.Context) (any, error) {
		promptTestNode(ctx, "secret")
		promptTestNode(ctx, "public")
		return nil, nil
	}, TraceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Span(context.Background(), "optin", func(ctx context.Context) (any, error) {
		GetCurrentSpan(ctx).SetPrompt("secret-span prompt")
		GetCurrentSpan(ctx).AddContext(map[string]any{"step": "secret-span"})
		return nil, nil
	}, WithName("secret-span"))
	_, active := c.Start(context.Background(), "optin", "secret-start")
	active.SetPrompt("secret-start prompt")
	active.AddContext(map[string]any{"step": "secret-start"})
	active.End()
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	byName := sentSpansByName(requests())
	if len(byName["secret"]) != 0 {
		t.Fatalf("error-free content-off child was sent: %#v", byName["secret"])
	}
	for _, name := range []string{"secret-span", "secret-start"} {
		assertPromptWithheld(t, byName, name)
	}
	assertPromptKept(t, byName, "public")
}

func planTestQuietSpan(name string, parent bool) map[string]any {
	span := planTestTraceSpan("t", name, parent)
	delete(planSpanData(span), "error")
	return span
}

func TestSimulationPlan_LoadedPlanSkipsErrorFreeContentOffChildren(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	plan := newSimulationPlan(func() (map[string]any, error) { return planTestPolicy(), nil }, true)
	defer plan.stop()
	plan.refresh()
	<-plan.firstReadDone
	framework := planTestQuietSpan("secret", true)
	framework["rawSpan"].(map[string]any)["span_origin"] = MakeSpanOrigin("langgraph")
	cases := []struct {
		label string
		span  map[string]any
		sent  bool
	}{
		{"error-free child", planTestQuietSpan("secret", true), false},
		{"errored child", planTestTraceSpan("t", "secret", true), true},
		{"root", planTestQuietSpan("secret", false), true},
		{"framework child", framework, true},
		{"listed child", planTestQuietSpan("public", true), true},
	}
	for _, tc := range cases {
		var recorded recordedSubmissions
		discarded := 0
		plan.sendSpan(tc.span, simulationPlanApplies, recorded.submit, func() { discarded++ })
		sent := recorded.snapshot()
		if tc.sent != (len(sent) == 1) || tc.sent == (discarded == 1) || len(sent)+discarded != 1 {
			t.Fatalf("%s: sent %d, discarded %d", tc.label, len(sent), discarded)
		}
		if !tc.sent {
			continue
		}
		data := planSpanData(sent[0])
		switch tc.label {
		case "errored child":
			assertWithoutContent(t, tc.label, data)
		case "root":
			if _, ok := data["input"]; ok || data["content_off_by_simulation_plan"] != true {
				t.Errorf("root = %#v", data)
			}
		default:
			if data["input"] != "private" || data["content_off_by_simulation_plan"] != nil {
				t.Errorf("%s lost content: %#v", tc.label, data)
			}
		}
	}
}

func TestSimulationPlan_HeldContentOffChildIsDiscardedOnRelease(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	entered, release := make(chan struct{}), make(chan struct{})
	plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return planTestPolicy(), nil }, true)
	defer plan.stop()
	var recorded recordedSubmissions
	var discarded atomic.Int32
	plan.sendSpan(planTestQuietSpan("secret", true), simulationPlanApplies, recorded.submit, func() { discarded.Add(1) })
	plan.sendSpan(planTestTraceSpan("t", "secret", true), simulationPlanApplies, recorded.submit, func() { discarded.Add(1) })
	<-entered
	plan.sendTrace(map[string]any{"traceId": "t", "completed": true}, recorded.submit)
	if sent := recorded.snapshot(); len(sent) != 0 || discarded.Load() != 0 {
		t.Fatalf("released before the first read: sent %d, discarded %d", len(sent), discarded.Load())
	}
	close(release)
	if !plan.release(time.Second, false) {
		t.Fatal("plan did not drain")
	}
	if discarded.Load() != 1 {
		t.Fatalf("discarded = %d, want 1", discarded.Load())
	}
	byLabel := deliveredOnce(t, recorded.snapshot(), []string{"secret", "completion:t"})
	assertWithoutContent(t, "errored secret", planSpanData(byLabel["secret"]))
}

func TestSimulationPlan_HeldDeclaredNodeKeepsContentOnRelease(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_SIM_PLAN", "")
	entered, release := make(chan struct{}), make(chan struct{})
	plan := newSimulationPlan(func() (map[string]any, error) { close(entered); <-release; return planTestPolicy(), nil }, true)
	defer plan.stop()
	var recorded recordedSubmissions
	plan.sendSpan(planTestQuietSpan("secret", true), simulationPlanIgnored, recorded.submit, func() { t.Error("declared node was discarded") })
	<-entered
	if sent := recorded.snapshot(); len(sent) != 0 {
		t.Fatalf("declared node sent before the first read: %d", len(sent))
	}
	close(release)
	if !plan.release(time.Second, false) {
		t.Fatal("plan did not drain")
	}
	sent := recorded.snapshot()
	if len(sent) != 1 {
		t.Fatalf("sent %d records", len(sent))
	}
	if data := planSpanData(sent[0]); data["input"] != "private" || data["prompt"] != "private" || data["content_off_by_simulation_plan"] != nil {
		t.Fatalf("declared node = %#v", data)
	}
}

func TestSimulationPlan_SkippedSpanIsNotCountedInCompletion(t *testing.T) {
	c, requests := subtreeClient(t)
	loadContentOffPlan(t, c, map[string][]string{"optin": {"secret-start"}})
	ctx, root := c.Start(context.Background(), "optin", "root")
	_, child := c.Start(ctx, "optin", "secret-start")
	child.End()
	root.End()
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	sent := requests()
	if byName := sentSpansByName(sent); len(byName["secret-start"]) != 0 || len(byName["root"]) != 1 {
		t.Fatalf("spans = %#v", byName)
	}
	completions := 0
	for _, request := range sent {
		if request["expectedSpanCount"] != nil {
			completions++
			if request["expectedSpanCount"] != float64(1) {
				t.Fatalf("expected span count = %v, want 1", request["expectedSpanCount"])
			}
		}
	}
	if completions != 1 {
		t.Fatalf("completions = %d, want 1", completions)
	}
}

func TestSimulationPlan_UnreadablePlanWithholdsChildPrompt(t *testing.T) {
	c, requests := subtreeClient(t)
	plan := newSimulationPlan(func() (map[string]any, error) { return nil, errors.New("offline") }, true)
	installPlan(t, c, plan)
	plan.refresh()
	<-plan.firstReadDone
	_, err := c.Trace(context.Background(), "unreadable", func(ctx context.Context) (any, error) {
		GetCurrentSpan(ctx).SetPrompt("unreadable prompt")
		promptTestNode(ctx, "child")
		return nil, nil
	}, TraceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Span(context.Background(), "optin-root", func(ctx context.Context) (any, error) {
		GetCurrentSpan(ctx).SetPrompt("optin-root prompt")
		return c.Span(ctx, "optin-root", func(ctx context.Context) (any, error) {
			GetCurrentSpan(ctx).SetPrompt("optin-child prompt")
			GetCurrentSpan(ctx).AddContext(map[string]any{"step": "optin-child"})
			return nil, nil
		}, WithName("optin-child"))
	})
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush")
	}
	byName := sentSpansByName(requests())
	assertPromptWithheld(t, byName, "child")
	assertPromptWithheld(t, byName, "optin-child")
	assertPromptKept(t, byName, "unreadable")
	assertPromptKept(t, byName, "optin-root")
}
