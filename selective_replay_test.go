package bitfab

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func selectiveFixture(t *testing.T) (*selectivePlan, *SelectiveReplayOptions) {
	t.Helper()
	raw := `{"version":4,"rootId":"root","options":{"mustRun":[{"traceFunctionKey":"shop","spanName":"price"}]},"unresolvedMustRun":[],"assertions":[],"nodes":[{"id":"root","parentId":null,"traceFunctionKey":"shop","spanName":"root","mustRun":true,"reusable":false,"reason":"root"},{"id":"price","parentId":"root","traceFunctionKey":"shop","spanName":"price","mustRun":true,"reusable":false,"reason":"must-run","recording":{"input":[],"output":7}},{"id":"load","parentId":"root","traceFunctionKey":"shop","spanName":"load","mustRun":false,"reusable":true,"reason":"eligible","recording":{"input":[],"output":"recorded"}}]}`
	var plan selectivePlan
	if err := json.Unmarshal([]byte(raw), &plan); err != nil {
		t.Fatal(err)
	}
	return &plan, &plan.Options
}

func selectiveCall(r *selectiveRuntime, name, parent string, safety bool) (any, bool, error) {
	return r.enter(ReplayNodeIdentity{"shop", name}, "live-"+name, parent, spanConfig{input: []any{}, replayReusable: true, mockOnReplay: safety})
}

func TestSelectiveReplayMatchingAndSafety(t *testing.T) {
	plan, options := selectiveFixture(t)
	runtime, err := newSelectiveRuntime(plan, options)
	if err != nil {
		t.Fatal(err)
	}
	selectiveCall(runtime, "root", "", false)
	output, mocked, err := selectiveCall(runtime, "load", "live-root", false)
	if err != nil || !mocked || output != "recorded" {
		t.Fatalf("reuse = %v %v %v", output, mocked, err)
	}
	if _, mocked, err := selectiveCall(runtime, "price", "live-root", false); err != nil || mocked {
		t.Fatal("required call skipped", err)
	}
	selectiveCall(runtime, "new-child", "live-price", false)
	if _, _, err := selectiveCall(runtime, "unsafe", "live-new-child", true); err == nil {
		t.Fatal("safety conflict was ignored")
	}
	if runtime.failure == nil || len(runtime.report().MustRunNotReached) != 0 {
		t.Fatal("missing execution evidence")
	}
}

func TestSelectiveReplayRejectsUnsafePlans(t *testing.T) {
	for _, kind := range []string{"version", "descendant", "assertion", "unresolved", "ancestor", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			plan, options := selectiveFixture(t)
			switch kind {
			case "version":
				plan.Version = 3
			case "descendant":
				parent := "price"
				plan.Nodes[2].ParentID = &parent
			case "assertion":
				json.Unmarshal([]byte(`[{"id":"check","target":{"kind":"span","name":"load"}}]`), &plan.Assertions)
			case "unresolved":
				plan.UnresolvedMustRun = options.MustRun
			case "ancestor":
				parent := "load"
				plan.Nodes[1].ParentID = &parent
			case "duplicate":
				plan.Nodes = append(plan.Nodes, plan.Nodes[2])
			}
			if _, err := newSelectiveRuntime(plan, options); err == nil {
				t.Fatal("unsafe plan accepted")
			}
		})
	}
}

func TestSelectiveReplayCaptureRoundTrip(t *testing.T) {
	cfg := spanConfig{replayReusable: true}
	data := map[string]any{}
	recordSelectiveJSON(data, selectiveFingerprint([]any{map[string]any{"a": "b"}}), map[string]any{"ok": true}, cfg)
	if data["replay_json_safe"] != true {
		t.Fatal("JSON map was not recorded", data)
	}
	data = map[string]any{}
	recordSelectiveJSON(data, "[]", 7, cfg)
	if data["replay_json_safe"] == true {
		t.Fatal("untyped integer would be returned as float64")
	}
	cfg.mockOutputType = reflect.TypeFor[int]()
	recordSelectiveJSON(data, "[]", 7, cfg)
	if data["replay_json_safe"] != true {
		t.Fatal("typed integer not recorded")
	}
	shared := map[string]any{"a": true}
	if selectiveFingerprint([]any{shared, shared}) != "" {
		t.Fatal("aliased values accepted")
	}
}

func TestSelectiveReplayCaptureConcreteBoundaries(t *testing.T) {
	value := 7
	for _, test := range []struct {
		name        string
		output      any
		target      reflect.Type
		unsupported bool
		want        bool
	}{
		{name: "typed-map", output: map[string]int{"a": 7}, target: reflect.TypeFor[map[string]int](), want: true},
		{name: "untyped-map", output: map[string]int{"a": 7}},
		{name: "typed-nil", output: ([]int)(nil), target: reflect.TypeFor[[]int](), want: true},
		{name: "typed-slice", output: []int{1, 2}, target: reflect.TypeFor[[]int](), want: true},
		{name: "pointer", output: &value, target: reflect.TypeFor[*int]()},
		{name: "bytes", output: []byte("hello"), target: reflect.TypeFor[[]byte]()},
		{name: "multiple-values", output: []any{"a", "b"}, unsupported: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := map[string]any{}
			recordSelectiveJSON(data, "[]", test.output, spanConfig{replayReusable: true, mockOutputType: test.target, selectiveUnsupportedOutput: test.unsupported})
			if (data["replay_json_safe"] == true) != test.want {
				t.Fatalf("incorrect output eligibility: %#v", data)
			}
		})
	}
}

func TestSelectiveReplayCaptureExplicitEmptyInput(t *testing.T) {
	client, requests := subtreeClient(t)
	_, err := client.Span(context.Background(), "shop", func(context.Context) (any, error) { return 7, nil }, WithName("load"), WithInput(), WithReplayReusable(true), WithMockOutputType[int]())
	if err != nil || !client.FlushTraces(time.Second) {
		t.Fatal("capture failed", err)
	}
	for _, request := range requests() {
		raw, ok := request["rawSpan"].(map[string]any)
		if !ok {
			continue
		}
		data := raw["span_data"].(map[string]any)
		recording, ok := data["replay_recording"].(map[string]any)
		if !ok || data["replay_json_safe"] != true {
			t.Fatalf("missing recording: %#v", data)
		}
		input, ok := recording["input"].([]any)
		if !ok || input == nil || len(input) != 0 {
			t.Fatalf("empty input must be a JSON array: %#v", recording)
		}
		return
	}
	t.Fatal("missing captured span")
}

func TestSelectiveReplayExecutesEffectsThroughSpans(t *testing.T) {
	client, _ := subtreeClient(t)
	plan, options := selectiveFixture(t)
	options.MustRun = append(options.MustRun, ReplayNodeIdentity{"shop", "cache"})
	live, disabled := true, false
	parent := "root"
	plan.Nodes = append(plan.Nodes, selectiveNode{ReplayNodeIdentity: ReplayNodeIdentity{"shop", "cache"}, ID: "cache", ParentID: &parent, MustRun: &live, Reusable: &disabled, Reason: "must-run"})
	runtime, err := newSelectiveRuntime(plan, options)
	if err != nil {
		t.Fatal(err)
	}
	ctx := withReplayContext(context.Background(), &replayContext{selective: runtime, traceID: "selective-test"})
	state := 0
	result, err := client.Span(ctx, "shop", func(ctx context.Context) (any, error) {
		_, err := client.Span(ctx, "shop", func(context.Context) (any, error) { state = 7; return nil, nil }, WithName("cache"))
		if err != nil {
			return nil, err
		}
		_, err = client.Span(ctx, "shop", func(context.Context) (any, error) { t.Error("reusable sibling ran"); return nil, nil }, WithName("load"), WithReplayReusable(true))
		if err != nil {
			return nil, err
		}
		return client.Span(ctx, "shop", func(context.Context) (any, error) { return state, nil }, WithName("price"))
	}, WithName("root"))
	if err != nil || result != 7 || len(runtime.report().MustRunNotReached) != 0 {
		t.Fatalf("effect not preserved: %v %v", result, err)
	}
}

func TestSelectiveReplayTraceConfiguredNodes(t *testing.T) {
	client, _ := subtreeClient(t)
	if err := client.Node("load", NodeOptions{ReplayReusable: true}); err != nil {
		t.Fatal(err)
	}
	plan, options := selectiveFixture(t)
	plan.Nodes[2].Recording.Output = json.RawMessage(`7`)
	runtime, err := newSelectiveRuntime(plan, options)
	if err != nil {
		t.Fatal(err)
	}
	ctx := withReplayContext(context.Background(), &replayContext{selective: runtime, traceID: "trace-node-replay"})
	output, err := client.Span(withManagedTraceRoot(ctx), "shop", func(ctx context.Context) (any, error) {
		return client.Trace(ctx, "shop", func(ctx context.Context) (any, error) {
			value := autoTestCall(ctx, "load", func() int { t.Error("configured reusable node ran"); return 0 })
			return autoTestCall(ctx, "price", func() int { return value }), nil
		}, TraceOptions{Name: "root"})
	}, WithName("root"))
	if err != nil || output != 7 || len(runtime.report().MustRunNotReached) != 0 {
		t.Fatalf("trace replay failed: %v %v %#v", output, err, runtime.report())
	}
}

func TestSelectiveReplayRequestAndAttempts(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	t.Setenv(disableCompressionEnv, "1")
	plan, options := selectiveFixture(t)
	state := &replayTestServerState{}
	items := []map[string]any{{"sourceTraceId": "source-trace-1", "sourceSpanId": "source-span-1", "selectiveReplayPlan": plan}}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, items))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	var calls atomic.Int32
	result, err := client.Replay(context.Background(), "shop", func(ctx context.Context, _ string, _ int) (any, error) {
		calls.Add(1)
		_, err := client.Span(ctx, "shop", func(context.Context) (any, error) { t.Error("reusable sibling ran"); return nil, nil }, WithName("load"), WithReplayReusable(true))
		if err != nil {
			return nil, err
		}
		return client.Span(ctx, "shop", func(context.Context) (any, error) { return 7, nil }, WithName("price"))
	}, &ReplayOptions{ExperimentalSelectiveReplay: options, Attempts: 2})
	if err != nil || calls.Load() != 2 || len(result.Items) != 2 {
		t.Fatalf("replay failed: %v %#v", err, result)
	}
	for _, item := range result.Items {
		if item.Error != nil || item.SelectiveReplay == nil || len(item.SelectiveReplay.MustRunNotReached) != 0 {
			t.Fatalf("bad item %#v", item)
		}
	}
	if state.startBody["experimentalSelectiveReplay"] == nil {
		t.Fatal("missing wire manifest")
	}
}

func TestSelectiveReplayConcurrentStickyFailure(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	t.Setenv(disableCompressionEnv, "1")
	plan, options := selectiveFixture(t)
	plan.Nodes[2].Recording.Output = json.RawMessage(`{"value":"recorded"}`)
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, []map[string]any{{"sourceTraceId": "source-trace-1", "sourceSpanId": "source-span-1", "selectiveReplayPlan": plan}}))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	var entered atomic.Int32
	ready := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := client.Replay(ctx, "shop", func(ctx context.Context, _ string, _ int) (any, error) {
		if entered.Add(1) == 2 {
			close(ready)
		}
		select {
		case <-ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		value, err := client.Span(ctx, "shop", func(context.Context) (any, error) { t.Error("reusable sibling ran"); return nil, nil }, WithName("load"), WithReplayReusable(true))
		if err != nil {
			return nil, err
		}
		output := value.(map[string]any)
		if output["value"] != "recorded" {
			t.Errorf("output shared across attempts: %#v", output)
		}
		output["value"] = "changed"
		if currentReplayContext(ctx).attempt == 1 {
			// Intentionally catch the error: the attempt must still fail.
			_, _ = client.Span(ctx, "shop", func(context.Context) (any, error) { t.Error("unsafe body ran"); return nil, nil }, WithName("unsafe"), WithMockOnReplay(true))
		}
		return client.Span(ctx, "shop", func(context.Context) (any, error) { return 7, nil }, WithName("price"))
	}, &ReplayOptions{ExperimentalSelectiveReplay: options, Attempts: 2, MaxConcurrency: 2})
	if err != nil || len(result.Items) != 2 {
		t.Fatalf("replay failed: %v %#v", err, result)
	}
	for _, item := range result.Items {
		if (item.Error != nil) != (item.Attempt == 1) || item.SelectiveReplay == nil || len(item.SelectiveReplay.MustRunNotReached) != 0 {
			t.Fatalf("attempt failure leaked or was swallowed: %#v", item)
		}
	}
}

func TestSelectiveReplayConcurrentParentMatching(t *testing.T) {
	plan, options := selectiveFixture(t)
	plan.Nodes[2].Recording.Input = []any{"first"}
	second := plan.Nodes[2]
	second.ID = "second"
	second.Recording = &selectiveRecording{Input: []any{"second"}, Output: json.RawMessage(`"second"`)}
	plan.Nodes = append(plan.Nodes, second)
	runtime, err := newSelectiveRuntime(plan, options)
	if err != nil {
		t.Fatal(err)
	}
	selectiveCall(runtime, "root", "", false)
	var wg sync.WaitGroup
	for _, input := range []string{"second", "first"} {
		wg.Go(func() {
			value, mocked, err := runtime.enter(ReplayNodeIdentity{"shop", "load"}, input, "live-root", spanConfig{input: []any{input}, replayReusable: true})
			want := "recorded"
			if input == "second" {
				want = "second"
			}
			if err != nil || !mocked || value != want {
				t.Errorf("wrong concurrent match: %v %v %v", value, mocked, err)
			}
		})
	}
	wg.Wait()
	if _, mocked, _ := runtime.enter(ReplayNodeIdentity{"shop", "load"}, "new", "unknown-parent", spanConfig{input: []any{"first"}, replayReusable: true}); mocked {
		t.Fatal("borrowed recording from another parent")
	}
	if _, mocked, _ := runtime.enter(ReplayNodeIdentity{"shop", "root"}, "unmatched-root", "unknown-parent", spanConfig{replayReusable: true}); mocked {
		t.Fatal("unmatched parent fell back to the recorded root")
	}
	last := runtime.report().Decisions
	if last[len(last)-1].OriginalSpanID != nil {
		t.Fatal("unmatched parent matched the recorded root")
	}
}

func TestSelectiveReplayCaptureDeclaredNode(t *testing.T) {
	client, requests := subtreeClient(t)
	if err := client.Node("load", NodeOptions{ReplayReusable: true}); err != nil {
		t.Fatal(err)
	}
	_, err := client.Trace(context.Background(), "shop", func(ctx context.Context) (any, error) {
		return autoTestCall(ctx, "load", func() int { return 7 }), nil
	}, TraceOptions{Name: "root"})
	if err != nil || !client.FlushTraces(time.Second) {
		t.Fatal("capture failed", err)
	}
	found := false
	for _, request := range requests() {
		raw, ok := request["rawSpan"].(map[string]any)
		if !ok {
			continue
		}
		data := raw["span_data"].(map[string]any)
		if data["name"] == "load" {
			found = true
			if data["replay_reusable"] != true || data["replay_json_safe"] != true || data["replay_recording"] == nil {
				t.Fatalf("missing native replay recording: %#v", data)
			}
		}
	}
	if !found {
		t.Fatal("missing configured node")
	}
}

func TestSelectiveReplayRejectsLossyStrings(t *testing.T) {
	for _, value := range []any{string([]byte{0xff}), map[string]any{string([]byte{0xff}): true}} {
		if fingerprint := selectiveFingerprint(value); fingerprint != "" {
			t.Fatalf("lossy JSON accepted: %q", fingerprint)
		}
	}
}

func TestSelectiveReplayNestedTraceScopes(t *testing.T) {
	client, _ := subtreeClient(t)
	plan, options := selectiveFixture(t)
	parent := "price"
	live := true
	plan.Nodes[2].ParentID = &parent
	plan.Nodes[2].MustRun = &live
	runtime, err := newSelectiveRuntime(plan, options)
	if err != nil {
		t.Fatal(err)
	}
	ctx := withReplayContext(context.Background(), &replayContext{selective: runtime, traceID: "nested-selective"})
	_, err = client.Span(withManagedTraceRoot(ctx), "shop", func(ctx context.Context) (any, error) {
		return client.Trace(ctx, "shop", func(ctx context.Context) (any, error) {
			return client.Trace(ctx, "shop", func(ctx context.Context) (any, error) {
				return autoTestCall(ctx, "new-child", func() int { return 7 }), nil
			}, TraceOptions{Name: "price"})
		}, TraceOptions{Name: "root"})
	}, WithName("root"))
	if err != nil {
		t.Fatal(err)
	}
	report := runtime.report()
	if len(report.MustRunNotReached) != 0 {
		t.Fatalf("nested trace not reached: %#v", report)
	}
	for _, decision := range report.Decisions {
		if decision.SpanName == "price" && (decision.OriginalSpanID == nil || *decision.OriginalSpanID != "price") {
			t.Fatalf("nested trace matched wrong parent: %#v", decision)
		}
		if decision.SpanName == "new-child" && decision.Reason != "must-run" {
			t.Fatalf("new descendant lost required scope: %#v", decision)
		}
	}
}

func TestSelectiveReplaySafetySurvivesCaptureFilters(t *testing.T) {
	zero, no := 0, false
	for _, test := range []struct {
		name       string
		opts       TraceOptions
		node       *NodeOptions
		background bool
	}{
		{name: "depth", opts: TraceOptions{MaxDepth: &zero}},
		{name: "count", opts: TraceOptions{MaxCapturedSubtreeSpans: &zero}},
		{name: "exclude", opts: TraceOptions{Exclude: []string{"unsafe"}}},
		{name: "capture", node: &NodeOptions{Capture: &no}},
		{name: "background-context", background: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, requests := subtreeClient(t)
			if test.node != nil {
				if err := client.Node("unsafe", *test.node); err != nil {
					t.Fatal(err)
				}
			}
			plan, options := selectiveFixture(t)
			runtime, err := newSelectiveRuntime(plan, options)
			if err != nil {
				t.Fatal(err)
			}
			ctx := withReplayContext(context.Background(), &replayContext{selective: runtime, traceID: "filtered-safety"})
			test.opts.MockOnReplayDefault = true
			func() {
				defer func() { recover() }()
				_, _ = client.Span(withManagedTraceRoot(ctx), "shop", func(ctx context.Context) (any, error) {
					return client.Trace(ctx, "shop", func(ctx context.Context) (any, error) {
						if test.background {
							ctx = context.Background()
						}
						return autoTestCall(ctx, "unsafe", func() int { t.Error("safety-mocked body executed"); return 1 }), nil
					}, test.opts)
				}, WithName("root"))
			}()
			if runtime.failure == nil {
				t.Fatal("missing sticky safety conflict")
			}
			if !client.FlushTraces(time.Second) {
				t.Fatal("capture did not flush")
			}
			if !test.background {
				for _, request := range requests() {
					if raw, ok := request["rawSpan"].(map[string]any); ok && raw["span_data"].(map[string]any)["name"] == "unsafe" {
						t.Fatalf("capture filter leaked intercepted span: %#v", raw)
					}
				}
			}
		})
	}
}

func TestSelectiveReplayInvalidPlanReleasesLease(t *testing.T) {
	dbState := &replayDBTestState{}
	server := newLegacyCarrierServer(t, replayDBHandler(t, &replayTestServerState{}, dbState, successfulDBResolveResponse()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	_, options := selectiveFixture(t)
	item := client.runReplayItem(context.Background(), "shop", nil, ReplayOptions{ExperimentalSelectiveReplay: options, DBBranch: &DBBranchOptions{}}, "experiment", replayServerItem{DBBranchLease: &dbBranchLeaseWire{NeonBranchID: "branch-1"}}, "local")
	if item.Error == nil {
		t.Fatal("invalid plan was accepted")
	}
	dbState.mu.Lock()
	defer dbState.mu.Unlock()
	if !reflect.DeepEqual(dbState.released, []string{"branch-1"}) || dbState.resolveBody != nil {
		t.Fatalf("invalid plan leaked or acquired a lease: released=%v resolved=%v", dbState.released, dbState.resolveBody)
	}
}

func TestSelectiveReplayHiddenScopeReparentsExportedChild(t *testing.T) {
	for _, excluded := range []bool{false, true} {
		t.Run(map[bool]string{false: "capture-false", true: "excluded"}[excluded], func(t *testing.T) {
			client, requests := subtreeClient(t)
			opts := TraceOptions{Name: "root"}
			if excluded {
				opts.Exclude = []string{"price"}
			} else {
				no := false
				if err := client.Node("price", NodeOptions{Capture: &no, Finalize: func(value any) (any, error) {
					t.Error("capture opt-out ran its recording finalizer")
					return value, nil
				}}); err != nil {
					t.Fatal(err)
				}
			}
			plan, options := selectiveFixture(t)
			runtime, err := newSelectiveRuntime(plan, options)
			if err != nil {
				t.Fatal(err)
			}
			ctx := withReplayContext(context.Background(), &replayContext{selective: runtime, traceID: "hidden-selective"})
			_, err = client.Span(withManagedTraceRoot(ctx), "shop", func(ctx context.Context) (any, error) {
				return client.Trace(ctx, "shop", func(ctx context.Context) (any, error) {
					return autoTestCall(ctx, "price", func() int {
						return autoTestCall(ctx, "leaf", func() int { return 7 })
					}), nil
				}, opts)
			}, WithName("root"))
			if err != nil || !client.FlushTraces(time.Second) {
				t.Fatal("capture failed", err)
			}
			report := runtime.report()
			if len(report.MustRunNotReached) != 0 || len(report.Decisions) != 3 || report.Decisions[2].Reason != "must-run" {
				t.Fatalf("hidden node lost required scope: %#v", report)
			}
			rootID, leafParent := "", ""
			for _, request := range requests() {
				raw, ok := request["rawSpan"].(map[string]any)
				if !ok {
					continue
				}
				switch raw["span_data"].(map[string]any)["name"] {
				case "root":
					rootID = raw["id"].(string)
				case "leaf":
					leafParent = raw["parent_id"].(string)
				case "price":
					t.Fatal("capture opt-out uploaded a span")
				}
			}
			if rootID == "" || leafParent != rootID {
				t.Fatalf("exported child has missing parent: root=%s parent=%s", rootID, leafParent)
			}
		})
	}
}

func TestSelectiveReplayHiddenParentsMatchExportedRecordings(t *testing.T) {
	for _, test := range []struct {
		filter   string
		recorded bool
	}{
		{"capture-false", false}, {"excluded", false}, {"wrapper", false},
		{"capture-false", true}, {"excluded", true}, {"wrapper", true},
	} {
		t.Run(test.filter+map[bool]string{false: "", true: "-previously-recorded"}[test.recorded], func(t *testing.T) {
			filter := test.filter
			client, requests := subtreeClient(t)
			opts := TraceOptions{Name: "root"}
			no, yes := false, true
			if filter == "capture-false" {
				if err := client.Node("hidden", NodeOptions{Capture: &no, Finalize: func(value any) (any, error) {
					t.Error("hidden finalizer executed")
					return value, nil
				}}); err != nil {
					t.Fatal(err)
				}
			} else if filter == "excluded" {
				opts.Exclude = []string{"hidden"}
			}
			if err := client.Node("load", NodeOptions{MockOnReplay: &yes}); err != nil {
				t.Fatal(err)
			}
			plan, options := selectiveFixture(t)
			plan.Nodes[2].Recording.Output = json.RawMessage(`7`)
			if test.recorded {
				root, hidden := "root", "hidden"
				plan.Nodes[2].ParentID = &hidden
				plan.Nodes = append(plan.Nodes, selectiveNode{ReplayNodeIdentity: ReplayNodeIdentity{"shop", "hidden"}, ID: hidden, ParentID: &root, MustRun: &no, Reusable: &no, Reason: "not-reusable"})
			}
			runtime, err := newSelectiveRuntime(plan, options)
			if err != nil {
				t.Fatal(err)
			}
			ctx := withReplayContext(context.Background(), &replayContext{selective: runtime, traceID: "hidden-recording"})
			result, err := client.Span(withManagedTraceRoot(ctx), "shop", func(ctx context.Context) (any, error) {
				return client.Trace(ctx, "shop", func(ctx context.Context) (any, error) {
					var value int
					hidden := EnterAutoNode(ctx, "hidden", "hidden", nil, []any{&value}, filter == "wrapper")
					if hidden == nil {
						t.Fatal("hidden node lost safety interception")
					}
					defer hidden.End(nil)
					value = autoTestCall(ctx, "load", func() int { t.Error("safety-mocked body executed"); return 0 })
					return value, nil
				}, opts)
			}, WithName("root"))
			if err != nil || result != 7 || runtime.failure != nil {
				t.Fatalf("hidden parent prevented reuse: result=%v err=%v failure=%v", result, err, runtime.failure)
			}
			if !client.FlushTraces(time.Second) {
				t.Fatal("capture did not flush")
			}
			rootID, loadParent := "", ""
			for _, request := range requests() {
				if raw, ok := request["rawSpan"].(map[string]any); ok {
					switch raw["span_data"].(map[string]any)["name"] {
					case "root":
						rootID = raw["id"].(string)
					case "load":
						loadParent = raw["parent_id"].(string)
					case "hidden":
						t.Fatal("hidden node uploaded content")
					}
				}
			}
			if rootID == "" || loadParent != rootID {
				t.Fatalf("capture ancestry changed: root=%s parent=%s", rootID, loadParent)
			}
		})
	}
}

func TestSelectiveReplayHiddenParentsShareConsumptionAndKeepScopes(t *testing.T) {
	for _, scope := range []string{"none", "must-run", "assertion", "unmatched-parent"} {
		t.Run(scope, func(t *testing.T) {
			plan, options := selectiveFixture(t)
			runtime, err := newSelectiveRuntime(plan, options)
			if err != nil {
				t.Fatal(err)
			}
			selectiveCall(runtime, "root", "", false)
			parent := "live-root"
			if scope == "unmatched-parent" {
				selectiveCall(runtime, "new-parent", parent, false)
				parent = "live-new-parent"
			}
			key := ReplayNodeIdentity{"shop", "hidden"}
			if scope == "must-run" {
				key.SpanName = "price"
			} else if scope == "assertion" {
				runtime.assertionNames["hidden"] = true
			}
			runtime.enter(key, "hidden-1", parent, spanConfig{selectiveInterceptionOnly: true})
			runtime.enter(ReplayNodeIdentity{"shop", "nested-hidden"}, "hidden-2", "hidden-1", spanConfig{selectiveInterceptionOnly: true})
			_, mocked, err := selectiveCall(runtime, "load", "hidden-2", true)
			if scope == "none" {
				if !mocked || err != nil {
					t.Fatalf("nested hidden recording not reused: %v", err)
				}
				runtime.enter(key, "hidden-3", parent, spanConfig{selectiveInterceptionOnly: true})
				if _, _, err := selectiveCall(runtime, "load", "hidden-3", true); err == nil {
					t.Fatal("sibling hidden parents reused the same recording twice")
				}
			} else if err == nil || runtime.failure == nil {
				t.Fatal("hidden parent bypassed live scope or unmatched-parent isolation")
			}
		})
	}
}

func TestSelectiveReplayDecisionReasons(t *testing.T) {
	for _, kind := range []string{"unmatched", "ambiguous", "drift", "missing", "unsupported", "decode", "finalizer", "undeclared"} {
		for _, safety := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "", true: "-safety"}[safety], func(t *testing.T) {
				plan, options := selectiveFixture(t)
				name, want := "load", "input-drift-or-missing-recording"
				cfg := spanConfig{input: []any{}, replayReusable: true, mockOnReplay: safety}
				switch kind {
				case "unmatched":
					name, want = "unknown", "unmatched-or-ambiguous-call"
				case "ambiguous":
					copy := plan.Nodes[2]
					copy.ID = "load-2"
					plan.Nodes = append(plan.Nodes, copy)
					want = "unmatched-or-ambiguous-call"
				case "drift":
					cfg.input = []any{1}
				case "missing":
					plan.Nodes[2].Recording = nil
				case "unsupported":
					cfg.selectiveUnsupportedOutput = true
					want = "unsupported-output-boundary"
				case "decode":
					cfg.mockOutputType = reflect.TypeFor[int]()
					want = "unsupported-output-boundary"
				case "finalizer":
					cfg.finalize = func(v any) (any, error) { return v, nil }
					want = "unsupported-output-boundary"
				case "undeclared":
					cfg.replayReusable = false
					want = "reuse-not-declared"
					if safety {
						want = "unchanged-inputs"
					}
				}
				runtime, err := newSelectiveRuntime(plan, options)
				if err != nil {
					t.Fatal(err)
				}
				selectiveCall(runtime, "root", "", false)
				if runtime.report().Decisions[0].Reason != "root" {
					t.Fatal("root reason overwritten")
				}
				runtime.enter(ReplayNodeIdentity{"shop", name}, "live-child", "live-root", cfg)
				decision := runtime.report().Decisions[1]
				if safety && kind != "undeclared" {
					want = "safety-conflict:" + want
				}
				if decision.Reason != want {
					t.Fatalf("reason=%s want=%s", decision.Reason, want)
				}
			})
		}
	}
}
