package bitfab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestReplayRegistryCLIOptionsSelectionAndFactory(t *testing.T) {
	state := &replayTestServerState{}
	base := replayTestHandler(t, state, replayItems())
	reads := []string{}
	server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/sdk/datasets/dataset/traces":
			writeReplayTestJSON(t, w, map[string]any{"datasetId": "dataset", "traceIds": []string{"b", "a", "b"}})
		case "/api/sdk/traces/a/assertions":
			reads = append(reads, "a")
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{}, "inheritedFrom": nil})
		case "/api/sdk/traces/b/assertions":
			reads = append(reads, "b")
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "assertion", "status": "confirmed"}}, "inheritedFrom": nil})
		default:
			base(w, r)
		}
	})
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	calls := 0
	err := registry.Register("pipeline", ReplayRegistration{Client: client, Function: BindReplayFunction("registry", func(string, int) { calls++ }), Options: ReplayOptions{DatasetIDs: []string{"dataset"}, OnlyWithAssertions: true, Name: "registered name", Notes: "registered notes"}, OptionsFactory: func(ctx context.Context, params ReplayRegistryContext, options *ReplayOptions) error {
		if params.Params["count"] != float64(2) {
			t.Fatalf("params: %+v", params.Params)
		}
		options.Attempts = 2
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	_, err = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--limit", "1", "--dry-run", "--param", "count=2", "--notes", "staging env override"}, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("dry run executed")
	}
	if !slices.Equal(reads, []string{"a", "b"}) {
		t.Fatalf("assertion reads: %v", reads)
	}
	state.mu.Lock()
	body := state.startBody
	state.mu.Unlock()
	if body["attempts"] != float64(2) || body["onlyWithAssertions"] != true {
		t.Fatalf("options lost: %+v", body)
	}
	if body["name"] != "registered name" || body["notes"] != "staging env override" {
		t.Fatalf("name or notes: %+v", body)
	}
	ids := body["traceIds"].([]any)
	if len(ids) != 1 || ids[0] != "b" {
		t.Fatalf("bound before eligibility: %+v", ids)
	}
	if registry.entries["pipeline"].Options.Attempts != 0 {
		t.Fatal("factory mutated registration")
	}
	var result map[string]any
	if err = json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
}

func TestReplayRegistryCLIMetadataFlag(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	err := registry.Register("pipeline", ReplayRegistration{
		Client:   client,
		Function: BindReplayFunction("registry", func(string, int) {}),
		Options:  ReplayOptions{TraceIDs: []string{"a"}, Metadata: map[string]string{"schedule": "registered"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	startMetadata := func() map[string]any {
		state.mu.Lock()
		defer state.mu.Unlock()
		metadata, _ := state.startBody["metadata"].(map[string]any)
		return metadata
	}
	var stdout, stderr bytes.Buffer
	if _, err = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--dry-run", "--metadata", "schedule=eod", "--metadata", "owner=ada=team"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if metadata := startMetadata(); len(metadata) != 2 || metadata["schedule"] != "eod" || metadata["owner"] != "ada=team" {
		t.Fatalf("metadata from flags = %+v", metadata)
	}
	if _, err = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--dry-run"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if metadata := startMetadata(); len(metadata) != 1 || metadata["schedule"] != "registered" {
		t.Fatalf("registered metadata lost without the flag: %+v", metadata)
	}
	state.mu.Lock()
	state.startBody = nil
	state.mu.Unlock()
	for _, bad := range []string{"schedule", "=eod"} {
		_, err = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--dry-run", "--metadata", bad}, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "--metadata expects key=value") {
			t.Fatalf("--metadata %q error = %v", bad, err)
		}
	}
	state.mu.Lock()
	body := state.startBody
	state.mu.Unlock()
	if body != nil {
		t.Fatalf("usage error still started a replay: %+v", body)
	}
}

func TestReplayRegistryCLIReportsItsFlagsAsTheInvocation(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{
		Client:   client,
		Function: BindReplayFunction("registry", func(string, int) {}),
		Options:  ReplayOptions{TraceIDs: []string{"a"}, Notes: "registered"},
	}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	args := []string{"pipeline", "--code-change", "skip", "--dry-run", "--name", "baseline", "--max-concurrency", "2", "--dataset-id", "ds-1", "--metadata", "team=search", "--metadata", "owner=ada", "--param", "model=gpt", "--no-db-branch"}
	if _, err := RunReplayCLI(context.Background(), registry, args, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	invocation, _ := state.startBody["invocation"].(map[string]any)
	state.mu.Unlock()
	want := map[string]any{
		"code-change":  "skip",
		"dry-run":      true,
		"name":         "baseline",
		"concurrency":  float64(2),
		"dataset-ids":  []any{"ds-1"},
		"metadata":     []any{"owner", "team"},
		"param":        []any{"model"},
		"no-db-branch": true,
	}
	if !reflect.DeepEqual(invocation["flags"], want) {
		t.Fatalf("flags = %#v, want %#v", invocation["flags"], want)
	}
}

func TestReplayRegistryCLIExperimentGroupNameFlag(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{
		Client:   client,
		Function: BindReplayFunction("registry", func(string, int) {}),
		Options:  ReplayOptions{TraceIDs: []string{"a"}},
	}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	args := []string{"pipeline", "--code-change", "skip", "--dry-run", "--experiment-group-id", "group-1", "--experiment-group-name", "Prompt sweep"}
	if _, err := RunReplayCLI(context.Background(), registry, args, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	body := state.startBody
	state.startBody = nil
	state.mu.Unlock()
	if body["experimentGroupId"] != "group-1" || body["experimentGroupName"] != "Prompt sweep" {
		t.Fatalf("start body = %+v", body)
	}
	invocation, _ := body["invocation"].(map[string]any)
	flags, _ := invocation["flags"].(map[string]any)
	if flags["experiment-group-id"] != "group-1" || flags["experiment-group-name"] != "Prompt sweep" {
		t.Fatalf("invocation flags = %#v", flags)
	}
	_, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--dry-run", "--experiment-group-name", "Prompt sweep"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "ExperimentGroupName requires ExperimentGroupID") {
		t.Fatalf("name without id error = %v", err)
	}
	state.mu.Lock()
	body = state.startBody
	state.mu.Unlock()
	if body != nil {
		t.Fatalf("name without id still started a replay: %+v", body)
	}
}

func TestReplayRegistryCLIBaselineExperimentIDFlag(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{
		Client:   client,
		Function: BindReplayFunction("registry", func(string, int) {}),
		Options:  ReplayOptions{TraceIDs: []string{"a"}},
	}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	args := []string{"pipeline", "--code-change", "skip", "--dry-run", "--baseline-experiment-id", "experiment-0"}
	if _, err := RunReplayCLI(context.Background(), registry, args, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	body := state.startBody
	state.mu.Unlock()
	if body["baselineExperimentId"] != "experiment-0" {
		t.Fatalf("start body = %+v", body)
	}
	invocation, _ := body["invocation"].(map[string]any)
	flags, _ := invocation["flags"].(map[string]any)
	if flags["baseline-experiment-id"] != "experiment-0" {
		t.Fatalf("invocation flags = %#v", flags)
	}
}

func TestReplayRegistryBoundExplicitOrderingInheritedAndErrors(t *testing.T) {
	reads := []string{}
	server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
		reads = append(reads, r.URL.Path)
		switch r.URL.Path {
		case "/api/sdk/traces/replayed/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "x", "status": "confirmed"}}, "inheritedFrom": "ancestor"})
		case "/api/sdk/traces/good/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "x", "status": "confirmed"}}, "inheritedFrom": nil})
		default:
			http.Error(w, "failed", 500)
		}
	})
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	ids, err := boundRegistryTraceIDs(context.Background(), client, []string{"replayed", "good", "unread"}, 1, registryAssertionNarrowing{onlyWithAssertions: true})
	if err != nil || !slices.Equal(ids, []string{"good"}) || len(reads) != 2 {
		t.Fatalf("selection %v %v reads %v", ids, err, reads)
	}
	if _, err = boundRegistryTraceIDs(context.Background(), client, []string{"bad", "good"}, 1, registryAssertionNarrowing{onlyWithAssertions: true}); err == nil {
		t.Fatal("transport error swallowed")
	}
}

func TestSeedRegistryCLIRecordsCasesAndRuns(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, nil))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	calls := 0
	if err := registry.Register("seed", ReplayRegistration{Client: client, TraceFunctionKey: "seed", Function: func(value int) int { calls++; return value * 2 }}); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "cases.jsonl")
	if err := os.WriteFile(file, []byte("{\"input\":[3],\"metadata\":{\"row\":1}}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	result, err := RunSeedCLI(context.Background(), registry, []string{"seed", "--cases", file}, &stdout, io.Discard)
	if err != nil || len(result.TraceIDs) != 1 || calls != 0 {
		t.Fatalf("case result %+v %v calls %d", result, err, calls)
	}
	result, err = RunSeedCLI(context.Background(), registry, []string{"seed", "--cases", file, "--run"}, io.Discard, io.Discard)
	if err != nil || len(result.TraceIDs) != 1 || calls != 1 {
		t.Fatalf("run result %+v %v calls %d", result, err, calls)
	}
	empty, err := SeedFromRegistry(context.Background(), registry, "seed", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(empty)
	if err != nil || !bytes.Contains(encoded, []byte(`"traceIds":[]`)) || bytes.Contains(encoded, []byte(`"reseeded"`)) {
		t.Fatalf("empty result shape %s %v", encoded, err)
	}
	for _, raw := range []string{"", `{"input":[],"expected":null}`, `{"input":null}`, `[1]`} {
		if _, err = parseSeedRegistryCases([]byte(raw), true); err == nil {
			t.Fatalf("accepted invalid cases %q", raw)
		}
	}
}

func TestReplayRegistryValidationAndEmptySelection(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, []map[string]any{}))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func() {}}); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1", "--limit", "0"}, {"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1", "--attempts", "101"}, {"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1", "--db-branch", "--no-db-branch"}} {
		if _, err := RunReplayCLI(context.Background(), registry, args, io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--dry-run"}, io.Discard, io.Discard); err == nil {
		t.Fatal("empty selection accepted")
	}
}

func TestReplayRegistryGradeHookChainsAndDryRunSkips(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	calls := 0
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func(string, int) {}, Options: ReplayOptions{Mock: MockNone, OnItemFinish: func(event ReplayItemFinishProgress) {
		calls++
		if event.Item.TraceID == nil {
			t.Error("hook lacks persisted trace")
		}
		panic("grade failed")
	}}}); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1"}, io.Discard, &stderr); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || !bytes.Contains(stderr.Bytes(), []byte("grade failed")) {
		t.Fatalf("hook calls=%d stderr=%s", calls, stderr.String())
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--dry-run"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("dry run invoked grading hook")
	}
}

// A trace whose assertions are all still awaiting review is not replayable
// under --only-with-assertions: only a confirmed assertion is checked on a
// replay, so narrowing must skip it exactly as the server's selection does.
func TestReplayRegistryCLISkipsAssertionsAwaitingReview(t *testing.T) {
	state := &replayTestServerState{}
	base := replayTestHandler(t, state, replayItems())
	server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/sdk/traces/draft/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "a1", "status": "draft"}}, "inheritedFrom": nil})
		case "/api/sdk/traces/reviewed/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "a2", "status": "confirmed"}}, "inheritedFrom": nil})
		default:
			base(w, r)
		}
	})
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{
		Client:   client,
		Function: BindReplayFunction("registry", func(string, int) {}),
	}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if _, err := RunReplayCLI(context.Background(), registry,
		[]string{"pipeline", "--code-change", "skip", "--trace-ids", "draft,reviewed", "--limit", "1", "--dry-run", "--only-with-assertions"},
		&stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	got, ok := state.startBody["traceIds"].([]any)
	if !ok || len(got) != 1 || got[0] != "reviewed" {
		t.Fatalf("selected trace ids = %#v, want only the reviewed trace", state.startBody["traceIds"])
	}
}

func TestReplayRegistryCLIForwardsOnlyDraftsWithADatasetSelection(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, Function: BindReplayFunction("registry", func(string, int) {})}); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	if _, err := RunReplayCLI(context.Background(), registry,
		[]string{"pipeline", "--code-change", "skip", "--dataset-id", "dataset", "--only-drafts", "--dry-run"},
		io.Discard, &stderr); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	body := state.startBody
	state.mu.Unlock()
	if body["onlyDrafts"] != true {
		t.Fatalf("onlyDrafts = %#v", body["onlyDrafts"])
	}
	if body["datasetId"] != "dataset" && !slices.Equal(anyStrings(body["datasetIds"]), []string{"dataset"}) {
		t.Fatalf("start body lost the dataset: %#v", body)
	}
	if !strings.Contains(stderr.String(), "with a draft assertion") {
		t.Fatalf("progress line lacks the narrowing: %s", stderr.String())
	}
	var help bytes.Buffer
	_, _ = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--help"}, io.Discard, &help)
	for _, want := range []string{"-only-drafts", "replay only traces carrying a draft assertion, one nobody has confirmed or rejected yet; with --dataset-ids the run is recorded as a subset of the dataset"} {
		if !strings.Contains(help.String(), want) {
			t.Fatalf("help lacks %q: %s", want, help.String())
		}
	}
}

func TestReplayRegistryCLIOmitsOnlyDraftsWhenNotRequested(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, Function: BindReplayFunction("registry", func(string, int) {}), Options: ReplayOptions{OnlyDrafts: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--dry-run"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	registered := state.startBody["onlyDrafts"]
	state.mu.Unlock()
	if registered != true {
		t.Fatalf("registered onlyDrafts = %#v, want true", registered)
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--dry-run", "--only-drafts=false"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	_, sent := state.startBody["onlyDrafts"]
	state.mu.Unlock()
	if sent {
		t.Fatalf("--only-drafts=false still sent onlyDrafts: %#v", state.startBody)
	}
}

func TestReplayRegistryCLISpendsADatasetBoundOnlyOnTracesWaitingForReview(t *testing.T) {
	state := &replayTestServerState{}
	base := replayTestHandler(t, state, replayItems())
	server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/sdk/datasets/dataset/traces":
			writeReplayTestJSON(t, w, map[string]any{"datasetId": "dataset", "traceIds": []string{"reviewed", "draft", "mixed", "extra"}})
		case "/api/sdk/traces/reviewed/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "a1", "status": "confirmed"}}, "inheritedFrom": nil})
		case "/api/sdk/traces/draft/assertions", "/api/sdk/traces/mixed/assertions", "/api/sdk/traces/extra/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "a2", "status": "draft"}}, "inheritedFrom": nil})
		default:
			base(w, r)
		}
	})
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, Function: BindReplayFunction("registry", func(string, int) {})}); err != nil {
		t.Fatal(err)
	}
	if _, err := RunReplayCLI(context.Background(), registry,
		[]string{"pipeline", "--code-change", "skip", "--dataset-id", "dataset", "--limit", "1", "--dry-run", "--only-drafts"},
		io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	body := state.startBody
	state.mu.Unlock()
	if got, _ := body["traceIds"].([]any); len(got) != 1 || got[0] != "draft" {
		t.Fatalf("selected trace ids = %#v, want only the first trace with a draft assertion", body["traceIds"])
	}
	if body["onlyDrafts"] != true {
		t.Fatalf("onlyDrafts = %#v", body["onlyDrafts"])
	}
	_, err := RunReplayCLI(context.Background(), registry,
		[]string{"pipeline", "--code-change", "skip", "--trace-ids", "reviewed,draft", "--limit", "1", "--dry-run", "--only-drafts", "--only-with-assertions"},
		io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "No traces with a draft assertion matched this replay selection.") {
		t.Fatalf("empty narrowing error = %v", err)
	}
}

func TestReplayRegistryCLIReplaysNamedMembersOfADatasetAsASubset(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, Function: BindReplayFunction("registry", func(string, int) {})}); err != nil {
		t.Fatal(err)
	}
	if _, err := RunReplayCLI(context.Background(), registry,
		[]string{"pipeline", "--code-change", "skip", "--trace-ids", "a,b,c", "--dataset-id", "ds-1", "--limit", "2", "--dry-run"},
		io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	traceIDs, _ := state.startBody["traceIds"].([]any)
	if len(traceIDs) != 2 || traceIDs[0] != "a" || traceIDs[1] != "b" {
		t.Fatalf("traceIds = %#v, want [a b]", state.startBody["traceIds"])
	}
	if state.startBody["datasetId"] != "ds-1" && !slices.Equal(anyStrings(state.startBody["datasetIds"]), []string{"ds-1"}) {
		t.Fatalf("start body lost the dataset: %#v", state.startBody)
	}
}

func anyStrings(value any) []string {
	items, _ := value.([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			out = append(out, text)
		}
	}
	return out
}

func TestReplayRegistryCLIBoundsSelectionByCategory(t *testing.T) {
	state := &replayTestServerState{}
	base := replayTestHandler(t, state, replayItems())
	server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/sdk/traces/uncategorized/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "a1", "status": "confirmed", "category_id": nil}}, "inheritedFrom": nil})
		case "/api/sdk/traces/other/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "a2", "status": "confirmed", "category_id": "cat-other"}}, "inheritedFrom": nil})
		case "/api/sdk/traces/pending/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "a3", "status": "draft", "category_id": "cat-1"}}, "inheritedFrom": nil})
		case "/api/sdk/traces/match/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "a4", "status": "confirmed", "category_id": "cat-1"}}, "inheritedFrom": nil})
		default:
			base(w, r)
		}
	})
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, Function: BindReplayFunction("registry", func(string, int) {})}); err != nil {
		t.Fatal(err)
	}
	if _, err := RunReplayCLI(context.Background(), registry,
		[]string{"pipeline", "--code-change", "skip", "--trace-ids", "uncategorized,other,pending,match", "--limit", "1", "--dry-run", "--category-ids", "cat-1,cat-2"},
		io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	body := state.startBody
	state.mu.Unlock()
	if got, _ := body["traceIds"].([]any); len(got) != 1 || got[0] != "match" {
		t.Fatalf("selected trace ids = %#v, want only the trace with a confirmed assertion in a named category", body["traceIds"])
	}
	if !reflect.DeepEqual(body["categoryIds"], []any{"cat-1", "cat-2"}) {
		t.Fatalf("categoryIds = %#v", body["categoryIds"])
	}
	_, err := RunReplayCLI(context.Background(), registry,
		[]string{"pipeline", "--code-change", "skip", "--trace-ids", "uncategorized,other,pending", "--limit", "1", "--dry-run", "--category-ids", "cat-1"},
		io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "No traces with a confirmed assertion in the given categories matched this replay selection.") {
		t.Fatalf("empty narrowing error = %v", err)
	}
}

func TestReplayRegistryCLICategoryIDs(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("registered", ReplayRegistration{Client: client, Function: BindReplayFunction("registry", func(string, int) {}), Options: ReplayOptions{DatasetIDs: []string{"ds"}, CategoryIDs: []string{"cat-registered"}}}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("bare", ReplayRegistration{Client: client, Function: BindReplayFunction("registry", func(string, int) {})}); err != nil {
		t.Fatal(err)
	}
	startBody := func() map[string]any {
		state.mu.Lock()
		defer state.mu.Unlock()
		body := state.startBody
		state.startBody = nil
		return body
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"registered", "--code-change", "skip", "--dry-run"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if got := startBody()["categoryIds"]; !reflect.DeepEqual(got, []any{"cat-registered"}) {
		t.Fatalf("registered categories = %#v", got)
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"registered", "--code-change", "skip", "--dry-run", "--category-ids", "cat-1, cat-2"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	body := startBody()
	if got := body["categoryIds"]; !reflect.DeepEqual(got, []any{"cat-1", "cat-2"}) {
		t.Fatalf("flag categories = %#v", got)
	}
	invocation, _ := body["invocation"].(map[string]any)
	flags, _ := invocation["flags"].(map[string]any)
	if !reflect.DeepEqual(flags["category-ids"], []any{"cat-1", "cat-2"}) {
		t.Fatalf("invocation flags = %#v", flags)
	}
	_, err := RunReplayCLI(context.Background(), registry, []string{"bare", "--code-change", "skip", "--dry-run", "--category-ids", "cat-1"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "--category-ids narrows a trace ID or dataset selection. Pass it with --trace-ids or --dataset-ids.") {
		t.Fatalf("no selection error = %v", err)
	}
	if body := startBody(); body != nil {
		t.Fatalf("refused categories still started a replay: %+v", body)
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"bare", "--code-change", "skip", "--dry-run", "--dataset-ids", "ds", "--category-ids", "cat-1"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if got := startBody()["categoryIds"]; !reflect.DeepEqual(got, []any{"cat-1"}) {
		t.Fatalf("dataset categories = %#v", got)
	}
}

func TestReplayRegistryCLIHelpDescribesCategoryIDs(t *testing.T) {
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: newTestClient("http://127.0.0.1:1"), TraceFunctionKey: "key", Function: func(string, int) {}}); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	_, _ = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--help"}, io.Discard, &stderr)
	for _, want := range []string{"-category-ids", "with --trace-ids or --dataset-ids: replay only the traces carrying a confirmed assertion in one of these categories; which assertions are judged stays the same"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("help lacks %q: %s", want, stderr.String())
		}
	}
}

func TestReplayRegistryCLISkipAssertionJudging(t *testing.T) {
	for _, tc := range []struct {
		name       string
		registered bool
		args       []string
		want       any
	}{
		{"flag omits judging", false, []string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1", "--skip-assertion-judging"}, nil},
		{"absent flag keeps judging on", false, []string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1"}, true},
		{"absent flag keeps a registered skip", true, []string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &replayTestServerState{}
			server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
			defer server.Close()
			client := newTestClient(server.URL)
			defer client.Close(time.Second)
			registry := NewReplayRegistry()
			if err := registry.Register("pipeline", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func(string, int) {}, Options: ReplayOptions{Mock: MockNone, SkipAssertionJudging: tc.registered}}); err != nil {
				t.Fatal(err)
			}
			if _, err := RunReplayCLI(context.Background(), registry, tc.args, io.Discard, io.Discard); err != nil {
				t.Fatal(err)
			}
			state.mu.Lock()
			got := state.startBody["judgeAssertions"]
			state.mu.Unlock()
			if got != tc.want {
				t.Fatalf("judgeAssertions = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReplayRegistryCLIDeprecatedJudgeAssertionsWarnsAndChangesNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		skip bool
		want any
	}{
		{"judging stays on", false, true},
		{"registered skip still wins", true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &replayTestServerState{}
			server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
			defer server.Close()
			client := newTestClient(server.URL)
			defer client.Close(time.Second)
			registry := NewReplayRegistry()
			if err := registry.Register("pipeline", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func(string, int) {}, Options: ReplayOptions{Mock: MockNone, SkipAssertionJudging: tc.skip}}); err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1", "--judge-assertions"}, io.Discard, &stderr); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(stderr.String(), "--judge-assertions is deprecated") || !strings.Contains(stderr.String(), "--skip-assertion-judging") {
				t.Fatalf("stderr = %q, want a deprecation warning naming --skip-assertion-judging", stderr.String())
			}
			state.mu.Lock()
			got := state.startBody["judgeAssertions"]
			state.mu.Unlock()
			if got != tc.want {
				t.Fatalf("judgeAssertions = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReplayRegistryCLIPrintsExperimentAtStartAndOnFailure(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func(string, int) {}, Options: ReplayOptions{Mock: MockNone}}); err != nil {
		t.Fatal(err)
	}
	startLine := "[replay] Experiment run-1: " + server.URL + "/experiments/run-1\n"

	var stderr bytes.Buffer
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1"}, io.Discard, &stderr); err != nil {
		t.Fatal(err)
	}
	output := stderr.String()
	replaying := strings.Index(output, "[replay] Replaying")
	started := strings.Index(output, startLine)
	summary := strings.Index(output, "Summary")
	if replaying < 0 || started < replaying || summary < started {
		t.Fatalf("stderr = %s", output)
	}

	state.mu.Lock()
	state.completeErr = true
	state.mu.Unlock()
	stderr.Reset()
	_, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1"}, io.Discard, &stderr)
	var replayErr *ReplayError
	if !errors.As(err, &replayErr) || replayErr.ExperimentID != "run-1" {
		t.Fatalf("error = %T %v", err, err)
	}
	output = stderr.String()
	failureLine := "\nExperiment run-1: " + server.URL + "/experiments/run-1\n"
	if !strings.Contains(output, startLine) || strings.Index(output, failureLine) <= strings.Index(output, startLine) {
		t.Fatalf("stderr = %s", output)
	}
}

func TestReplayRegistryCLIFailOnErrorReturnsAnErrorOnlyWhenAnItemErrored(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("working", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func(string, int) {}, Options: ReplayOptions{Mock: MockNone}}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("broken", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func(string, int, bool) {}, Options: ReplayOptions{Mock: MockNone}}); err != nil {
		t.Fatal(err)
	}

	if _, err := RunReplayCLI(context.Background(), registry, []string{"broken", "--code-change", "skip", "--experiment-group-id", "group-1"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("without the flag an errored item must not fail the command: %v", err)
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"working", "--code-change", "skip", "--experiment-group-id", "group-1", "--fail-on-error"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("no item errored: %v", err)
	}

	var stdout bytes.Buffer
	_, err := RunReplayCLI(context.Background(), registry, []string{"broken", "--code-change", "skip", "--experiment-group-id", "group-1", "--fail-on-error"}, &stdout, io.Discard)
	want := "[replay] 2 of 2 replayed items errored; exiting 1 because of --fail-on-error"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v", err)
	}
	if !strings.Contains(stdout.String(), `"run-1"`) {
		t.Fatalf("result was not printed before the error: %s", stdout.String())
	}

	_, err = RunReplayCLI(context.Background(), registry, []string{"broken", "--code-change", "skip", "--dry-run", "--fail-on-error"}, io.Discard, io.Discard)
	if err == nil || err.Error() != "[replay] 2 of 2 resolved items errored; exiting 1 because of --fail-on-error" {
		t.Fatalf("dry run error = %v", err)
	}
}

func TestReplayRegistryCLIHelpListsFailOnError(t *testing.T) {
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: newTestClient("http://127.0.0.1:1"), TraceFunctionKey: "key", Function: func(string, int) {}}); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	_, _ = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1", "--help"}, io.Discard, &stderr)
	if !strings.Contains(stderr.String(), "-fail-on-error") {
		t.Fatalf("help = %s", stderr.String())
	}
}

func TestRecordRegistryFlagSendsCommaSeparatedIDsAsLists(t *testing.T) {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.String("trace-ids", "", "")
	fs.String("dataset-id", "", "")
	fs.String("grader-ids", "", "")
	if err := fs.Parse([]string{"--trace-ids", "a, b,,c", "--dataset-id", "ds-1", "--grader-ids", "g-1"}); err != nil {
		t.Fatal(err)
	}
	flags := map[string]any{}
	fs.Visit(func(f *flag.Flag) { recordRegistryFlag(flags, f) })
	want := map[string]any{
		"trace-ids":   []string{"a", "b", "c"},
		"dataset-ids": []string{"ds-1"},
		"grader-ids":  []string{"g-1"},
	}
	if !reflect.DeepEqual(flags, want) {
		t.Fatalf("flags = %#v, want %#v", flags, want)
	}
}

func TestReplayRegistryCLIPrintsTheResolvedAPIKeyWithoutReplaying(t *testing.T) {
	cases := []struct {
		name     string
		override string
		env      string
		client   func() *Client
		want     string
	}{
		{"configured key", "", "", func() *Client { return NewClient("configured-key", WithServiceURL("http://127.0.0.1:1")) }, `{"apiKey":"configured-key","serviceUrl":"http://127.0.0.1:1"}`},
		{"key func", "", "", func() *Client {
			return NewClient("", WithServiceURL("http://127.0.0.1:1"), WithAPIKeyFunc(func() string { return "func-key" }))
		}, `{"apiKey":"func-key","serviceUrl":"http://127.0.0.1:1"}`},
		{"env fallback", "", "env-key", func() *Client { return NewClient("", WithServiceURL("http://127.0.0.1:1")) }, `{"apiKey":"env-key","serviceUrl":"http://127.0.0.1:1"}`},
		{"override", "override-key", "env-key", func() *Client { return NewClient("configured-key", WithServiceURL("http://127.0.0.1:1")) }, `{"apiKey":"override-key","serviceUrl":"http://127.0.0.1:1"}`},
		{"no key under strict", "", "", func() *Client { return NewClient("", WithServiceURL("http://127.0.0.1:1"), WithStrict(true)) }, `{"apiKey":null,"serviceUrl":"http://127.0.0.1:1"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(replayAPIKeyEnv, tc.override)
			t.Setenv("BITFAB_API_KEY", tc.env)
			client := tc.client()
			defer client.Close(time.Second)
			registry := NewReplayRegistry()
			calls := 0
			if err := registry.Register("pipeline", ReplayRegistration{Client: client, Function: BindReplayFunction("registry", func(string) { calls++ })}); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			result, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--print-api-key", "--trace-ids", "a"}, &stdout, &stderr)
			if err != nil || result != nil {
				t.Fatalf("probe returned %v %v", result, err)
			}
			if got := stdout.String(); got != "@@bitfab:api-key "+tc.want+"\n" {
				t.Fatalf("stdout %q", got)
			}
			if calls != 0 || stderr.Len() != 0 {
				t.Fatalf("probe replayed: calls=%d stderr=%q", calls, stderr.String())
			}
		})
	}
}

func TestReplayRegistryCLIAPIKeyProbeRejectsAnUnknownPipeline(t *testing.T) {
	client := NewClient("key", WithServiceURL("http://127.0.0.1:1"))
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, Function: BindReplayFunction("registry", func(string) {})}); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	_, err := RunReplayCLI(context.Background(), registry, []string{"missing", "--print-api-key"}, &stdout, io.Discard)
	if err == nil || !strings.Contains(err.Error(), `unknown pipeline "missing"`) {
		t.Fatalf("error %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout %q", stdout.String())
	}
}

func TestReplayRegistryCLIIgnoresThePrintAPIKeyEnvironmentVariable(t *testing.T) {
	t.Setenv("BITFAB_REPLAY_PRINT_API_KEY", "1")
	client := NewClient("key", WithServiceURL("http://127.0.0.1:1"))
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, Function: BindReplayFunction("registry", func(string) {})}); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	_, _ = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1", "--trace-ids", "a"}, &stdout, io.Discard)
	if strings.Contains(stdout.String(), "@@bitfab:api-key") {
		t.Fatalf("stdout %q", stdout.String())
	}
}

func TestReplayRegistryCLIRequiresACodeChangeChoice(t *testing.T) {
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: newTestClient("http://127.0.0.1:1"), TraceFunctionKey: "key", Function: func(string, int) {}}); err != nil {
		t.Fatal(err)
	}
	pairing := "bitfab: --code-change-file goes with --code-change manual, and --code-change manual needs --code-change-file PATH."
	basePairing := "bitfab: --code-change-base goes with --code-change ref, and --code-change ref needs --code-change-base BRANCH|COMMIT."
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"pipeline", "--dry-run"}, `bitfab: --code-change is required: ` + codeChangeChoices + `. "ref" takes the branch or commit from --code-change-base, and "manual" takes the edits from --code-change-file PATH.`},
		{[]string{"pipeline", "--code-change", "change.json"}, `bitfab: Unknown --code-change 'change.json'. Pass ` + codeChangeChoices + `. A file of edits goes in --code-change manual --code-change-file PATH.`},
		{[]string{"pipeline", "--no-code-change"}, "bitfab: --no-code-change was replaced by --code-change skip."},
		{[]string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1", "-no-code-change=true"}, "bitfab: --no-code-change was replaced by --code-change skip."},
		{[]string{"pipeline", "--code-change", "manual", "--experiment-group-id", "group-1"}, pairing},
		{[]string{"pipeline", "--code-change", "working-tree", "--experiment-group-id", "group-1", "--code-change-file", "change.json"}, pairing},
		{[]string{"pipeline", "--code-change", "ref"}, basePairing},
		{[]string{"pipeline", "--code-change", "default-branch", "--code-change-base", "dev"}, basePairing},
	} {
		_, err := RunReplayCLI(context.Background(), registry, test.args, io.Discard, io.Discard)
		if err == nil || err.Error() != test.want {
			t.Fatalf("%v: error = %v, want %q", test.args, err, test.want)
		}
		if check := checkRegistryCLI(registry, test.args, io.Discard); check == nil || check.Error() != test.want {
			t.Fatalf("%v: check error = %v, want %q", test.args, check, test.want)
		}
	}
}

func TestReplayRegistryCLIPassesEachCodeChangeType(t *testing.T) {
	t.Chdir(initReplayGitRepo(t, "main"))
	for _, codeChange := range []string{"working-tree", "last-branch", "default-branch", "skip"} {
		t.Run(codeChange, func(t *testing.T) {
			state := &replayTestServerState{}
			server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
			defer server.Close()
			client := newTestClient(server.URL)
			defer client.Close(time.Second)
			registry := NewReplayRegistry()
			if err := registry.Register("pipeline", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func(string, int) {}}); err != nil {
				t.Fatal(err)
			}
			if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", codeChange, "--dry-run"}, io.Discard, io.Discard); err != nil {
				t.Fatal(err)
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			if _, present := state.startBody["codeChangeFiles"]; present {
				t.Fatalf("clean tree sent files: %#v", state.startBody)
			}
			flags := state.startBody["invocation"].(map[string]any)["flags"].(map[string]any)
			if flags["code-change"] != codeChange {
				t.Fatalf("flags = %#v", flags)
			}
		})
	}
}

func TestReplayRegistryCLIForwardsTheRefBase(t *testing.T) {
	root := initReplayGitRepo(t, "main")
	writeFixture(t, root, "base.txt", "changed\n")
	t.Chdir(root)
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func(string, int) {}}); err != nil {
		t.Fatal(err)
	}
	args := []string{"pipeline", "--code-change", "ref", "--code-change-base", "main", "--dry-run"}
	if err := checkRegistryCLI(registry, args, io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, err := RunReplayCLI(context.Background(), registry, args, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.startBody["codeChangeType"] != "ref" {
		t.Fatalf("start body = %#v", state.startBody)
	}
	if description, _ := state.startBody["codeChangeDescription"].(string); !strings.Contains(description, "vs main") {
		t.Fatalf("description = %#v", state.startBody["codeChangeDescription"])
	}
	flags := state.startBody["invocation"].(map[string]any)["flags"].(map[string]any)
	if flags["code-change"] != "ref" || flags["code-change-base"] != "main" {
		t.Fatalf("flags = %#v", flags)
	}
}

func TestReplayRegistryCLIManualReadsTheCodeChangeFile(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func(string, int) {}}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "change.json")
	if err := os.WriteFile(path, []byte(`{"description":"shorter prompt","files":[{"path":"prompt.go","before":"a","after":"b"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "manual", "--code-change-file", path, "--dry-run"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	body := state.startBody
	state.mu.Unlock()
	files, _ := body["codeChangeFiles"].([]any)
	if body["codeChangeDescription"] != "shorter prompt" || len(files) != 1 {
		t.Fatalf("start body = %#v", body)
	}

	invalid := filepath.Join(t.TempDir(), "invalid.json")
	if err := os.WriteFile(invalid, []byte(`{"files":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "manual", "--experiment-group-id", "group-1", "--code-change-file", invalid}, io.Discard, io.Discard)
	if want := "bitfab: Invalid --code-change-file '" + invalid + "': expected { description, files }."; err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

func TestReplayRegistryRefusesRegisteredCodeChangeOptions(t *testing.T) {
	client := newTestClient("http://127.0.0.1:1")
	defer client.Close(time.Second)
	want := "bitfab: registry options cannot set CodeChange, CodeChangeDescription, CodeChangeFiles, or CodeChangeBase; pass --code-change on the command line"
	for _, options := range []ReplayOptions{
		{CodeChange: CodeChangeSkip},
		{CodeChangeBase: "dev"},
		{CodeChangeDescription: "x"},
		{CodeChangeFiles: []CodeChangeFile{}},
	} {
		err := NewReplayRegistry().Register("pipeline", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func() {}, Options: options})
		if err == nil || err.Error() != want {
			t.Fatalf("%+v: error = %v", options, err)
		}
	}
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func() {}, OptionsFactory: func(_ context.Context, _ ReplayRegistryContext, options *ReplayOptions) error {
		options.CodeChange = CodeChangeWorkingTree
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1"}, io.Discard, io.Discard); err == nil || err.Error() != want {
		t.Fatalf("factory error = %v", err)
	}
}

func TestReplayRegistryCLIHelpDescribesCodeChange(t *testing.T) {
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: newTestClient("http://127.0.0.1:1"), TraceFunctionKey: "key", Function: func(string, int) {}}); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	_, _ = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--help"}, io.Discard, &stderr)
	for _, want := range []string{"-code-change working-tree|last-branch|default-branch|ref|skip|manual", "(required: the code change this run tests;", "-code-change-base BRANCH|COMMIT", "(with --code-change ref: the branch or commit to diff against)", "-code-change-file PATH", "(with --code-change manual: a JSON file of { description, files })"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("help lacks %q: %s", want, stderr.String())
		}
	}
	if strings.Contains(stderr.String(), "no-code-change") {
		t.Fatalf("help still lists --no-code-change: %s", stderr.String())
	}
}

func newExperimentGroupRegistry(t *testing.T) (*ReplayRegistry, *replayTestServerState) {
	t.Helper()
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	t.Cleanup(server.Close)
	client := newTestClient(server.URL)
	t.Cleanup(func() { client.Close(time.Second) })
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{
		Client:   client,
		Function: BindReplayFunction("registry", func(string, int) {}),
		Options:  ReplayOptions{TraceIDs: []string{"a"}},
	}); err != nil {
		t.Fatal(err)
	}
	return registry, state
}

func TestReplayRegistryCLIRequiresAnExperimentGroup(t *testing.T) {
	registry, state := newExperimentGroupRegistry(t)
	for _, args := range [][]string{
		{"pipeline", "--code-change", "skip"},
		{"pipeline", "--code-change", "skip", "--experiment-group-id", " "},
		{"pipeline", "--code-change", "skip", "--experiment-group-name", "Prompt sweep"},
	} {
		_, err := RunReplayCLI(context.Background(), registry, args, io.Discard, io.Discard)
		if err == nil || err.Error() != registryExperimentGroupRequiredMessage {
			t.Fatalf("RunReplayCLI(%v) error = %v", args, err)
		}
		if err = checkRegistryCLI(registry, args, io.Discard); err == nil || err.Error() != registryExperimentGroupRequiredMessage {
			t.Fatalf("checkRegistryCLI(%v) error = %v", args, err)
		}
	}
	if !strings.Contains(registryExperimentGroupRequiredMessage, "--experiment-group-id is required") || !strings.Contains(registryExperimentGroupRequiredMessage, "pass a new UUID with --experiment-group-name") || !strings.HasSuffix(registryExperimentGroupRequiredMessage, " To run without a group, pass --skip-experiment-group.") {
		t.Fatalf("message = %q", registryExperimentGroupRequiredMessage)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.startBody != nil {
		t.Fatalf("a missing group still started a replay: %+v", state.startBody)
	}
}

func TestReplayRegistryCLISkipExperimentGroupFlag(t *testing.T) {
	registry, state := newExperimentGroupRegistry(t)
	args := []string{"pipeline", "--code-change", "skip", "--skip-experiment-group"}
	if err := checkRegistryCLI(registry, args, io.Discard); err != nil {
		t.Fatalf("checkRegistryCLI(%v) error = %v", args, err)
	}
	if _, err := RunReplayCLI(context.Background(), registry, args, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.startBody == nil {
		t.Fatal("skipping the group did not start a replay")
	}
	for _, key := range []string{"requireExperimentGroup", "experimentGroupId", "experimentGroupName"} {
		if _, present := state.startBody[key]; present {
			t.Fatalf("skipped group still sent %s: %+v", key, state.startBody)
		}
	}
}

func TestReplayRegistryCLIRefusesSkipExperimentGroupWithAGroup(t *testing.T) {
	registry, state := newExperimentGroupRegistry(t)
	want := "bitfab: --skip-experiment-group cannot be combined with --experiment-group-id or --experiment-group-name"
	for _, args := range [][]string{
		{"pipeline", "--code-change", "skip", "--skip-experiment-group", "--experiment-group-id", "group-1"},
		{"pipeline", "--code-change", "skip", "--skip-experiment-group", "--experiment-group-name", "Prompt sweep"},
	} {
		_, err := RunReplayCLI(context.Background(), registry, args, io.Discard, io.Discard)
		if err == nil || err.Error() != want {
			t.Fatalf("RunReplayCLI(%v) error = %v", args, err)
		}
		if err = checkRegistryCLI(registry, args, io.Discard); err == nil || err.Error() != want {
			t.Fatalf("checkRegistryCLI(%v) error = %v", args, err)
		}
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.startBody != nil {
		t.Fatalf("a conflicting skip still started a replay: %+v", state.startBody)
	}
}

func TestReplayRegistryCLIIgnoresSkipExperimentGroupSetInCode(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	t.Cleanup(server.Close)
	client := newTestClient(server.URL)
	t.Cleanup(func() { client.Close(time.Second) })
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{
		Client:   client,
		Function: BindReplayFunction("registry", func(string, int) {}),
		Options:  ReplayOptions{TraceIDs: []string{"a"}, SkipExperimentGroup: true},
	}); err != nil {
		t.Fatal(err)
	}
	args := []string{"pipeline", "--code-change", "skip"}
	if _, err := RunReplayCLI(context.Background(), registry, args, io.Discard, io.Discard); err == nil || err.Error() != registryExperimentGroupRequiredMessage {
		t.Fatalf("RunReplayCLI(%v) error = %v", args, err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.startBody != nil {
		t.Fatalf("a skip set in code satisfied the command line: %+v", state.startBody)
	}
}

func TestReplayRegistryCLIDryRunNeedsNoExperimentGroup(t *testing.T) {
	registry, state := newExperimentGroupRegistry(t)
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--code-change", "skip", "--dry-run"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.startBody["requireExperimentGroup"] != true || state.startBody["dryRun"] != true {
		t.Fatalf("start body = %+v", state.startBody)
	}
}

func TestReplayRegistryCLIAgentSessionIDFlag(t *testing.T) {
	clearAgentSessionEnv(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "from-env")
	registry, state := newExperimentGroupRegistry(t)
	run := func(extra ...string) map[string]any {
		t.Helper()
		args := append([]string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1"}, extra...)
		if _, err := RunReplayCLI(context.Background(), registry, args, io.Discard, io.Discard); err != nil {
			t.Fatal(err)
		}
		state.mu.Lock()
		defer state.mu.Unlock()
		body := state.startBody
		state.startBody = nil
		return body
	}
	body := run("--agent-session-id", "from-flag")
	if body["agentSessionId"] != "from-flag" || body["requireExperimentGroup"] != true || body["experimentGroupId"] != "group-1" {
		t.Fatalf("start body = %+v", body)
	}
	invocation, _ := body["invocation"].(map[string]any)
	flags, _ := invocation["flags"].(map[string]any)
	if flags["agent-session-id"] != "from-flag" {
		t.Fatalf("invocation flags = %#v", flags)
	}
	if body = run(); body["agentSessionId"] != "from-env" {
		t.Fatalf("start body without the flag = %+v", body)
	}
}
