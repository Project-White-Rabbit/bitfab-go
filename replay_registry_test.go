package bitfab

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestReplayRegistryCLIOptionsSelectionAndFactory(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
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
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "assertion"}}, "inheritedFrom": nil})
		default:
			base(w, r)
		}
	})
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	calls := 0
	err := registry.Register("pipeline", ReplayRegistration{Client: client, Function: BindReplayFunction("registry", func(string, int) { calls++ }), Options: ReplayOptions{DatasetIDs: []string{"dataset"}, OnlyWithAssertions: true}, OptionsFactory: func(ctx context.Context, params ReplayRegistryContext, options *ReplayOptions) error {
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
	_, err = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--limit", "1", "--dry-run", "--param", "count=2", "--no-code-change"}, &stdout, &stderr)
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

func TestReplayRegistryBoundExplicitOrderingInheritedAndErrors(t *testing.T) {
	reads := []string{}
	server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
		reads = append(reads, r.URL.Path)
		switch r.URL.Path {
		case "/api/sdk/traces/replayed/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "x"}}, "inheritedFrom": "ancestor"})
		case "/api/sdk/traces/good/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "x"}}, "inheritedFrom": nil})
		default:
			http.Error(w, "failed", 500)
		}
	})
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	ids, err := boundRegistryTraceIDs(context.Background(), client, []string{"replayed", "good", "unread"}, 1, true)
	if err != nil || !slices.Equal(ids, []string{"good"}) || len(reads) != 2 {
		t.Fatalf("selection %v %v reads %v", ids, err, reads)
	}
	if _, err = boundRegistryTraceIDs(context.Background(), client, []string{"bad", "good"}, 1, true); err == nil {
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
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, []map[string]any{}))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: client, TraceFunctionKey: "key", Function: func() {}}); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"pipeline", "--limit", "0"}, {"pipeline", "--attempts", "101"}, {"pipeline", "--trace-ids", "a", "--dataset-id", "b"}, {"pipeline", "--db-branch", "--no-db-branch"}} {
		if _, err := RunReplayCLI(context.Background(), registry, args, io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--dry-run"}, io.Discard, io.Discard); err == nil {
		t.Fatal("empty selection accepted")
	}
}

func TestReplayRegistryGradeHookChainsAndDryRunSkips(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
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
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline"}, io.Discard, &stderr); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || !bytes.Contains(stderr.Bytes(), []byte("grade failed")) {
		t.Fatalf("hook calls=%d stderr=%s", calls, stderr.String())
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--dry-run"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("dry run invoked grading hook")
	}
}
