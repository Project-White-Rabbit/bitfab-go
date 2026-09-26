package bitfab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "assertion", "approvalState": "approved"}}, "inheritedFrom": nil})
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
	_, err = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--limit", "1", "--dry-run", "--param", "count=2", "--no-code-change", "--notes", "staging env override"}, &stdout, &stderr)
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
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
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
	if _, err = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--dry-run", "--metadata", "schedule=eod", "--metadata", "owner=ada=team"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if metadata := startMetadata(); len(metadata) != 2 || metadata["schedule"] != "eod" || metadata["owner"] != "ada=team" {
		t.Fatalf("metadata from flags = %+v", metadata)
	}
	if _, err = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--dry-run"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if metadata := startMetadata(); len(metadata) != 1 || metadata["schedule"] != "registered" {
		t.Fatalf("registered metadata lost without the flag: %+v", metadata)
	}
	state.mu.Lock()
	state.startBody = nil
	state.mu.Unlock()
	for _, bad := range []string{"schedule", "=eod"} {
		_, err = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--dry-run", "--metadata", bad}, &stdout, &stderr)
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

func TestReplayRegistryBoundExplicitOrderingInheritedAndErrors(t *testing.T) {
	reads := []string{}
	server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
		reads = append(reads, r.URL.Path)
		switch r.URL.Path {
		case "/api/sdk/traces/replayed/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "x", "approvalState": "approved"}}, "inheritedFrom": "ancestor"})
		case "/api/sdk/traces/good/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "x", "approvalState": "approved"}}, "inheritedFrom": nil})
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

// A trace whose assertions are all still awaiting review is not replayable
// under --only-with-assertions: only an approved assertion is checked on a
// replay, so narrowing must skip it exactly as the server's selection does.
func TestReplayRegistryCLISkipsAssertionsAwaitingReview(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	base := replayTestHandler(t, state, replayItems())
	server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/sdk/traces/draft/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "a1", "approvalState": "pending"}}, "inheritedFrom": nil})
		case "/api/sdk/traces/reviewed/assertions":
			writeReplayTestJSON(t, w, map[string]any{"assertions": []any{map[string]any{"id": "a2", "approvalState": "approved"}}, "inheritedFrom": nil})
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
		[]string{"pipeline", "--trace-ids", "draft,reviewed", "--limit", "1", "--dry-run", "--only-with-assertions", "--no-code-change"},
		&stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	got, ok := state.startBody["traceIds"].([]any)
	if !ok || len(got) != 1 || got[0] != "reviewed" {
		t.Fatalf("selected trace ids = %#v, want only the reviewed trace", state.startBody["traceIds"])
	}
}

func TestReplayRegistryCLISkipAssertionJudging(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	for _, tc := range []struct {
		name       string
		registered bool
		args       []string
		want       any
	}{
		{"flag omits judging", false, []string{"pipeline", "--skip-assertion-judging"}, nil},
		{"absent flag keeps judging on", false, []string{"pipeline"}, true},
		{"absent flag keeps a registered skip", true, []string{"pipeline"}, nil},
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
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
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
			if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--judge-assertions"}, io.Discard, &stderr); err != nil {
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
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
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
	if _, err := RunReplayCLI(context.Background(), registry, []string{"pipeline"}, io.Discard, &stderr); err != nil {
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
	_, err := RunReplayCLI(context.Background(), registry, []string{"pipeline"}, io.Discard, &stderr)
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
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
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

	if _, err := RunReplayCLI(context.Background(), registry, []string{"broken"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("without the flag an errored item must not fail the command: %v", err)
	}
	if _, err := RunReplayCLI(context.Background(), registry, []string{"working", "--fail-on-error"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("no item errored: %v", err)
	}

	var stdout bytes.Buffer
	_, err := RunReplayCLI(context.Background(), registry, []string{"broken", "--fail-on-error"}, &stdout, io.Discard)
	want := "[replay] 2 of 2 replayed items errored; exiting 1 because of --fail-on-error"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v", err)
	}
	if !strings.Contains(stdout.String(), `"run-1"`) {
		t.Fatalf("result was not printed before the error: %s", stdout.String())
	}

	_, err = RunReplayCLI(context.Background(), registry, []string{"broken", "--dry-run", "--fail-on-error"}, io.Discard, io.Discard)
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
	_, _ = RunReplayCLI(context.Background(), registry, []string{"pipeline", "--help"}, io.Discard, &stderr)
	if !strings.Contains(stderr.String(), "-fail-on-error") {
		t.Fatalf("help = %s", stderr.String())
	}
}
