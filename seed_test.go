package bitfab

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestSeedCaseAndRunRecordWithoutCaptureOrDatabasePins(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, nil))
	defer server.Close()
	client := NewClient("test-key", WithServiceURL(server.URL), WithCaptureEnabled(false), WithSimulationPlan(false))
	defer client.Close(time.Second)
	calls := 0
	fn := func(ctx context.Context, value int) (int, error) {
		calls++
		if GetCurrentTrace(ctx).TraceID() == "" {
			t.Error("missing active trace")
		}
		return value * 2, nil
	}
	id, err := client.SeedCase(context.Background(), "seed", SeedCaseOptions{Input: []any{3}, Expected: 7, Function: fn, Metadata: map[string]any{"case": "a"}, SessionID: "session", Name: "case"})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("case executed callable")
	}
	runID, err := client.SeedTrace(context.Background(), "seed", fn, &SeedOptions{Args: []any{3}})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || id == runID {
		t.Fatalf("execution or identity mismatch %s %s %d", id, runID, calls)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.traces) != 2 {
		t.Fatalf("traces: %+v", state.traces)
	}
	for _, trace := range state.traces {
		raw := trace["externalTrace"].(map[string]any)
		if raw["ingestion_type"] != "seeded" || raw["db_snapshot_ref"] != nil {
			t.Fatalf("seed flags: %+v", raw)
		}
	}
}

func TestReseedAdoptsOnlySuccessfulRun(t *testing.T) {
	state := &replayTestServerState{}
	base := replayTestHandler(t, state, nil)
	adopted := 0
	server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/sdk/traces/original/reseedSource":
			writeReplayTestJSON(t, w, map[string]any{"traceFunctionKey": "seed", "input": 3, "metadata": map[string]any{"row": 1}, "sessionId": "session", "name": "case"})
		case "/api/sdk/traces/original/reseed":
			adopted++
			writeReplayTestJSON(t, w, map[string]any{"traceId": "original", "previousRunTraceId": "previous"})
		default:
			base(w, r)
		}
	})
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	result, err := client.ReseedTrace(context.Background(), "seed", func(v int) int { return v + 1 }, "original")
	if err != nil || result.TraceID != "original" || result.PreviousRunTraceID != "previous" || adopted != 1 {
		t.Fatalf("%+v %v", result, err)
	}
	_, err = client.ReseedTrace(context.Background(), "seed", func(int) error { return errors.New("failure") }, "original")
	if err == nil || adopted != 1 {
		t.Fatalf("failed run adopted: %d %v", adopted, err)
	}
	_, err = client.ReseedTrace(context.Background(), "wrong", func(int) {}, "original")
	if err == nil || adopted != 1 {
		t.Fatal("key mismatch accepted")
	}
}

func TestSeedValidationPreventsExecution(t *testing.T) {
	client := NewClient("test-key", WithSimulationPlan(false))
	defer client.Close(time.Second)
	calls := 0
	fn := func(int) { calls++ }
	if _, err := client.SeedCase(context.Background(), "seed", SeedCaseOptions{Function: fn}); err == nil {
		t.Fatal("missing input accepted")
	}
	if _, err := client.SeedTrace(context.Background(), "seed", fn, nil); err == nil {
		t.Fatal("missing args accepted")
	}
	if _, err := client.SeedTrace(context.Background(), " ", func() { calls++ }, nil); err == nil {
		t.Fatal("empty key accepted")
	}
	if calls != 0 {
		t.Fatal("invalid seed executed")
	}
}
