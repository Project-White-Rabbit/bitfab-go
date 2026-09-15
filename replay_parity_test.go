package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestReplayAttemptsCarryIdentityMetadataAndIndependentBranches(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	items := replayItems()
	for _, item := range items {
		item["originalMetadata"] = map[string]any{"case": "sample"}
		item["ingestionType"] = "seeded"
	}
	base := replayTestHandler(t, state, items)
	var mu sync.Mutex
	var leases, releases []string
	server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/sdk/replay/resolveDbBranchLease":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				return
			}
			attempt, _ := body["attempt"].(float64)
			id := fmt.Sprintf("%s-%d", body["traceId"], int(attempt))
			mu.Lock()
			leases = append(leases, id)
			mu.Unlock()
			response := successfulDBResolveResponse()
			response["lease"].(map[string]any)["neonBranchId"] = id
			response["lease"].(map[string]any)["databaseUrl"] = "postgres://" + id
			writeReplayTestJSON(t, w, response)
		case "/api/sdk/replay/releaseDbBranchLease":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			releases = append(releases, body["neonBranchId"].(string))
			mu.Unlock()
			writeReplayTestJSON(t, w, map[string]any{})
		default:
			base(w, r)
		}
	})
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var starts []ReplayItemStartProgress
	var finishes []ReplayItemFinishProgress
	result, err := client.Replay(context.Background(), "attempts", func(ctx context.Context, name string, count int) string {
		return GetCurrentReplayBranch(ctx).DatabaseURL()
	}, &ReplayOptions{Attempts: 2, MaxConcurrency: 2, Mock: MockNone, DBBranch: &DBBranchOptions{}, OnlyWithAssertions: true,
		AdaptInputs: func(inputs []any, ctx AdaptContext) ([]any, error) {
			if ctx.Metadata["case"] != "sample" {
				return nil, fmt.Errorf("metadata missing")
			}
			return inputs, nil
		}, OnItemStart: func(p ReplayItemStartProgress) { starts = append(starts, p) }, OnItemFinish: func(p ReplayItemFinishProgress) { finishes = append(finishes, p) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempts != 2 || len(result.Items) != 4 || len(starts) != 4 || len(finishes) != 4 {
		t.Fatalf("unexpected results: %+v", result)
	}
	startedItems := map[string]int{}
	finishedItems := map[string]int{}
	for _, event := range starts {
		startedItems[fmt.Sprintf("%s-%d", event.Item.OriginalTraceID, event.Item.Attempt)]++
	}
	for _, event := range finishes {
		finishedItems[fmt.Sprintf("%s-%d", event.Item.OriginalTraceID, event.Item.Attempt)]++
	}
	ids := map[string]bool{}
	for index, item := range result.Items {
		attempt := index / 2
		if item.Attempt != attempt {
			t.Fatalf("attempt lost at %d", index)
		}
		if item.Error != nil || item.TraceID == nil || ids[*item.TraceID] {
			t.Fatalf("missing/duplicate trace: %+v", item)
		}
		ids[*item.TraceID] = true
		if item.IngestionType == nil || *item.IngestionType != "seeded" {
			t.Fatal("ingestion type lost")
		}
		expected := fmt.Sprintf("%s-%d", item.OriginalTraceID, attempt)
		if startedItems[expected] != 1 || finishedItems[expected] != 1 {
			t.Fatalf("missing or duplicated attempt lifecycle for %s", expected)
		}
		if item.Result != "postgres://"+expected {
			t.Fatalf("wrong branch: %+v", item)
		}
	}
	slices.Sort(leases)
	slices.Sort(releases)
	if len(leases) != 4 || !slices.Equal(leases, releases) {
		t.Fatalf("leases=%v releases=%v", leases, releases)
	}
	if state.startBody["attempts"] != float64(2) || state.startBody["onlyWithAssertions"] != true || state.startBody["includeOriginalMetadata"] != true {
		t.Fatalf("missing request options: %v", state.startBody)
	}
	attempts := map[float64]int{}
	for _, trace := range state.traces {
		raw := trace["externalTrace"].(map[string]any)
		attempt, ok := raw["replay_attempt"].(float64)
		if !ok {
			t.Fatal("trace missing attempt")
		}
		attempts[attempt]++
	}
	if attempts[0] != 2 || attempts[1] != 2 {
		t.Fatalf("wrong completion attempts: %v", attempts)
	}
}

func TestReplayDryRunSkipsExecutionBranchesAndPersistence(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, replayItems()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(time.Second)
	result, err := client.Replay(context.Background(), "dry", func(string, int) { t.Error("dry run executed") }, &ReplayOptions{DryRun: true, Attempts: 2, DBBranch: &DBBranchOptions{}, Mock: MockAll})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 4 || len(state.traces) != 0 || state.statusCalls != 0 {
		t.Fatalf("unexpected dry run: %+v", result)
	}
	if _, ok := state.startBody["includeDbBranchLease"]; ok {
		t.Fatal("dry run requested leases")
	}
	for _, item := range result.Items {
		if item.Error != nil || len(item.Input) != 2 || item.DurationMS != nil || item.TraceID != nil {
			t.Fatalf("bad dry item: %+v", item)
		}
	}
}

func TestReplayAttemptsValidateBeforeStarting(t *testing.T) {
	for _, attempts := range []int{-1, 101} {
		if _, err := normalizeReplayOptions(&ReplayOptions{Attempts: attempts}); err == nil {
			t.Fatalf("accepted attempts %d", attempts)
		}
	}
	for _, attempts := range []int{0, 1, 100} {
		if _, err := normalizeReplayOptions(&ReplayOptions{Attempts: attempts}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReplayMissingIngestionTypeRemainsUnknown(t *testing.T) {
	item := baseReplayItem(replayServerItem{OriginalTraceID: "original"})
	if item.IngestionType != nil {
		t.Fatal("missing ingestion type must remain unknown")
	}
	encoded, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err = json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if value, present := fields["ingestionType"]; !present || value != nil {
		t.Fatalf("unexpected missing-type contract: %s", encoded)
	}
}
