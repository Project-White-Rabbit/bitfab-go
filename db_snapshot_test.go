package bitfab

import (
	"context"
	"testing"
	"time"
)

func TestDBSnapshotProviderCapturesSpanAndSubtreeRootsButNotSeeds(t *testing.T) {
	option, err := WithDBSnapshot(DBSnapshotConfig{Provider: "neon"})
	if err != nil {
		t.Fatal(err)
	}
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, nil))
	defer server.Close()
	client := NewClient("key", WithServiceURL(server.URL), WithSimulationPlan(false), option)
	defer client.Close(time.Second)
	if _, err := client.Span(context.Background(), "span", func(context.Context) (any, error) { return 1, nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Trace(context.Background(), "trace", func(context.Context) (any, error) { return 1, nil }, TraceOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SeedCase(context.Background(), "seed", SeedCaseOptions{Input: []any{1}, Expected: 1}); err != nil {
		t.Fatal(err)
	}
	client.FlushTraces(time.Second)
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.traces) != 3 {
		t.Fatalf("trace count %d", len(state.traces))
	}
	for _, payload := range state.traces {
		trace := payload["externalTrace"].(map[string]any)
		if trace["ingestion_type"] == "seeded" {
			if trace["db_snapshot_ref"] != nil {
				t.Fatal("seed received pin")
			}
			continue
		}
		ref := trace["db_snapshot_ref"].(map[string]any)
		if ref["provider"] != "neon" || ref["sdkWallClockBeforeFn"] != trace["started_at"] {
			t.Fatalf("bad pin: %+v", ref)
		}
	}
}

func TestDBSnapshotProviderValidation(t *testing.T) {
	for _, provider := range []string{"", "postgres", "NEON"} {
		if _, err := WithDBSnapshot(DBSnapshotConfig{Provider: provider}); err == nil {
			t.Fatalf("accepted %q", provider)
		}
	}
	client := NewClient("key", WithSimulationPlan(false))
	defer client.Close(time.Second)
	if ref := client.buildDBSnapshotRef("timestamp"); ref.Provider != "" || ref.SDKWallClockBeforeFn != "timestamp" {
		t.Fatalf("default pin %+v", ref)
	}
}
