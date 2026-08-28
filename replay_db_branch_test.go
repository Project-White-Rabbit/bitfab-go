package bitfab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type replayDBTestState struct {
	mu          sync.Mutex
	resolveBody map[string]any
	released    []string
	resolveErr  bool
	releaseErr  bool
}

func replayDBHandler(
	t *testing.T,
	traceState *replayTestServerState,
	dbState *replayDBTestState,
	resolveResponse map[string]any,
) http.HandlerFunc {
	t.Helper()
	items := replayItems()[:1]
	items[0]["dbSnapshotRef"] = map[string]any{
		"sdkWallClockBeforeFn": "2026-08-20T12:00:00.000Z",
		"origin":               "origin-1",
	}
	base := replayTestHandler(t, traceState, items)
	return func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/sdk/replay/resolveDbBranchLease":
			if dbState.resolveErr {
				http.Error(writer, "branch service unavailable", http.StatusServiceUnavailable)
				return
			}
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatalf("decode resolve body: %v", err)
			}
			dbState.mu.Lock()
			dbState.resolveBody = body
			dbState.mu.Unlock()
			writeReplayTestJSON(t, writer, resolveResponse)
		case "/api/sdk/replay/releaseDbBranchLease":
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatalf("decode release body: %v", err)
			}
			dbState.mu.Lock()
			dbState.released = append(dbState.released, body["neonBranchId"].(string))
			dbState.mu.Unlock()
			if dbState.releaseErr {
				http.Error(writer, "release unavailable", http.StatusServiceUnavailable)
				return
			}
			writeReplayTestJSON(t, writer, map[string]any{"released": true})
		default:
			base.ServeHTTP(writer, request)
		}
	}
}

func successfulDBResolveResponse() map[string]any {
	return map[string]any{
		"dbSnapshotRef": map[string]any{
			"sdkWallClockBeforeFn": "2026-08-20T12:00:00.000Z",
			"origin":               "origin-1",
		},
		"lease": map[string]any{
			"neonBranchId":       "branch-1",
			"envKey":             "DATABASE_URL",
			"databaseUrl":        "postgres://secret@historical/db",
			"expiresAt":          "2026-08-24T13:00:00.000Z",
			"snapshotTimestamp":  "2026-08-20T12:00:00.000Z",
			"providerConsoleUrl": "https://console.neon.tech/branch-1",
			"readOnly":           true,
			"region":             "aws-us-west-2",
			"futureField":        "preserved",
		},
		"leaseError": nil,
		"timings": map[string]any{
			"startedAt":      "2026-08-24T12:00:00.000Z",
			"branchCreateMs": 25,
			"warmupMs":       4,
			"totalMs":        40,
		},
	}
}

func TestReplayUsesHistoricalDBBranchReportsAccessAndReleases(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	traceState := &replayTestServerState{}
	dbState := &replayDBTestState{}
	server := newLegacyCarrierServer(t, replayDBHandler(t, traceState, dbState, successfulDBResolveResponse()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var branchCalls atomic.Int64
	var seenURL string

	result, err := client.Replay(
		context.Background(),
		"db-workflow",
		func(ctx context.Context, _ string, _ int) (string, error) {
			branchCalls.Add(1)
			branch := GetCurrentReplayBranch(ctx)
			if branch == nil || branch.TraceID != "original-trace-1" || branch.Extra["futureField"] != "preserved" {
				return "", &testError{message: "historical branch metadata missing"}
			}
			encoded, err := json.Marshal(branch)
			formatted := fmt.Sprintf("%#v %+v %s", branch, branch, branch)
			if err != nil || strings.Contains(string(encoded), "postgres://") || strings.Contains(formatted, "postgres://") {
				return "", &testError{message: "branch serialization exposed its connection string"}
			}
			seenURL = branch.DatabaseURL()
			return "used-historical", nil
		},
		&ReplayOptions{DBBranch: &DBBranchOptions{
			MinCU:     1,
			MaxCU:     1,
			WarmupSQL: "SELECT count(*) FROM documents",
		}},
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	item := result.Items[0]
	if item.Result != "used-historical" || seenURL != "postgres://secret@historical/db" || item.DBSnapshotRef == nil || item.DBBranchTimings == nil {
		t.Fatalf("item = %#v", item)
	}
	if branchCalls.Load() != 1 {
		t.Fatalf("branch function calls = %d", branchCalls.Load())
	}
	serialized, err := SerializeReplayResult(result)
	if err != nil || strings.Contains(serialized, "postgres://") {
		t.Fatalf("serialized result leaked branch URL: %s (%v)", serialized, err)
	}

	dbState.mu.Lock()
	resolveBody := dbState.resolveBody
	released := append([]string(nil), dbState.released...)
	dbState.mu.Unlock()
	traceState.mu.Lock()
	startBody := traceState.startBody
	traceState.mu.Unlock()
	if startBody["includeDbBranchLease"] != true || startBody["lazyDbBranchLease"] != true {
		t.Fatalf("start body = %#v", startBody)
	}
	settings := resolveBody["dbBranchSettings"].(map[string]any)
	if settings["minCu"] != float64(1) || settings["maxCu"] != float64(1) || settings["warmupSql"] == nil {
		t.Fatalf("resolve body = %#v", resolveBody)
	}
	if len(released) != 1 || released[0] != "branch-1" {
		t.Fatalf("released branches = %#v", released)
	}

	traceState.mu.Lock()
	defer traceState.mu.Unlock()
	if len(traceState.traces) != 1 {
		t.Fatalf("traces = %#v", traceState.traces)
	}
	rawTrace := traceState.traces[0]["externalTrace"].(map[string]any)
	if rawTrace["db_snapshot_ref"] == nil {
		t.Fatalf("trace omitted db_snapshot_ref: %#v", rawTrace)
	}
	usage := rawTrace["db_snapshot_usage"].(map[string]any)
	if usage["accessed"] != true || usage["original_trace_id"] != "original-trace-1" || usage["source_trace_id"] != "original-trace-1" || usage["region"] != "aws-us-west-2" || usage["timings"] == nil {
		t.Fatalf("db snapshot usage = %#v", usage)
	}
}

func TestReplayDBBranchResolutionFailureFailsItemClosed(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	traceState := &replayTestServerState{}
	dbState := &replayDBTestState{}
	response := map[string]any{
		"dbSnapshotRef": map[string]any{"sdkWallClockBeforeFn": "2026-08-20T12:00:00.000Z"},
		"lease":         nil,
		"leaseError": map[string]any{
			"code":    "branch_create_failed",
			"message": "Neon rejected the branch",
		},
		"timings": map[string]any{
			"startedAt": "2026-08-24T12:00:00.000Z",
			"totalMs":   18,
		},
	}
	server := newLegacyCarrierServer(t, replayDBHandler(t, traceState, dbState, response))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var calls atomic.Int64

	result, err := client.Replay(
		context.Background(),
		"db-workflow",
		func(context.Context, string, int) string {
			calls.Add(1)
			return "unsafe"
		},
		&ReplayOptions{DBBranch: &DBBranchOptions{}},
	)
	if err != nil {
		t.Fatalf("Replay returned whole-run error: %v", err)
	}
	item := result.Items[0]
	var branchErr *DBBranchReplayError
	if !errors.As(item.ReplayError, &branchErr) {
		t.Fatalf("replay error = %T %v", item.ReplayError, item.ReplayError)
	}
	if branchErr == nil || branchErr.Code != "branch_create_failed" || item.Error == nil || !strings.Contains(*item.Error, "live database") {
		t.Fatalf("item = %#v", item)
	}
	if calls.Load() != 0 || item.DurationMS != nil || item.DBBranchTimings == nil {
		t.Fatalf("closed failure item=%#v calls=%d", item, calls.Load())
	}
}

func TestReplayDBBranchTransportFailureRetainsCauseAndFailsClosed(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	traceState := &replayTestServerState{}
	dbState := &replayDBTestState{resolveErr: true}
	server := newLegacyCarrierServer(t, replayDBHandler(t, traceState, dbState, nil))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)
	var calls atomic.Int64

	result, err := client.Replay(
		context.Background(),
		"db-workflow",
		func(context.Context, string, int) string {
			calls.Add(1)
			return "unsafe"
		},
		&ReplayOptions{DBBranch: &DBBranchOptions{}},
	)
	if err != nil {
		t.Fatalf("Replay returned whole-run error: %v", err)
	}
	item := result.Items[0]
	var branchErr *DBBranchReplayError
	if !errors.As(item.ReplayError, &branchErr) {
		t.Fatalf("replay error = %T %v", item.ReplayError, item.ReplayError)
	}
	if branchErr.Code != "lease_request_failed" || branchErr.Cause == nil || !strings.Contains(branchErr.Cause.Error(), "503") {
		t.Fatalf("branch error = %#v", branchErr)
	}
	if calls.Load() != 0 {
		t.Fatalf("replayed function ran %d times", calls.Load())
	}
	serialized, serializeErr := SerializeReplayResult(result)
	if serializeErr != nil || !strings.Contains(serialized, `"code":"lease_request_failed"`) || !strings.Contains(serialized, `"cause"`) {
		t.Fatalf("serialized=%s error=%v", serialized, serializeErr)
	}
}

func TestReplayReleasesDBBranchWhenFunctionErrors(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	traceState := &replayTestServerState{}
	dbState := &replayDBTestState{}
	server := newLegacyCarrierServer(t, replayDBHandler(t, traceState, dbState, successfulDBResolveResponse()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	result, err := client.Replay(
		context.Background(),
		"db-workflow",
		func(context.Context, string, int) (string, error) {
			return "", errors.New("workflow failed")
		},
		&ReplayOptions{DBBranch: &DBBranchOptions{}},
	)
	if err != nil {
		t.Fatalf("Replay returned whole-run error: %v", err)
	}
	if result.Items[0].TraceError == nil || result.Items[0].ReplayError != nil {
		t.Fatalf("item = %#v", result.Items[0])
	}
	dbState.mu.Lock()
	defer dbState.mu.Unlock()
	if !reflect.DeepEqual(dbState.released, []string{"branch-1"}) {
		t.Fatalf("released branches = %#v", dbState.released)
	}
}

func TestReplayDBBranchReleaseFailureDoesNotFailRun(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	traceState := &replayTestServerState{}
	dbState := &replayDBTestState{releaseErr: true}
	server := newLegacyCarrierServer(t, replayDBHandler(t, traceState, dbState, successfulDBResolveResponse()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	result, err := client.Replay(
		context.Background(),
		"db-workflow",
		func(context.Context, string, int) string { return "ok" },
		&ReplayOptions{DBBranch: &DBBranchOptions{}},
	)
	if err != nil {
		t.Fatalf("Replay returned error after release failure: %v", err)
	}
	if result.Items[0].Result != "ok" || result.Items[0].Error != nil {
		t.Fatalf("item = %#v", result.Items[0])
	}
	dbState.mu.Lock()
	defer dbState.mu.Unlock()
	if !reflect.DeepEqual(dbState.released, []string{"branch-1"}) {
		t.Fatalf("released branches = %#v", dbState.released)
	}
}

func TestReplayDBSnapshotUsageRecordsUnaccessedBranch(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	traceState := &replayTestServerState{}
	dbState := &replayDBTestState{}
	server := newLegacyCarrierServer(t, replayDBHandler(t, traceState, dbState, successfulDBResolveResponse()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	result, err := client.Replay(
		context.Background(),
		"db-workflow",
		func(ctx context.Context, _ string, _ int) (bool, error) {
			branch := GetCurrentReplayBranch(ctx)
			return branch != nil && branch.Region == "aws-us-west-2", nil
		},
		&ReplayOptions{DBBranch: &DBBranchOptions{}},
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if result.Items[0].Result != true {
		t.Fatalf("item = %#v", result.Items[0])
	}
	traceState.mu.Lock()
	defer traceState.mu.Unlock()
	rawTrace := traceState.traces[0]["externalTrace"].(map[string]any)
	usage := rawTrace["db_snapshot_usage"].(map[string]any)
	if usage["accessed"] != false || usage["neon_branch_id"] != "branch-1" {
		t.Fatalf("db snapshot usage = %#v", usage)
	}
}

func TestReplayWithoutDBBranchSkipsResolveAndRelease(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	traceState := &replayTestServerState{}
	dbState := &replayDBTestState{}
	server := newLegacyCarrierServer(t, replayDBHandler(t, traceState, dbState, successfulDBResolveResponse()))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	result, err := client.Replay(
		context.Background(),
		"db-workflow",
		func(ctx context.Context, _ string, _ int) bool {
			return GetCurrentReplayBranch(ctx) == nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if result.Items[0].Result != true {
		t.Fatalf("item = %#v", result.Items[0])
	}
	dbState.mu.Lock()
	defer dbState.mu.Unlock()
	if dbState.resolveBody != nil || len(dbState.released) != 0 {
		t.Fatalf("resolve=%#v released=%#v", dbState.resolveBody, dbState.released)
	}
	traceState.mu.Lock()
	defer traceState.mu.Unlock()
	if traceState.startBody["includeDbBranchLease"] != nil || traceState.startBody["lazyDbBranchLease"] != nil {
		t.Fatalf("start body = %#v", traceState.startBody)
	}
}

func TestRootTraceAlwaysCapturesDBSnapshotRef(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, nil))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	if _, err := client.Span(context.Background(), "snapshot-workflow", func(context.Context) (any, error) {
		return "ok", nil
	}); err != nil {
		t.Fatalf("Span returned error: %v", err)
	}
	if !client.FlushTraces(5 * time.Second) {
		t.Fatal("trace did not flush")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.traces) != 1 {
		t.Fatalf("traces = %#v", state.traces)
	}
	rawTrace := state.traces[0]["externalTrace"].(map[string]any)
	ref := rawTrace["db_snapshot_ref"].(map[string]any)
	if ref["sdkWallClockBeforeFn"] == "" || ref["provider"] != nil {
		t.Fatalf("db snapshot ref = %#v", ref)
	}
}

type testError struct {
	message string
}

func (err *testError) Error() string {
	return err.message
}
