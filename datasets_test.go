package bitfab

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

const testDatasetID = "11111111-1111-4111-8111-111111111111"

func testDataset() map[string]any {
	return map[string]any{
		"id":               testDatasetID,
		"traceFunctionKey": "checkout-agent",
		"name":             "Refund failures",
		"description":      nil,
		"traceCount":       2,
		"graders":          []any{},
		"createdAt":        "2026-08-30T00:00:00.000Z",
		"updatedAt":        "2026-08-30T00:00:00.000Z",
	}
}

func testRerun(status string) map[string]any {
	var result any
	if status == "completed" {
		result = map[string]any{"tracesGraded": 2, "gradersRun": 1}
	}
	return map[string]any{
		"id":        "22222222-2222-4222-8222-222222222222",
		"status":    status,
		"graderIds": []string{"33333333-3333-4333-8333-333333333333"},
		"progress":  nil,
		"result":    result,
		"error":     nil,
		"createdAt": "2026-08-30T00:00:00.000Z",
		"updatedAt": "2026-08-30T00:00:00.000Z",
	}
}

type datasetRequest struct {
	method string
	path   string
	query  string
	body   map[string]any
}

type datasetsServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []datasetRequest
	respond  func(r datasetRequest) any
}

func newDatasetsServer(t *testing.T, respond func(r datasetRequest) any) *datasetsServer {
	t.Helper()
	s := &datasetsServer{respond: respond}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("Authorization = %q, want Bearer test-key", r.Header.Get("Authorization"))
		}
		rec := datasetRequest{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery}
		if r.Method == http.MethodPost {
			_ = json.NewDecoder(r.Body).Decode(&rec.body)
		}
		s.mu.Lock()
		s.requests = append(s.requests, rec)
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.respond(rec))
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *datasetsServer) recorded() []datasetRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]datasetRequest(nil), s.requests...)
}

func TestDatasets_ExposedOnClient(t *testing.T) {
	client := NewClient("test-key")
	if client.Datasets == nil {
		t.Fatal("client.Datasets is nil")
	}
}

func TestDatasets_SaveOmitsEmptyDescription(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"dataset": testDataset(), "created": true}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))

	result, err := client.Datasets.Save(context.Background(), SaveDatasetParams{
		TraceFunctionKey: "checkout-agent",
		Name:             "Refund failures",
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if !result.Created || result.Dataset.ID != testDatasetID {
		t.Errorf("result = %+v", result)
	}
	req := server.recorded()[0]
	if req.method != http.MethodPost || req.path != "/api/sdk/datasets" {
		t.Errorf("request = %s %s", req.method, req.path)
	}
	if _, present := req.body["description"]; present {
		t.Errorf("description must be omitted, body = %v", req.body)
	}
	if req.body["traceFunctionKey"] != "checkout-agent" || req.body["name"] != "Refund failures" {
		t.Errorf("body = %v", req.body)
	}
}

func TestDatasets_ListAndGet(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		if r.path == "/api/sdk/datasets" {
			return map[string]any{"datasets": []any{testDataset()}}
		}
		if r.path == "/api/sdk/datasets/"+testDatasetID+"/traces" {
			return map[string]any{"datasetId": testDatasetID, "traceIds": []string{"a", "b"}}
		}
		return map[string]any{"dataset": testDataset()}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()

	all, err := client.Datasets.List(ctx, ListDatasetsParams{})
	if err != nil || len(all) != 1 || all[0].TraceCount != 2 {
		t.Fatalf("List = %+v, %v", all, err)
	}
	if _, err := client.Datasets.List(ctx, ListDatasetsParams{TraceFunctionKey: "checkout agent"}); err != nil {
		t.Fatalf("List scoped: %v", err)
	}
	got, err := client.Datasets.Get(ctx, testDatasetID)
	if err != nil || got.Name != "Refund failures" || got.Description != nil {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	traces, err := client.Datasets.ListTraces(ctx, testDatasetID)
	if err != nil || len(traces.TraceIDs) != 2 {
		t.Fatalf("ListTraces = %+v, %v", traces, err)
	}

	requests := server.recorded()
	if requests[0].query != "" {
		t.Errorf("unscoped list query = %q", requests[0].query)
	}
	if requests[1].query != "traceFunctionKey=checkout+agent" {
		t.Errorf("scoped list query = %q", requests[1].query)
	}
	if requests[2].path != "/api/sdk/datasets/"+testDatasetID {
		t.Errorf("get path = %q", requests[2].path)
	}
}

func TestDatasets_MembershipAndGraderEndpoints(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{
			"dataset":       testDataset(),
			"addedTraceIds": []string{"t1"},
		}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()

	added, err := client.Datasets.AddTraces(ctx, testDatasetID, []string{"t1"})
	if err != nil || len(added.AddedTraceIDs) != 1 {
		t.Fatalf("AddTraces = %+v, %v", added, err)
	}
	if _, err := client.Datasets.RemoveTraces(ctx, testDatasetID, []string{"t1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Datasets.AddGraders(ctx, testDatasetID, []string{"g1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Datasets.RemoveGraders(ctx, testDatasetID, []string{"g1"}); err != nil {
		t.Fatal(err)
	}

	base := "/api/sdk/datasets/" + testDatasetID
	want := []struct {
		path string
		key  string
	}{
		{base + "/traces", "traceIds"},
		{base + "/removeTraces", "traceIds"},
		{base + "/graders", "graderIds"},
		{base + "/removeGraders", "graderIds"},
	}
	requests := server.recorded()
	for i, w := range want {
		if requests[i].method != http.MethodPost || requests[i].path != w.path {
			t.Errorf("request %d = %s %s, want POST %s", i, requests[i].method, requests[i].path, w.path)
		}
		if _, ok := requests[i].body[w.key]; !ok {
			t.Errorf("request %d body = %v, want key %s", i, requests[i].body, w.key)
		}
	}
}

func TestDatasets_RerunGradersPollsUntilSettled(t *testing.T) {
	polls := 0
	server := newDatasetsServer(t, func(r datasetRequest) any {
		if r.method == http.MethodPost {
			return map[string]any{"run": testRerun("pending"), "joinedExisting": false}
		}
		polls++
		if polls == 1 {
			return map[string]any{"run": testRerun("running")}
		}
		return map[string]any{"run": testRerun("completed")}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))

	result, err := client.Datasets.RerunGraders(context.Background(), testDatasetID, RerunGradersOptions{
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("RerunGraders: %v", err)
	}
	if result.Run.Status != GraderRerunCompleted || result.Run.Result == nil || result.Run.Result.TracesGraded != 2 {
		t.Errorf("result = %+v", result)
	}
	requests := server.recorded()
	if len(requests[0].body) != 0 {
		t.Errorf("start body = %v, want empty", requests[0].body)
	}
	if polls != 2 || requests[1].query != "runId="+testRerun("pending")["id"].(string) {
		t.Errorf("polls = %d, query = %q", polls, requests[1].query)
	}
}

func TestDatasets_RerunGradersNoWaitForwardsGraderIDs(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"run": testRerun("pending"), "joinedExisting": true}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))

	result, err := client.Datasets.RerunGraders(context.Background(), testDatasetID, RerunGradersOptions{
		GraderIDs: []string{"g1"},
		NoWait:    true,
	})
	if err != nil {
		t.Fatalf("RerunGraders: %v", err)
	}
	if !result.JoinedExisting || result.Run.Status != GraderRerunPending {
		t.Errorf("result = %+v", result)
	}
	requests := server.recorded()
	if len(requests) != 1 {
		t.Errorf("requests = %d, want 1", len(requests))
	}
	if ids, _ := requests[0].body["graderIds"].([]any); len(ids) != 1 || ids[0] != "g1" {
		t.Errorf("body = %v", requests[0].body)
	}
}

func TestDatasets_RerunGradersStopsAtTimeout(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		if r.method == http.MethodPost {
			return map[string]any{"run": testRerun("pending"), "joinedExisting": false}
		}
		return map[string]any{"run": testRerun("running")}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))

	result, err := client.Datasets.RerunGraders(context.Background(), testDatasetID, RerunGradersOptions{
		Timeout:      5 * time.Millisecond,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("RerunGraders: %v", err)
	}
	if result.Run.Status.Terminal() {
		t.Errorf("status = %q, want non-terminal", result.Run.Status)
	}
}

func TestDatasets_GetGraderRerun(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		if r.query == "" {
			return map[string]any{"run": nil}
		}
		return map[string]any{"run": testRerun("completed")}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()

	active, err := client.Datasets.GetGraderRerun(ctx, testDatasetID, "")
	if err != nil || active != nil {
		t.Fatalf("active = %+v, %v", active, err)
	}
	byID, err := client.Datasets.GetGraderRerun(ctx, testDatasetID, "r1")
	if err != nil || byID == nil || byID.Status != GraderRerunCompleted {
		t.Fatalf("byID = %+v, %v", byID, err)
	}
	if server.recorded()[1].query != "runId=r1" {
		t.Errorf("query = %q", server.recorded()[1].query)
	}
}
