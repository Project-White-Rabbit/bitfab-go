package bitfab

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func experimentJSON(id string) map[string]any {
	return map[string]any{
		"id": id, "name": "candidate", "notes": nil, "status": "completed", "traceFunctionKey": "orders",
		"createdAt": "2026-09-20T00:00:00.000Z", "completedAt": "2026-09-20T00:05:00.000Z",
		"codeChangeDescription": nil, "codeChangeTotals": map[string]any{"files": 2, "added": 10, "removed": 3},
		"experimentGroupId": nil, "experimentGroupName": nil, "datasetId": "dataset-1", "datasetName": "Checkout",
		"datasetIds": []string{"dataset-1"}, "datasetNames": []string{"Checkout"}, "attempts": 3, "plannedTraceCount": 12,
		"runBy":       map[string]any{"id": "user-1", "fullName": "Ada", "email": nil, "imageUrl": nil},
		"githubEmail": nil, "gitBranch": "main", "commitSha": "abc123", "baseSha": nil, "experimentSha": nil,
		"metadata": map[string]any{"schedule": "eod"},
	}
}

func experimentRollupJSON(id string) map[string]any {
	tally := map[string]any{
		"fixed": 1, "stillPassing": 2, "regressed": 3, "stillFailing": 4, "originalUnlabeled": 5,
		"unpaired": 6, "classified": 7, "passing": 8, "failing": 9,
	}
	return map[string]any{
		"experimentId": id,
		"totals": map[string]any{
			"total": 12, "succeeded": 10, "failed": 1, "pending": 0, "awaitingLabels": 0,
			"errored": 1, "withErrors": 1, "ungradable": 0, "skipped": 0,
		},
		"rollup": map[string]any{
			"traces": tally, "skippedTraces": 1, "labels": tally, "checks": tally,
			"categories": []any{map[string]any{
				"category": map[string]any{"id": "category-1", "title": "Safety", "description": "no invented ids"},
				"labels":   tally,
			}},
			"jitter": map[string]any{"disagreeing": 2, "attempts": 30, "traces": 12},
		},
	}
}

func TestExperiments_ExposedOnClient(t *testing.T) {
	client := NewClient("test-key")
	if client.Experiments == nil {
		t.Fatal("client.Experiments is nil")
	}
}

func TestExperiments_ListEncodesEveryFilterAndDecodesThePage(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"experiments": []any{experimentJSON("experiment-1")}, "nextCursor": "next", "hasMore": true}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	after := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 9, 24, 12, 30, 0, 500000000, time.FixedZone("PDT", -7*3600))
	page, err := client.Experiments.List(context.Background(), ListExperimentsParams{
		DatasetID: "dataset-1", TraceFunctionKey: "orders", GitBranch: "main", ExperimentGroupID: "group-1",
		Status: "completed", Metadata: map[string]string{"schedule": "eod", "owner": "ada"},
		CreatedAfter: &after, CreatedBefore: &before, Cursor: "cursor-1", Limit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := url.Values{
		"datasetId": {"dataset-1"}, "traceFunctionKey": {"orders"}, "gitBranch": {"main"}, "experimentGroupId": {"group-1"},
		"status": {"completed"}, "metadata": {`{"owner":"ada","schedule":"eod"}`},
		"createdAfter": {"2026-09-01T00:00:00Z"}, "createdBefore": {"2026-09-24T12:30:00.5-07:00"},
		"cursor": {"cursor-1"}, "limit": {"5"},
	}
	requests := server.recorded()
	if len(requests) != 1 || requests[0].method != http.MethodGet || requests[0].path != "/api/sdk/experiments" || requests[0].query != want.Encode() {
		t.Fatalf("requests = %+v, want query %s", requests, want.Encode())
	}
	if len(page.Experiments) != 1 || page.NextCursor == nil || *page.NextCursor != "next" || !page.HasMore {
		t.Fatalf("page = %+v", page)
	}
	experiment := page.Experiments[0]
	if experiment.ID != "experiment-1" || experiment.Notes != nil || *experiment.Name != "candidate" ||
		experiment.Status != "completed" || *experiment.CompletedAt != "2026-09-20T00:05:00.000Z" ||
		experiment.CodeChangeTotals == nil || experiment.CodeChangeTotals.Added != 10 ||
		experiment.ExperimentGroupID != nil || *experiment.DatasetID != "dataset-1" ||
		len(experiment.DatasetIDs) != 1 || experiment.Attempts != 3 || *experiment.PlannedTraceCount != 12 ||
		experiment.RunBy == nil || *experiment.RunBy.FullName != "Ada" || experiment.RunBy.Email != nil ||
		experiment.GitHubEmail != nil || *experiment.GitBranch != "main" || *experiment.CommitSHA != "abc123" ||
		experiment.BaseSHA != nil || experiment.Metadata["schedule"] != "eod" {
		t.Fatalf("experiment = %+v", experiment)
	}
}

func TestExperiments_ListUnfilteredSendsNoQuery(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"experiments": []any{}, "nextCursor": nil, "hasMore": false}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	page, err := client.Experiments.List(context.Background(), ListExperimentsParams{})
	if err != nil {
		t.Fatal(err)
	}
	requests := server.recorded()
	if len(requests) != 1 || requests[0].path != "/api/sdk/experiments" || requests[0].query != "" {
		t.Fatalf("requests = %+v", requests)
	}
	if len(page.Experiments) != 0 || page.NextCursor != nil || page.HasMore {
		t.Fatalf("page = %+v", page)
	}
}

func TestExperiments_Get(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"experiment": experimentJSON("experiment-1")}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	experiment, err := client.Experiments.Get(context.Background(), "experiment-1")
	if err != nil {
		t.Fatal(err)
	}
	requests := server.recorded()
	if len(requests) != 1 || requests[0].path != "/api/sdk/experiments/experiment-1" || requests[0].query != "" {
		t.Fatalf("requests = %+v", requests)
	}
	if experiment.ID != "experiment-1" || *experiment.TraceFunctionKey != "orders" || experiment.Metadata["schedule"] != "eod" {
		t.Fatalf("experiment = %+v", experiment)
	}
}

func TestExperiments_GetRollup(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return experimentRollupJSON("experiment-1")
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	rollup, err := client.Experiments.GetRollup(context.Background(), "experiment-1")
	if err != nil {
		t.Fatal(err)
	}
	requests := server.recorded()
	if len(requests) != 1 || requests[0].path != "/api/sdk/experiments/experiment-1/rollup" {
		t.Fatalf("requests = %+v", requests)
	}
	if rollup.ExperimentID != "experiment-1" || rollup.Totals.Total != 12 || rollup.Totals.Succeeded != 10 ||
		rollup.Rollup.Traces.Passing != 8 || rollup.Rollup.Traces.Failing != 9 || rollup.Rollup.SkippedTraces != 1 ||
		rollup.Rollup.Labels.Fixed != 1 || rollup.Rollup.Checks.OriginalUnlabeled != 5 ||
		len(rollup.Rollup.Categories) != 1 || rollup.Rollup.Categories[0].Category.Title != "Safety" ||
		rollup.Rollup.Categories[0].Labels.Regressed != 3 || rollup.Rollup.Jitter.Disagreeing != 2 ||
		rollup.Rollup.Jitter.Attempts != 30 || rollup.Rollup.Jitter.Traces != 12 {
		t.Fatalf("rollup = %+v", rollup)
	}
}

func TestExperiments_GetRollupAllSendsOneRequest(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"rollups": []any{experimentRollupJSON("a"), experimentRollupJSON("b"), experimentRollupJSON("c")}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	rollups, err := client.Experiments.GetRollupAll(context.Background(), []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	requests := server.recorded()
	wantQuery := url.Values{"experimentIds": {"a,b,c"}}.Encode()
	if len(requests) != 1 || requests[0].path != "/api/sdk/experiments/rollups" || requests[0].query != wantQuery {
		t.Fatalf("requests = %+v, want query %s", requests, wantQuery)
	}
	if len(rollups) != 3 || rollups[0].ExperimentID != "a" || rollups[1].ExperimentID != "b" || rollups[2].ExperimentID != "c" {
		t.Fatalf("rollups = %+v", rollups)
	}
}

func TestExperiments_GetRollupAllEmptyMakesNoRequest(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	for _, ids := range [][]string{nil, {}} {
		rollups, err := client.Experiments.GetRollupAll(context.Background(), ids)
		if err != nil || rollups == nil || len(rollups) != 0 {
			t.Fatalf("GetRollupAll(%v) = %+v, %v", ids, rollups, err)
		}
	}
	if len(server.recorded()) != 0 {
		t.Fatal("empty ids made a request")
	}
}

func TestExperiments_NotFoundIsAStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		http.Error(w, `{"error":"Experiment not found"}`, http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	calls := map[string]func() error{
		"Get":          func() error { _, err := client.Experiments.Get(ctx, "missing"); return err },
		"GetRollup":    func() error { _, err := client.Experiments.GetRollup(ctx, "missing"); return err },
		"GetRollupAll": func() error { _, err := client.Experiments.GetRollupAll(ctx, []string{"missing"}); return err },
	}
	for name, call := range calls {
		err := call()
		var statusErr *httpStatusError
		if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
			t.Fatalf("%s error = %#v, want httpStatusError with status 404", name, err)
		}
	}
}
