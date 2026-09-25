package bitfab

import (
	"context"
	"net/url"
	"testing"
)

func TestGraders_FiltersAndVerdictDetails(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"labels": []any{
			map[string]any{
				"traceId": "one", "graderId": "grader", "graderName": "No invented IDs", "graderStatus": "active",
				"label": false, "labelReason": "invented order 55", "failureDiagnostic": "hallucinated identifier",
				"labelConfidence": "High", "source": "live_grader", "evaluatedAt": "2026-09-03T00:00:00.000Z",
			},
			map[string]any{"traceId": "two", "graderId": "grader", "graderStatus": "active", "source": "human", "label": nil},
		}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	for _, params := range []GetGraderLabelsParams{
		{TraceIDs: []string{"one", "two"}},
		{GraderID: "grader", Limit: 10},
		{TraceIDs: []string{"one"}, GraderID: "grader", Limit: 2},
	} {
		labels, err := client.Graders.GetLabels(context.Background(), params)
		if err != nil || len(labels) != 2 {
			t.Fatalf("GetLabels = %+v, %v", labels, err)
		}
		if labels[0].Label == nil || *labels[0].Label || *labels[0].FailureDiagnostic != "hallucinated identifier" ||
			labels[0].Source != GraderLabelSourceLive || labels[1].Label != nil || labels[1].EvaluatedAt != nil {
			t.Fatalf("labels = %+v", labels)
		}
	}
	requests := server.recorded()
	wantQueries := []url.Values{
		{"traceIds": {"one,two"}},
		{"graderId": {"grader"}, "limit": {"10"}},
		{"traceIds": {"one"}, "graderId": {"grader"}, "limit": {"2"}},
	}
	for index, request := range requests {
		if request.path != "/api/sdk/graderLabels" || request.query != wantQueries[index].Encode() {
			t.Fatalf("request = %+v", request)
		}
	}
}

func TestGraders_RequiresASelector(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	if _, err := client.Graders.GetLabels(context.Background(), GetGraderLabelsParams{}); err == nil {
		t.Fatal("empty selector was accepted")
	}
	if len(server.recorded()) != 0 {
		t.Fatal("empty selector made a request")
	}
}

func TestGraders_SaveLabelPostsTheBatchRoute(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"labels": []any{
			map[string]any{"graderId": "grader", "traceId": "one", "label": false, "action": "set"},
		}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))

	outcome, err := client.Graders.SaveLabel(context.Background(), GraderLabelUpdate{
		GraderID: "grader", TraceID: "one", Label: false, Reason: "refunded twice", Confidence: "High",
	})
	if err != nil || outcome.GraderID != "grader" || outcome.Label || outcome.Action != "set" {
		t.Fatalf("SaveLabel = %+v, %v", outcome, err)
	}
	requests := server.recorded()
	if len(requests) != 1 || requests[0].method != "POST" || requests[0].path != "/api/sdk/graderLabels" {
		t.Fatalf("requests = %+v", requests)
	}
	labels, ok := requests[0].body["labels"].([]any)
	if !ok || len(labels) != 1 {
		t.Fatalf("body = %+v", requests[0].body)
	}
	sent := labels[0].(map[string]any)
	if sent["graderId"] != "grader" || sent["reason"] != "refunded twice" || sent["confidence"] != "High" {
		t.Fatalf("sent = %+v", sent)
	}
	if _, present := sent["failureDiagnostic"]; present {
		t.Fatalf("empty failureDiagnostic was sent: %+v", sent)
	}
}
