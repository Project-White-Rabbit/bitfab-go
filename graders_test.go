package bitfab

import (
	"context"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestGraders_FiltersAndVerdictDetails(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"verdicts": []any{
			map[string]any{
				"traceId": "one", "graderId": "grader", "graderName": "No invented IDs", "graderStatus": "active",
				"verdict": "fail", "reason": "invented order 55", "failureDiagnostic": "hallucinated identifier",
				"confidence": "High", "source": "live_grader", "evaluatedAt": "2026-09-03T00:00:00.000Z",
			},
			map[string]any{"traceId": "two", "graderId": "grader", "graderStatus": "active", "source": "human", "verdict": nil},
		}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	for _, params := range []GetGraderVerdictsParams{
		{TraceIDs: []string{"one", "two"}},
		{GraderID: "grader", Limit: 10},
		{TraceIDs: []string{"one"}, GraderID: "grader", Limit: 2},
	} {
		verdicts, err := client.Graders.GetVerdicts(context.Background(), params)
		if err != nil || len(verdicts) != 2 {
			t.Fatalf("GetVerdicts = %+v, %v", verdicts, err)
		}
		first := verdicts[0]
		if first.Verdict == nil || *first.Verdict != VerdictFail || *first.Reason != "invented order 55" ||
			*first.Confidence != VerdictConfidenceHigh || *first.FailureDiagnostic != "hallucinated identifier" ||
			first.Source != GraderVerdictSourceLive || verdicts[1].Verdict != nil || verdicts[1].EvaluatedAt != nil {
			t.Fatalf("verdicts = %+v", verdicts)
		}
		if first.Label == nil || *first.Label || first.LabelReason != first.Reason || first.LabelConfidence != first.Confidence || verdicts[1].Label != nil {
			t.Fatalf("deprecated fields = %+v", verdicts)
		}
	}
	requests := server.recorded()
	wantQueries := []url.Values{
		{"traceIds": {"one,two"}},
		{"graderId": {"grader"}, "limit": {"10"}},
		{"traceIds": {"one"}, "graderId": {"grader"}, "limit": {"2"}},
	}
	for index, request := range requests {
		if request.path != "/api/sdk/graderVerdicts" || request.query != wantQueries[index].Encode() {
			t.Fatalf("request = %+v", request)
		}
	}
}

func TestGraders_RequiresASelector(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	if _, err := client.Graders.GetVerdicts(context.Background(), GetGraderVerdictsParams{}); err == nil {
		t.Fatal("empty selector was accepted")
	}
	if len(server.recorded()) != 0 {
		t.Fatal("empty selector made a request")
	}
}

func TestGraders_SaveVerdictPostsTheBatchRoute(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"verdicts": []any{
			map[string]any{"graderId": "grader", "traceId": "one", "verdict": "fail", "action": "set"},
		}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))

	outcome, err := client.Graders.SaveVerdict(context.Background(), GraderVerdictUpdate{
		GraderID: "grader", TraceID: "one", Verdict: VerdictFail, Reason: "refunded twice", Confidence: VerdictConfidenceHigh,
	})
	if err != nil || outcome.GraderID != "grader" || outcome.Verdict != VerdictFail || outcome.Label || outcome.Action != VerdictActionSet {
		t.Fatalf("SaveVerdict = %+v, %v", outcome, err)
	}
	requests := server.recorded()
	if len(requests) != 1 || requests[0].method != "POST" || requests[0].path != "/api/sdk/graderVerdicts" {
		t.Fatalf("requests = %+v", requests)
	}
	want := map[string]any{"verdicts": []any{map[string]any{
		"graderId": "grader", "traceId": "one", "verdict": "fail", "reason": "refunded twice", "confidence": "High",
	}}}
	if !reflect.DeepEqual(requests[0].body, want) {
		t.Fatalf("body = %#v", requests[0].body)
	}
}

func TestGraders_DeprecatedMethodsSendTheNewSpelling(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		if r.method == "GET" {
			return map[string]any{"verdicts": []any{map[string]any{"traceId": "one", "graderId": "grader", "verdict": "pass", "reason": "ok", "source": "live_grader"}}}
		}
		return map[string]any{"verdicts": []any{map[string]any{"graderId": "grader", "traceId": "one", "verdict": "pass", "action": "set"}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	outcomes, err := client.Graders.SaveLabelAll(ctx, []GraderLabelUpdate{{GraderID: "grader", TraceID: "one", Label: true}})
	if err != nil || len(outcomes) != 1 || !outcomes[0].Label || outcomes[0].Verdict != VerdictPass {
		t.Fatalf("SaveLabelAll = %+v, %v", outcomes, err)
	}
	if got := server.recorded()[0].body["verdicts"].([]any)[0].(map[string]any)["verdict"]; got != "pass" {
		t.Fatalf("body = %#v", server.recorded()[0].body)
	}
	labels, err := client.Graders.GetLabels(ctx, GetGraderLabelsParams{TraceIDs: []string{"one"}})
	if err != nil || len(labels) != 1 || labels[0].Label == nil || !*labels[0].Label || *labels[0].LabelReason != "ok" {
		t.Fatalf("GetLabels = %+v, %v", labels, err)
	}
}

func TestGraders_RefusesAbstainAndConflictingVerdicts(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	if _, err := client.Graders.SaveVerdict(ctx, GraderVerdictUpdate{GraderID: "grader", TraceID: "one", Verdict: VerdictAbstained}); err == nil || !strings.Contains(err.Error(), "must be pass or fail") {
		t.Fatalf("abstained = %v", err)
	}
	if _, err := client.Graders.SaveVerdict(ctx, GraderVerdictUpdate{GraderID: "grader", TraceID: "one", Verdict: VerdictFail, Label: true}); err == nil || !strings.Contains(err.Error(), "set only Verdict") {
		t.Fatalf("both set = %v", err)
	}
	if len(server.recorded()) != 0 {
		t.Fatal("a refused update reached the server")
	}
}
