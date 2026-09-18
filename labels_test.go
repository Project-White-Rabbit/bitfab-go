package bitfab

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"testing"
)

func TestLabels_GenerateLabelEvidenceUsesSelectedTraceAndAssertion(t *testing.T) {
	spanName := "lookup-record"
	want := []AssertionLabelEvidence{{
		SpanID:   "span",
		Text:     "Returned the right record",
		SpanName: &spanName,
		Parameters: []AssertionEvidenceParameter{{
			Name: "recordId",
			Type: "string",
		}},
		SpanType: "function",
		IsMocked: false,
	}}
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"traceId": "one", "assertionId": "assertion", "evidence": want}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	evidence, err := client.Labels.GenerateLabelEvidence(context.Background(), "trace", "assertion")
	if err != nil || !reflect.DeepEqual(evidence, want) {
		t.Fatalf("GenerateLabelEvidence = %#v, %v", evidence, err)
	}
	request := server.recorded()[0]
	if request.path != "/api/sdk/traces/trace/assertions/assertion/evidence" || request.query != "" {
		t.Fatalf("request = %+v", request)
	}
}

func TestLabels_SaveReplayAssertionPreservesFalseAndAttemptZero(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"labels": []any{map[string]any{"key": "original#0", "traceId": "replayed", "action": "set"}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	attempt := 0
	result, err := client.Labels.Save(context.Background(), LabelUpdate{
		LabelTarget: LabelTarget{OriginalTraceID: "original", Attempt: &attempt, AssertionID: "assertion"},
		Label:       false, Annotation: "arrived late", Confidence: LabelConfidenceHigh,
	}, WithLabelTestRunID("run"))
	if err != nil || result.TraceID != "replayed" || result.Action != LabelActionSet {
		t.Fatalf("Save = %+v, %v", result, err)
	}
	want := map[string]any{"testRunId": "run", "labels": []any{map[string]any{
		"originalTraceId": "original", "attempt": float64(0), "assertionId": "assertion",
		"label": false, "annotation": "arrived late", "confidence": "High",
	}}}
	request := server.recorded()[0]
	if request.path != "/api/sdk/traces/labels" || !reflect.DeepEqual(request.body, want) {
		t.Fatalf("request = %+v, want %#v", request, want)
	}
}

func TestLabels_SaveAndReadEvidence(t *testing.T) {
	evidence := Justification{{SpanID: "span", Text: "Returned the right record"}}
	server := newDatasetsServer(t, func(r datasetRequest) any {
		if r.method == http.MethodGet {
			return map[string]any{"labels": []any{map[string]any{
				"traceId": "one", "labelStatus": "labeled", "label": true,
				"annotation": "good", "evidence": evidence, "approved": false,
				"passed": 0, "failed": 0, "assertions": []any{},
			}}}
		}
		return map[string]any{"labels": []any{map[string]any{
			"key": "one", "traceId": "one", "action": "set",
		}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	if _, err := client.Labels.Save(ctx, LabelUpdate{
		LabelTarget: LabelTarget{TraceID: "one"}, Label: true,
		Annotation: "good", Evidence: &evidence,
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"labels": []any{map[string]any{
		"traceId": "one", "label": true, "annotation": "good",
		"evidence": []any{map[string]any{"spanId": "span", "text": "Returned the right record"}},
	}}}
	if !reflect.DeepEqual(server.recorded()[0].body, want) {
		t.Fatalf("request = %#v, want %#v", server.recorded()[0].body, want)
	}
	got, err := client.Labels.Get(ctx, "one")
	if err != nil || got == nil || got.Evidence == nil || !reflect.DeepEqual(*got.Evidence, evidence) {
		t.Fatalf("Get = %+v, %v", got, err)
	}
}

func TestLabels_MixedBatchAndTargetedSkipArchive(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		labels := r.body["labels"].([]any)
		outcomes := make([]any, len(labels))
		for index, raw := range labels {
			action := "set"
			update := raw.(map[string]any)
			if update["skip"] == true {
				action = "skipped"
			} else if update["archive"] == true {
				action = "archived"
			}
			outcomes[index] = map[string]any{"key": "key", "traceId": "trace", "action": action}
		}
		return map[string]any{"labels": outcomes}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	_, err := client.Labels.SaveAll(ctx, []LabelUpdate{
		{LabelTarget: LabelTarget{TraceID: "one"}, Label: true, Annotation: "works"},
		{LabelTarget: LabelTarget{TraceID: "two", AssertionID: "check"}, Skip: true},
		{LabelTarget: LabelTarget{OriginalTraceID: "three"}, Archive: true},
	}, WithLabelTestRunID("run"))
	if err != nil {
		t.Fatal(err)
	}
	attempt := 2
	target := LabelTarget{OriginalTraceID: "original", Attempt: &attempt, AssertionID: "assertion"}
	skipped, err := client.Labels.Skip(ctx, target, WithLabelTestRunID("run"))
	if err != nil || skipped.Action != LabelActionSkipped {
		t.Fatalf("Skip = %+v, %v", skipped, err)
	}
	archived, err := client.Labels.Archive(ctx, target, WithLabelTestRunID("run"))
	if err != nil || archived.Action != LabelActionArchived {
		t.Fatalf("Archive = %+v, %v", archived, err)
	}
	if _, err := client.Labels.Skip(ctx, LabelTarget{TraceID: "direct"}); err != nil {
		t.Fatal(err)
	}
	requests := server.recorded()
	for index, action := range []string{"skip", "archive"} {
		want := map[string]any{"testRunId": "run", "labels": []any{map[string]any{
			"originalTraceId": "original", "attempt": float64(2), "assertionId": "assertion", action: true,
		}}}
		if !reflect.DeepEqual(requests[index+1].body, want) {
			t.Errorf("%s request = %#v", action, requests[index+1].body)
		}
	}
	wantDirect := map[string]any{"labels": []any{map[string]any{"traceId": "direct", "skip": true}}}
	if !reflect.DeepEqual(requests[3].body, wantDirect) {
		t.Fatalf("direct skip = %#v", requests[3].body)
	}
}

func TestLabels_HumanWritesUseDedicatedEndpoint(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		outcomes := make([]any, 0)
		for _, raw := range r.body["labels"].([]any) {
			update := raw.(map[string]any)
			outcomes = append(outcomes, map[string]any{
				"traceId": update["traceId"], "assertionId": update["assertionId"], "label": update["label"], "action": "set",
			})
		}
		return map[string]any{"labels": outcomes}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	result, err := client.Labels.SaveHuman(context.Background(), HumanLabelUpdate{
		TraceID: "one", AssertionID: "assertion", Label: false, Annotation: "human check", Confidence: LabelConfidenceMedium,
	})
	if err != nil || result.Label || result.AssertionID == nil || *result.AssertionID != "assertion" {
		t.Fatalf("SaveHuman = %+v, %v", result, err)
	}
	results, err := client.Labels.SaveHumanAll(context.Background(), []HumanLabelUpdate{
		{TraceID: "two", Label: true, Annotation: "pass"},
		{TraceID: "three", Label: false, Annotation: "fail"},
	})
	if err != nil || len(results) != 2 || results[0].AssertionID != nil {
		t.Fatalf("SaveHumanAll = %+v, %v", results, err)
	}
	for _, request := range server.recorded() {
		if request.method != http.MethodPost || request.path != "/api/sdk/traces/labels/human" {
			t.Fatalf("request = %+v", request)
		}
	}
	want := map[string]any{"labels": []any{map[string]any{
		"traceId": "one", "assertionId": "assertion", "label": false, "annotation": "human check", "confidence": "Medium",
	}}}
	if !reflect.DeepEqual(server.recorded()[0].body, want) {
		t.Fatalf("human body = %#v", server.recorded()[0].body)
	}
}

func TestLabels_ReadsPreserveNullsAndAssertionVerdicts(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		query, _ := url.ParseQuery(r.query)
		if query.Get("traceIds") == "missing" {
			return map[string]any{"labels": []any{}}
		}
		return map[string]any{"labels": []any{map[string]any{
			"traceId": "one", "labelStatus": "skipped", "label": nil, "annotation": nil,
			"approved": false, "passed": 0, "failed": 1,
			"assertions": []any{map[string]any{
				"assertionId": "assertion", "assertion": "arrives on time", "labelStatus": "labeled", "label": false,
				"annotation": "arrived late", "confidence": "High", "labelSource": "human", "approved": true,
			}},
		}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	labels, err := client.Labels.GetAll(ctx, []string{"one", "two"})
	if err != nil || len(labels) != 1 || labels[0].Label != nil || labels[0].Annotation != nil || labels[0].Failed != 1 {
		t.Fatalf("GetAll = %+v, %v", labels, err)
	}
	verdict := labels[0].Assertions[0]
	if verdict.Label == nil || *verdict.Label || !verdict.Approved || verdict.LabelSource != LabelSourceHuman {
		t.Fatalf("assertion = %+v", verdict)
	}
	missing, err := client.Labels.Get(ctx, "missing")
	if err != nil || missing != nil {
		t.Fatalf("Get missing = %+v, %v", missing, err)
	}
	empty, err := client.Labels.GetAll(ctx, nil)
	if err != nil || empty == nil || len(empty) != 0 || len(server.recorded()) != 2 {
		t.Fatalf("empty read = %+v, %v", empty, err)
	}
	if server.recorded()[0].query != "traceIds=one%2Ctwo" {
		t.Fatalf("query = %s", server.recorded()[0].query)
	}
}

func TestLabels_InvalidTargetsDoNotSendPartialBatch(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	negative := -1
	zero := 0
	for _, target := range []LabelTarget{
		{}, {TraceID: "one", OriginalTraceID: "two"},
		{OriginalTraceID: "one", Attempt: &negative}, {TraceID: "one", Attempt: &zero},
	} {
		_, err := client.Labels.SaveAll(context.Background(), []LabelUpdate{
			{LabelTarget: LabelTarget{TraceID: "valid"}, Label: true, Annotation: "good"},
			{LabelTarget: target, Skip: true},
		})
		if err == nil {
			t.Fatalf("invalid target accepted: %+v", target)
		}
	}
	if len(server.recorded()) != 0 {
		t.Fatal("invalid batch sent a request")
	}
}
