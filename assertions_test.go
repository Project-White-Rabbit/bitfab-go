package bitfab

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

func TestAssertions_SaveBatchPreservesOmittedNullAndTarget(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		return map[string]any{"assertions": []any{
			map[string]any{"id": "assertion-1", "traceId": "trace-1", "assertion": "lands before 9am", "source": "human",
				"category_assertion_id": "category-1", "category": map[string]any{"id": "category-1", "title": "Timing", "description": "Arrival requirements"},
				"targetOnEvaluatedTrace": map[string]any{"kind": "span", "name": "book", "occurrence": 0}},
			map[string]any{"id": "assertion-2", "traceId": "trace-2", "assertion": "arrives on time", "source": "human"},
		}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	pass := "before 9am local time"
	categoryID := "category-1"
	result, err := client.Traces.SaveAssertionsAll(context.Background(), SaveAssertionsAllParams{
		Source: TraceAssertionHuman,
		Updates: []TraceAssertionsUpdate{
			{TraceID: "trace-1", Assertions: []SaveAssertion{{
				ID: "assertion-1", Assertion: "lands before 9am", PassCriteria: &pass, CategoryAssertionID: &categoryID,
				ClearFailCriteria:      true,
				TargetOnEvaluatedTrace: &TraceTarget{Kind: TraceTargetSpan, Name: "book", Occurrence: SpanOccurrenceAt(0)},
			}}},
			{TraceID: "trace-2", Assertions: []SaveAssertion{{
				Assertion: "arrives on time", ClearTargetOnEvaluatedTrace: true, ClearCategoryAssertionID: true,
			}}},
		},
	})
	if err != nil || len(result) != 2 {
		t.Fatalf("SaveAssertionsAll = %+v, %v", result, err)
	}
	if result[0].TargetOnEvaluatedTrace.Occurrence != SpanOccurrenceAt(0) || result[1].TargetOnEvaluatedTrace != nil {
		t.Fatalf("decoded targets = %+v", result)
	}
	if result[0].CategoryAssertionID == nil || *result[0].CategoryAssertionID != categoryID || result[0].Category == nil || result[0].Category.Title != "Timing" {
		t.Fatalf("decoded category = %+v", result[0])
	}
	requests := server.recorded()
	if len(requests) != 1 || requests[0].method != http.MethodPost || requests[0].path != "/api/sdk/traces/assertions" {
		t.Fatalf("requests = %+v", requests)
	}
	want := map[string]any{
		"source": "human",
		"updates": []any{
			map[string]any{"traceId": "trace-1", "assertions": []any{map[string]any{
				"id": "assertion-1", "assertion": "lands before 9am", "passCriteria": pass, "failCriteria": nil,
				"category_assertion_id":  "category-1",
				"targetOnEvaluatedTrace": map[string]any{"kind": "span", "name": "book", "occurrence": float64(0)},
			}}},
			map[string]any{"traceId": "trace-2", "assertions": []any{map[string]any{
				"assertion": "arrives on time", "targetOnEvaluatedTrace": nil, "category_assertion_id": nil,
			}}},
		},
	}
	if !reflect.DeepEqual(requests[0].body, want) {
		t.Fatalf("body = %#v, want %#v", requests[0].body, want)
	}
}

func TestAssertions_SingularUsesBatchAndDefaultsToAgent(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{"assertions": []any{}} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	_, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
		TraceID: "trace", Assertions: []SaveAssertion{{Assertion: "works"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"source": "agent", "updates": []any{
		map[string]any{"traceId": "trace", "assertions": []any{map[string]any{"assertion": "works"}}},
	}}
	request := server.recorded()[0]
	if request.path != "/api/sdk/traces/assertions" || !reflect.DeepEqual(request.body, want) {
		t.Fatalf("request = %+v", request)
	}
	empty, err := client.Traces.SaveAssertionsAll(context.Background(), SaveAssertionsAllParams{})
	if err != nil || len(empty) != 0 || empty == nil || len(server.recorded()) != 1 {
		t.Fatalf("empty batch = %+v, %v", empty, err)
	}
}

func TestAssertions_InheritedReadAndArchive(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		if r.method == http.MethodGet {
			return map[string]any{"assertions": []any{}, "inheritedFrom": "original"}
		}
		return map[string]any{"archived": []string{"assertion"}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	result, err := client.Traces.GetAssertions(context.Background(), "a/b")
	if err != nil || result.InheritedFrom == nil || *result.InheritedFrom != "original" {
		t.Fatalf("GetAssertions = %+v, %v", result, err)
	}
	archived, err := client.Traces.ArchiveAssertions(context.Background(), ArchiveAssertionsParams{
		TraceID: "trace", AssertionIDs: []string{"assertion"},
	})
	if err != nil || !reflect.DeepEqual(archived, []string{"assertion"}) {
		t.Fatalf("ArchiveAssertions = %+v, %v", archived, err)
	}
	if traceAssertionsPath("a/b", "") != "/api/sdk/traces/a%2Fb/assertions" {
		t.Fatal("trace ID was not escaped")
	}
	request := server.recorded()[1]
	if request.path != "/api/sdk/traces/trace/assertions/archive" ||
		!reflect.DeepEqual(request.body, map[string]any{"assertionIds": []any{"assertion"}}) {
		t.Fatalf("archive request = %+v", request)
	}
}

func TestAssertions_RejectInvalidTargetBeforeWriting(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	for _, target := range []TraceTarget{
		{}, {Kind: TraceTargetSpan}, {Kind: TraceTargetSpan, Name: "book", Occurrence: SpanOccurrenceAt(-1)},
		{Kind: TraceTargetOutput, Name: "book"},
	} {
		_, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
			TraceID: "trace", Assertions: []SaveAssertion{{Assertion: "works", TargetOnEvaluatedTrace: &target}},
		})
		if err == nil {
			t.Fatalf("invalid target accepted: %+v", target)
		}
	}
	if len(server.recorded()) != 0 {
		t.Fatal("invalid target made a request")
	}
}

func TestAssertions_CategoryAssignmentRejectsSetAndClear(t *testing.T) {
	client := NewClient("test-key")
	categoryID := "category-1"
	_, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
		TraceID: "trace",
		Assertions: []SaveAssertion{{
			Assertion: "works", CategoryAssertionID: &categoryID, ClearCategoryAssertionID: true,
		}},
	})
	if err == nil {
		t.Fatal("setting and clearing a category was accepted")
	}
}

func TestAssertions_JustificationsAndApprovalFields(t *testing.T) {
	approvedAt := "2026-09-10T00:00:00Z"
	server := newDatasetsServer(t, func(r datasetRequest) any {
		if r.method == http.MethodGet {
			return map[string]any{"assertions": []any{map[string]any{
				"id": "assertion-1", "traceId": "trace", "assertion": "Arrives on time", "source": "agent",
				"justification":         []any{map[string]any{"spanId": "span-1", "text": "Shows arrival"}},
				"categoryJustification": []any{map[string]any{"spanId": "span-2", "text": "Shows timing"}},
				"approvalState":         "approved", "approvedBy": map[string]any{"id": "user-1", "fullName": nil, "email": "ada@example.com", "imageUrl": nil},
				"approvedAt": approvedAt,
			}}, "inheritedFrom": nil}
		}
		return map[string]any{"assertions": []any{}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	justification := Justification{{SpanID: "span-1", Text: "Shows arrival"}}
	categoryJustification := Justification{{SpanID: "span-2", Text: "Shows timing"}}
	if _, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
		TraceID: "trace", Assertions: []SaveAssertion{{
			Assertion: "Arrives on time", Justification: &justification, CategoryJustification: &categoryJustification,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	request := server.recorded()[0]
	assertions := request.body["updates"].([]any)[0].(map[string]any)["assertions"].([]any)
	assertion := assertions[0].(map[string]any)
	if !reflect.DeepEqual(assertion["justification"], []any{map[string]any{"spanId": "span-1", "text": "Shows arrival"}}) ||
		!reflect.DeepEqual(assertion["categoryJustification"], []any{map[string]any{"spanId": "span-2", "text": "Shows timing"}}) {
		t.Fatalf("assertion justifications = %+v", assertion)
	}

	result, err := client.Traces.GetAssertions(context.Background(), "trace")
	if err != nil || len(result.Assertions) != 1 {
		t.Fatalf("GetAssertions = %+v, %v", result, err)
	}
	got := result.Assertions[0]
	if got.ApprovalState != ApprovalApproved || got.ApprovedBy == nil || got.ApprovedBy.Email == nil ||
		*got.ApprovedBy.Email != "ada@example.com" || got.ApprovedAt == nil || *got.ApprovedAt != approvedAt ||
		len(got.Justification) != 1 || len(got.CategoryJustification) != 1 {
		t.Fatalf("approval and justifications = %+v", got)
	}
}

func TestAssertions_JustificationOmissionAndClearing(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"assertions": []any{}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	if _, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
		TraceID: "trace", Assertions: []SaveAssertion{{Assertion: "Preserve evidence"}},
	}); err != nil {
		t.Fatal(err)
	}
	var clearJustification Justification
	if _, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
		TraceID: "trace", Assertions: []SaveAssertion{{
			Assertion: "Clear evidence", Justification: &clearJustification, CategoryJustification: &clearJustification,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	requests := server.recorded()
	omitted := requests[0].body["updates"].([]any)[0].(map[string]any)["assertions"].([]any)[0].(map[string]any)
	if _, ok := omitted["justification"]; ok {
		t.Fatalf("omitted justification sent: %+v", omitted)
	}
	if _, ok := omitted["categoryJustification"]; ok {
		t.Fatalf("omitted category justification sent: %+v", omitted)
	}
	cleared := requests[1].body["updates"].([]any)[0].(map[string]any)["assertions"].([]any)[0].(map[string]any)
	if value, ok := cleared["justification"]; !ok || value != nil {
		t.Fatalf("justification clear not sent: %+v", cleared)
	}
	if value, ok := cleared["categoryJustification"]; !ok || value != nil {
		t.Fatalf("category justification clear not sent: %+v", cleared)
	}
}

func TestTraceTarget_RoundTripsOccurrences(t *testing.T) {
	for _, occurrence := range []SpanOccurrence{"", FirstSpanOccurrence, LastSpanOccurrence, SpanOccurrenceAt(0), SpanOccurrenceAt(3)} {
		target := TraceTarget{Kind: TraceTargetSpan, Name: "lookup", Occurrence: occurrence}
		encoded, err := json.Marshal(target)
		if err != nil {
			t.Fatal(err)
		}
		var decoded TraceTarget
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != target {
			t.Fatalf("round trip %s = %+v, %v", encoded, decoded, err)
		}
	}
}

func TestAssertions_AssigneeRejectsSetAndClear(t *testing.T) {
	client := NewClient("test-key")
	email := "dana@bitfab.dev"
	_, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
		TraceID: "trace",
		Assertions: []SaveAssertion{{
			Assertion: "works", AssigneeEmail: &email, ClearAssigneeEmail: true,
		}},
	})
	if err == nil {
		t.Fatal("setting and clearing an assignee was accepted")
	}
}

func TestAssertions_AssigneeOmissionSettingClearingAndReadback(t *testing.T) {
	email := "dana@bitfab.dev"
	server := newDatasetsServer(t, func(r datasetRequest) any {
		if r.method == http.MethodGet {
			return map[string]any{"assertions": []any{map[string]any{
				"id": "assertion-1", "traceId": "trace", "assertion": "Arrives on time", "source": "agent",
				"assignee": map[string]any{
					"id": "user-9", "fullName": "Dana Reviewer", "email": email, "imageUrl": nil,
				},
			}}, "inheritedFrom": nil}
		}
		return map[string]any{"assertions": []any{}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	for _, assertion := range []SaveAssertion{
		{Assertion: "Preserve the assignee"},
		{Assertion: "Route it to Dana", AssigneeEmail: &email},
		{Assertion: "Leave it to nobody", ClearAssigneeEmail: true},
	} {
		if _, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
			TraceID: "trace", Assertions: []SaveAssertion{assertion},
		}); err != nil {
			t.Fatal(err)
		}
	}
	requests := server.recorded()
	sent := func(index int) map[string]any {
		return requests[index].body["updates"].([]any)[0].(map[string]any)["assertions"].([]any)[0].(map[string]any)
	}
	if _, ok := sent(0)["assigneeEmail"]; ok {
		t.Fatalf("omitted assignee sent: %+v", sent(0))
	}
	if sent(1)["assigneeEmail"] != email {
		t.Fatalf("assignee email = %+v", sent(1))
	}
	if cleared, ok := sent(2)["assigneeEmail"]; !ok || cleared != nil {
		t.Fatalf("cleared assignee = %+v", sent(2))
	}

	result, err := client.Traces.GetAssertions(context.Background(), "trace")
	if err != nil || len(result.Assertions) != 1 {
		t.Fatalf("GetAssertions = %+v, %v", result, err)
	}
	got := result.Assertions[0]
	if got.Assignee == nil || got.Assignee.Email == nil || *got.Assignee.Email != email {
		t.Fatalf("assignee = %+v", got.Assignee)
	}
}
