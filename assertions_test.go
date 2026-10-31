package bitfab

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestAssertions_SaveBatchPreservesOmittedNullAndTarget(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		return map[string]any{"assertions": []any{
			map[string]any{"id": "assertion-1", "traceId": "trace-1", "assertion": "lands before 9am", "source": "human",
				"category_id": "category-1", "category_assertion_id": "category-1", "category": map[string]any{"id": "category-1", "title": "Timing", "description": "Arrival requirements"},
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
				ID: "assertion-1", Assertion: "lands before 9am", PassCriteria: &pass, CategoryID: &categoryID,
				ClearFailCriteria:      true,
				TargetOnEvaluatedTrace: &TraceTarget{Kind: TraceTargetSpan, Name: "book", Occurrence: SpanOccurrenceAt(0)},
			}}},
			{TraceID: "trace-2", Assertions: []SaveAssertion{{
				Assertion: "arrives on time", ClearTargetOnEvaluatedTrace: true, ClearCategoryID: true,
			}}},
		},
	})
	if err != nil || len(result) != 2 {
		t.Fatalf("SaveAssertionsAll = %+v, %v", result, err)
	}
	if result[0].TargetOnEvaluatedTrace.Occurrence != SpanOccurrenceAt(0) || result[1].TargetOnEvaluatedTrace != nil {
		t.Fatalf("decoded targets = %+v", result)
	}
	if result[0].CategoryID == nil || *result[0].CategoryID != categoryID || result[0].Category == nil || result[0].Category.Title != "Timing" {
		t.Fatalf("decoded category = %+v", result[0])
	}
	requests := server.recorded()
	if len(requests) != 1 || requests[0].method != http.MethodPost || requests[0].path != "/api/sdk/traces/assertions" {
		t.Fatalf("requests = %+v", requests)
	}
	want := map[string]any{
		"source":        "human",
		"guidedRewrite": true,
		"updates": []any{
			map[string]any{"traceId": "trace-1", "assertions": []any{map[string]any{
				"id": "assertion-1", "assertion": "lands before 9am", "passCriteria": pass, "failCriteria": nil,
				"category_id":            "category-1",
				"targetOnEvaluatedTrace": map[string]any{"kind": "span", "name": "book", "occurrence": float64(0)},
			}}},
			map[string]any{"traceId": "trace-2", "assertions": []any{map[string]any{
				"assertion": "arrives on time", "targetOnEvaluatedTrace": nil, "category_id": nil,
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
	want := map[string]any{"source": "agent", "guidedRewrite": true, "updates": []any{
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

func TestAssertions_SkipGuidedRewriteSendsFalse(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{"assertions": []any{}} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	if _, err := client.Traces.SaveAssertionsAll(ctx, SaveAssertionsAllParams{
		SkipGuidedRewrite: true,
		Updates:           []TraceAssertionsUpdate{{TraceID: "trace", Assertions: []SaveAssertion{{Assertion: "works"}}}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Traces.SaveAssertions(ctx, SaveAssertionsParams{
		TraceID: "trace", Assertions: []SaveAssertion{{Assertion: "works"}}, SkipGuidedRewrite: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Traces.SaveAssertions(ctx, SaveAssertionsParams{
		TraceID: "trace", Assertions: []SaveAssertion{{Assertion: "works"}},
	}); err != nil {
		t.Fatal(err)
	}
	requests := server.recorded()
	if len(requests) != 3 {
		t.Fatalf("requests = %+v", requests)
	}
	for index, want := range []bool{false, false, true} {
		if got, present := requests[index].body["guidedRewrite"]; !present || got != want {
			t.Fatalf("request %d guidedRewrite = %#v (present %v), want %v", index, got, present, want)
		}
	}
}

func TestAssertions_SaveDecodesRewriteOntoEachAssertion(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		return map[string]any{"assertions": []any{
			map[string]any{"id": "assertion-1", "traceId": "trace", "assertion": "Lands before 9am", "source": "agent",
				"rewrite": map[string]any{
					"status": "rewritten", "sentAssertion": "Lands early and is cheap",
					"changes":            []any{"Split the price claim out because it is a second claim."},
					"separateAssertions": []any{"Costs under $300"},
					"unresolved":         []any{},
					"error":              nil,
				}},
			map[string]any{"id": "assertion-2", "traceId": "trace", "assertion": "Arrives on time", "source": "agent",
				"rewrite": map[string]any{
					"status": "check_failed", "sentAssertion": "Arrives on time",
					"changes": []any{}, "separateAssertions": []any{}, "unresolved": []any{},
					"error": "model call timed out",
				}},
		}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	saved, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
		TraceID: "trace", Assertions: []SaveAssertion{{Assertion: "Lands early and is cheap"}, {Assertion: "Arrives on time"}},
	})
	if err != nil || len(saved) != 2 {
		t.Fatalf("SaveAssertions = %+v, %v", saved, err)
	}
	sent := "Lands early and is cheap"
	wantRewritten := &AssertionRewrite{
		Status: AssertionRewriteRewritten, SentAssertion: &sent,
		Changes:            []string{"Split the price claim out because it is a second claim."},
		SeparateAssertions: []string{"Costs under $300"},
		Unresolved:         []string{},
	}
	if !reflect.DeepEqual(saved[0].Rewrite, wantRewritten) {
		t.Fatalf("first rewrite = %#v, want %#v", saved[0].Rewrite, wantRewritten)
	}
	failed := saved[1].Rewrite
	if failed == nil || failed.Status != AssertionRewriteCheckFailed || failed.Error == nil || *failed.Error != "model call timed out" {
		t.Fatalf("second rewrite = %#v", failed)
	}
}

func TestAssertions_SaveSendsCodeKindWithoutCodeAndDecodesCodeDraft(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"assertions": []any{
			map[string]any{"id": "a1", "traceId": "trace", "assertion": "Books the 9am flight", "kind": "judge",
				"codeDraft": map[string]any{
					"requestedKind": "hybrid", "status": "not_written", "reason": "No span holds the booking.",
				}},
			map[string]any{"id": "a2", "traceId": "trace", "assertion": "Arrives on time", "kind": "judge", "codeDraft": nil},
		}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	saved, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
		TraceID: "trace", Assertions: []SaveAssertion{
			{Assertion: "Books the 9am flight", Kind: AssertionKindHybrid},
			{Assertion: "Arrives on time"},
		},
	})
	if err != nil || len(saved) != 2 {
		t.Fatalf("SaveAssertions = %+v, %v", saved, err)
	}
	reason := "No span holds the booking."
	want := &AssertionCodeDraft{RequestedKind: AssertionKindHybrid, Status: AssertionCodeDraftNotWritten, Reason: &reason}
	if !reflect.DeepEqual(saved[0].CodeDraft, want) {
		t.Fatalf("first codeDraft = %#v, want %#v", saved[0].CodeDraft, want)
	}
	if saved[1].CodeDraft != nil {
		t.Fatalf("second codeDraft = %#v", saved[1].CodeDraft)
	}
	sent := server.recorded()[0].body["updates"].([]any)[0].(map[string]any)["assertions"].([]any)[0].(map[string]any)
	if _, hasCode := sent["code"]; sent["kind"] != "hybrid" || hasCode {
		t.Fatalf("sent = %#v", sent)
	}
}

func TestAssertions_SaveTimeoutOnlyWhenAnEntryAsksForCode(t *testing.T) {
	code := &AssertionCode{Source: "return 'pass'"}
	cases := []struct {
		assertion SaveAssertion
		want      time.Duration
	}{
		{SaveAssertion{Assertion: "a", Kind: AssertionKindCode}, 300 * time.Second},
		{SaveAssertion{Assertion: "a", Kind: AssertionKindHybrid}, 300 * time.Second},
		{SaveAssertion{Assertion: "a", Kind: AssertionKindCode, Code: code}, 0},
		{SaveAssertion{Assertion: "a", Kind: AssertionKindJudge}, 0},
		{SaveAssertion{Assertion: "a"}, 0},
	}
	for _, c := range cases {
		got := saveTimeout([]TraceAssertionsUpdate{{TraceID: "trace", Assertions: []SaveAssertion{c.assertion}}})
		if got != c.want {
			t.Fatalf("saveTimeout(%+v) = %v, want %v", c.assertion, got, c.want)
		}
	}
}

func rewriteRow(id, text, status string) map[string]any {
	return map[string]any{"id": id, "traceId": "trace", "assertion": text, "source": "agent",
		"category_assertion_id": "category-1",
		"rewrite": map[string]any{
			"status": status, "sentAssertion": "sent " + id,
			"changes": []any{}, "separateAssertions": []any{}, "unresolved": []any{}, "error": nil,
		}}
}

func TestAssertions_SaveReturnsPendingRowsWithoutReadingAgain(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"assertions": []any{
			rewriteRow("a1", "sent a1", "pending"),
			rewriteRow("a2", "sent a2", "not_checked"),
		}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	saved, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
		TraceID: "trace", Assertions: []SaveAssertion{{Assertion: "sent a1"}, {Assertion: "sent a2"}},
	})
	if err != nil || len(saved) != 2 {
		t.Fatalf("SaveAssertions = %+v, %v", saved, err)
	}
	if saved[0].Rewrite == nil || saved[0].Rewrite.Status != AssertionRewritePending {
		t.Fatalf("first = %+v", saved[0])
	}
	if saved[1].Rewrite == nil || saved[1].Rewrite.Status != AssertionRewriteNotChecked {
		t.Fatalf("second = %+v", saved[1])
	}
	if requests := server.recorded(); len(requests) != 1 || requests[0].method != http.MethodPost {
		t.Fatalf("requests = %+v", requests)
	}
}

func TestAssertions_GetAssertionsDecodesRewrite(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		rewritten := rewriteRow("a1", "Lands before 9am", "rewritten")
		rewritten["rewrite"].(map[string]any)["separateAssertions"] = []any{"Costs under $300"}
		unchecked := map[string]any{"id": "a2", "traceId": "trace", "assertion": "Arrives on time", "source": "human", "rewrite": nil}
		return map[string]any{"assertions": []any{rewritten, unchecked}, "inheritedFrom": nil}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	read, err := client.Traces.GetAssertions(context.Background(), "trace")
	if err != nil || len(read.Assertions) != 2 {
		t.Fatalf("GetAssertions = %+v, %v", read, err)
	}
	sent := "sent a1"
	want := &AssertionRewrite{
		Status: AssertionRewriteRewritten, SentAssertion: &sent,
		Changes: []string{}, SeparateAssertions: []string{"Costs under $300"}, Unresolved: []string{},
	}
	if !reflect.DeepEqual(read.Assertions[0].Rewrite, want) {
		t.Fatalf("first rewrite = %#v, want %#v", read.Assertions[0].Rewrite, want)
	}
	if read.Assertions[1].Rewrite != nil {
		t.Fatalf("second rewrite = %#v, want nil", read.Assertions[1].Rewrite)
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
			Assertion: "works", CategoryID: &categoryID, ClearCategoryID: true,
		}},
	})
	if err == nil {
		t.Fatal("setting and clearing a category was accepted")
	}
}

func TestAssertions_DeprecatedCategoryAssertionIDMapsToCategoryID(t *testing.T) {
	legacyID := "legacy"
	newID := "new"
	cases := []struct {
		name      string
		assertion SaveAssertion
		want      map[string]any
		wantError bool
	}{
		{name: "legacy set", assertion: SaveAssertion{Assertion: "a", CategoryAssertionID: &legacyID}, want: map[string]any{"assertion": "a", "category_id": "legacy"}},
		{name: "legacy clear", assertion: SaveAssertion{Assertion: "a", ClearCategoryAssertionID: true}, want: map[string]any{"assertion": "a", "category_id": nil}},
		{name: "new wins over legacy set", assertion: SaveAssertion{Assertion: "a", CategoryID: &newID, CategoryAssertionID: &legacyID}, want: map[string]any{"assertion": "a", "category_id": "new"}},
		{name: "new set wins over legacy clear", assertion: SaveAssertion{Assertion: "a", CategoryID: &newID, ClearCategoryAssertionID: true}, want: map[string]any{"assertion": "a", "category_id": "new"}},
		{name: "new clear wins over legacy set", assertion: SaveAssertion{Assertion: "a", ClearCategoryID: true, CategoryAssertionID: &legacyID}, want: map[string]any{"assertion": "a", "category_id": nil}},
		{name: "legacy set and clear", assertion: SaveAssertion{Assertion: "a", CategoryAssertionID: &legacyID, ClearCategoryAssertionID: true}, wantError: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := tc.assertion.payload()
			if tc.wantError {
				if err == nil {
					t.Fatalf("payload = %+v, want error", payload)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(payload, tc.want) {
				t.Fatalf("payload = %#v, %v, want %#v", payload, err, tc.want)
			}
		})
	}
}

func TestAssertions_ReadFillsCategoryIDAndDeprecatedAlias(t *testing.T) {
	for _, body := range []string{
		`{"id":"a","category_id":"c1","category_assertion_id":"c1"}`,
		`{"id":"a","category_id":"c1"}`,
		`{"id":"a","category_assertion_id":"c1"}`,
	} {
		var assertion TraceAssertion
		if err := json.Unmarshal([]byte(body), &assertion); err != nil {
			t.Fatal(err)
		}
		if assertion.CategoryID == nil || *assertion.CategoryID != "c1" || assertion.CategoryAssertionID == nil || *assertion.CategoryAssertionID != "c1" {
			t.Fatalf("%s decoded to %+v", body, assertion)
		}
	}
	var uncategorized TraceAssertion
	if err := json.Unmarshal([]byte(`{"id":"a","category_id":null}`), &uncategorized); err != nil {
		t.Fatal(err)
	}
	if uncategorized.CategoryID != nil || uncategorized.CategoryAssertionID != nil {
		t.Fatalf("uncategorized decoded to %+v", uncategorized)
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

func TestAssertions_HumanNoteRejectsSetAndClear(t *testing.T) {
	client := NewClient("test-key")
	note := "Authored by Jean-Claude"
	_, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
		TraceID: "trace",
		Assertions: []SaveAssertion{{
			Assertion: "works", HumanNote: &note, ClearHumanNote: true,
		}},
	})
	if err == nil {
		t.Fatal("setting and clearing a human note was accepted")
	}
}

func TestAssertions_HumanNoteOmissionSettingAndClearing(t *testing.T) {
	note := "Authored by Jean-Claude"
	server := newDatasetsServer(t, func(r datasetRequest) any {
		return map[string]any{"assertions": []any{}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	for _, assertion := range []SaveAssertion{
		{Assertion: "Preserve the note"},
		{Assertion: "Record who wrote it", HumanNote: &note},
		{Assertion: "Drop the note", ClearHumanNote: true},
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
	if _, ok := sent(0)["humanNote"]; ok {
		t.Fatalf("omitted human note sent: %+v", sent(0))
	}
	if sent(1)["humanNote"] != note {
		t.Fatalf("human note = %+v", sent(1))
	}
	if cleared, ok := sent(2)["humanNote"]; !ok || cleared != nil {
		t.Fatalf("cleared human note = %+v", sent(2))
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

func TestAssertions_GetAssertionsReportsGeneration(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{
			"assertions":    []any{},
			"inheritedFrom": nil,
			"generation": map[string]any{
				"status":       "completed",
				"error":        nil,
				"assertionIds": []any{"draft-1"},
				"startedAt":    "2026-09-18T00:00:00.000Z",
				"finishedAt":   "2026-09-18T00:01:00.000Z",
			},
		}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))

	result, err := client.Traces.GetAssertions(context.Background(), "trace")
	if err != nil || result.Generation == nil {
		t.Fatalf("GetAssertions = %+v, %v", result, err)
	}
	if result.Generation.Status != AssertionGenerationCompleted ||
		!reflect.DeepEqual(result.Generation.AssertionIDs, []string{"draft-1"}) ||
		result.Generation.FinishedAt == nil {
		t.Fatalf("generation = %+v", result.Generation)
	}
}

func TestAssertions_SaveSendsKindAndCode(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"assertions": []any{
			map[string]any{"id": "assertion-1", "traceId": "trace", "assertion": "books", "source": "agent", "kind": "code",
				"code": map[string]any{"source": `return "pass"`, "spanNames": []any{"book"}}, "codeBrokenReason": "The code threw: nope"},
			map[string]any{"id": "assertion-2", "traceId": "trace", "assertion": "books", "source": "agent", "kind": "judge", "code": nil},
		}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	code := AssertionCode{Source: `return "pass"`, SpanNames: []string{"book"}}
	result, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
		TraceID: "trace", Assertions: []SaveAssertion{
			{Assertion: "books", Kind: AssertionKindCode, Code: &code},
			{ID: "assertion-2", Assertion: "books", Kind: AssertionKindJudge},
		},
	})
	if err != nil || len(result) != 2 {
		t.Fatalf("SaveAssertions = %+v, %v", result, err)
	}
	if result[0].Kind != AssertionKindCode || result[0].Code == nil || !reflect.DeepEqual(*result[0].Code, code) ||
		result[0].CodeBrokenReason == nil || result[1].Kind != AssertionKindJudge || result[1].Code != nil {
		t.Fatalf("decoded code = %+v", result)
	}
	want := map[string]any{"source": "agent", "guidedRewrite": true, "updates": []any{
		map[string]any{"traceId": "trace", "assertions": []any{
			map[string]any{"assertion": "books", "kind": "code", "code": map[string]any{"source": `return "pass"`, "spanNames": []any{"book"}}},
			map[string]any{"id": "assertion-2", "assertion": "books", "kind": "judge"},
		}},
	}}
	if body := server.recorded()[0].body; !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %#v, want %#v", body, want)
	}
}

func TestAssertions_SaveSendsEmptySpanNamesWhenUnset(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"assertions": []any{}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	_, err := client.Traces.SaveAssertions(context.Background(), SaveAssertionsParams{
		TraceID: "trace", Assertions: []SaveAssertion{
			{Assertion: "books", Kind: AssertionKindCode, Code: &AssertionCode{Source: `return "pass"`}},
		},
	})
	if err != nil {
		t.Fatalf("SaveAssertions: %v", err)
	}
	sent := server.recorded()[0].body["updates"].([]any)[0].(map[string]any)["assertions"].([]any)[0].(map[string]any)
	if names := sent["code"].(map[string]any)["spanNames"]; !reflect.DeepEqual(names, []any{}) {
		t.Fatalf("spanNames = %#v, want an empty list", names)
	}
}
