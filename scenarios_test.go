package bitfab

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func testScenarioResponse() map[string]any {
	return map[string]any{"scenario": map[string]any{
		"id": "scenario-1", "createdAt": "2026-10-01T12:00:00.000Z", "updatedAt": "2026-10-02T12:00:00.000Z",
		"organizationId": "org-1", "traceFunctionId": "function-1", "sourceTraceId": "trace-1",
		"extractor": "noah", "name": "Late flight", "description": "Rebook a late flight",
		"trigger": map[string]any{
			"inputId": 7, "inputType": "email", "channel": "email", "from": "ann@example.com", "to": nil, "cc": nil,
			"subject": "Delay", "type": "request", "eventTime": "Mon 9am", "eventTimeSource": "message date",
			"source": "request", "content": "My flight is late", "contentPassages": []any{"My flight is late"},
			"contentIsWholeMessage": true,
			"sources":               []any{map[string]any{"spanId": "span-1", "sourceField": "input", "text": "My flight is late"}},
		},
		"data": map[string]any{
			"hasHistory": true, "topic": "travel",
			"history": []any{map[string]any{
				"role": "fact", "kind": "booking", "quote": "booked UA 12", "summary": nil, "origin": nil,
				"sentAt": nil, "weekday": nil, "sincePrevious": nil, "source": nil,
			}},
			"missing": []any{"seat preference"},
		},
		"actualOutcome": map[string]any{
			"summary": "Rebooked onto UA 14",
			"actions": []any{map[string]any{
				"name": "rebook", "details": map[string]any{"flight": "UA 14"}, "result": "confirmed",
				"failed": false, "at": nil,
			}},
			"source": map[string]any{"spanId": "span-2", "sourceField": "output", "text": "Rebooked onto UA 14"},
		},
		"triggerSummary": "Kind: request", "dataSummary": "About: travel",
		"categories": []any{
			map[string]any{"id": "category-1", "title": "Rebooking", "description": "Flight changes"},
			map[string]any{"id": "category-2", "title": "Travel", "description": ""},
		},
	}}
}

func TestScenarios_GetDecodesScenario(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	scenario, err := client.Traces.GetScenario(context.Background(), "a/b")
	if err != nil {
		t.Fatal(err)
	}
	request := server.recorded()[0]
	if request.method != http.MethodGet || request.path != "/api/sdk/traces/a/b/scenario" {
		t.Fatalf("request = %+v", request)
	}
	if traceScenarioPath("a/b") != "/api/sdk/traces/a%2Fb/scenario" {
		t.Fatal("trace ID was not escaped")
	}
	if scenario.ID != "scenario-1" || scenario.CreatedAt != "2026-10-01T12:00:00.000Z" || scenario.Extractor != "noah" ||
		scenario.TriggerSummary != "Kind: request" || scenario.DataSummary != "About: travel" {
		t.Fatalf("scenario = %+v", scenario)
	}
	trigger := scenario.Trigger
	if trigger.InputID == nil || *trigger.InputID != 7 || trigger.To != nil || trigger.Subject == nil || *trigger.Subject != "Delay" ||
		len(trigger.Sources) != 1 || trigger.Sources[0].SourceField != ScenarioSourceInput {
		t.Fatalf("trigger = %+v", trigger)
	}
	if len(scenario.Data.History) != 1 || scenario.Data.History[0].Source != nil ||
		!reflect.DeepEqual(scenario.Data.Missing, []string{"seat preference"}) {
		t.Fatalf("data = %+v", scenario.Data)
	}
	outcome := scenario.ActualOutcome
	if outcome == nil || outcome.Summary != "Rebooked onto UA 14" || len(outcome.Actions) != 1 ||
		outcome.Source == nil || outcome.Source.SourceField != ScenarioSourceOutput {
		t.Fatalf("actualOutcome = %+v", outcome)
	}
	action := outcome.Actions[0]
	if action.Name != "rebook" || action.Details["flight"] != "UA 14" || action.Result == nil || *action.Result != "confirmed" ||
		action.Failed || action.At != nil {
		t.Fatalf("action = %+v", action)
	}
}

func TestScenarios_GetDecodesMissingActualOutcomeAsNil(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		response := testScenarioResponse()
		response["scenario"].(map[string]any)["actualOutcome"] = nil
		return response
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	scenario, err := client.Traces.GetScenario(context.Background(), "trace-1")
	if err != nil {
		t.Fatal(err)
	}
	if scenario.ActualOutcome != nil {
		t.Fatalf("actualOutcome = %+v, want nil", scenario.ActualOutcome)
	}
}

func TestScenarios_GenerateStartsOnTheTracePathAndReturnsAtOnce(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"generation": map[string]any{
			"status": "started", "traceId": "trace-1", "scenarioId": nil,
		}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	generation, err := client.Traces.GenerateScenario(context.Background(), "trace-1")
	if err != nil {
		t.Fatal(err)
	}
	request := server.recorded()[0]
	if request.method != http.MethodPost || request.path != "/api/sdk/traces/trace-1/scenario/generate" || len(request.body) != 0 {
		t.Fatalf("request = %+v", request)
	}
	if generation.Status != ScenarioGenerationStarted || generation.TraceID != "trace-1" || generation.ScenarioID != nil {
		t.Fatalf("generation = %+v", generation)
	}
}

func TestScenarios_GetNotFoundIsAStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"Scenario not found"}`, http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	client := NewClient("test-key", WithServiceURL(server.URL))
	_, err := client.Traces.GetScenario(context.Background(), "missing")
	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
		t.Fatalf("error = %#v, want httpStatusError with status 404", err)
	}
}

func TestScenarios_SaveSendsOnlySetFieldsAndAppendData(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	name := "Late flight"
	summary := "booked"
	scenario, err := client.Traces.SaveScenario(context.Background(), SaveScenarioParams{
		TraceID: "trace-1",
		Name:    &name,
		AppendData: &ScenarioDataAppend{History: []ScenarioHistoryEntry{{
			Role: "fact", Kind: "booking", Quote: "booked UA 12", Summary: &summary,
		}}},
	})
	if err != nil || scenario.ID != "scenario-1" {
		t.Fatalf("SaveScenario = %+v, %v", scenario, err)
	}
	request := server.recorded()[0]
	if request.method != http.MethodPost || request.path != "/api/sdk/traces/trace-1/scenario" {
		t.Fatalf("request = %+v", request)
	}
	want := map[string]any{
		"name": "Late flight",
		"appendData": map[string]any{"history": []any{map[string]any{
			"role": "fact", "kind": "booking", "quote": "booked UA 12", "summary": "booked", "origin": nil,
			"sentAt": nil, "weekday": nil, "sincePrevious": nil,
		}}},
	}
	if !reflect.DeepEqual(request.body, want) {
		t.Fatalf("body = %#v, want %#v", request.body, want)
	}
}

func TestScenarios_SaveReplacesTriggerAndDataWithArrays(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	_, err := client.Traces.SaveScenario(context.Background(), SaveScenarioParams{
		TraceID: "trace-1",
		Trigger: &ScenarioTrigger{InputType: "email", Channel: "email", From: "ann@example.com", Type: "request",
			EventTime: "Mon 9am", EventTimeSource: "run start", Source: "request", Content: "late"},
		Data: &ScenarioData{Topic: "travel"},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := server.recorded()[0].body
	trigger := body["trigger"].(map[string]any)
	if !reflect.DeepEqual(trigger["contentPassages"], []any{}) || !reflect.DeepEqual(trigger["sources"], []any{}) ||
		trigger["inputId"] != nil || trigger["to"] != nil {
		t.Fatalf("trigger = %#v", trigger)
	}
	want := map[string]any{"hasHistory": false, "topic": "travel", "history": []any{}, "missing": []any{}}
	if !reflect.DeepEqual(body["data"], want) {
		t.Fatalf("data = %#v", body["data"])
	}
}

func TestScenarios_SaveRejectsInvalidUpdatesBeforeWriting(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	updates := map[string]SaveScenarioParams{
		"empty":       {},
		"both":        {Data: &ScenarioData{}, AppendData: &ScenarioDataAppend{Missing: []string{"x"}}},
		"emptyAppend": {AppendData: &ScenarioDataAppend{}},
		"setAndClearActualOutcome": {
			ActualOutcome: &ScenarioActualOutcome{Summary: "done"}, ClearActualOutcome: true,
		},
		"setAndClearCategoryIDs": {CategoryIDs: []string{"category-1"}, ClearCategoryIDs: true},
	}
	for name, update := range updates {
		if _, err := client.Traces.SaveScenario(context.Background(), update); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
	if len(server.recorded()) != 0 {
		t.Fatalf("requests = %+v", server.recorded())
	}
}

func TestScenarios_SaveSendsActualOutcomeAlone(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	result := "confirmed"
	at := "2026-10-01T12:05:00.000Z"
	_, err := client.Traces.SaveScenario(context.Background(), SaveScenarioParams{
		TraceID: "trace-1",
		ActualOutcome: &ScenarioActualOutcome{
			Summary: "Rebooked onto UA 14",
			Actions: []ScenarioAction{
				{Name: "rebook", Details: map[string]any{"flight": "UA 14"}, Result: &result, At: &at},
				{Name: "notify", Failed: true},
			},
			Source: &ScenarioSource{SpanID: "span-2", SourceField: ScenarioSourceOutput, Text: "Rebooked onto UA 14"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"actualOutcome": map[string]any{
		"summary": "Rebooked onto UA 14",
		"actions": []any{
			map[string]any{
				"name": "rebook", "details": map[string]any{"flight": "UA 14"}, "result": "confirmed", "failed": false,
				"at": "2026-10-01T12:05:00.000Z",
			},
			map[string]any{"name": "notify", "details": map[string]any{}, "result": nil, "failed": true, "at": nil},
		},
		"source": map[string]any{"spanId": "span-2", "sourceField": "output", "text": "Rebooked onto UA 14"},
	}}
	if body := server.recorded()[0].body; !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %#v, want %#v", body, want)
	}
}

func TestScenarios_SaveSendsActualOutcomeWithoutSourceOrActions(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	_, err := client.Traces.SaveScenario(context.Background(), SaveScenarioParams{
		TraceID:       "trace-1",
		ActualOutcome: &ScenarioActualOutcome{Summary: "Did nothing"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"actualOutcome": map[string]any{"summary": "Did nothing", "actions": []any{}}}
	if body := server.recorded()[0].body; !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %#v, want %#v", body, want)
	}
}

func TestScenarios_SaveClearsActualOutcomeWithNull(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	_, err := client.Traces.SaveScenario(context.Background(), SaveScenarioParams{
		TraceID:            "trace-1",
		ClearActualOutcome: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	body := server.recorded()[0].body
	value, present := body["actualOutcome"]
	if !present || value != nil || len(body) != 1 {
		t.Fatalf("body = %#v, want only actualOutcome: null", body)
	}
}

func TestScenarios_SaveLeavesOutActualOutcomeWhenOmitted(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	name := "Late flight"
	_, err := client.Traces.SaveScenario(context.Background(), SaveScenarioParams{TraceID: "trace-1", Name: &name})
	if err != nil {
		t.Fatal(err)
	}
	if body := server.recorded()[0].body; !reflect.DeepEqual(body, map[string]any{"name": "Late flight"}) {
		t.Fatalf("body = %#v, want only name", body)
	}
}

func TestScenarios_GetDecodesCategories(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	scenario, err := client.Traces.GetScenario(context.Background(), "trace-1")
	if err != nil {
		t.Fatal(err)
	}
	want := []CategorySummary{
		{ID: "category-1", Title: "Rebooking", Description: "Flight changes"},
		{ID: "category-2", Title: "Travel"},
	}
	if !reflect.DeepEqual(scenario.Categories, want) {
		t.Fatalf("categories = %+v, want %+v", scenario.Categories, want)
	}
}

func TestScenarios_SaveCategoryIDs(t *testing.T) {
	cases := map[string]struct {
		params SaveScenarioParams
		want   map[string]any
	}{
		"replace": {
			params: SaveScenarioParams{CategoryIDs: []string{"category-1", "category-2"}},
			want:   map[string]any{"categoryIds": []any{"category-1", "category-2"}},
		},
		"clear": {
			params: SaveScenarioParams{ClearCategoryIDs: true},
			want:   map[string]any{"categoryIds": []any{}},
		},
		"emptyNonNilSlice": {
			params: SaveScenarioParams{CategoryIDs: []string{}},
			want:   map[string]any{"categoryIds": []any{}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
			client := NewClient("test-key", WithServiceURL(server.URL))
			tc.params.TraceID = "trace-1"
			if _, err := client.Traces.SaveScenario(context.Background(), tc.params); err != nil {
				t.Fatal(err)
			}
			if body := server.recorded()[0].body; !reflect.DeepEqual(body, tc.want) {
				t.Fatalf("body = %#v, want %#v", body, tc.want)
			}
		})
	}
}

func TestScenarios_SaveLeavesOutCategoryIDsWhenOmitted(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	name := "Late flight"
	if _, err := client.Traces.SaveScenario(context.Background(), SaveScenarioParams{TraceID: "trace-1", Name: &name}); err != nil {
		t.Fatal(err)
	}
	if _, present := server.recorded()[0].body["categoryIds"]; present {
		t.Fatalf("body = %#v, want no categoryIds", server.recorded()[0].body)
	}
}

func TestScenarios_SaveSendsRecordsAndSources(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	at := "2026-09-30T08:00:00.000Z"
	quote := "prefers aisle seats"
	captured := "2026-10-01T12:00:00.000Z"
	excluded := 2
	_, err := client.Traces.SaveScenario(context.Background(), SaveScenarioParams{
		TraceID: "trace-1",
		Records: []ScenarioRecordInput{
			{Kind: "memory", Ref: "Memory:812", At: &at, AsOf: "run", Title: "Seat", Text: "Ann prefers aisle seats", Quote: &quote},
			{Kind: "order", Ref: "Order:7", AsOf: "now", Title: "Booking", Text: "UA 12"},
		},
		Sources: &ScenarioDataSources{
			FrameAt:    "2026-10-01T11:59:00.000Z",
			CapturedAt: &captured,
			Windows:    map[string]float64{"memory": 30},
			Coverage: ScenarioDataSourcesCoverage{
				CalendarExcludedCount: &excluded,
				FilledColumns:         ScenarioFilledColumns{AbsentNullable: []string{"memory.note"}},
				Extra:                 map[string]any{"orders": "full"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"records": []any{
			map[string]any{
				"kind": "memory", "ref": "Memory:812", "at": at, "asOf": "run", "title": "Seat",
				"text": "Ann prefers aisle seats", "quote": quote,
			},
			map[string]any{
				"kind": "order", "ref": "Order:7", "at": nil, "asOf": "now", "title": "Booking",
				"text": "UA 12", "quote": nil,
			},
		},
		"sources": map[string]any{
			"frameAt": "2026-10-01T11:59:00.000Z", "capturedAt": captured,
			"windows": map[string]any{"memory": float64(30)},
			"coverage": map[string]any{
				"calendarExcludedCount": float64(2),
				"filledColumns":         map[string]any{"absentNullable": []any{"memory.note"}, "defaultedNonNullable": []any{}},
				"orders":                "full",
			},
		},
	}
	if body := server.recorded()[0].body; !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %#v, want %#v", body, want)
	}
}

func TestScenarios_SaveLeavesRecordsOutOfData(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	if _, err := client.Traces.SaveScenario(context.Background(), SaveScenarioParams{
		TraceID: "trace-1",
		Data: &ScenarioData{
			Topic:   "travel",
			Records: []ScenarioRecord{{ScenarioRecordInput: ScenarioRecordInput{Kind: "memory", Ref: "Memory:1"}}},
			Sources: &ScenarioDataSources{FrameAt: "2026-10-01T12:00:00Z"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"hasHistory": false, "topic": "travel", "history": []any{}, "missing": []any{}}
	if body := server.recorded()[0].body; !reflect.DeepEqual(body["data"], want) {
		t.Fatalf("data = %#v, want %#v", body["data"], want)
	}
}

func TestScenarios_SaveSendsEmptyRecords(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	if _, err := client.Traces.SaveScenario(context.Background(), SaveScenarioParams{
		TraceID: "trace-1", Records: []ScenarioRecordInput{},
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"records": []any{}}
	if body := server.recorded()[0].body; !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %#v, want %#v", body, want)
	}
}

func TestScenarios_SaveRefusesRecordsMixedWithOtherFields(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return testScenarioResponse() })
	client := NewClient("test-key", WithServiceURL(server.URL))
	name := "Late flight"
	updates := map[string]SaveScenarioParams{
		"sourcesWithoutRecords": {Sources: &ScenarioDataSources{FrameAt: "2026-10-01T11:59:00.000Z"}},
		"recordsWithName":       {Records: []ScenarioRecordInput{}, Name: &name},
		"recordsWithData":       {Records: []ScenarioRecordInput{}, Data: &ScenarioData{Topic: "travel"}},
		"recordsWithClearIDs":   {Records: []ScenarioRecordInput{}, ClearCategoryIDs: true},
	}
	for name, update := range updates {
		update.TraceID = "trace-1"
		if _, err := client.Traces.SaveScenario(context.Background(), update); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
	if len(server.recorded()) != 0 {
		t.Fatalf("requests = %+v", server.recorded())
	}
}

func TestScenarios_GetDecodesRecordsAndSources(t *testing.T) {
	response := testScenarioResponse()
	data := response["scenario"].(map[string]any)["data"].(map[string]any)
	data["records"] = []any{map[string]any{
		"kind": "memory", "ref": "Memory:812", "at": nil, "asOf": "now", "title": "Seat", "text": "aisle",
		"quote": nil, "source": map[string]any{"spanId": "span-1", "sourceField": "input", "text": "aisle"},
	}}
	data["sources"] = map[string]any{
		"frameAt": "2026-10-01T11:59:00.000Z", "capturedAt": nil, "windows": map[string]any{"memory": 30},
		"coverage": map[string]any{
			"filledColumns": map[string]any{"absentNullable": []any{}, "defaultedNonNullable": []any{"order.status"}},
			"orders":        "full",
		},
	}
	server := newDatasetsServer(t, func(datasetRequest) any { return response })
	client := NewClient("test-key", WithServiceURL(server.URL))
	scenario, err := client.Traces.GetScenario(context.Background(), "trace-1")
	if err != nil {
		t.Fatal(err)
	}
	records := scenario.Data.Records
	if len(records) != 1 || records[0].Ref != "Memory:812" || records[0].Source == nil || records[0].Source.SpanID != "span-1" {
		t.Fatalf("records = %+v", records)
	}
	sources := scenario.Data.Sources
	if sources == nil || sources.Windows["memory"] != 30 || sources.CapturedAt != nil ||
		!reflect.DeepEqual(sources.Coverage.FilledColumns.DefaultedNonNullable, []string{"order.status"}) ||
		!reflect.DeepEqual(sources.Coverage.Extra, map[string]any{"orders": "full"}) {
		t.Fatalf("sources = %+v", sources)
	}
}
