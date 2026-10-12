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
