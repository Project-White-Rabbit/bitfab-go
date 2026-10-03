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
