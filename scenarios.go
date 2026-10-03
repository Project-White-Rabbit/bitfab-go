package bitfab

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
)

type ScenarioSourceField string

const (
	ScenarioSourceInput  ScenarioSourceField = "input"
	ScenarioSourceOutput ScenarioSourceField = "output"
)

type ScenarioSource struct {
	SpanID      string              `json:"spanId"`
	SourceField ScenarioSourceField `json:"sourceField"`
	Text        string              `json:"text"`
}

type ScenarioTrigger struct {
	InputID               *int             `json:"inputId"`
	InputType             string           `json:"inputType"`
	Channel               string           `json:"channel"`
	From                  string           `json:"from"`
	To                    *string          `json:"to"`
	Cc                    *string          `json:"cc"`
	Subject               *string          `json:"subject"`
	Type                  string           `json:"type"`
	EventTime             string           `json:"eventTime"`
	EventTimeSource       string           `json:"eventTimeSource"`
	Source                string           `json:"source"`
	Content               string           `json:"content"`
	ContentPassages       []string         `json:"contentPassages"`
	ContentIsWholeMessage bool             `json:"contentIsWholeMessage"`
	Sources               []ScenarioSource `json:"sources"`
}

func (trigger ScenarioTrigger) MarshalJSON() ([]byte, error) {
	type wire ScenarioTrigger
	if trigger.ContentPassages == nil {
		trigger.ContentPassages = []string{}
	}
	if trigger.Sources == nil {
		trigger.Sources = []ScenarioSource{}
	}
	return json.Marshal(wire(trigger))
}

type ScenarioDataOrigin struct {
	Section    string  `json:"section"`
	Channel    *string `json:"channel"`
	From       *string `json:"from"`
	To         *string `json:"to"`
	Cc         *string `json:"cc"`
	Subject    *string `json:"subject"`
	SentAtText *string `json:"sentAtText"`
}

type ScenarioHistoryEntry struct {
	Role          string              `json:"role"`
	Kind          string              `json:"kind"`
	Quote         string              `json:"quote"`
	Summary       *string             `json:"summary"`
	Origin        *ScenarioDataOrigin `json:"origin"`
	SentAt        *string             `json:"sentAt"`
	Weekday       *string             `json:"weekday"`
	SincePrevious *string             `json:"sincePrevious"`
	Source        *ScenarioSource     `json:"source,omitempty"`
}

type ScenarioData struct {
	HasHistory bool                   `json:"hasHistory"`
	Topic      string                 `json:"topic"`
	History    []ScenarioHistoryEntry `json:"history"`
	Missing    []string               `json:"missing"`
}

func (data ScenarioData) MarshalJSON() ([]byte, error) {
	type wire ScenarioData
	if data.History == nil {
		data.History = []ScenarioHistoryEntry{}
	}
	if data.Missing == nil {
		data.Missing = []string{}
	}
	return json.Marshal(wire(data))
}

type ScenarioDataAppend struct {
	History []ScenarioHistoryEntry `json:"history,omitempty"`
	Missing []string               `json:"missing,omitempty"`
}

type Scenario struct {
	ID              string          `json:"id"`
	CreatedAt       string          `json:"createdAt"`
	UpdatedAt       string          `json:"updatedAt"`
	OrganizationID  string          `json:"organizationId"`
	TraceFunctionID string          `json:"traceFunctionId"`
	SourceTraceID   string          `json:"sourceTraceId"`
	Extractor       string          `json:"extractor"`
	Name            string          `json:"name"`
	Description     string          `json:"description"`
	Trigger         ScenarioTrigger `json:"trigger"`
	Data            ScenarioData    `json:"data"`
	TriggerSummary  string          `json:"triggerSummary"`
	DataSummary     string          `json:"dataSummary"`
}

type SaveScenarioParams struct {
	TraceID     string              `json:"-"`
	Name        *string             `json:"name,omitempty"`
	Description *string             `json:"description,omitempty"`
	Trigger     *ScenarioTrigger    `json:"trigger,omitempty"`
	Data        *ScenarioData       `json:"data,omitempty"`
	AppendData  *ScenarioDataAppend `json:"appendData,omitempty"`
}

func (update SaveScenarioParams) payload() (map[string]any, error) {
	if update.Data != nil && update.AppendData != nil {
		return nil, errors.New("bitfab: a scenario update cannot replace and append data together")
	}
	if update.AppendData != nil && len(update.AppendData.History)+len(update.AppendData.Missing) == 0 {
		return nil, errors.New("bitfab: a scenario data append must name at least one history entry or missing item")
	}
	if update.Name == nil && update.Description == nil && update.Trigger == nil && update.Data == nil && update.AppendData == nil {
		return nil, errors.New("bitfab: a scenario update must set at least one field")
	}
	encoded, err := json.Marshal(update)
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	if err := json.Unmarshal(encoded, &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func traceScenarioPath(traceID string) string {
	return "/api/sdk/traces/" + url.PathEscape(traceID) + "/scenario"
}

func (t *TracesClient) GetScenario(ctx context.Context, traceID string) (*Scenario, error) {
	var response struct {
		Scenario Scenario `json:"scenario"`
	}
	if err := t.httpClient.get(ctx, traceScenarioPath(traceID), &response); err != nil {
		return nil, err
	}
	return &response.Scenario, nil
}

func (t *TracesClient) SaveScenario(ctx context.Context, params SaveScenarioParams) (*Scenario, error) {
	payload, err := params.payload()
	if err != nil {
		return nil, err
	}
	var response struct {
		Scenario Scenario `json:"scenario"`
	}
	if err := t.httpClient.requestInto(ctx, traceScenarioPath(params.TraceID), payload, &response); err != nil {
		return nil, err
	}
	return &response.Scenario, nil
}
