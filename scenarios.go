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

// ScenarioAction is one thing a run did, such as a tool call, as the caller
// recorded it. Details holds the action's arguments and a nil map is sent as
// an empty object. Result is nil when the action returned nothing to show,
// Failed reports whether it failed, and At is when it happened, nil if unknown.
type ScenarioAction struct {
	Name    string         `json:"name"`
	Details map[string]any `json:"details"`
	Result  *string        `json:"result"`
	Failed  bool           `json:"failed"`
	At      *string        `json:"at"`
}

// MarshalJSON sends a nil Details as an empty object.
func (action ScenarioAction) MarshalJSON() ([]byte, error) {
	type wire ScenarioAction
	if action.Details == nil {
		action.Details = map[string]any{}
	}
	return json.Marshal(wire(action))
}

// ScenarioActualOutcome is what one run of the scenario actually did, never
// what it should have done. Bitfab never fills it in. The caller sends it with
// SaveScenario. Summary describes the outcome in words, Actions lists what the
// run did in order (a nil slice is sent as empty), and Source names the span
// the outcome was read from, or is nil when there is none.
type ScenarioActualOutcome struct {
	Summary string           `json:"summary"`
	Actions []ScenarioAction `json:"actions"`
	Source  *ScenarioSource  `json:"source,omitempty"`
}

// MarshalJSON sends a nil Actions as an empty list.
func (outcome ScenarioActualOutcome) MarshalJSON() ([]byte, error) {
	type wire ScenarioActualOutcome
	if outcome.Actions == nil {
		outcome.Actions = []ScenarioAction{}
	}
	return json.Marshal(wire(outcome))
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
	// ActualOutcome is what the run actually did, as last saved by a caller.
	// It is nil when nothing has been saved or it was cleared.
	ActualOutcome  *ScenarioActualOutcome `json:"actualOutcome"`
	TriggerSummary string                 `json:"triggerSummary"`
	DataSummary    string                 `json:"dataSummary"`
	// Categories are the categories the scenario is filed under, sorted by title.
	// It is empty when the scenario is filed under none.
	Categories []CategorySummary `json:"categories"`
}

type SaveScenarioParams struct {
	TraceID     string              `json:"-"`
	Name        *string             `json:"name,omitempty"`
	Description *string             `json:"description,omitempty"`
	Trigger     *ScenarioTrigger    `json:"trigger,omitempty"`
	Data        *ScenarioData       `json:"data,omitempty"`
	AppendData  *ScenarioDataAppend `json:"appendData,omitempty"`
	// ActualOutcome replaces the saved actual outcome whole. Nil leaves the
	// saved one untouched.
	ActualOutcome *ScenarioActualOutcome `json:"actualOutcome,omitempty"`
	// ClearActualOutcome removes the saved actual outcome by sending null.
	// Setting it together with ActualOutcome is refused.
	ClearActualOutcome bool `json:"-"`
	// CategoryIDs replaces the whole set of categories the scenario is filed
	// under. Nil leaves the saved set untouched. Use ClearCategoryIDs to empty it.
	// An id outside the organization fails the save.
	CategoryIDs []string `json:"-"`
	// ClearCategoryIDs files the scenario under no categories by sending an
	// empty list. Setting it together with CategoryIDs is refused.
	ClearCategoryIDs bool `json:"-"`
}

func (update SaveScenarioParams) payload() (map[string]any, error) {
	if update.Data != nil && update.AppendData != nil {
		return nil, errors.New("bitfab: a scenario update cannot replace and append data together")
	}
	if update.AppendData != nil && len(update.AppendData.History)+len(update.AppendData.Missing) == 0 {
		return nil, errors.New("bitfab: a scenario data append must name at least one history entry or missing item")
	}
	if update.ActualOutcome != nil && update.ClearActualOutcome {
		return nil, errors.New("bitfab: cannot set and clear a scenario actual outcome together")
	}
	if update.CategoryIDs != nil && update.ClearCategoryIDs {
		return nil, errors.New("bitfab: cannot set and clear scenario categories together")
	}
	if update.Name == nil && update.Description == nil && update.Trigger == nil && update.Data == nil &&
		update.AppendData == nil && update.ActualOutcome == nil && !update.ClearActualOutcome &&
		update.CategoryIDs == nil && !update.ClearCategoryIDs {
		return nil, errors.New("bitfab: a scenario update must set at least one of name, description, trigger, data, appendData, actualOutcome, or categoryIds")
	}
	encoded, err := json.Marshal(update)
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	if err := json.Unmarshal(encoded, &payload); err != nil {
		return nil, err
	}
	if update.ClearActualOutcome {
		payload["actualOutcome"] = nil
	}
	if update.ClearCategoryIDs {
		payload["categoryIds"] = []string{}
	} else if update.CategoryIDs != nil {
		payload["categoryIds"] = update.CategoryIDs
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
