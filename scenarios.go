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

// ScenarioRecordInput is one record your own systems hold that relates to the
// run, such as a message, a memory, or an order, as sent to SaveScenario.
// Kind is any name for the kind of record, such as "memory" or "order", and Ref
// is your own pointer to it, such as "Memory:812". At is when the record came
// to exist as an ISO 8601 time, or nil. AsOf is "run" when the record is as it
// was at the run, or "now" when no history is kept for it. Quote is the exact
// text Bitfab looks for in the run's spans, or nil to look for Text instead.
type ScenarioRecordInput struct {
	Kind  string  `json:"kind"`
	Ref   string  `json:"ref"`
	At    *string `json:"at"`
	AsOf  string  `json:"asOf"`
	Title string  `json:"title"`
	Text  string  `json:"text"`
	Quote *string `json:"quote"`
}

// ScenarioRecord is a saved record. Source is the span Bitfab found it in, or
// nil when it shows up in none of the run's spans.
type ScenarioRecord struct {
	ScenarioRecordInput
	Source *ScenarioSource `json:"source"`
}

// ScenarioDataSources says where a scenario's records came from: when the run
// started, when its data was captured, how many days each windowed kind
// reaches, and how fully each kind of data was read.
type ScenarioDataSources struct {
	FrameAt    string                      `json:"frameAt"`
	CapturedAt *string                     `json:"capturedAt"`
	Windows    map[string]float64          `json:"windows"`
	Coverage   ScenarioDataSourcesCoverage `json:"coverage"`
}

// MarshalJSON sends a nil Windows as an empty object.
func (sources ScenarioDataSources) MarshalJSON() ([]byte, error) {
	type wire ScenarioDataSources
	if sources.Windows == nil {
		sources.Windows = map[string]float64{}
	}
	return json.Marshal(wire(sources))
}

// ScenarioDataSourcesCoverage says how fully each kind of data was read.
// CalendarExcludedCount is left out when nil. Extra carries any other keys,
// sent and read beside the named ones.
type ScenarioDataSourcesCoverage struct {
	CalendarExcludedCount *int                  `json:"calendarExcludedCount,omitempty"`
	FilledColumns         ScenarioFilledColumns `json:"filledColumns"`
	Extra                 map[string]any        `json:"-"`
}

// MarshalJSON sends Extra's keys beside the named fields.
func (coverage ScenarioDataSourcesCoverage) MarshalJSON() ([]byte, error) {
	type wire ScenarioDataSourcesCoverage
	named, err := json.Marshal(wire(coverage))
	if err != nil {
		return nil, err
	}
	if len(coverage.Extra) == 0 {
		return named, nil
	}
	merged := map[string]any{}
	for key, value := range coverage.Extra {
		merged[key] = value
	}
	var fields map[string]any
	if err := json.Unmarshal(named, &fields); err != nil {
		return nil, err
	}
	for key, value := range fields {
		merged[key] = value
	}
	return json.Marshal(merged)
}

// UnmarshalJSON reads the named fields and keeps every other key in Extra.
func (coverage *ScenarioDataSourcesCoverage) UnmarshalJSON(data []byte) error {
	type wire ScenarioDataSourcesCoverage
	var named wire
	if err := json.Unmarshal(data, &named); err != nil {
		return err
	}
	var extra map[string]any
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "calendarExcludedCount")
	delete(extra, "filledColumns")
	*coverage = ScenarioDataSourcesCoverage(named)
	if len(extra) > 0 {
		coverage.Extra = extra
	}
	return nil
}

// ScenarioFilledColumns names the columns that were read with a filled-in
// value: nullable columns that were absent, and non-nullable columns that fell
// back to their default.
type ScenarioFilledColumns struct {
	AbsentNullable       []string `json:"absentNullable"`
	DefaultedNonNullable []string `json:"defaultedNonNullable"`
}

// MarshalJSON sends nil lists as empty lists.
func (columns ScenarioFilledColumns) MarshalJSON() ([]byte, error) {
	type wire ScenarioFilledColumns
	if columns.AbsentNullable == nil {
		columns.AbsentNullable = []string{}
	}
	if columns.DefaultedNonNullable == nil {
		columns.DefaultedNonNullable = []string{}
	}
	return json.Marshal(wire(columns))
}

type ScenarioData struct {
	HasHistory bool                   `json:"hasHistory"`
	Topic      string                 `json:"topic"`
	History    []ScenarioHistoryEntry `json:"history"`
	Missing    []string               `json:"missing"`
	// Records are the scenario's saved records, nil when none have been saved.
	// To save new records, send them with SaveScenarioParams.Records, which
	// matches each record to the span it shows up in.
	Records []ScenarioRecord `json:"records,omitempty"`
	// Sources says where the records came from, nil when not saved.
	Sources *ScenarioDataSources `json:"sources,omitempty"`
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

// ScenarioCreationStatus is how far building a scenario has got.
type ScenarioCreationStatus string

const (
	// ScenarioCreationEmpty means nothing has started building the scenario.
	ScenarioCreationEmpty ScenarioCreationStatus = "empty"
	// ScenarioCreationGenerating means Bitfab is building the scenario, or is
	// waiting for the scenario callback to save it.
	ScenarioCreationGenerating ScenarioCreationStatus = "generating"
	// ScenarioCreationCreated means the scenario has been built.
	ScenarioCreationCreated ScenarioCreationStatus = "created"
	// ScenarioCreationError means building the scenario failed. CreationError
	// says why, and GenerateScenario starts it again.
	ScenarioCreationError ScenarioCreationStatus = "error"
)

// Scenario is the scenario built from a trace, in any creation state.
type Scenario struct {
	ID              string `json:"id"`
	CreatedAt       string `json:"createdAt"`
	UpdatedAt       string `json:"updatedAt"`
	OrganizationID  string `json:"organizationId"`
	TraceFunctionID string `json:"traceFunctionId"`
	SourceTraceID   string `json:"sourceTraceId"`
	Extractor       string `json:"extractor"`
	Name            string `json:"name"`
	Description     string `json:"description"`
	// Trigger is nil until the scenario is built.
	Trigger *ScenarioTrigger `json:"trigger"`
	// Data is nil until the scenario is built.
	Data *ScenarioData `json:"data"`
	// ActualOutcome is what the run actually did, as last saved by a caller.
	// It is nil when nothing has been saved or it was cleared.
	ActualOutcome *ScenarioActualOutcome `json:"actualOutcome"`
	// TriggerSummary is nil until the scenario is built.
	TriggerSummary *string `json:"triggerSummary"`
	// DataSummary is nil until the scenario is built.
	DataSummary *string `json:"dataSummary"`
	// Categories are the categories the scenario is filed under, sorted by title.
	// It is empty when the scenario is filed under none.
	Categories []CategorySummary `json:"categories"`
	// CreationStatus is how far building the scenario has got.
	CreationStatus ScenarioCreationStatus `json:"creationStatus"`
	// CreationError says why building the scenario failed. It is nil unless
	// CreationStatus is ScenarioCreationError.
	CreationError *string `json:"creationError"`
}

type SaveScenarioParams struct {
	TraceID     string           `json:"-"`
	Name        *string          `json:"name,omitempty"`
	Description *string          `json:"description,omitempty"`
	Trigger     *ScenarioTrigger `json:"trigger,omitempty"`
	// Data replaces the scenario's data whole, apart from its records. Its
	// Records and Sources are left out of the request and the saved ones are
	// kept, so data read back from GetScenario can be sent as it is. Use
	// Records to replace the saved records.
	Data       *ScenarioData       `json:"data,omitempty"`
	AppendData *ScenarioDataAppend `json:"appendData,omitempty"`
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
	// Records replaces the scenario's records whole. Bitfab looks for each
	// record's Quote, or failing that its Text, in the run's spans and saves
	// the span it shows up in. Nil leaves the saved records untouched, and a
	// non-nil empty slice saves no records. Records are saved on their own,
	// with only Sources beside them, so setting any other field with them is
	// refused. A non-empty list is refused while the scenario's trigger is empty.
	Records []ScenarioRecordInput `json:"-"`
	// Sources says where the records came from and is sent only with Records.
	// Leaving it nil while saving Records clears the saved sources.
	Sources *ScenarioDataSources `json:"sources,omitempty"`
}

func (update SaveScenarioParams) payload() (map[string]any, error) {
	if update.Sources != nil && update.Records == nil {
		return nil, errors.New("bitfab: scenario sources can only be saved together with records")
	}
	if update.Records != nil && (update.Name != nil || update.Description != nil || update.Trigger != nil ||
		update.Data != nil || update.AppendData != nil || update.ActualOutcome != nil || update.ClearActualOutcome ||
		update.CategoryIDs != nil || update.ClearCategoryIDs) {
		return nil, errors.New("bitfab: scenario records must be saved on their own, with only sources beside them")
	}
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
		update.CategoryIDs == nil && !update.ClearCategoryIDs && update.Records == nil {
		return nil, errors.New("bitfab: a scenario update must set at least one of name, description, trigger, data, appendData, actualOutcome, categoryIds, or records")
	}
	if update.Data != nil {
		data := *update.Data
		data.Records = nil
		data.Sources = nil
		update.Data = &data
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
	if update.Records != nil {
		payload["records"] = update.Records
	}
	return payload, nil
}

func traceScenarioPath(traceID string) string {
	return "/api/sdk/traces/" + url.PathEscape(traceID) + "/scenario"
}

// GetScenario reads the scenario built from a trace, in any creation state.
// After GenerateScenario, poll it until CreationStatus is
// ScenarioCreationCreated or ScenarioCreationError. A trace with no scenario at
// all returns an error.
func (t *TracesClient) GetScenario(ctx context.Context, traceID string) (*Scenario, error) {
	var response struct {
		Scenario Scenario `json:"scenario"`
	}
	if err := t.httpClient.get(ctx, traceScenarioPath(traceID), &response); err != nil {
		return nil, err
	}
	return &response.Scenario, nil
}

// ScenarioGenerationStatus says what GenerateScenario started.
type ScenarioGenerationStatus string

const (
	// ScenarioGenerationStarted means Bitfab is building the scenario in the
	// background. Poll GetScenario until it is created or fails.
	ScenarioGenerationStarted ScenarioGenerationStatus = "started"
	// ScenarioGenerationAlreadyGenerated means the trace already has a
	// scenario, so nothing was started and ScenarioID names it.
	ScenarioGenerationAlreadyGenerated ScenarioGenerationStatus = "already_generated"
)

// ScenarioGeneration is what GenerateScenario returns. ScenarioID is nil while
// the trace has no scenario yet. CreationStatus is the scenario's creation
// status right after the call, the same one GetScenario reads.
type ScenarioGeneration struct {
	Status         ScenarioGenerationStatus `json:"status"`
	TraceID        string                   `json:"traceId"`
	ScenarioID     *string                  `json:"scenarioId"`
	CreationStatus ScenarioCreationStatus   `json:"creationStatus"`
}

// GenerateScenarioOption configures GenerateScenario.
type GenerateScenarioOption func(*generateScenarioConfig)

type generateScenarioConfig struct {
	regenerate bool
}

// WithRegenerate builds a scenario the trace already has again: its name,
// description, trigger, data, and actual outcome are cleared and built over,
// and the organization's scenario callback, when one is set up, is asked
// again. Its categories, review status, labels, and assertions are kept, and
// asking while it is generating starts nothing more.
func WithRegenerate() GenerateScenarioOption {
	return func(config *generateScenarioConfig) { config.regenerate = true }
}

// GenerateScenario starts building the scenario for one trace and returns at
// once, without waiting for it. Poll GetScenario until its CreationStatus is
// ScenarioCreationCreated or ScenarioCreationError. A trace that already has a
// scenario is left as it is, unless building it failed, which starts it again,
// or WithRegenerate is passed. A replay, a trace outside the
// organization, or a trace Bitfab cannot build a scenario for (not
// noah-agent-turn and no scenario callback) returns an error. Adding a trace
// to a dataset starts the same build only when the organization has a
// scenario callback.
func (t *TracesClient) GenerateScenario(ctx context.Context, traceID string, options ...GenerateScenarioOption) (*ScenarioGeneration, error) {
	config := generateScenarioConfig{}
	for _, option := range options {
		option(&config)
	}
	payload := map[string]any{}
	if config.regenerate {
		payload["regenerate"] = true
	}
	var response struct {
		Generation ScenarioGeneration `json:"generation"`
	}
	if err := t.httpClient.requestInto(ctx, traceScenarioPath(traceID)+"/generate", payload, &response); err != nil {
		return nil, err
	}
	return &response.Generation, nil
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
