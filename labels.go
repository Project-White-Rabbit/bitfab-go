package bitfab

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// LabelConfidence expresses the author's certainty in a verdict.
type LabelConfidence string

const (
	LabelConfidenceVeryLow  LabelConfidence = "VeryLow"
	LabelConfidenceLow      LabelConfidence = "Low"
	LabelConfidenceMedium   LabelConfidence = "Medium"
	LabelConfidenceHigh     LabelConfidence = "High"
	LabelConfidenceVeryHigh LabelConfidence = "VeryHigh"
)

// LabelAction reports how a verdict write changed the saved label.
type LabelAction string

const (
	LabelActionSet           LabelAction = "set"
	LabelActionArchived      LabelAction = "archived"
	LabelActionNoActiveLabel LabelAction = "no-active-label"
	LabelActionSkipped       LabelAction = "skipped"
)

// LabelStatus distinguishes a verdict from an explicit skip or an absent label.
type LabelStatus string

const (
	LabelStatusLabeled   LabelStatus = "labeled"
	LabelStatusSkipped   LabelStatus = "skipped"
	LabelStatusUnlabeled LabelStatus = "unlabeled"
)

// LabelSource identifies the author of a saved verdict.
type LabelSource string

const (
	LabelSourceHuman LabelSource = "human"
	LabelSourceAgent LabelSource = "agent"
)

// LabelTarget names exactly one TraceID or OriginalTraceID. A replay verdict
// uses OriginalTraceID with WithLabelTestRunID and, optionally, a zero-based
// Attempt. AssertionID narrows either target to one assertion.
type LabelTarget struct {
	TraceID         string
	OriginalTraceID string
	Attempt         *int
	AssertionID     string
}

func (target LabelTarget) payload() (map[string]any, error) {
	if (target.TraceID == "") == (target.OriginalTraceID == "") {
		return nil, fmt.Errorf("bitfab: pass exactly one of TraceID or OriginalTraceID for a label")
	}
	payload := map[string]any{}
	if target.TraceID != "" {
		if target.Attempt != nil {
			return nil, fmt.Errorf("bitfab: label Attempt requires OriginalTraceID")
		}
		payload["traceId"] = target.TraceID
	} else {
		payload["originalTraceId"] = target.OriginalTraceID
		if target.Attempt != nil {
			if *target.Attempt < 0 {
				return nil, fmt.Errorf("bitfab: label Attempt must be non-negative")
			}
			payload["attempt"] = *target.Attempt
		}
	}
	if target.AssertionID != "" {
		payload["assertionId"] = target.AssertionID
	}
	return payload, nil
}

// LabelUpdate sets a pass/fail verdict, skips a check, or archives its previous
// verdict. With neither Skip nor Archive set, Label and Annotation are sent.
// A false Label is a failing verdict and is never omitted.
type LabelUpdate struct {
	LabelTarget
	Label      bool
	Annotation string
	Confidence LabelConfidence
	Evidence   *Justification
	Skip       bool
	Archive    bool
}

func (update LabelUpdate) payload() (map[string]any, error) {
	payload, err := update.LabelTarget.payload()
	if err != nil {
		return nil, err
	}
	if update.Skip && update.Archive {
		return nil, fmt.Errorf("bitfab: a label update cannot both skip and archive")
	}
	switch {
	case update.Skip:
		payload["skip"] = true
	case update.Archive:
		payload["archive"] = true
	default:
		payload["label"] = update.Label
		payload["annotation"] = update.Annotation
		if update.Confidence != "" {
			payload["confidence"] = update.Confidence
		}
		if update.Evidence != nil {
			payload["evidence"] = *update.Evidence
		}
	}
	return payload, nil
}

// LabelOutcome reports the resolved trace and action for an agent-authored write.
type LabelOutcome struct {
	Key     string      `json:"key"`
	TraceID string      `json:"traceId"`
	Action  LabelAction `json:"action"`
}

// HumanLabelUpdate writes a human-authored verdict, validated on write.
type HumanLabelUpdate struct {
	TraceID     string          `json:"traceId"`
	AssertionID string          `json:"assertionId,omitempty"`
	Label       bool            `json:"label"`
	Annotation  string          `json:"annotation"`
	Confidence  LabelConfidence `json:"confidence,omitempty"`
	Evidence    *Justification  `json:"evidence,omitempty"`
}

// HumanLabelOutcome reports a human-authored verdict saved by the server.
type HumanLabelOutcome struct {
	TraceID     string      `json:"traceId"`
	AssertionID *string     `json:"assertionId"`
	Label       bool        `json:"label"`
	Action      LabelAction `json:"action"`
}

// AssertionVerdict is one assertion's effective saved verdict and author.
type AssertionVerdict struct {
	AssertionID string           `json:"assertionId"`
	Assertion   *string          `json:"assertion"`
	LabelStatus LabelStatus      `json:"labelStatus"`
	Label       *bool            `json:"label"`
	Annotation  *string          `json:"annotation"`
	Evidence    *Justification   `json:"evidence"`
	Confidence  *LabelConfidence `json:"confidence"`
	LabelSource LabelSource      `json:"labelSource"`
	Approved    bool             `json:"approved"`
}

// TraceLabels is a trace's effective verdict with individual assertion verdicts.
type TraceLabels struct {
	TraceID     string             `json:"traceId"`
	LabelStatus LabelStatus        `json:"labelStatus"`
	Label       *bool              `json:"label"`
	Annotation  *string            `json:"annotation"`
	Evidence    *Justification     `json:"evidence"`
	Approved    bool               `json:"approved"`
	Passed      int                `json:"passed"`
	Failed      int                `json:"failed"`
	Assertions  []AssertionVerdict `json:"assertions"`
}

type labelWriteConfig struct {
	testRunID string
}

// LabelWriteOption configures an agent label write.
type LabelWriteOption func(*labelWriteConfig)

// WithLabelTestRunID resolves OriginalTraceID and Attempt against a replay run.
func WithLabelTestRunID(testRunID string) LabelWriteOption {
	return func(config *labelWriteConfig) { config.testRunID = testRunID }
}

// LabelsClient reads and writes verdicts. Reach it through Client.Labels.
type LabelsClient struct {
	httpClient *httpClient
}

// GenerateLabelEvidence generates suggested evidence for an assertion from
// the selected original or replay trace.
func (l *LabelsClient) GenerateLabelEvidence(ctx context.Context, traceID, assertionID string) ([]PotentialAssertionEvidence, error) {
	var response struct {
		Evidence []PotentialAssertionEvidence `json:"evidence"`
	}
	path := "/api/sdk/traces/" + url.PathEscape(traceID) + "/assertions/" + url.PathEscape(assertionID) + "/evidence"
	if err := l.httpClient.get(ctx, path, &response); err != nil {
		return nil, err
	}
	return response.Evidence, nil
}

// Save writes one agent-authored verdict, skip, or archive operation.
func (l *LabelsClient) Save(ctx context.Context, update LabelUpdate, options ...LabelWriteOption) (*LabelOutcome, error) {
	outcomes, err := l.SaveAll(ctx, []LabelUpdate{update}, options...)
	if err != nil {
		return nil, err
	}
	if len(outcomes) != 1 {
		return nil, fmt.Errorf("bitfab: saving one label returned %d outcomes", len(outcomes))
	}
	return &outcomes[0], nil
}

// SaveAll sends up to 200 agent-authored verdict updates in one request.
// OriginalTraceID targets use the run supplied by WithLabelTestRunID.
func (l *LabelsClient) SaveAll(ctx context.Context, updates []LabelUpdate, options ...LabelWriteOption) ([]LabelOutcome, error) {
	config := labelWriteConfig{}
	for _, option := range options {
		option(&config)
	}
	labels := make([]map[string]any, 0, len(updates))
	for _, update := range updates {
		payload, err := update.payload()
		if err != nil {
			return nil, err
		}
		labels = append(labels, payload)
	}
	payload := map[string]any{"labels": labels}
	if config.testRunID != "" {
		payload["testRunId"] = config.testRunID
	}
	var response struct {
		Labels []LabelOutcome `json:"labels"`
	}
	if err := l.httpClient.requestInto(ctx, "/api/sdk/traces/labels", payload, &response); err != nil {
		return nil, err
	}
	return response.Labels, nil
}

// Skip withholds a verdict for a trace, replay attempt, or assertion whose check did not run.
func (l *LabelsClient) Skip(ctx context.Context, target LabelTarget, options ...LabelWriteOption) (*LabelOutcome, error) {
	return l.Save(ctx, LabelUpdate{LabelTarget: target, Skip: true}, options...)
}

// Archive clears a previous verdict for a trace, replay attempt, or assertion.
func (l *LabelsClient) Archive(ctx context.Context, target LabelTarget, options ...LabelWriteOption) (*LabelOutcome, error) {
	return l.Save(ctx, LabelUpdate{LabelTarget: target, Archive: true}, options...)
}

// SaveHuman writes one human-authored verdict, validated immediately by the server.
func (l *LabelsClient) SaveHuman(ctx context.Context, update HumanLabelUpdate) (*HumanLabelOutcome, error) {
	outcomes, err := l.SaveHumanAll(ctx, []HumanLabelUpdate{update})
	if err != nil {
		return nil, err
	}
	if len(outcomes) != 1 {
		return nil, fmt.Errorf("bitfab: saving one human label returned %d outcomes", len(outcomes))
	}
	return &outcomes[0], nil
}

// SaveHumanAll sends up to 200 human-authored verdicts in one atomic request.
func (l *LabelsClient) SaveHumanAll(ctx context.Context, updates []HumanLabelUpdate) ([]HumanLabelOutcome, error) {
	if updates == nil {
		updates = []HumanLabelUpdate{}
	}
	var response struct {
		Labels []HumanLabelOutcome `json:"labels"`
	}
	if err := l.httpClient.requestInto(ctx, "/api/sdk/traces/labels/human",
		map[string]any{"labels": updates}, &response); err != nil {
		return nil, err
	}
	return response.Labels, nil
}

// Get reads one trace's effective verdict. A missing trace returns nil.
func (l *LabelsClient) Get(ctx context.Context, traceID string) (*TraceLabels, error) {
	labels, err := l.GetAll(ctx, []string{traceID})
	if err != nil {
		return nil, err
	}
	if len(labels) == 0 {
		return nil, nil
	}
	return &labels[0], nil
}

// GetAll reads effective verdicts and per-assertion details for up to 100 traces.
// An empty ID list returns an empty slice without a request.
func (l *LabelsClient) GetAll(ctx context.Context, traceIDs []string) ([]TraceLabels, error) {
	if len(traceIDs) == 0 {
		return []TraceLabels{}, nil
	}
	query := url.Values{"traceIds": {strings.Join(traceIDs, ",")}}
	var response struct {
		Labels []TraceLabels `json:"labels"`
	}
	if err := l.httpClient.get(ctx, "/api/sdk/traces/labels?"+query.Encode(), &response); err != nil {
		return nil, err
	}
	return response.Labels, nil
}
