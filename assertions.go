package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
)

// TraceTargetKind selects the output or a named span on the evaluated trace.
type TraceTargetKind string

const (
	TraceTargetOutput TraceTargetKind = "output"
	TraceTargetSpan   TraceTargetKind = "span"
)

// TraceTarget identifies what an assertion checks on the evaluated trace.
// A span target requires Name. Occurrence is first, last, or SpanOccurrenceAt(n).
// An omitted occurrence leaves the server's default in effect.
type TraceTarget struct {
	Kind       TraceTargetKind `json:"kind"`
	Name       string          `json:"name,omitempty"`
	Occurrence SpanOccurrence  `json:"-"`
}

func (target TraceTarget) payload() (map[string]any, error) {
	payload := map[string]any{"kind": target.Kind}
	switch target.Kind {
	case TraceTargetOutput:
		if target.Name != "" || target.Occurrence != "" {
			return nil, fmt.Errorf("bitfab: an output assertion target cannot name a span or occurrence")
		}
	case TraceTargetSpan:
		if target.Name == "" {
			return nil, fmt.Errorf("bitfab: a span assertion target requires a name")
		}
		payload["name"] = target.Name
		if target.Occurrence == FirstSpanOccurrence || target.Occurrence == LastSpanOccurrence {
			payload["occurrence"] = target.Occurrence
		} else if target.Occurrence != "" {
			index, err := strconv.Atoi(string(target.Occurrence))
			if err != nil || index < 0 {
				return nil, fmt.Errorf("bitfab: assertion target occurrence must be first, last, or a non-negative index")
			}
			payload["occurrence"] = index
		}
	default:
		return nil, fmt.Errorf("bitfab: assertion target kind must be output or span")
	}
	return payload, nil
}

// MarshalJSON encodes numeric occurrences as JSON numbers, including zero.
func (target TraceTarget) MarshalJSON() ([]byte, error) {
	payload, err := target.payload()
	if err != nil {
		return nil, err
	}
	return json.Marshal(payload)
}

// UnmarshalJSON reads first/last and zero-based numeric occurrences.
func (target *TraceTarget) UnmarshalJSON(data []byte) error {
	var wire struct {
		Kind       TraceTargetKind `json:"kind"`
		Name       string          `json:"name"`
		Occurrence json.RawMessage `json:"occurrence"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*target = TraceTarget{Kind: wire.Kind, Name: wire.Name}
	if len(wire.Occurrence) != 0 && string(wire.Occurrence) != "null" {
		var named string
		if err := json.Unmarshal(wire.Occurrence, &named); err == nil {
			target.Occurrence = SpanOccurrence(named)
		} else {
			var index int
			if err := json.Unmarshal(wire.Occurrence, &index); err != nil {
				return err
			}
			target.Occurrence = SpanOccurrenceAt(index)
		}
	}
	_, err := target.payload()
	return err
}

// TraceAssertionSource identifies who authored an assertion.
type TraceAssertionSource string

const (
	TraceAssertionAgent TraceAssertionSource = "agent"
	TraceAssertionHuman TraceAssertionSource = "human"
)

// TraceAssertion describes what should happen when a trace is replayed.
type TraceAssertion struct {
	ApprovalFields
	ID                     string                    `json:"id"`
	TraceID                string                    `json:"traceId"`
	Assertion              string                    `json:"assertion"`
	PassCriteria           *string                   `json:"passCriteria"`
	FailCriteria           *string                   `json:"failCriteria"`
	TargetOnEvaluatedTrace *TraceTarget              `json:"targetOnEvaluatedTrace"`
	Source                 TraceAssertionSource      `json:"source"`
	HumanNote              *string                   `json:"humanNote"`
	CategoryAssertionID    *string                   `json:"category_assertion_id"`
	Category               *AssertionCategorySummary `json:"category"`
	Justification          Justification             `json:"justification"`
	CategoryJustification  Justification             `json:"categoryJustification"`
	Assignee               *Assignee                 `json:"assignee"`
	CreatedAt              string                    `json:"createdAt"`
	UpdatedAt              string                    `json:"updatedAt"`
}

// AssertionEvidenceParameter describes one argument by name and runtime type,
// without including its captured value.
type AssertionEvidenceParameter struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// AssertionLabelEvidence is suggested assertion evidence enriched with the
// span context a judge needs to interpret it.
type AssertionLabelEvidence struct {
	SpanID      string                       `json:"spanId"`
	Text        string                       `json:"text"`
	SourceField string                       `json:"sourceField"`
	SpanName    *string                      `json:"spanName"`
	Parameters  []AssertionEvidenceParameter `json:"parameters"`
	SpanType    string                       `json:"spanType"`
	IsMocked    bool                         `json:"isMocked"`
}

// SaveAssertion creates an assertion or updates ID in place. Nil optional fields
// preserve existing values. Set the corresponding Clear field to send null for
// criteria, targets, categories, and assignees. Point a justification to a nil slice to clear it.
type SaveAssertion struct {
	ID                          string
	Assertion                   string
	PassCriteria                *string
	FailCriteria                *string
	TargetOnEvaluatedTrace      *TraceTarget
	CategoryAssertionID         *string
	Justification               *Justification
	CategoryJustification       *Justification
	AssigneeEmail               *string
	ClearPassCriteria           bool
	ClearFailCriteria           bool
	ClearTargetOnEvaluatedTrace bool
	ClearCategoryAssertionID    bool
	ClearAssigneeEmail          bool
}

func (assertion SaveAssertion) payload() (map[string]any, error) {
	payload := map[string]any{"assertion": assertion.Assertion}
	if assertion.ID != "" {
		payload["id"] = assertion.ID
	}
	if assertion.ClearPassCriteria {
		if assertion.PassCriteria != nil {
			return nil, fmt.Errorf("bitfab: cannot set and clear assertion pass criteria together")
		}
		payload["passCriteria"] = nil
	} else if assertion.PassCriteria != nil {
		payload["passCriteria"] = *assertion.PassCriteria
	}
	if assertion.ClearFailCriteria {
		if assertion.FailCriteria != nil {
			return nil, fmt.Errorf("bitfab: cannot set and clear assertion fail criteria together")
		}
		payload["failCriteria"] = nil
	} else if assertion.FailCriteria != nil {
		payload["failCriteria"] = *assertion.FailCriteria
	}
	if assertion.ClearTargetOnEvaluatedTrace {
		if assertion.TargetOnEvaluatedTrace != nil {
			return nil, fmt.Errorf("bitfab: cannot set and clear an assertion target together")
		}
		payload["targetOnEvaluatedTrace"] = nil
	} else if assertion.TargetOnEvaluatedTrace != nil {
		target, err := assertion.TargetOnEvaluatedTrace.payload()
		if err != nil {
			return nil, err
		}
		payload["targetOnEvaluatedTrace"] = target
	}
	if assertion.ClearCategoryAssertionID {
		if assertion.CategoryAssertionID != nil {
			return nil, fmt.Errorf("bitfab: cannot set and clear an assertion category together")
		}
		payload["category_assertion_id"] = nil
	} else if assertion.CategoryAssertionID != nil {
		payload["category_assertion_id"] = *assertion.CategoryAssertionID
	}
	if assertion.ClearAssigneeEmail {
		if assertion.AssigneeEmail != nil {
			return nil, fmt.Errorf("bitfab: cannot set and clear an assertion assignee together")
		}
		payload["assigneeEmail"] = nil
	} else if assertion.AssigneeEmail != nil {
		payload["assigneeEmail"] = *assertion.AssigneeEmail
	}
	if assertion.Justification != nil {
		payload["justification"] = *assertion.Justification
	}
	if assertion.CategoryJustification != nil {
		payload["categoryJustification"] = *assertion.CategoryJustification
	}
	return payload, nil
}

// TraceAssertionsResult includes the ancestor supplying inherited assertions.
type TraceAssertionsResult struct {
	Assertions    []TraceAssertion `json:"assertions"`
	InheritedFrom *string          `json:"inheritedFrom"`
}

// TraceAssertionsUpdate is one trace's assertions in a batch save.
type TraceAssertionsUpdate struct {
	TraceID    string
	Assertions []SaveAssertion
}

// SaveAssertionsParams saves assertions on one original trace. Source defaults to agent.
type SaveAssertionsParams struct {
	TraceID    string
	Assertions []SaveAssertion
	Source     TraceAssertionSource
}

// SaveAssertionsAllParams saves an atomic batch of assertions. Source defaults to agent.
type SaveAssertionsAllParams struct {
	Updates []TraceAssertionsUpdate
	Source  TraceAssertionSource
}

// ArchiveAssertionsParams retires assertions by ID without deleting them.
type ArchiveAssertionsParams struct {
	TraceID      string
	AssertionIDs []string
}

func traceAssertionsPath(traceID, suffix string) string {
	return "/api/sdk/traces/" + url.PathEscape(traceID) + "/assertions" + suffix
}

// GetAssertions reads the trace's assertions, inheriting from replay ancestors when needed.
func (t *TracesClient) GetAssertions(ctx context.Context, traceID string) (*TraceAssertionsResult, error) {
	var result TraceAssertionsResult
	if err := t.httpClient.get(ctx, traceAssertionsPath(traceID, ""), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// SaveAssertions upserts one original trace's assertions through the atomic batch endpoint.
func (t *TracesClient) SaveAssertions(ctx context.Context, params SaveAssertionsParams) ([]TraceAssertion, error) {
	return t.SaveAssertionsAll(ctx, SaveAssertionsAllParams{
		Updates: []TraceAssertionsUpdate{{TraceID: params.TraceID, Assertions: params.Assertions}},
		Source:  params.Source,
	})
}

// SaveAssertionsAll upserts at most 500 traces, 50 assertions per trace, and 1000
// assertions in total. The server validates and saves the whole batch atomically.
// An empty update list returns an empty slice without a request.
func (t *TracesClient) SaveAssertionsAll(ctx context.Context, params SaveAssertionsAllParams) ([]TraceAssertion, error) {
	if len(params.Updates) == 0 {
		return []TraceAssertion{}, nil
	}
	updates := make([]map[string]any, 0, len(params.Updates))
	for _, update := range params.Updates {
		assertions := make([]map[string]any, 0, len(update.Assertions))
		for _, assertion := range update.Assertions {
			payload, err := assertion.payload()
			if err != nil {
				return nil, err
			}
			assertions = append(assertions, payload)
		}
		updates = append(updates, map[string]any{"traceId": update.TraceID, "assertions": assertions})
	}
	source := params.Source
	if source == "" {
		source = TraceAssertionAgent
	}
	var response struct {
		Assertions []TraceAssertion `json:"assertions"`
	}
	if err := t.httpClient.requestInto(ctx, "/api/sdk/traces/assertions", map[string]any{
		"updates": updates, "source": source,
	}, &response); err != nil {
		return nil, err
	}
	return response.Assertions, nil
}

// ArchiveAssertions hides the specified assertions from subsequent reads.
func (t *TracesClient) ArchiveAssertions(ctx context.Context, params ArchiveAssertionsParams) ([]string, error) {
	var response struct {
		Archived []string `json:"archived"`
	}
	ids := params.AssertionIDs
	if ids == nil {
		ids = []string{}
	}
	if err := t.httpClient.requestInto(ctx, traceAssertionsPath(params.TraceID, "/archive"),
		map[string]any{"assertionIds": ids}, &response); err != nil {
		return nil, err
	}
	return response.Archived, nil
}
