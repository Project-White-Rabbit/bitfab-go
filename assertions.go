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
	ID                     string               `json:"id"`
	TraceID                string               `json:"traceId"`
	Assertion              string               `json:"assertion"`
	PassCriteria           *string              `json:"passCriteria"`
	FailCriteria           *string              `json:"failCriteria"`
	TargetOnEvaluatedTrace *TraceTarget         `json:"targetOnEvaluatedTrace"`
	Source                 TraceAssertionSource `json:"source"`
	HumanNote              *string              `json:"humanNote"`
	CategoryID             *string              `json:"category_id"`
	Category               *CategorySummary     `json:"category"`
	Justification          Justification        `json:"justification"`
	CategoryJustification  Justification        `json:"categoryJustification"`
	Assignee               *Assignee            `json:"assignee"`
	CreatedAt              string               `json:"createdAt"`
	UpdatedAt              string               `json:"updatedAt"`
	// Rewrite reports Bitfab's latest check of the text sent for this
	// assertion, on saves and on GetAssertions reads alike. It is nil when no
	// check ever ran on it.
	Rewrite *AssertionRewrite `json:"rewrite,omitempty"`
	// Deprecated: Use CategoryID instead.
	CategoryAssertionID *string `json:"category_assertion_id"`
}

// AssertionRewriteStatus says where Bitfab's check of an assertion's sent text stands.
type AssertionRewriteStatus string

const (
	// AssertionRewritePending means the check is still running. Bitfab rewrites
	// the assertion in place when it finishes, usually within a minute, and
	// GetAssertions then reports the result.
	AssertionRewritePending AssertionRewriteStatus = "pending"
	// AssertionRewriteRewritten means the sent text broke the writing rules and
	// the rewritten text was saved in its place.
	AssertionRewriteRewritten AssertionRewriteStatus = "rewritten"
	// AssertionRewriteUnchanged means the sent text was checked and saved as sent.
	AssertionRewriteUnchanged AssertionRewriteStatus = "unchanged"
	// AssertionRewriteNotChecked means the check was skipped, because
	// SkipGuidedRewrite was set or the entry sent no assertion text.
	AssertionRewriteNotChecked AssertionRewriteStatus = "not_checked"
	// AssertionRewriteCheckFailed means the check could not run, or had not
	// finished 15 minutes after the save, so the text stays as sent. Error says why.
	AssertionRewriteCheckFailed AssertionRewriteStatus = "check_failed"
)

// AssertionRewrite reports how Bitfab checked one saved assertion's text
// against the writing rules and what it changed. The check runs in the background
// after the save, so a save returns it as pending, and GetAssertions reads the
// finished report.
type AssertionRewrite struct {
	// Status says whether the text was rewritten, kept, not checked, or could not be checked.
	Status AssertionRewriteStatus `json:"status"`
	// SentAssertion is the text the caller sent, or nil when the entry sent no text.
	SentAssertion *string `json:"sentAssertion"`
	// Changes holds one sentence per change, saying what changed and why.
	Changes []string `json:"changes"`
	// SeparateAssertions are other claims the sent text bundled together. The
	// server did not save them. Save each one as its own assertion.
	SeparateAssertions []string `json:"separateAssertions"`
	// Unresolved lists problems rewording could not fix, and what to do about each.
	Unresolved []string `json:"unresolved"`
	// Error says why the check could not run. It is set only when Status is check_failed.
	Error *string `json:"error"`
}

// UnmarshalJSON fills CategoryID and CategoryAssertionID from whichever key the server sent.
func (assertion *TraceAssertion) UnmarshalJSON(data []byte) error {
	type traceAssertionFields TraceAssertion
	var decoded traceAssertionFields
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.CategoryID == nil {
		decoded.CategoryID = decoded.CategoryAssertionID
	}
	decoded.CategoryAssertionID = decoded.CategoryID
	*assertion = TraceAssertion(decoded)
	return nil
}

// AssertionEvidenceParameter describes one argument by name and runtime type,
// without including its captured value.
type AssertionEvidenceParameter struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// AssertionLabelEvidence is suggested assertion evidence with its span context.
//
// Deprecated: Use AssertionVerdictEvidence instead.
type AssertionLabelEvidence = AssertionVerdictEvidence

// AssertionVerdictEvidence is suggested assertion evidence enriched with the
// span context a judge needs to interpret it.
type AssertionVerdictEvidence struct {
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
// the human note, criteria, targets, categories, and assignees. Point a justification to a nil slice to clear it.
type SaveAssertion struct {
	ID                          string
	Assertion                   string
	HumanNote                   *string
	PassCriteria                *string
	FailCriteria                *string
	TargetOnEvaluatedTrace      *TraceTarget
	CategoryID                  *string
	Justification               *Justification
	CategoryJustification       *Justification
	AssigneeEmail               *string
	ClearHumanNote              bool
	ClearPassCriteria           bool
	ClearFailCriteria           bool
	ClearTargetOnEvaluatedTrace bool
	ClearCategoryID             bool
	ClearAssigneeEmail          bool
	// Deprecated: Use CategoryID instead. CategoryID and ClearCategoryID take precedence when either is set.
	CategoryAssertionID *string
	// Deprecated: Use ClearCategoryID instead. CategoryID and ClearCategoryID take precedence when either is set.
	ClearCategoryAssertionID bool
}

func (assertion SaveAssertion) category() (*string, bool) {
	if assertion.CategoryID != nil || assertion.ClearCategoryID {
		return assertion.CategoryID, assertion.ClearCategoryID
	}
	return assertion.CategoryAssertionID, assertion.ClearCategoryAssertionID
}

func (assertion SaveAssertion) payload() (map[string]any, error) {
	payload := map[string]any{"assertion": assertion.Assertion}
	if assertion.ID != "" {
		payload["id"] = assertion.ID
	}
	if assertion.ClearHumanNote {
		if assertion.HumanNote != nil {
			return nil, fmt.Errorf("bitfab: cannot set and clear an assertion human note together")
		}
		payload["humanNote"] = nil
	} else if assertion.HumanNote != nil {
		payload["humanNote"] = *assertion.HumanNote
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
	if categoryID, clearCategory := assertion.category(); clearCategory {
		if categoryID != nil {
			return nil, fmt.Errorf("bitfab: cannot set and clear a category together")
		}
		payload["category_id"] = nil
	} else if categoryID != nil {
		payload["category_id"] = *categoryID
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

// AssertionGenerationStatus is where a trace's assertion generation stands.
type AssertionGenerationStatus string

const (
	AssertionGenerationRunning   AssertionGenerationStatus = "running"
	AssertionGenerationCompleted AssertionGenerationStatus = "completed"
	AssertionGenerationFailed    AssertionGenerationStatus = "failed"
)

// AssertionGeneration reports the latest assertion generation on a trace.
// AssertionIDs name the drafts a completed generation saved.
type AssertionGeneration struct {
	Status       AssertionGenerationStatus `json:"status"`
	Error        *string                   `json:"error"`
	AssertionIDs []string                  `json:"assertionIds"`
	StartedAt    string                    `json:"startedAt"`
	FinishedAt   *string                   `json:"finishedAt"`
}

// TraceAssertionsResult includes the ancestor supplying inherited assertions
// and the trace's assertion generation, nil when none ran.
type TraceAssertionsResult struct {
	Assertions    []TraceAssertion     `json:"assertions"`
	InheritedFrom *string              `json:"inheritedFrom"`
	Generation    *AssertionGeneration `json:"generation"`
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
	// SkipGuidedRewrite saves each assertion's text exactly as sent, with no
	// check. By default Bitfab checks the text against the writing rules
	// in the background after the save and, when it bundles two claims, hides a
	// condition, points at a value instead of stating it, names an id from the
	// recorded run, or cannot be measured, rewrites it in place.
	SkipGuidedRewrite bool
}

// SaveAssertionsAllParams saves an atomic batch of assertions. Source defaults to agent.
type SaveAssertionsAllParams struct {
	Updates []TraceAssertionsUpdate
	Source  TraceAssertionSource
	// SkipGuidedRewrite saves each assertion's text exactly as sent, with no
	// check. By default Bitfab checks the text against the writing rules
	// in the background after the save and, when it bundles two claims, hides a
	// condition, points at a value instead of stating it, names an id from the
	// recorded run, or cannot be measured, rewrites it in place.
	SkipGuidedRewrite bool
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

// SaveAssertions upserts one original trace's assertions through the atomic
// batch endpoint, as SaveAssertionsAll describes.
func (t *TracesClient) SaveAssertions(ctx context.Context, params SaveAssertionsParams) ([]TraceAssertion, error) {
	return t.SaveAssertionsAll(ctx, SaveAssertionsAllParams{
		Updates:           []TraceAssertionsUpdate{{TraceID: params.TraceID, Assertions: params.Assertions}},
		Source:            params.Source,
		SkipGuidedRewrite: params.SkipGuidedRewrite,
	})
}

// SaveAssertionsAll upserts at most 500 traces, 50 assertions per trace, and 1000
// assertions in total. The server validates and saves the whole batch
// atomically, with the text as sent, and the call returns once it is saved.
// Unless SkipGuidedRewrite is set, each assertion that carries text comes back
// with Rewrite.Status AssertionRewritePending, and Bitfab checks it against the
// writing rules in the background and rewrites it in place when it breaks one,
// usually within a minute.
// Read the result later with GetAssertions, whose rows carry Rewrite, and save
// each of its SeparateAssertions as its own assertion. The assertions come back
// in the order sent. An empty update list returns an empty slice without a
// request.
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
		"updates": updates, "source": source, "guidedRewrite": !params.SkipGuidedRewrite,
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
