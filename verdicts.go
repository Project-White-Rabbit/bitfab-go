package bitfab

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

const (
	traceVerdictsPath      = "/api/sdk/traces/verdicts"
	humanTraceVerdictsPath = "/api/sdk/traces/verdicts/human"
)

// VerdictConfidence expresses the author's certainty in a verdict.
type VerdictConfidence string

const (
	VerdictConfidenceVeryLow  VerdictConfidence = "VeryLow"
	VerdictConfidenceLow      VerdictConfidence = "Low"
	VerdictConfidenceMedium   VerdictConfidence = "Medium"
	VerdictConfidenceHigh     VerdictConfidence = "High"
	VerdictConfidenceVeryHigh VerdictConfidence = "VeryHigh"
)

// LabelConfidence expresses the author's certainty in a verdict.
//
// Deprecated: Use VerdictConfidence instead.
type LabelConfidence = VerdictConfidence

const (
	// Deprecated: Use VerdictConfidenceVeryLow instead.
	LabelConfidenceVeryLow = VerdictConfidenceVeryLow
	// Deprecated: Use VerdictConfidenceLow instead.
	LabelConfidenceLow = VerdictConfidenceLow
	// Deprecated: Use VerdictConfidenceMedium instead.
	LabelConfidenceMedium = VerdictConfidenceMedium
	// Deprecated: Use VerdictConfidenceHigh instead.
	LabelConfidenceHigh = VerdictConfidenceHigh
	// Deprecated: Use VerdictConfidenceVeryHigh instead.
	LabelConfidenceVeryHigh = VerdictConfidenceVeryHigh
)

// VerdictAction reports how a write changed the saved verdict.
type VerdictAction string

const (
	VerdictActionSet             VerdictAction = "set"
	VerdictActionArchived        VerdictAction = "archived"
	VerdictActionNoActiveVerdict VerdictAction = "no-active-verdict"
	VerdictActionSkipped         VerdictAction = "skipped"
	VerdictActionAbstained       VerdictAction = "abstained"
)

// LabelAction reports how a write changed the saved verdict.
//
// Deprecated: Use VerdictAction instead.
type LabelAction = VerdictAction

const (
	// Deprecated: Use VerdictActionSet instead.
	LabelActionSet = VerdictActionSet
	// Deprecated: Use VerdictActionArchived instead.
	LabelActionArchived = VerdictActionArchived
	// Deprecated: Use VerdictActionNoActiveVerdict instead. It now equals
	// VerdictActionNoActiveVerdict ("no-active-verdict"), the value the server
	// reports for an archive with nothing to clear, so existing comparisons
	// against it still match.
	LabelActionNoActiveLabel = VerdictActionNoActiveVerdict
	// Deprecated: Use VerdictActionSkipped instead.
	LabelActionSkipped = VerdictActionSkipped
	// Deprecated: Use VerdictActionAbstained instead.
	LabelActionAbstained = VerdictActionAbstained
)

// VerdictStatus distinguishes a judged check from an explicit skip or one
// that has not been judged yet.
type VerdictStatus string

const (
	VerdictStatusJudged    VerdictStatus = "judged"
	VerdictStatusSkipped   VerdictStatus = "skipped"
	VerdictStatusNotJudged VerdictStatus = "not-judged"
)

// LabelStatus distinguishes a verdict from an explicit skip or an absent one.
//
// Deprecated: Use VerdictStatus instead. Its values differ (judged, skipped,
// not-judged), so it is a separate type rather than an alias.
type LabelStatus string

const (
	// Deprecated: Use VerdictStatusJudged instead.
	LabelStatusLabeled LabelStatus = "labeled"
	// Deprecated: Use VerdictStatusSkipped instead.
	LabelStatusSkipped LabelStatus = "skipped"
	// Deprecated: Use VerdictStatusNotJudged instead.
	LabelStatusUnlabeled LabelStatus = "unlabeled"
)

// Verdict is the outcome of judging an assertion. VerdictAbstained means the
// judge could not find the evidence the assertion needs; it counts as a fail.
// People and graders only ever write VerdictPass or VerdictFail.
type Verdict string

const (
	VerdictPass      Verdict = "pass"
	VerdictFail      Verdict = "fail"
	VerdictAbstained Verdict = "abstained"
)

// VerdictSource identifies the author of a saved verdict.
type VerdictSource string

const (
	VerdictSourceHuman VerdictSource = "human"
	VerdictSourceAgent VerdictSource = "agent"
)

// LabelSource identifies the author of a saved verdict.
//
// Deprecated: Use VerdictSource instead.
type LabelSource = VerdictSource

const (
	// Deprecated: Use VerdictSourceHuman instead.
	LabelSourceHuman = VerdictSourceHuman
	// Deprecated: Use VerdictSourceAgent instead.
	LabelSourceAgent = VerdictSourceAgent
)

func verdictFromBool(passed bool) Verdict {
	if passed {
		return VerdictPass
	}
	return VerdictFail
}

func resolvePassFailVerdict(verdict Verdict, deprecatedLabel bool, subject string) (Verdict, error) {
	if verdict == "" {
		return verdictFromBool(deprecatedLabel), nil
	}
	if deprecatedLabel {
		return "", fmt.Errorf("bitfab: a %s sets both Verdict and the deprecated Label; set only Verdict", subject)
	}
	return verdict, nil
}

// VerdictTarget names exactly one TraceID or OriginalTraceID. A replay verdict
// uses OriginalTraceID with WithVerdictExperimentID and, optionally, a
// zero-based Attempt.
type VerdictTarget struct {
	TraceID         string
	OriginalTraceID string
	Attempt         *int
	AssertionID     string
}

// LabelTarget names the trace or replay attempt a verdict scores.
//
// Deprecated: Use VerdictTarget instead.
type LabelTarget = VerdictTarget

func (target VerdictTarget) payload() (map[string]any, error) {
	if (target.TraceID == "") == (target.OriginalTraceID == "") {
		return nil, fmt.Errorf("bitfab: pass exactly one of TraceID or OriginalTraceID for a verdict")
	}
	payload := map[string]any{}
	if target.TraceID != "" {
		if target.Attempt != nil {
			return nil, fmt.Errorf("bitfab: verdict Attempt requires OriginalTraceID")
		}
		payload["traceId"] = target.TraceID
	} else {
		payload["originalTraceId"] = target.OriginalTraceID
		if target.Attempt != nil {
			if *target.Attempt < 0 {
				return nil, fmt.Errorf("bitfab: verdict Attempt must be non-negative")
			}
			payload["attempt"] = *target.Attempt
		}
	}
	if target.AssertionID != "" {
		payload["assertionId"] = target.AssertionID
	}
	return payload, nil
}

func (target VerdictTarget) target() string {
	if target.TraceID != "" {
		return target.TraceID
	}
	return target.OriginalTraceID
}

// VerdictUpdate sets a verdict, skips a check, or archives its previous
// verdict. With neither Skip nor Archive set, Verdict and Annotation are sent:
// VerdictPass or VerdictFail need an Annotation, and VerdictAbstained records
// that the evidence did not show the part of the run the assertion checks, so
// it counts as a fail in every pass rate but is read back as an abstention,
// with Annotation saying what the evidence would have had to show. With Skip
// set, Annotation records why the verdict was withheld.
//
// An empty Verdict falls back to the deprecated Label field, so a zero
// VerdictUpdate is a failing verdict. Setting both Verdict and Label is
// refused. The deprecated LabelTarget is merged with the embedded
// VerdictTarget field by field, so a field set on only one of them is used;
// the update is refused only when both set the same field to different values.
type VerdictUpdate struct {
	VerdictTarget
	Verdict    Verdict
	Annotation string
	Confidence VerdictConfidence
	Evidence   *Justification
	Skip       bool
	Archive    bool
	// Deprecated: Use Verdict with VerdictAbstained instead.
	Abstain bool
	// Deprecated: Use Verdict instead. It is read only when Verdict is empty,
	// true as VerdictPass and false as VerdictFail.
	Label bool
	// Deprecated: Use the embedded VerdictTarget instead. Its fields are merged
	// with the embedded VerdictTarget, and a field set to different values on
	// both is refused.
	LabelTarget LabelTarget
}

// LabelUpdate sets a verdict, skips a check, or archives its previous verdict.
//
// Deprecated: Use VerdictUpdate instead.
type LabelUpdate = VerdictUpdate

func mergeTargetField(name, current, deprecated string) (string, error) {
	if current == "" {
		return deprecated, nil
	}
	if deprecated != "" && deprecated != current {
		return "", fmt.Errorf("bitfab: a verdict update sets %s to %q on VerdictTarget and %q on the deprecated LabelTarget; set only VerdictTarget", name, current, deprecated)
	}
	return current, nil
}

func (update VerdictUpdate) resolvedTarget() (VerdictTarget, error) {
	current, deprecated := update.VerdictTarget, update.LabelTarget
	traceID, err := mergeTargetField("TraceID", current.TraceID, deprecated.TraceID)
	if err != nil {
		return VerdictTarget{}, err
	}
	originalTraceID, err := mergeTargetField("OriginalTraceID", current.OriginalTraceID, deprecated.OriginalTraceID)
	if err != nil {
		return VerdictTarget{}, err
	}
	assertionID, err := mergeTargetField("AssertionID", current.AssertionID, deprecated.AssertionID)
	if err != nil {
		return VerdictTarget{}, err
	}
	attempt := current.Attempt
	if attempt == nil {
		attempt = deprecated.Attempt
	} else if deprecated.Attempt != nil && *deprecated.Attempt != *attempt {
		return VerdictTarget{}, fmt.Errorf("bitfab: a verdict update sets Attempt to %d on VerdictTarget and %d on the deprecated LabelTarget; set only VerdictTarget", *attempt, *deprecated.Attempt)
	}
	return VerdictTarget{TraceID: traceID, OriginalTraceID: originalTraceID, Attempt: attempt, AssertionID: assertionID}, nil
}

func (update VerdictUpdate) resolvedVerdict() (Verdict, error) {
	if update.Abstain {
		if update.Verdict != "" && update.Verdict != VerdictAbstained {
			return "", fmt.Errorf("bitfab: a verdict update sets Verdict %q and the deprecated Abstain; set only Verdict", update.Verdict)
		}
		if update.Label {
			return "", fmt.Errorf("bitfab: a verdict update sets both the deprecated Abstain and the deprecated Label; set only Verdict")
		}
		return VerdictAbstained, nil
	}
	verdict, err := resolvePassFailVerdict(update.Verdict, update.Label, "verdict update")
	if err != nil {
		return "", err
	}
	switch verdict {
	case VerdictPass, VerdictFail, VerdictAbstained:
		return verdict, nil
	default:
		return "", fmt.Errorf("bitfab: verdict %q is not one of pass, fail, or abstained", verdict)
	}
}

func (update VerdictUpdate) payload() (map[string]any, error) {
	target, err := update.resolvedTarget()
	if err != nil {
		return nil, err
	}
	payload, err := target.payload()
	if err != nil {
		return nil, err
	}
	if update.Skip && update.Archive {
		return nil, fmt.Errorf("bitfab: a verdict update can only do one of skip, archive, and set a verdict")
	}
	if (update.Skip || update.Archive) && (update.Verdict != "" || update.Abstain) {
		return nil, fmt.Errorf("bitfab: a verdict update can only do one of skip, archive, and set a verdict")
	}
	if !update.Archive && target.AssertionID == "" {
		return nil, fmt.Errorf("bitfab: agent verdicts must name an assertion: %s. Set AssertionID to the assertion each verdict scores on Save, SaveAll, Skip, and Abstain. Only Archive may omit it, to clear an old whole-trace verdict", target.target())
	}
	switch {
	case update.Skip:
		payload["skip"] = true
		if update.Annotation != "" {
			payload["annotation"] = update.Annotation
		}
	case update.Archive:
		payload["archive"] = true
	default:
		verdict, err := update.resolvedVerdict()
		if err != nil {
			return nil, err
		}
		payload["verdict"] = verdict
		if verdict == VerdictAbstained {
			if update.Confidence != "" || update.Evidence != nil {
				return nil, fmt.Errorf("bitfab: an abstained verdict carries no Confidence or Evidence, since nothing was found to judge")
			}
			if update.Annotation != "" {
				payload["annotation"] = update.Annotation
			}
			break
		}
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

// VerdictOutcome reports the resolved trace and action for an agent-authored write.
type VerdictOutcome struct {
	Key     string        `json:"key"`
	TraceID string        `json:"traceId"`
	Action  VerdictAction `json:"action"`
}

// LabelOutcome reports the resolved trace and action for an agent-authored write.
//
// Deprecated: Use VerdictOutcome instead.
type LabelOutcome = VerdictOutcome

// HumanVerdictUpdate is a person's pass or fail verdict on one assertion. A
// person never abstains. An empty Verdict falls back to the deprecated Label
// field; setting both is refused.
type HumanVerdictUpdate struct {
	TraceID     string
	AssertionID string
	Verdict     Verdict
	Annotation  string
	Confidence  VerdictConfidence
	Evidence    *Justification
	// Deprecated: Use Verdict instead. It is read only when Verdict is empty,
	// true as VerdictPass and false as VerdictFail.
	Label bool
}

// HumanLabelUpdate is a person's pass or fail verdict on one assertion.
//
// Deprecated: Use HumanVerdictUpdate instead.
type HumanLabelUpdate = HumanVerdictUpdate

func (update HumanVerdictUpdate) payload() (map[string]any, error) {
	verdict, err := resolvePassFailVerdict(update.Verdict, update.Label, "human verdict")
	if err != nil {
		return nil, err
	}
	if verdict != VerdictPass && verdict != VerdictFail {
		return nil, fmt.Errorf("bitfab: a human verdict must be pass or fail, got %q", verdict)
	}
	payload := map[string]any{
		"traceId":     update.TraceID,
		"assertionId": update.AssertionID,
		"verdict":     verdict,
		"annotation":  update.Annotation,
	}
	if update.Confidence != "" {
		payload["confidence"] = update.Confidence
	}
	if update.Evidence != nil {
		payload["evidence"] = *update.Evidence
	}
	return payload, nil
}

// HumanVerdictOutcome reports a human-authored verdict saved by the server.
type HumanVerdictOutcome struct {
	TraceID     string        `json:"traceId"`
	AssertionID *string       `json:"assertionId"`
	Verdict     Verdict       `json:"verdict"`
	Action      VerdictAction `json:"action"`
	// Deprecated: Use Verdict instead. It is true only for VerdictPass.
	Label bool `json:"-"`
}

// HumanLabelOutcome reports a human-authored verdict saved by the server.
//
// Deprecated: Use HumanVerdictOutcome instead.
type HumanLabelOutcome = HumanVerdictOutcome

// AssertionVerdict is one assertion's effective saved verdict and author.
type AssertionVerdict struct {
	AssertionID string  `json:"assertionId"`
	Assertion   *string `json:"assertion"`
	// Verdict is nil when there is none.
	Verdict       *Verdict           `json:"verdict"`
	VerdictStatus VerdictStatus      `json:"verdictStatus"`
	Source        VerdictSource      `json:"source"`
	Annotation    *string            `json:"annotation"`
	Evidence      *Justification     `json:"evidence"`
	Confidence    *VerdictConfidence `json:"confidence"`
	Approved      bool               `json:"approved"`
	// Deprecated: Use VerdictStatus instead.
	LabelStatus LabelStatus `json:"labelStatus"`
	// Deprecated: Use Verdict instead. It carries the verdict as a bool, false
	// for both fail and abstained.
	Label *bool `json:"label"`
	// Deprecated: Use Source instead.
	LabelSource VerdictSource `json:"labelSource"`
}

// TraceVerdicts is a trace's effective verdict with individual assertion verdicts.
type TraceVerdicts struct {
	TraceID string `json:"traceId"`
	// Verdict is nil when the trace has none.
	Verdict       *Verdict           `json:"verdict"`
	VerdictStatus VerdictStatus      `json:"verdictStatus"`
	Annotation    *string            `json:"annotation"`
	Evidence      *Justification     `json:"evidence"`
	Approved      bool               `json:"approved"`
	Passed        int                `json:"passed"`
	Failed        int                `json:"failed"`
	Abstained     int                `json:"abstained"`
	Assertions    []AssertionVerdict `json:"assertions"`
	// Deprecated: Use VerdictStatus instead.
	LabelStatus LabelStatus `json:"labelStatus"`
	// Deprecated: Use Verdict instead.
	Label *bool `json:"label"`
}

// TraceLabels is a trace's effective verdict with individual assertion verdicts.
//
// Deprecated: Use TraceVerdicts instead.
type TraceLabels = TraceVerdicts

type verdictWriteConfig struct {
	experimentID string
	testRunID    string
}

// VerdictWriteOption configures an agent verdict write.
type VerdictWriteOption func(*verdictWriteConfig)

// LabelWriteOption configures an agent verdict write.
//
// Deprecated: Use VerdictWriteOption instead.
type LabelWriteOption = VerdictWriteOption

// WithVerdictExperimentID resolves OriginalTraceID and Attempt against a replay experiment.
func WithVerdictExperimentID(experimentID string) VerdictWriteOption {
	return func(config *verdictWriteConfig) { config.experimentID = experimentID }
}

// WithLabelExperimentID resolves OriginalTraceID and Attempt against a replay experiment.
//
// Deprecated: Use WithVerdictExperimentID instead.
func WithLabelExperimentID(experimentID string) VerdictWriteOption {
	return WithVerdictExperimentID(experimentID)
}

// WithLabelTestRunID resolves OriginalTraceID and Attempt against a replay experiment.
//
// Deprecated: Use WithVerdictExperimentID instead.
func WithLabelTestRunID(testRunID string) VerdictWriteOption {
	return func(config *verdictWriteConfig) { config.testRunID = testRunID }
}

// VerdictsClient reads and writes verdicts. Reach it through Client.Verdicts.
type VerdictsClient struct {
	httpClient *httpClient
}

// LabelsClient reads and writes verdicts.
//
// Deprecated: Use VerdictsClient instead.
type LabelsClient = VerdictsClient

// GenerateEvidence generates suggested evidence for an assertion from the
// selected original or replay trace.
func (v *VerdictsClient) GenerateEvidence(ctx context.Context, traceID, assertionID string) ([]AssertionVerdictEvidence, error) {
	var response struct {
		Evidence []AssertionVerdictEvidence `json:"evidence"`
	}
	path := "/api/sdk/traces/" + url.PathEscape(traceID) + "/assertions/" + url.PathEscape(assertionID) + "/evidence"
	if err := v.httpClient.get(ctx, path, &response); err != nil {
		return nil, err
	}
	return response.Evidence, nil
}

// GenerateLabelEvidence generates suggested evidence for an assertion from
// the selected original or replay trace.
//
// Deprecated: Use GenerateEvidence instead.
func (v *VerdictsClient) GenerateLabelEvidence(ctx context.Context, traceID, assertionID string) ([]AssertionVerdictEvidence, error) {
	return v.GenerateEvidence(ctx, traceID, assertionID)
}

// Save writes one agent-authored verdict, skip, or archive operation.
func (v *VerdictsClient) Save(ctx context.Context, update VerdictUpdate, options ...VerdictWriteOption) (*VerdictOutcome, error) {
	outcomes, err := v.SaveAll(ctx, []VerdictUpdate{update}, options...)
	if err != nil {
		return nil, err
	}
	if len(outcomes) != 1 {
		return nil, fmt.Errorf("bitfab: saving one verdict returned %d outcomes", len(outcomes))
	}
	return &outcomes[0], nil
}

// SaveAll sends up to 200 agent-authored verdict updates in one request.
// OriginalTraceID targets use the experiment supplied by WithVerdictExperimentID.
func (v *VerdictsClient) SaveAll(ctx context.Context, updates []VerdictUpdate, options ...VerdictWriteOption) ([]VerdictOutcome, error) {
	config := verdictWriteConfig{}
	for _, option := range options {
		option(&config)
	}
	experimentID, err := resolveExperimentID(config.experimentID, config.testRunID)
	if err != nil {
		return nil, err
	}
	verdicts := make([]map[string]any, 0, len(updates))
	for _, update := range updates {
		payload, err := update.payload()
		if err != nil {
			return nil, err
		}
		verdicts = append(verdicts, payload)
	}
	payload := map[string]any{"verdicts": verdicts}
	if experimentID != "" {
		setExperimentIDKeys(payload, experimentID)
	}
	var response struct {
		Verdicts []VerdictOutcome `json:"verdicts"`
	}
	if err := v.httpClient.requestInto(ctx, traceVerdictsPath, payload, &response); err != nil {
		return nil, err
	}
	return response.Verdicts, nil
}

// Skip withholds a verdict for a trace, replay attempt, or assertion whose check did not run.
func (v *VerdictsClient) Skip(ctx context.Context, target VerdictTarget, options ...VerdictWriteOption) (*VerdictOutcome, error) {
	return v.Save(ctx, VerdictUpdate{VerdictTarget: target, Skip: true}, options...)
}

// Abstain records that the evidence does not show the part of the run the
// assertion checks, so there was nothing to judge. It counts as a fail in every
// pass rate but is read back as an abstention. An abstention that carries a
// reason goes through Save with Verdict set to VerdictAbstained and Annotation set.
func (v *VerdictsClient) Abstain(ctx context.Context, target VerdictTarget, options ...VerdictWriteOption) (*VerdictOutcome, error) {
	return v.Save(ctx, VerdictUpdate{VerdictTarget: target, Verdict: VerdictAbstained}, options...)
}

// Archive clears a previous verdict for a trace, replay attempt, or assertion.
func (v *VerdictsClient) Archive(ctx context.Context, target VerdictTarget, options ...VerdictWriteOption) (*VerdictOutcome, error) {
	return v.Save(ctx, VerdictUpdate{VerdictTarget: target, Archive: true}, options...)
}

// SaveHuman writes one human-authored pass or fail verdict on an assertion.
func (v *VerdictsClient) SaveHuman(ctx context.Context, update HumanVerdictUpdate) (*HumanVerdictOutcome, error) {
	outcomes, err := v.SaveHumanAll(ctx, []HumanVerdictUpdate{update})
	if err != nil {
		return nil, err
	}
	if len(outcomes) != 1 {
		return nil, fmt.Errorf("bitfab: saving one human verdict returned %d outcomes", len(outcomes))
	}
	return &outcomes[0], nil
}

// SaveHumanAll sends up to 200 human-authored verdicts in one atomic request.
func (v *VerdictsClient) SaveHumanAll(ctx context.Context, updates []HumanVerdictUpdate) ([]HumanVerdictOutcome, error) {
	var unscoped []string
	verdicts := make([]map[string]any, 0, len(updates))
	for _, update := range updates {
		if update.AssertionID == "" {
			unscoped = append(unscoped, update.TraceID)
			continue
		}
		payload, err := update.payload()
		if err != nil {
			return nil, err
		}
		verdicts = append(verdicts, payload)
	}
	if len(unscoped) > 0 {
		return nil, fmt.Errorf("bitfab: human verdicts must name an assertion: %s. Set AssertionID to the assertion each verdict scores on SaveHuman and SaveHumanAll", strings.Join(unscoped, ", "))
	}
	var response struct {
		Verdicts []HumanVerdictOutcome `json:"verdicts"`
	}
	if err := v.httpClient.requestInto(ctx, humanTraceVerdictsPath,
		map[string]any{"verdicts": verdicts}, &response); err != nil {
		return nil, err
	}
	for index := range response.Verdicts {
		response.Verdicts[index].Label = response.Verdicts[index].Verdict == VerdictPass
	}
	return response.Verdicts, nil
}

// Get reads one trace's effective verdict. A missing trace returns nil.
func (v *VerdictsClient) Get(ctx context.Context, traceID string) (*TraceVerdicts, error) {
	verdicts, err := v.GetAll(ctx, []string{traceID})
	if err != nil {
		return nil, err
	}
	if len(verdicts) == 0 {
		return nil, nil
	}
	return &verdicts[0], nil
}

// GetAll reads effective verdicts and per-assertion details for up to 100 traces.
// An empty ID list returns an empty slice without a request.
func (v *VerdictsClient) GetAll(ctx context.Context, traceIDs []string) ([]TraceVerdicts, error) {
	if len(traceIDs) == 0 {
		return []TraceVerdicts{}, nil
	}
	query := url.Values{"traceIds": {strings.Join(traceIDs, ",")}}
	var response struct {
		Verdicts []TraceVerdicts `json:"verdicts"`
	}
	if err := v.httpClient.get(ctx, traceVerdictsPath+"?"+query.Encode(), &response); err != nil {
		return nil, err
	}
	return response.Verdicts, nil
}
