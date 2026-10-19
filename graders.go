package bitfab

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const graderVerdictsPath = "/api/sdk/graderVerdicts"

// GraderVerdictSource distinguishes a grader-run verdict from a human-authored one.
type GraderVerdictSource string

const (
	GraderVerdictSourceHuman GraderVerdictSource = "human"
	GraderVerdictSourceLive  GraderVerdictSource = "live_grader"
)

// GraderLabelSource distinguishes a grader-run verdict from a human-authored one.
//
// Deprecated: Use GraderVerdictSource instead.
type GraderLabelSource = GraderVerdictSource

const (
	// Deprecated: Use GraderVerdictSourceHuman instead.
	GraderLabelSourceHuman = GraderVerdictSourceHuman
	// Deprecated: Use GraderVerdictSourceLive instead.
	GraderLabelSourceLive = GraderVerdictSourceLive
)

// GraderVerdict is a grader's verdict with its reason, diagnostic, and confidence.
type GraderVerdict struct {
	TraceID      string  `json:"traceId"`
	GraderID     string  `json:"graderId"`
	GraderName   *string `json:"graderName"`
	GraderStatus string  `json:"graderStatus"`
	GraderType   string  `json:"graderType"`
	// Verdict is VerdictPass or VerdictFail, or nil when the grader has not judged the trace.
	Verdict           *Verdict            `json:"verdict"`
	Reason            *string             `json:"reason"`
	FailureDiagnostic *string             `json:"failureDiagnostic"`
	Confidence        *VerdictConfidence  `json:"confidence"`
	Source            GraderVerdictSource `json:"source"`
	EvaluatedAt       *string             `json:"evaluatedAt"`
	// Deprecated: Use Verdict instead.
	Label *bool `json:"-"`
	// Deprecated: Use Reason instead.
	LabelReason *string `json:"-"`
	// Deprecated: Use Confidence instead.
	LabelConfidence *VerdictConfidence `json:"-"`
}

// GraderLabel is a grader's verdict with its reason, diagnostic, and confidence.
//
// Deprecated: Use GraderVerdict instead.
type GraderLabel = GraderVerdict

func (verdict *GraderVerdict) fillDeprecatedFields() {
	if verdict.Verdict != nil {
		passed := *verdict.Verdict == VerdictPass
		verdict.Label = &passed
	}
	verdict.LabelReason = verdict.Reason
	verdict.LabelConfidence = verdict.Confidence
}

// GetGraderVerdictsParams selects verdicts by traces, grader, or both.
// A zero Limit leaves the server's default in effect.
type GetGraderVerdictsParams struct {
	TraceIDs []string
	GraderID string
	Limit    int
}

// GetGraderLabelsParams selects verdicts by traces, grader, or both.
//
// Deprecated: Use GetGraderVerdictsParams instead.
type GetGraderLabelsParams = GetGraderVerdictsParams

// GradersClient reads and writes grader verdicts. Reach it through Client.Graders.
type GradersClient struct {
	httpClient *httpClient
}

// GetVerdicts reads grader verdicts with optional trace and grader filters.
func (g *GradersClient) GetVerdicts(ctx context.Context, params GetGraderVerdictsParams) ([]GraderVerdict, error) {
	if len(params.TraceIDs) == 0 && params.GraderID == "" {
		return nil, fmt.Errorf("bitfab: pass TraceIDs, GraderID, or both; there is nothing to read otherwise")
	}
	query := url.Values{}
	if len(params.TraceIDs) > 0 {
		query.Set("traceIds", strings.Join(params.TraceIDs, ","))
	}
	if params.GraderID != "" {
		query.Set("graderId", params.GraderID)
	}
	if params.Limit != 0 {
		query.Set("limit", strconv.Itoa(params.Limit))
	}
	var response struct {
		Verdicts []GraderVerdict `json:"verdicts"`
	}
	if err := g.httpClient.get(ctx, graderVerdictsPath+"?"+query.Encode(), &response); err != nil {
		return nil, err
	}
	for index := range response.Verdicts {
		response.Verdicts[index].fillDeprecatedFields()
	}
	return response.Verdicts, nil
}

// GetLabels reads grader verdicts with optional trace and grader filters.
//
// Deprecated: Use GetVerdicts instead.
func (g *GradersClient) GetLabels(ctx context.Context, params GetGraderVerdictsParams) ([]GraderVerdict, error) {
	return g.GetVerdicts(ctx, params)
}

// GraderVerdictUpdate records a grader's pass or fail verdict on one trace. A
// grader never abstains. An empty Verdict falls back to the deprecated Label
// field; setting both is refused. Empty Reason, FailureDiagnostic, and
// Confidence are omitted.
type GraderVerdictUpdate struct {
	GraderID          string
	TraceID           string
	Verdict           Verdict
	Reason            string
	FailureDiagnostic string
	Confidence        VerdictConfidence
	// Deprecated: Use Verdict instead. It is read only when Verdict is empty,
	// true as VerdictPass and false as VerdictFail.
	Label bool
}

// GraderLabelUpdate records a grader's pass or fail verdict on one trace.
//
// Deprecated: Use GraderVerdictUpdate instead.
type GraderLabelUpdate = GraderVerdictUpdate

func (update GraderVerdictUpdate) payload() (map[string]any, error) {
	verdict, err := resolvePassFailVerdict(update.Verdict, update.Label, "grader verdict")
	if err != nil {
		return nil, err
	}
	if verdict != VerdictPass && verdict != VerdictFail {
		return nil, fmt.Errorf("bitfab: a grader verdict must be pass or fail, got %q", verdict)
	}
	payload := map[string]any{
		"graderId": update.GraderID,
		"traceId":  update.TraceID,
		"verdict":  verdict,
	}
	if update.Reason != "" {
		payload["reason"] = update.Reason
	}
	if update.FailureDiagnostic != "" {
		payload["failureDiagnostic"] = update.FailureDiagnostic
	}
	if update.Confidence != "" {
		payload["confidence"] = update.Confidence
	}
	return payload, nil
}

// GraderVerdictOutcome reports a grader verdict saved by the server.
type GraderVerdictOutcome struct {
	GraderID string        `json:"graderId"`
	TraceID  string        `json:"traceId"`
	Verdict  Verdict       `json:"verdict"`
	Action   VerdictAction `json:"action"`
	// Deprecated: Use Verdict instead. It is true only for VerdictPass.
	Label bool `json:"-"`
}

// GraderLabelOutcome reports a grader verdict saved by the server.
//
// Deprecated: Use GraderVerdictOutcome instead.
type GraderLabelOutcome = GraderVerdictOutcome

// SaveVerdict records one grader verdict.
func (g *GradersClient) SaveVerdict(ctx context.Context, update GraderVerdictUpdate) (*GraderVerdictOutcome, error) {
	outcomes, err := g.SaveVerdictAll(ctx, []GraderVerdictUpdate{update})
	if err != nil {
		return nil, err
	}
	if len(outcomes) != 1 {
		return nil, fmt.Errorf("bitfab: saving one grader verdict returned %d outcomes", len(outcomes))
	}
	return &outcomes[0], nil
}

// SaveVerdictAll records grader verdicts in one request. An invalid update
// sends nothing.
func (g *GradersClient) SaveVerdictAll(ctx context.Context, updates []GraderVerdictUpdate) ([]GraderVerdictOutcome, error) {
	verdicts := make([]map[string]any, 0, len(updates))
	for _, update := range updates {
		payload, err := update.payload()
		if err != nil {
			return nil, err
		}
		verdicts = append(verdicts, payload)
	}
	var response struct {
		Verdicts []GraderVerdictOutcome `json:"verdicts"`
	}
	if err := g.httpClient.requestInto(ctx, graderVerdictsPath,
		map[string]any{"verdicts": verdicts}, &response); err != nil {
		return nil, err
	}
	for index := range response.Verdicts {
		response.Verdicts[index].Label = response.Verdicts[index].Verdict == VerdictPass
	}
	return response.Verdicts, nil
}

// SaveLabel records one grader verdict.
//
// Deprecated: Use SaveVerdict instead.
func (g *GradersClient) SaveLabel(ctx context.Context, update GraderVerdictUpdate) (*GraderVerdictOutcome, error) {
	return g.SaveVerdict(ctx, update)
}

// SaveLabelAll records grader verdicts in one request.
//
// Deprecated: Use SaveVerdictAll instead.
func (g *GradersClient) SaveLabelAll(ctx context.Context, updates []GraderVerdictUpdate) ([]GraderVerdictOutcome, error) {
	return g.SaveVerdictAll(ctx, updates)
}
