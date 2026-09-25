package bitfab

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const graderLabelsPath = "/api/sdk/graderLabels"

// GraderLabelSource distinguishes a grader-run verdict from a human-authored one.
type GraderLabelSource string

const (
	GraderLabelSourceHuman GraderLabelSource = "human"
	GraderLabelSourceLive  GraderLabelSource = "live_grader"
)

// GraderLabel is a grader's verdict with its reason, diagnostic, and confidence.
type GraderLabel struct {
	TraceID           string            `json:"traceId"`
	GraderID          string            `json:"graderId"`
	GraderName        *string           `json:"graderName"`
	GraderStatus      string            `json:"graderStatus"`
	GraderType        string            `json:"graderType"`
	Label             *bool             `json:"label"`
	LabelReason       *string           `json:"labelReason"`
	FailureDiagnostic *string           `json:"failureDiagnostic"`
	LabelConfidence   *LabelConfidence  `json:"labelConfidence"`
	Source            GraderLabelSource `json:"source"`
	EvaluatedAt       *string           `json:"evaluatedAt"`
}

// GetGraderLabelsParams selects verdicts by traces, grader, or both.
// A zero Limit leaves the server's default in effect.
type GetGraderLabelsParams struct {
	TraceIDs []string
	GraderID string
	Limit    int
}

// GradersClient reads grader verdicts. Reach it through Client.Graders.
type GradersClient struct {
	httpClient *httpClient
}

// GetLabels reads grader verdicts with optional trace and grader filters.
func (g *GradersClient) GetLabels(ctx context.Context, params GetGraderLabelsParams) ([]GraderLabel, error) {
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
		Labels []GraderLabel `json:"labels"`
	}
	if err := g.httpClient.get(ctx, graderLabelsPath+"?"+query.Encode(), &response); err != nil {
		return nil, err
	}
	return response.Labels, nil
}

type GraderLabelUpdate struct {
	GraderID          string          `json:"graderId"`
	TraceID           string          `json:"traceId"`
	Label             bool            `json:"label"`
	Reason            string          `json:"reason,omitempty"`
	FailureDiagnostic string          `json:"failureDiagnostic,omitempty"`
	Confidence        LabelConfidence `json:"confidence,omitempty"`
}

type GraderLabelOutcome struct {
	GraderID string      `json:"graderId"`
	TraceID  string      `json:"traceId"`
	Label    bool        `json:"label"`
	Action   LabelAction `json:"action"`
}

func (g *GradersClient) SaveLabel(ctx context.Context, update GraderLabelUpdate) (*GraderLabelOutcome, error) {
	outcomes, err := g.SaveLabelAll(ctx, []GraderLabelUpdate{update})
	if err != nil {
		return nil, err
	}
	if len(outcomes) != 1 {
		return nil, fmt.Errorf("bitfab: saving one grader label returned %d outcomes", len(outcomes))
	}
	return &outcomes[0], nil
}

func (g *GradersClient) SaveLabelAll(ctx context.Context, updates []GraderLabelUpdate) ([]GraderLabelOutcome, error) {
	if updates == nil {
		updates = []GraderLabelUpdate{}
	}
	var response struct {
		Labels []GraderLabelOutcome `json:"labels"`
	}
	if err := g.httpClient.requestInto(ctx, graderLabelsPath,
		map[string]any{"labels": updates}, &response); err != nil {
		return nil, err
	}
	return response.Labels, nil
}
