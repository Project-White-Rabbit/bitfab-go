package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

// DatasetGraderRef identifies a grader assigned to a dataset.
type DatasetGraderRef struct {
	ID   string  `json:"id"`
	Name *string `json:"name"`
}

// Dataset is a named bucket of traces scoped to one trace function.
// Experiments replay against it and its graders score its members.
type Dataset struct {
	ID               string             `json:"id"`
	TraceFunctionKey string             `json:"traceFunctionKey"`
	Name             string             `json:"name"`
	Description      *string            `json:"description"`
	TraceCount       int                `json:"traceCount"`
	Graders          []DatasetGraderRef `json:"graders"`
	CreatedAt        string             `json:"createdAt"`
	UpdatedAt        string             `json:"updatedAt"`
}

// SaveDatasetParams names the dataset to create or update. An empty
// Description leaves an existing description untouched.
type SaveDatasetParams struct {
	TraceFunctionKey string
	Name             string
	Description      string
}

// SaveDatasetResult reports whether Save created the dataset or updated an
// existing one.
type SaveDatasetResult struct {
	Dataset Dataset `json:"dataset"`
	Created bool    `json:"created"`
}

// ListDatasetsParams scopes List to one trace function when TraceFunctionKey
// is set; the zero value lists every dataset in the organization.
type ListDatasetsParams struct {
	TraceFunctionKey string
}

// DatasetTraceIDs is the membership of a dataset.
type DatasetTraceIDs struct {
	DatasetID string   `json:"datasetId"`
	TraceIDs  []string `json:"traceIds"`
}

// AddDatasetTracesResult reports partial acceptance of an AddTraces call.
type AddDatasetTracesResult struct {
	Dataset                Dataset  `json:"dataset"`
	AddedTraceIDs          []string `json:"addedTraceIds"`
	AlreadyPresentTraceIDs []string `json:"alreadyPresentTraceIds"`
	SkippedTraceIDs        []string `json:"skippedTraceIds"`
}

// RemoveDatasetTracesResult reports which traces left the dataset.
type RemoveDatasetTracesResult struct {
	Dataset            Dataset  `json:"dataset"`
	RemovedTraceIDs    []string `json:"removedTraceIds"`
	NotPresentTraceIDs []string `json:"notPresentTraceIds"`
}

// AddDatasetGradersResult reports partial acceptance of an AddGraders call.
type AddDatasetGradersResult struct {
	Dataset                  Dataset  `json:"dataset"`
	AddedGraderIDs           []string `json:"addedGraderIds"`
	AlreadyAssignedGraderIDs []string `json:"alreadyAssignedGraderIds"`
	SkippedGraderIDs         []string `json:"skippedGraderIds"`
}

// RemoveDatasetGradersResult reports which graders were unassigned.
type RemoveDatasetGradersResult struct {
	Dataset              Dataset  `json:"dataset"`
	RemovedGraderIDs     []string `json:"removedGraderIds"`
	NotAssignedGraderIDs []string `json:"notAssignedGraderIds"`
}

// GraderRerunStatus is the lifecycle state of a grader re-run.
type GraderRerunStatus string

const (
	GraderRerunPending   GraderRerunStatus = "pending"
	GraderRerunRunning   GraderRerunStatus = "running"
	GraderRerunCompleted GraderRerunStatus = "completed"
	GraderRerunErrored   GraderRerunStatus = "errored"
)

// Terminal reports whether the run has finished, successfully or not.
func (s GraderRerunStatus) Terminal() bool {
	return s == GraderRerunCompleted || s == GraderRerunErrored
}

// GraderRerunProgress is the running tally of a grader re-run.
type GraderRerunProgress struct {
	CompletedTraces int `json:"completedTraces"`
	TotalTraces     int `json:"totalTraces"`
	GraderCount     int `json:"graderCount"`
}

// GraderRerunResult is the summary of a completed grader re-run.
type GraderRerunResult struct {
	TracesGraded int `json:"tracesGraded"`
	GradersRun   int `json:"gradersRun"`
}

// GraderRerun is one grader re-run over a dataset.
type GraderRerun struct {
	ID        string               `json:"id"`
	Status    GraderRerunStatus    `json:"status"`
	GraderIDs []string             `json:"graderIds"`
	Progress  *GraderRerunProgress `json:"progress"`
	Result    *GraderRerunResult   `json:"result"`
	Error     *string              `json:"error"`
	CreatedAt string               `json:"createdAt"`
	UpdatedAt string               `json:"updatedAt"`
}

// RerunGradersOptions configures RerunGraders. GraderIDs defaults to every
// grader assigned to the dataset. The zero value waits for the run to finish
// (up to Timeout, default 90s, polling every PollInterval, default 1s); set
// NoWait to return as soon as the run is queued.
type RerunGradersOptions struct {
	GraderIDs    []string
	NoWait       bool
	Timeout      time.Duration
	PollInterval time.Duration
}

// RerunGradersResult is the run RerunGraders started or joined, in the last
// state it observed.
type RerunGradersResult struct {
	Run            GraderRerun `json:"run"`
	JoinedExisting bool        `json:"joinedExisting"`
}

const (
	defaultRerunTimeout      = 90 * time.Second
	defaultRerunPollInterval = time.Second
)

// DatasetsClient creates, reads, and modifies datasets for the authenticated
// organization, the same operations the Bitfab MCP dataset tools expose to a
// coding agent. Reach it as Client.Datasets.
type DatasetsClient struct {
	httpClient *httpClient
}

func datasetPath(datasetID, suffix string) string {
	return "/api/sdk/datasets/" + url.PathEscape(datasetID) + suffix
}

func decodeDatasetResponse(response map[string]any, target any) error {
	encoded, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("bitfab: decode dataset response: %w", err)
	}
	if err := json.Unmarshal(encoded, target); err != nil {
		return fmt.Errorf("bitfab: decode dataset response: %w", err)
	}
	return nil
}

func (d *DatasetsClient) post(ctx context.Context, endpoint string, payload map[string]any, target any) error {
	response, err := d.httpClient.request(ctx, endpoint, payload, 0)
	if err != nil {
		return err
	}
	return decodeDatasetResponse(response, target)
}

// Save creates a dataset, or updates the one already named this way under the
// same trace function. The result reports which happened.
func (d *DatasetsClient) Save(ctx context.Context, params SaveDatasetParams) (*SaveDatasetResult, error) {
	payload := map[string]any{
		"traceFunctionKey": params.TraceFunctionKey,
		"name":             params.Name,
	}
	if params.Description != "" {
		payload["description"] = params.Description
	}
	var result SaveDatasetResult
	if err := d.post(ctx, "/api/sdk/datasets", payload, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// List returns the organization's datasets, scoped to one trace function
// when params.TraceFunctionKey is set.
func (d *DatasetsClient) List(ctx context.Context, params ListDatasetsParams) ([]Dataset, error) {
	endpoint := "/api/sdk/datasets"
	if params.TraceFunctionKey != "" {
		query := url.Values{}
		query.Set("traceFunctionKey", params.TraceFunctionKey)
		endpoint += "?" + query.Encode()
	}
	var response struct {
		Datasets []Dataset `json:"datasets"`
	}
	if err := d.httpClient.get(ctx, endpoint, &response); err != nil {
		return nil, err
	}
	return response.Datasets, nil
}

// Get fetches one dataset by id. A dataset outside this organization fails
// with a 404.
func (d *DatasetsClient) Get(ctx context.Context, datasetID string) (*Dataset, error) {
	var response struct {
		Dataset Dataset `json:"dataset"`
	}
	if err := d.httpClient.get(ctx, datasetPath(datasetID, ""), &response); err != nil {
		return nil, err
	}
	return &response.Dataset, nil
}

// ListTraces returns the ids of every trace in the dataset, the same
// membership a replay with DatasetID selects.
func (d *DatasetsClient) ListTraces(ctx context.Context, datasetID string) (*DatasetTraceIDs, error) {
	var result DatasetTraceIDs
	if err := d.httpClient.get(ctx, datasetPath(datasetID, "/traces"), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// AddTraces adds traces to the dataset (1 to 100 ids per call). Traces outside
// the organization or under another trace function are reported in
// SkippedTraceIDs rather than failing the call.
func (d *DatasetsClient) AddTraces(ctx context.Context, datasetID string, traceIDs []string) (*AddDatasetTracesResult, error) {
	var result AddDatasetTracesResult
	if err := d.post(ctx, datasetPath(datasetID, "/traces"), map[string]any{"traceIds": traceIDs}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// RemoveTraces removes traces from the dataset. The traces themselves are
// never deleted.
func (d *DatasetsClient) RemoveTraces(ctx context.Context, datasetID string, traceIDs []string) (*RemoveDatasetTracesResult, error) {
	var result RemoveDatasetTracesResult
	if err := d.post(ctx, datasetPath(datasetID, "/removeTraces"), map[string]any{"traceIds": traceIDs}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// AddGraders assigns graders to the dataset (1 to 100 ids per call). Graders
// outside the organization or under another trace function are reported in
// SkippedGraderIDs rather than failing the call.
func (d *DatasetsClient) AddGraders(ctx context.Context, datasetID string, graderIDs []string) (*AddDatasetGradersResult, error) {
	var result AddDatasetGradersResult
	if err := d.post(ctx, datasetPath(datasetID, "/graders"), map[string]any{"graderIds": graderIDs}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// RemoveGraders unassigns graders from the dataset.
func (d *DatasetsClient) RemoveGraders(ctx context.Context, datasetID string, graderIDs []string) (*RemoveDatasetGradersResult, error) {
	var result RemoveDatasetGradersResult
	if err := d.post(ctx, datasetPath(datasetID, "/removeGraders"), map[string]any{"graderIds": graderIDs}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// RerunGraders re-runs graders over every trace in the dataset. An unassigned
// grader id is rejected. Unless options.NoWait is set it waits for the run to
// finish (bounded by options.Timeout) and returns the last state seen either
// way. A request matching an in-flight run joins it.
func (d *DatasetsClient) RerunGraders(ctx context.Context, datasetID string, options RerunGradersOptions) (*RerunGradersResult, error) {
	payload := map[string]any{}
	if options.GraderIDs != nil {
		payload["graderIds"] = options.GraderIDs
	}
	var started RerunGradersResult
	if err := d.post(ctx, datasetPath(datasetID, "/rerunGraders"), payload, &started); err != nil {
		return nil, err
	}
	if options.NoWait {
		return &started, nil
	}

	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultRerunTimeout
	}
	interval := options.PollInterval
	if interval <= 0 {
		interval = defaultRerunPollInterval
	}
	deadline := time.Now().Add(timeout)
	run := started.Run
	for !run.Status.Terminal() && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
		latest, err := d.GetGraderRerun(ctx, datasetID, run.ID)
		if err != nil {
			return nil, err
		}
		if latest != nil {
			run = *latest
		}
	}
	return &RerunGradersResult{Run: run, JoinedExisting: started.JoinedExisting}, nil
}

// GetGraderRerun returns the run named by runID, or the dataset's active run
// when runID is empty. A nil run means nothing is active or the run is not
// this dataset's.
func (d *DatasetsClient) GetGraderRerun(ctx context.Context, datasetID, runID string) (*GraderRerun, error) {
	endpoint := datasetPath(datasetID, "/rerunGraders")
	if runID != "" {
		query := url.Values{}
		query.Set("runId", runID)
		endpoint += "?" + query.Encode()
	}
	var response struct {
		Run *GraderRerun `json:"run"`
	}
	if err := d.httpClient.get(ctx, endpoint, &response); err != nil {
		return nil, err
	}
	return response.Run, nil
}
