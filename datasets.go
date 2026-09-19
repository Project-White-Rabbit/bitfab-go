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
// Description leaves an existing description untouched unless ClearDescription
// is true, which sends an explicit empty description. Setting a nonempty
// Description together with ClearDescription is rejected.
type SaveDatasetParams struct {
	TraceFunctionKey string
	Name             string
	Description      string
	ClearDescription bool
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
// grader assigned to the dataset. Timeout bounds the wait and defaults to 90s.
type RerunGradersOptions struct {
	GraderIDs []string
	Timeout   time.Duration
}

// RerunGradersResult is the completed run RerunGraders started or joined.
type RerunGradersResult struct {
	Run            GraderRerun `json:"run"`
	JoinedExisting bool        `json:"joinedExisting"`
}

// GraderRerunError is returned by RerunGraders when the run ends errored.
type GraderRerunError struct {
	DatasetID string
	Run       GraderRerun
}

func (e *GraderRerunError) Error() string {
	reason := "no reason recorded"
	if e.Run.Error != nil && *e.Run.Error != "" {
		reason = *e.Run.Error
	}
	return fmt.Sprintf("bitfab: grader rerun on dataset %s errored: %s", e.DatasetID, reason)
}

// GraderRerunTimeoutError is returned by RerunGraders when Timeout elapses
// before the run finishes. The run keeps going on Bitfab. Read it later with
// GetGraderRerun and Run.ID.
type GraderRerunTimeoutError struct {
	DatasetID string
	Timeout   time.Duration
	Run       GraderRerun
}

func (e *GraderRerunTimeoutError) Error() string {
	return fmt.Sprintf(
		"bitfab: grader rerun %s on dataset %s still %s after %s. It keeps going on Bitfab, read it later with GetGraderRerun",
		e.Run.ID, e.DatasetID, e.Run.Status, e.Timeout,
	)
}

const (
	defaultRerunTimeout = 90 * time.Second
	rerunPollGap        = time.Second
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
	if params.ClearDescription && params.Description != "" {
		return nil, fmt.Errorf("bitfab: cannot set Description and ClearDescription together")
	}
	payload := map[string]any{
		"traceFunctionKey": params.TraceFunctionKey,
		"name":             params.Name,
	}
	if params.Description != "" || params.ClearDescription {
		payload["description"] = params.Description
	}
	var result SaveDatasetResult
	if err := d.post(ctx, "/api/sdk/datasets", payload, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// List returns the organization's datasets, scoped to one trace function
// when params.TraceFunctionKey is set. Fetches all pages automatically.
func (d *DatasetsClient) List(ctx context.Context, params ListDatasetsParams) ([]Dataset, error) {
	query := url.Values{"limit": {"100"}}
	if params.TraceFunctionKey != "" {
		query.Set("traceFunctionKey", params.TraceFunctionKey)
	}
	datasets := make([]Dataset, 0)
	for {
		var response struct {
			Datasets   []Dataset `json:"datasets"`
			NextCursor *string   `json:"nextCursor"`
		}
		if err := d.httpClient.get(ctx, "/api/sdk/datasets?"+query.Encode(), &response); err != nil {
			return nil, err
		}
		datasets = append(datasets, response.Datasets...)
		if response.NextCursor == nil {
			return datasets, nil
		}
		query.Set("cursor", *response.NextCursor)
	}
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
// membership a replay with DatasetID selects. Fetches all pages automatically.
func (d *DatasetsClient) ListTraces(ctx context.Context, datasetID string) (*DatasetTraceIDs, error) {
	result := DatasetTraceIDs{TraceIDs: make([]string, 0)}
	query := url.Values{"limit": {"100"}}
	for {
		var page struct {
			DatasetTraceIDs
			NextCursor *string `json:"nextCursor"`
		}
		if err := d.httpClient.get(ctx, datasetPath(datasetID, "/traces")+"?"+query.Encode(), &page); err != nil {
			return nil, err
		}
		result.DatasetID = page.DatasetID
		result.TraceIDs = append(result.TraceIDs, page.TraceIDs...)
		if page.NextCursor == nil {
			return &result, nil
		}
		query.Set("cursor", *page.NextCursor)
	}
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

// RerunGraders re-runs graders over every trace in the dataset and blocks
// until the run finishes. An unassigned grader id is rejected. A request
// matching an in-flight run joins it. An errored run returns a
// *GraderRerunError, and a run still going after options.Timeout returns a
// *GraderRerunTimeoutError. Run it in a goroutine to do other work meanwhile,
// and cancel it with ctx.
func (d *DatasetsClient) RerunGraders(ctx context.Context, datasetID string, options RerunGradersOptions) (*RerunGradersResult, error) {
	payload := map[string]any{}
	if options.GraderIDs != nil {
		payload["graderIds"] = options.GraderIDs
	}
	var started RerunGradersResult
	if err := d.post(ctx, datasetPath(datasetID, "/rerunGraders"), payload, &started); err != nil {
		return nil, err
	}

	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultRerunTimeout
	}
	deadline := time.Now().Add(timeout)
	run := started.Run
	for !run.Status.Terminal() {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, &GraderRerunTimeoutError{DatasetID: datasetID, Timeout: timeout, Run: run}
		}
		timer := time.NewTimer(min(rerunPollGap, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		latest, err := d.GetGraderRerun(ctx, datasetID, run.ID)
		if err != nil {
			return nil, err
		}
		if latest != nil {
			run = *latest
		}
	}
	if run.Status == GraderRerunErrored {
		return nil, &GraderRerunError{DatasetID: datasetID, Run: run}
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
