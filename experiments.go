package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const experimentsPath = "/api/sdk/experiments"

// ListExperimentsParams narrows a listing. Every field is optional and the
// filters combine with AND. With nothing set the listing pages the whole
// organization newest first, so Limit 1 alone fetches the latest run.
type ListExperimentsParams struct {
	// DatasetID keeps runs launched against this dataset.
	DatasetID        string
	TraceFunctionKey string
	// GitBranch is an exact match on the branch recorded at replay start.
	GitBranch         string
	ExperimentGroupID string
	// Status is pending, completed, failed, or interrupted.
	Status string
	// Metadata keeps runs carrying every pair given, 1 to 10 keys.
	Metadata map[string]string
	// CreatedAfter is an inclusive lower bound on the run's CreatedAt.
	CreatedAfter *time.Time
	// CreatedBefore is an exclusive upper bound on the run's CreatedAt.
	CreatedBefore *time.Time
	// Cursor continues from a previous page's NextCursor.
	Cursor string
	// Limit is 1 to 100. Zero leaves the server's default of 20 in effect.
	Limit int
}

// ExperimentRunner is the person who launched a run.
type ExperimentRunner struct {
	ID       string  `json:"id"`
	FullName *string `json:"fullName"`
	Email    *string `json:"email"`
	ImageURL *string `json:"imageUrl"`
}

// CodeChangeTotals sizes the code change captured with a run: files touched,
// lines added, and lines removed.
type CodeChangeTotals struct {
	Files   int `json:"files"`
	Added   int `json:"added"`
	Removed int `json:"removed"`
}

// Experiment is one replay run as the server records it. Metadata is always
// present and empty when the run set none.
type Experiment struct {
	ID                    string            `json:"id"`
	Name                  *string           `json:"name"`
	Notes                 *string           `json:"notes"`
	Status                string            `json:"status"`
	TraceFunctionKey      *string           `json:"traceFunctionKey"`
	CreatedAt             string            `json:"createdAt"`
	CompletedAt           *string           `json:"completedAt"`
	CodeChangeDescription *string           `json:"codeChangeDescription"`
	CodeChangeTotals      *CodeChangeTotals `json:"codeChangeTotals"`
	ExperimentGroupID     *string           `json:"experimentGroupId"`
	ExperimentGroupName   *string           `json:"experimentGroupName"`
	DatasetID             *string           `json:"datasetId"`
	DatasetName           *string           `json:"datasetName"`
	DatasetIDs            []string          `json:"datasetIds"`
	DatasetNames          []string          `json:"datasetNames"`
	Attempts              int               `json:"attempts"`
	PlannedTraceCount     *int              `json:"plannedTraceCount"`
	RunBy                 *ExperimentRunner `json:"runBy"`
	GitHubEmail           *string           `json:"githubEmail"`
	GitBranch             *string           `json:"gitBranch"`
	CommitSHA             *string           `json:"commitSha"`
	BaseSHA               *string           `json:"baseSha"`
	ExperimentSHA         *string           `json:"experimentSha"`
	Metadata              map[string]string `json:"metadata"`
}

// ExperimentPage is one page of a listing. Pass NextCursor back as Cursor to
// read the next page; the client never pages on its own.
type ExperimentPage struct {
	Experiments []Experiment `json:"experiments"`
	NextCursor  *string      `json:"nextCursor"`
	HasMore     bool         `json:"hasMore"`
}

// ExperimentTotals counts the replays a run produced by status. Total is the
// scenario count.
type ExperimentTotals struct {
	Total          int `json:"total"`
	Succeeded      int `json:"succeeded"`
	Failed         int `json:"failed"`
	Pending        int `json:"pending"`
	AwaitingLabels int `json:"awaitingLabels"`
	Errored        int `json:"errored"`
	WithErrors     int `json:"withErrors"`
	Ungradable     int `json:"ungradable"`
	Skipped        int `json:"skipped"`
}

// ExperimentTally counts verdicts against their originals. A trace or an
// assertion passes when at least 75 percent of its attempts passed, and
// Passing / (Passing + Failing) is the pass rate.
type ExperimentTally struct {
	Fixed             int `json:"fixed"`
	StillPassing      int `json:"stillPassing"`
	Regressed         int `json:"regressed"`
	StillFailing      int `json:"stillFailing"`
	OriginalUnlabeled int `json:"originalUnlabeled"`
	Unpaired          int `json:"unpaired"`
	Classified        int `json:"classified"`
	Passing           int `json:"passing"`
	Failing           int `json:"failing"`
}

// ExperimentCategoryTally tallies the assertions in one category.
type ExperimentCategoryTally struct {
	Category AssertionCategorySummary `json:"category"`
	Labels   ExperimentTally          `json:"labels"`
}

// ExperimentJitter counts attempts whose own verdict disagreed with their
// trace's verdict.
type ExperimentJitter struct {
	Disagreeing int `json:"disagreeing"`
	Attempts    int `json:"attempts"`
	Traces      int `json:"traces"`
}

// ExperimentRollup breaks a run down: Traces tallies replayed traces, Labels
// tallies scored assertions and graders, Checks tallies raw checks before the
// pass threshold, and Categories tallies the assertions in each category.
type ExperimentRollup struct {
	Traces        ExperimentTally           `json:"traces"`
	SkippedTraces int                       `json:"skippedTraces"`
	Labels        ExperimentTally           `json:"labels"`
	Checks        ExperimentTally           `json:"checks"`
	Categories    []ExperimentCategoryTally `json:"categories"`
	Jitter        ExperimentJitter          `json:"jitter"`
}

// ExperimentRollupResult is one run's totals and rollup.
type ExperimentRollupResult struct {
	ExperimentID string           `json:"experimentId"`
	Totals       ExperimentTotals `json:"totals"`
	Rollup       ExperimentRollup `json:"rollup"`
}

// ExperimentsClient lists and reads replay runs. Reach it through
// Client.Experiments.
type ExperimentsClient struct {
	httpClient *httpClient
}

func experimentPath(id string) string {
	return experimentsPath + "/" + url.PathEscape(id)
}

// List returns one page of runs matching params, newest first.
func (e *ExperimentsClient) List(ctx context.Context, params ListExperimentsParams) (*ExperimentPage, error) {
	query := url.Values{}
	for key, value := range map[string]string{
		"datasetId":         params.DatasetID,
		"traceFunctionKey":  params.TraceFunctionKey,
		"gitBranch":         params.GitBranch,
		"experimentGroupId": params.ExperimentGroupID,
		"status":            params.Status,
		"cursor":            params.Cursor,
	} {
		if value != "" {
			query.Set(key, value)
		}
	}
	if len(params.Metadata) > 0 {
		encoded, err := json.Marshal(params.Metadata)
		if err != nil {
			return nil, fmt.Errorf("bitfab: failed to encode metadata filter: %w", err)
		}
		query.Set("metadata", string(encoded))
	}
	if params.CreatedAfter != nil {
		query.Set("createdAfter", params.CreatedAfter.Format(time.RFC3339Nano))
	}
	if params.CreatedBefore != nil {
		query.Set("createdBefore", params.CreatedBefore.Format(time.RFC3339Nano))
	}
	if params.Limit != 0 {
		query.Set("limit", strconv.Itoa(params.Limit))
	}
	endpoint := experimentsPath
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var page ExperimentPage
	if err := e.httpClient.get(ctx, endpoint, &page); err != nil {
		return nil, err
	}
	return &page, nil
}

// Get reads one run in the API key's organization.
func (e *ExperimentsClient) Get(ctx context.Context, id string) (*Experiment, error) {
	var response struct {
		Experiment Experiment `json:"experiment"`
	}
	if err := e.httpClient.get(ctx, experimentPath(id), &response); err != nil {
		return nil, err
	}
	return &response.Experiment, nil
}

// GetRollup reads one run's totals and rollup.
func (e *ExperimentsClient) GetRollup(ctx context.Context, id string) (*ExperimentRollupResult, error) {
	var result ExperimentRollupResult
	if err := e.httpClient.get(ctx, experimentPath(id)+"/rollup", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetRollupAll reads the rollups for up to 100 runs in one request, in the
// order the ids were given. An empty ids makes no request. A single missing id
// fails the whole call.
func (e *ExperimentsClient) GetRollupAll(ctx context.Context, ids []string) ([]ExperimentRollupResult, error) {
	if len(ids) == 0 {
		return []ExperimentRollupResult{}, nil
	}
	query := url.Values{"experimentIds": {strings.Join(ids, ",")}}
	var response struct {
		Rollups []ExperimentRollupResult `json:"rollups"`
	}
	if err := e.httpClient.get(ctx, experimentsPath+"/rollups?"+query.Encode(), &response); err != nil {
		return nil, err
	}
	return response.Rollups, nil
}
