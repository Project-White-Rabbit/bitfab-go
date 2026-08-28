package bitfab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"
)

const replayDBBranchRequestTimeout = 300 * time.Second

// DBSnapshotRef pins a trace to the SDK-observed wall clock before user code ran.
type DBSnapshotRef struct {
	Provider             string `json:"provider,omitempty"`
	SDKWallClockBeforeFn string `json:"sdkWallClockBeforeFn"`
	Origin               string `json:"origin,omitempty"`
}

// DBBranchOptions enables a historical database branch and optionally tunes it.
// A non-nil ReplayOptions.DBBranch enables branching; an empty value uses the
// connected database project's defaults.
type DBBranchOptions struct {
	MinCU     float64 `json:"minCu,omitempty"`
	MaxCU     float64 `json:"maxCu,omitempty"`
	WarmupSQL string  `json:"warmupSql,omitempty"`
}

// DBBranchTimings contains server-measured database branch provisioning durations.
type DBBranchTimings struct {
	StartedAt        string   `json:"startedAt"`
	ProjectResolveMS *float64 `json:"projectResolveMs,omitempty"`
	BranchCreateMS   *float64 `json:"branchCreateMs,omitempty"`
	ConnectionURIMS  *float64 `json:"connectionUriMs,omitempty"`
	ComputeConnectMS *float64 `json:"computeConnectMs,omitempty"`
	BaseProbeMS      *float64 `json:"baseProbeMs,omitempty"`
	WarmupMS         *float64 `json:"warmupMs,omitempty"`
	TotalMS          float64  `json:"totalMs"`
}

type dbBranchLeaseWire struct {
	NeonBranchID       string         `json:"neonBranchId"`
	EnvKey             string         `json:"envKey"`
	DatabaseURL        string         `json:"databaseUrl"`
	ExpiresAt          string         `json:"expiresAt"`
	SnapshotTimestamp  string         `json:"snapshotTimestamp,omitempty"`
	ProviderConsoleURL string         `json:"providerConsoleUrl,omitempty"`
	ReadOnly           *bool          `json:"readOnly,omitempty"`
	Region             string         `json:"region,omitempty"`
	Extra              map[string]any `json:"-"`
}

func (lease *dbBranchLeaseWire) UnmarshalJSON(data []byte) error {
	type alias dbBranchLeaseWire
	var known alias
	if err := json.Unmarshal(data, &known); err != nil {
		return err
	}
	*lease = dbBranchLeaseWire(known)
	var all map[string]any
	if err := json.Unmarshal(data, &all); err != nil {
		return err
	}
	for _, key := range []string{
		"neonBranchId",
		"envKey",
		"databaseUrl",
		"expiresAt",
		"snapshotTimestamp",
		"providerConsoleUrl",
		"readOnly",
		"region",
	} {
		delete(all, key)
	}
	if len(all) > 0 {
		lease.Extra = all
	}
	return nil
}

type dbBranchLeaseErrorWire struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type resolveDBBranchResponse struct {
	DBSnapshotRef *DBSnapshotRef          `json:"dbSnapshotRef"`
	Lease         *dbBranchLeaseWire      `json:"lease"`
	LeaseError    *dbBranchLeaseErrorWire `json:"leaseError"`
	Timings       *DBBranchTimings        `json:"timings"`
}

// ReplayBranch is the historical database branch available to one replay item.
// The connection string is intentionally private and excluded from formatting
// and JSON; obtain it through DatabaseURL, which records that it was accessed.
type ReplayBranch struct {
	NeonBranchID       string         `json:"neonBranchId"`
	EnvKey             string         `json:"envKey"`
	ExpiresAt          string         `json:"expiresAt"`
	SnapshotTimestamp  string         `json:"snapshotTimestamp,omitempty"`
	ProviderConsoleURL string         `json:"providerConsoleUrl,omitempty"`
	ReadOnly           *bool          `json:"readOnly,omitempty"`
	Region             string         `json:"region,omitempty"`
	TraceID            string         `json:"traceId"`
	Extra              map[string]any `json:"extra,omitempty"`
	databaseURL        string
	replay             *replayContext
}

// DatabaseURL returns this item's historical connection string and records its use.
func (branch *ReplayBranch) DatabaseURL() string {
	if branch == nil {
		return ""
	}
	if branch.replay != nil {
		branch.replay.dbSnapshotAccessed.Store(true)
	}
	return branch.databaseURL
}

// String deliberately omits the database connection string.
func (branch *ReplayBranch) String() string {
	if branch == nil {
		return "<nil>"
	}
	return fmt.Sprintf(
		"ReplayBranch{TraceID:%q, ExpiresAt:%q, Region:%q, SnapshotTimestamp:%q}",
		branch.TraceID,
		branch.ExpiresAt,
		branch.Region,
		branch.SnapshotTimestamp,
	)
}

// Format redacts the connection string for every fmt verb, including %#v.
func (branch *ReplayBranch) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, branch.String())
}

// GetCurrentReplayBranch returns the historical database branch attached to ctx.
// It returns nil outside replay and when the source trace had no snapshot branch.
func GetCurrentReplayBranch(ctx context.Context) *ReplayBranch {
	replay := currentReplayContext(ctx)
	if replay == nil || replay.dbBranchLease == nil || replay.sourceTraceID == "" {
		return nil
	}
	lease := replay.dbBranchLease
	return &ReplayBranch{
		NeonBranchID:       lease.NeonBranchID,
		EnvKey:             lease.EnvKey,
		ExpiresAt:          lease.ExpiresAt,
		SnapshotTimestamp:  lease.SnapshotTimestamp,
		ProviderConsoleURL: lease.ProviderConsoleURL,
		ReadOnly:           lease.ReadOnly,
		Region:             lease.Region,
		TraceID:            replay.sourceTraceID,
		Extra:              cloneAnyMap(lease.Extra),
		databaseURL:        lease.DatabaseURL,
		replay:             replay,
	}
}

// DBBranchReplayError retains a structured branch resolution failure.
type DBBranchReplayError struct {
	Code            string
	Message         string
	OriginalTraceID string
	Cause           error
}

func (err *DBBranchReplayError) Error() string {
	return err.Message
}

func (err *DBBranchReplayError) Unwrap() error {
	return err.Cause
}

func (err *DBBranchReplayError) itemMessage() string {
	return fmt.Sprintf(
		"Replay requested a database branch for trace %s but it could not be resolved (%s): %s. The function was not run, because replaying it against the live database would produce a result that looks valid but did not use the historical data you asked for.",
		err.OriginalTraceID,
		err.Code,
		err.Message,
	)
}

func normalizeDBBranchOptions(options *DBBranchOptions) (map[string]any, error) {
	if options == nil {
		return nil, nil
	}
	if err := validateComputeUnits(options.MinCU, "min CU"); err != nil {
		return nil, err
	}
	if err := validateComputeUnits(options.MaxCU, "max CU"); err != nil {
		return nil, err
	}
	if options.MinCU > 0 && options.MaxCU > 0 && options.MinCU > options.MaxCU {
		return nil, fmt.Errorf("bitfab: replay DB branch min CU must be less than or equal to max CU")
	}
	if len(options.WarmupSQL) > 10_000 {
		return nil, fmt.Errorf("bitfab: replay DB branch warmup SQL must be at most 10000 characters")
	}
	if err := validateComputeRange(options.MinCU, options.MaxCU); err != nil {
		return nil, err
	}
	settings := map[string]any{}
	if options.MinCU > 0 {
		settings["minCu"] = options.MinCU
	}
	if options.MaxCU > 0 {
		settings["maxCu"] = options.MaxCU
	}
	if options.WarmupSQL != "" {
		settings["warmupSql"] = options.WarmupSQL
	}
	if len(settings) == 0 {
		return nil, nil
	}
	return settings, nil
}

func validateComputeUnits(value float64, name string) error {
	if value == 0 {
		return nil
	}
	if value == 0.25 || value == 0.5 {
		return nil
	}
	if value >= 1 && value <= 16 && value == float64(int(value)) {
		return nil
	}
	if value >= 18 && value <= 56 && value == float64(int(value)) && int(value)%2 == 0 {
		return nil
	}
	return fmt.Errorf("bitfab: replay DB branch %s must be a supported Neon compute size", name)
}

func validateComputeRange(minCU, maxCU float64) error {
	if minCU == 0 && maxCU == 0 {
		return nil
	}
	if minCU > 0 && maxCU > 0 {
		if minCU == maxCU || (maxCU <= 16 && maxCU-minCU <= 8) {
			return nil
		}
		return fmt.Errorf("bitfab: replay DB branch autoscaling range may not span more than 8 CU or exceed 16 CU; use equal bounds for a fixed size above 16 CU")
	}
	if max(minCU, maxCU) > 16 {
		return fmt.Errorf("bitfab: a lone replay DB branch compute bound may not exceed 16 CU")
	}
	return nil
}

func (c *Client) resolveReplayDBBranch(
	ctx context.Context,
	testRunID string,
	originalTraceID string,
	settings map[string]any,
) (resolveDBBranchResponse, error) {
	payload := map[string]any{
		"testRunId": testRunID,
		"traceId":   originalTraceID,
	}
	if len(settings) > 0 {
		payload["dbBranchSettings"] = settings
	}
	response, err := c.httpClient.request(
		ctx,
		"/api/sdk/replay/resolveDbBranchLease",
		payload,
		replayDBBranchRequestTimeout,
	)
	if err != nil {
		return resolveDBBranchResponse{}, &DBBranchReplayError{
			Code:            "lease_request_failed",
			Message:         "Bitfab could not request the database branch: " + err.Error(),
			OriginalTraceID: originalTraceID,
			Cause:           err,
		}
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return resolveDBBranchResponse{}, err
	}
	var resolved resolveDBBranchResponse
	if err := json.Unmarshal(encoded, &resolved); err != nil {
		return resolveDBBranchResponse{}, err
	}
	return resolved, nil
}

func (c *Client) releaseReplayDBBranch(ctx context.Context, neonBranchID string) {
	if strings.TrimSpace(neonBranchID) == "" {
		return
	}
	_, err := c.httpClient.request(
		ctx,
		"/api/sdk/replay/releaseDbBranchLease",
		map[string]any{"neonBranchId": neonBranchID},
		30*time.Second,
	)
	if err != nil {
		log.Printf("Bitfab: failed to release DB branch %s (the TTL janitor will retry cleanup): %v", neonBranchID, err)
	}
}

func dbSnapshotUsage(replay *replayContext) map[string]any {
	if replay == nil || replay.dbBranchLease == nil {
		return nil
	}
	lease := replay.dbBranchLease
	usage := map[string]any{
		"neon_branch_id": lease.NeonBranchID,
		"accessed":       replay.dbSnapshotAccessed.Load(),
	}
	if lease.SnapshotTimestamp != "" {
		usage["snapshot_timestamp"] = lease.SnapshotTimestamp
	}
	if lease.Region != "" {
		usage["region"] = lease.Region
	}
	if replay.sourceTraceID != "" {
		usage["original_trace_id"] = replay.sourceTraceID
		usage["source_trace_id"] = replay.sourceTraceID
	}
	if replay.dbBranchTimings != nil {
		usage["timings"] = replay.dbBranchTimings
	}
	return usage
}

func replayDBBranchError(err *dbBranchLeaseErrorWire, originalTraceID string) error {
	if err == nil {
		return nil
	}
	return &DBBranchReplayError{
		Code:            err.Code,
		Message:         err.Message,
		OriginalTraceID: originalTraceID,
	}
}

func cloneAnyMap(source map[string]any) map[string]any {
	if len(source) == 0 {
		return nil
	}
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func asDBBranchReplayError(err error) *DBBranchReplayError {
	var branchErr *DBBranchReplayError
	if errors.As(err, &branchErr) {
		return branchErr
	}
	return nil
}
