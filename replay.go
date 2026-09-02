package bitfab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	defaultReplayLimit       = 5
	defaultReplayConcurrency = 10
	replayPersistenceTimeout = 30 * time.Second
	maxReplayTraceIDs        = 100
	maxReplayLimit           = 5000
)

// CodeChangeFile records one file changed by the code under test.
type CodeChangeFile struct {
	Path   string `json:"path"`
	Before string `json:"before"`
	After  string `json:"after"`
}

// TokenUsage is token usage captured for one trace.
type TokenUsage struct {
	Input  *int64 `json:"input"`
	Output *int64 `json:"output"`
	Cached *int64 `json:"cached"`
	Total  *int64 `json:"total"`
}

type TraceOutlineSpanError struct {
	Source string  `json:"source"`
	Error  string  `json:"error"`
	Step   *string `json:"step,omitempty"`
}

type TraceOutlineSpan struct {
	SpanID           string                  `json:"spanId"`
	Name             *string                 `json:"name"`
	Type             string                  `json:"type"`
	TraceFunctionKey *string                 `json:"traceFunctionKey"`
	DurationMS       *int64                  `json:"durationMs"`
	Tokens           *TokenUsage             `json:"tokens"`
	Model            *string                 `json:"model"`
	Errors           []TraceOutlineSpanError `json:"errors"`
	Mocked           bool                    `json:"mocked"`
	Children         []TraceOutlineSpan      `json:"children"`
}

type TraceOutline struct {
	TraceID          string             `json:"traceId"`
	Name             *string            `json:"name"`
	Status           string             `json:"status"`
	TraceFunctionKey *string            `json:"traceFunctionKey"`
	DurationMS       *int64             `json:"durationMs"`
	SpanCount        int                `json:"spanCount"`
	Spans            []TraceOutlineSpan `json:"spans"`
}

// AdaptContext identifies the historical trace whose inputs are being adapted.
type AdaptContext struct {
	OriginalTraceID string `json:"originalTraceId"`
	OriginalSpanID  string `json:"originalSpanId"`
	SourceTraceID   string `json:"sourceTraceId"`
	SourceSpanID    string `json:"sourceSpanId"`
}

// ReplayInputAdapter reshapes recorded positional inputs for a changed function signature.
type ReplayInputAdapter func(inputs []any, ctx AdaptContext) ([]any, error)

// ReplayItem is one historical trace executed against the current function.
type ReplayItem struct {
	TraceID              *string          `json:"traceId"`
	OriginalTraceID      string           `json:"originalTraceId"`
	OriginalSpanID       string           `json:"originalSpanId"`
	SourceTraceID        string           `json:"sourceTraceId"`
	SourceSpanID         string           `json:"sourceSpanId"`
	Input                []any            `json:"input"`
	Result               any              `json:"result"`
	OriginalOutput       any              `json:"originalOutput"`
	Error                *string          `json:"error"`
	TraceError           error            `json:"-"`
	ReplayError          error            `json:"-"`
	DurationMS           *int64           `json:"durationMs"`
	OriginalDurationMS   *int64           `json:"originalDurationMs"`
	OriginalTokens       *TokenUsage      `json:"originalTokens"`
	OriginalModel        *string          `json:"originalModel"`
	Tokens               *TokenUsage      `json:"tokens"`
	Model                *string          `json:"model"`
	DBSnapshotRef        *DBSnapshotRef   `json:"dbSnapshotRef"`
	DBBranchTimings      *DBBranchTimings `json:"dbBranchTimings"`
	TraceOutline         *TraceOutline    `json:"traceOutline"`
	OriginalTraceOutline *TraceOutline    `json:"originalTraceOutline"`
	localTraceID         string
}

// MarshalJSON preserves structured Go errors without losing the compatible Error message.
func (item ReplayItem) MarshalJSON() ([]byte, error) {
	type alias ReplayItem
	return json.Marshal(struct {
		alias
		TraceError  any `json:"traceError"`
		ReplayError any `json:"replayError"`
	}{
		alias:       alias(item),
		TraceError:  replayErrorJSON(item.TraceError),
		ReplayError: replayErrorJSON(item.ReplayError),
	})
}

func replayErrorJSON(err error) any {
	if err == nil {
		return nil
	}
	result := map[string]any{
		"type":    reflect.TypeOf(err).String(),
		"message": err.Error(),
	}
	if branchErr := asDBBranchReplayError(err); branchErr != nil {
		result["code"] = branchErr.Code
		result["originalTraceId"] = branchErr.OriginalTraceID
	}
	if cause := errors.Unwrap(err); cause != nil {
		result["cause"] = replayErrorJSON(cause)
	}
	return result
}

// ReplayResult contains every replayed item and the experiment it created.
type ReplayResult struct {
	Items      []ReplayItem `json:"items"`
	TestRunID  string       `json:"testRunId"`
	TestRunURL string       `json:"testRunUrl"`
}

// ReplayError is a whole-run failure that retains all items collected before it failed.
type ReplayError struct {
	Message    string
	Items      []ReplayItem
	TestRunID  string
	TestRunURL string
	Cause      error
}

func (err *ReplayError) Error() string {
	return err.Message
}

// Unwrap returns the underlying persistence or finalization error.
func (err *ReplayError) Unwrap() error {
	return err.Cause
}

// ReplayItemStartProgress is emitted when a replay worker starts an item.
type ReplayItemStartProgress struct {
	Type      string       `json:"type"`
	TestRunID string       `json:"testRunId"`
	Started   int          `json:"started"`
	Completed int          `json:"completed"`
	Total     int          `json:"total"`
	Succeeded int          `json:"succeeded"`
	Errored   int          `json:"errored"`
	Item      AdaptContext `json:"item"`
}

// ReplayItemFinishProgress is emitted exactly once after each item settles.
type ReplayItemFinishProgress struct {
	TestRunID string     `json:"testRunId"`
	Completed int        `json:"completed"`
	Total     int        `json:"total"`
	Succeeded int        `json:"succeeded"`
	Errored   int        `json:"errored"`
	Item      ReplayItem `json:"item"`
}

// ReplayOptions configures Client.Replay.
type ReplayOptions struct {
	Limit                    int
	TraceIDs                 []string
	Name                     string
	MaxConcurrency           int
	CodeChangeDescription    *string
	CodeChangeFiles          []CodeChangeFile
	DisableCodeChangeCapture bool
	Mock                     MockStrategy
	MockOverrides            []MockOverride
	DBBranch                 *DBBranchOptions
	ExperimentGroupID        string
	DatasetID                string
	GraderIDs                []string
	AdaptInputs              ReplayInputAdapter
	OnItemStart              func(ReplayItemStartProgress)
	OnItemFinish             func(ReplayItemFinishProgress)
	dbBranchSettings         map[string]any
}

// ReportReplayProgress writes a replay lifecycle event using the Bitfab plugin wire protocol.
func ReportReplayProgress(progress any) {
	defer func() { recover() }()
	encoded, err := json.Marshal(progress)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(os.Stderr, "@@bitfab:progress %s\n", encoded)
}

// SerializeReplayResult returns the complete replay result as JSON.
func SerializeReplayResult(result ReplayResult) (string, error) {
	encoded, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("bitfab: serialize replay result: %w", err)
	}
	return string(encoded), nil
}

type replayServerItem struct {
	OriginalTraceID    string                  `json:"originalTraceId"`
	OriginalSpanID     string                  `json:"originalSpanId"`
	SourceTraceID      string                  `json:"sourceTraceId"`
	SourceSpanID       string                  `json:"sourceSpanId"`
	OriginalDurationMS *int64                  `json:"originalDurationMs"`
	DurationMS         *int64                  `json:"durationMs"`
	OriginalTokens     *TokenUsage             `json:"originalTokens"`
	Tokens             *TokenUsage             `json:"tokens"`
	OriginalModel      *string                 `json:"originalModel"`
	Model              *string                 `json:"model"`
	DBSnapshotRef      *DBSnapshotRef          `json:"dbSnapshotRef"`
	DBBranchLease      *dbBranchLeaseWire      `json:"dbBranchLease"`
	DBBranchLeaseError *dbBranchLeaseErrorWire `json:"dbBranchLeaseError"`
	DBBranchTimings    *DBBranchTimings        `json:"dbBranchTimings"`
}

func (item replayServerItem) originalTraceID() string {
	if item.OriginalTraceID != "" {
		return item.OriginalTraceID
	}
	return item.SourceTraceID
}

func (item replayServerItem) originalSpanID() string {
	if item.OriginalSpanID != "" {
		return item.OriginalSpanID
	}
	return item.SourceSpanID
}

type startReplayResponse struct {
	TestRunID  string             `json:"testRunId"`
	TestRunURL string             `json:"testRunUrl"`
	Items      []replayServerItem `json:"items"`
}

type externalReplaySpan struct {
	ID              string `json:"id"`
	ExternalTraceID string `json:"externalTraceId"`
	RawData         struct {
		SpanData struct {
			Input  any `json:"input"`
			Output any `json:"output"`
		} `json:"span_data"`
	} `json:"rawData"`
}

type replayStatusResponse struct {
	TraceIDs map[string]string `json:"traceIds"`
}

type completeReplayResponse struct {
	TraceIDs              map[string]string        `json:"traceIds"`
	Tokens                map[string]*TokenUsage   `json:"tokens"`
	TraceOutlines         map[string]*TraceOutline `json:"traceOutlines"`
	OriginalTraceOutlines map[string]*TraceOutline `json:"originalTraceOutlines"`
	TraceCount            *int                     `json:"traceCount"`
}

// ReplayFunction binds a callable to the trace function key it was designed to record.
// Create one with BindReplayFunction or Function.BindReplay.
type ReplayFunction struct {
	traceFunctionKey string
	fn               any
}

// BindReplayFunction declares the trace function key associated with fn.
// Client.Replay rejects the value when its declared key differs from the key
// selected for historical inputs.
func BindReplayFunction(traceFunctionKey string, fn any) ReplayFunction {
	return ReplayFunction{traceFunctionKey: traceFunctionKey, fn: fn}
}

// BindReplay declares that fn uses this Function's trace function key.
func (f *Function) BindReplay(fn any) ReplayFunction {
	if f == nil {
		return ReplayFunction{fn: fn}
	}
	return BindReplayFunction(f.traceFunctionKey, fn)
}

// Replay replays this Function's historical inputs through fn.
func (f *Function) Replay(ctx context.Context, fn any, options *ReplayOptions) (ReplayResult, error) {
	if f == nil || f.client == nil {
		return ReplayResult{}, fmt.Errorf("bitfab: replay requires a bound function client")
	}
	return f.client.Replay(ctx, f.traceFunctionKey, f.BindReplay(fn), options)
}

func resolveReplayFunction(traceFunctionKey string, fn any) (any, error) {
	bound, ok := fn.(ReplayFunction)
	if !ok {
		if pointer, pointerOK := fn.(*ReplayFunction); pointerOK && pointer != nil {
			bound = *pointer
			ok = true
		}
	}
	if !ok {
		return fn, nil
	}
	if bound.traceFunctionKey != traceFunctionKey {
		return nil, fmt.Errorf(
			"bitfab: function is bound to trace function key %q but replay was called with %q; pass matching keys or the unbound function",
			bound.traceFunctionKey,
			traceFunctionKey,
		)
	}
	return bound.fn, nil
}

type replayCallable struct {
	value      reflect.Value
	params     []reflect.Type
	hasContext bool
	variadic   bool
}

var (
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
)

func prepareReplayCallable(fn any) (*replayCallable, error) {
	if fn == nil {
		return nil, fmt.Errorf("bitfab: replay function is required")
	}
	value := reflect.ValueOf(fn)
	if value.Kind() != reflect.Func || value.IsNil() {
		return nil, fmt.Errorf("bitfab: replay function must be a non-nil function")
	}
	typeOfFn := value.Type()
	callable := &replayCallable{value: value, variadic: typeOfFn.IsVariadic()}
	start := 0
	if typeOfFn.NumIn() > 0 && typeOfFn.In(0) == contextType {
		callable.hasContext = true
		start = 1
	}
	for index := start; index < typeOfFn.NumIn(); index++ {
		callable.params = append(callable.params, typeOfFn.In(index))
	}
	if typeOfFn.NumOut() > 0 {
		for index := 0; index < typeOfFn.NumOut()-1; index++ {
			if typeOfFn.Out(index).Implements(errorType) {
				return nil, fmt.Errorf("bitfab: replay function error must be its final return value")
			}
		}
	}
	return callable, nil
}

func (callable *replayCallable) inputs(raw any) ([]any, error) {
	paramCount := len(callable.params)
	if paramCount == 0 {
		if raw == nil {
			return []any{}, nil
		}
		if values, ok := raw.([]any); ok && len(values) == 0 {
			return []any{}, nil
		}
		return nil, fmt.Errorf("recorded input is present; function now expects no inputs")
	}
	if paramCount == 1 && !callable.variadic {
		return []any{raw}, nil
	}
	values, ok := raw.([]any)
	if !ok {
		if callable.variadic {
			values = []any{raw}
		} else {
			return nil, fmt.Errorf("recorded input is %T; function now expects %d positional inputs", raw, paramCount)
		}
	}
	if !callable.variadic && len(values) != paramCount {
		return nil, fmt.Errorf("recorded input has %d values; function now expects %d", len(values), paramCount)
	}
	if callable.variadic && len(values) < paramCount-1 {
		return nil, fmt.Errorf("recorded input has %d values; variadic function requires at least %d", len(values), paramCount-1)
	}
	return values, nil
}

func (callable *replayCallable) validateInputs(inputs []any) error {
	paramCount := len(callable.params)
	if !callable.variadic && len(inputs) != paramCount {
		return fmt.Errorf("adapted input has %d values; function expects %d", len(inputs), paramCount)
	}
	if callable.variadic && len(inputs) < paramCount-1 {
		return fmt.Errorf("adapted input has %d values; variadic function requires at least %d", len(inputs), paramCount-1)
	}
	return nil
}

func decodeReplayValue(input any, target reflect.Type) (reflect.Value, error) {
	if input == nil {
		switch target.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			return reflect.Zero(target), nil
		default:
			return reflect.Value{}, fmt.Errorf("null cannot be passed to %s", target)
		}
	}
	value := reflect.ValueOf(input)
	if value.Type().AssignableTo(target) {
		return value, nil
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("encode recorded input for %s: %w", target, err)
	}
	decoded := reflect.New(target)
	if err := json.Unmarshal(encoded, decoded.Interface()); err != nil {
		return reflect.Value{}, fmt.Errorf("decode recorded input into %s: %w", target, err)
	}
	return decoded.Elem(), nil
}

func (callable *replayCallable) invoke(ctx context.Context, inputs []any) (result any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic in replayed function: %v", recovered)
		}
	}()

	args := make([]reflect.Value, 0, len(inputs)+1)
	if callable.hasContext {
		args = append(args, reflect.ValueOf(ctx))
	}
	fixed := len(callable.params)
	if callable.variadic {
		fixed--
	}
	for index := 0; index < fixed; index++ {
		decoded, decodeErr := decodeReplayValue(inputs[index], callable.params[index])
		if decodeErr != nil {
			return nil, fmt.Errorf("input %d: %w", index, decodeErr)
		}
		args = append(args, decoded)
	}
	if callable.variadic {
		elementType := callable.params[len(callable.params)-1].Elem()
		for index := fixed; index < len(inputs); index++ {
			decoded, decodeErr := decodeReplayValue(inputs[index], elementType)
			if decodeErr != nil {
				return nil, fmt.Errorf("input %d: %w", index, decodeErr)
			}
			args = append(args, decoded)
		}
	}

	outputs := callable.value.Call(args)
	if len(outputs) > 0 && outputs[len(outputs)-1].Type().Implements(errorType) {
		errorValue := outputs[len(outputs)-1]
		outputs = outputs[:len(outputs)-1]
		nilError := false
		switch errorValue.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			nilError = errorValue.IsNil()
		}
		if !nilError {
			err = errorValue.Interface().(error)
		}
	}
	values := make([]any, len(outputs))
	for index, output := range outputs {
		values[index] = output.Interface()
	}
	switch len(values) {
	case 0:
		return nil, err
	case 1:
		return values[0], err
	default:
		return values, err
	}
}

// Replay fetches historical traces, runs their inputs through fn, and creates an experiment.
//
// fn may optionally take context.Context as its first parameter. Its remaining
// parameters and return values stay fully typed; replay decodes recorded JSON
// into those parameter types. A final error return is treated as the item's
// trace error. Each invocation is wrapped in a root Bitfab span under
// traceFunctionKey, so plain functions and functions that create nested spans
// are both replayable.
func (c *Client) Replay(
	ctx context.Context,
	traceFunctionKey string,
	fn any,
	options *ReplayOptions,
) (ReplayResult, error) {
	resolvedFn, err := resolveReplayFunction(traceFunctionKey, fn)
	if err != nil {
		return ReplayResult{}, err
	}
	callable, err := prepareReplayCallable(resolvedFn)
	if err != nil {
		return ReplayResult{}, err
	}
	if !c.enabled || strings.TrimSpace(c.apiKey) == "" {
		return ReplayResult{}, fmt.Errorf("bitfab: replay requires an enabled client with an API key")
	}
	if strings.TrimSpace(traceFunctionKey) == "" {
		return ReplayResult{}, fmt.Errorf("bitfab: replay trace function key is required")
	}

	resolved, err := normalizeReplayOptions(options)
	if err != nil {
		return ReplayResult{}, err
	}
	if resolved.CodeChangeFiles == nil && !resolved.DisableCodeChangeCapture {
		if captured := resolveAutoCodeChange(ctx, resolved.Name); captured != nil {
			resolved.CodeChangeFiles = captured.Files
			if resolved.CodeChangeDescription == nil {
				resolved.CodeChangeDescription = captured.Description
			}
		}
	}
	resolved.MockOverrides = append(resolved.MockOverrides, c.registeredMockOverrides()...)
	start, err := c.startReplay(ctx, traceFunctionKey, resolved)
	if err != nil {
		return ReplayResult{}, err
	}
	result := ReplayResult{
		Items:      make([]ReplayItem, len(start.Items)),
		TestRunID:  start.TestRunID,
		TestRunURL: c.replayURL(start.TestRunURL),
	}

	localTraceIDs := make([]string, len(start.Items))
	for index := range localTraceIDs {
		localTraceIDs[index] = randomUUID()
	}
	c.httpClient.trackTraceDeliveries(localTraceIDs)

	c.runReplayItems(ctx, traceFunctionKey, callable, resolved, start, localTraceIDs, &result)
	executedTraceIDs := make([]string, 0, len(result.Items))
	for _, item := range result.Items {
		if item.localTraceID != "" {
			executedTraceIDs = append(executedTraceIDs, item.localTraceID)
		}
	}

	persisted, err := c.waitForReplayPersistence(ctx, start.TestRunID, executedTraceIDs)
	if err != nil {
		return ReplayResult{}, newReplayRunError(err, result)
	}
	for index := range result.Items {
		if traceID := persisted[result.Items[index].localTraceID]; traceID != "" {
			result.Items[index].TraceID = &traceID
		}
	}

	complete, err := c.completeReplay(ctx, start.TestRunID)
	if err != nil {
		return ReplayResult{}, newReplayRunError(err, result)
	}
	c.enrichReplayItems(&result, complete)
	if err := writeReplayResultFile(result); err != nil {
		log.Printf("Bitfab: %v", err)
	}
	return result, nil
}

func normalizeReplayOptions(options *ReplayOptions) (ReplayOptions, error) {
	resolved := ReplayOptions{
		Limit:          defaultReplayLimit,
		MaxConcurrency: defaultReplayConcurrency,
		Mock:           MockMarked,
	}
	if options != nil {
		resolved = *options
		if resolved.Limit == 0 {
			resolved.Limit = defaultReplayLimit
		}
		if resolved.MaxConcurrency == 0 {
			resolved.MaxConcurrency = defaultReplayConcurrency
		}
	}
	if resolved.Limit < 1 || resolved.Limit > maxReplayLimit {
		return ReplayOptions{}, fmt.Errorf("bitfab: replay limit must be between 1 and %d", maxReplayLimit)
	}
	if resolved.MaxConcurrency < 1 {
		return ReplayOptions{}, fmt.Errorf("bitfab: replay max concurrency must be at least 1")
	}
	mockStrategy, err := normalizeMockStrategy(resolved.Mock)
	if err != nil {
		return ReplayOptions{}, err
	}
	resolved.Mock = mockStrategy
	for index, override := range resolved.MockOverrides {
		if override.Match == nil {
			return ReplayOptions{}, fmt.Errorf("bitfab: replay mock override %d requires a matcher", index)
		}
	}
	resolved.dbBranchSettings, err = normalizeDBBranchOptions(resolved.DBBranch)
	if err != nil {
		return ReplayOptions{}, err
	}
	if resolved.TraceIDs != nil {
		if len(resolved.TraceIDs) == 0 {
			return ReplayOptions{}, fmt.Errorf("bitfab: replay trace IDs must contain at least one ID")
		}
		if len(resolved.TraceIDs) > maxReplayTraceIDs {
			return ReplayOptions{}, fmt.Errorf("bitfab: replay supports at most %d trace IDs", maxReplayTraceIDs)
		}
		if options != nil && options.Limit != 0 {
			log.Printf("Bitfab: replay limit is ignored when trace IDs are provided")
		}
	}
	return resolved, nil
}

func (c *Client) replayURL(path string) string {
	parsed, err := url.Parse(path)
	if err == nil && parsed.IsAbs() {
		return path
	}
	return strings.TrimRight(c.serviceURL, "/") + "/" + strings.TrimLeft(path, "/")
}

func newReplayRunError(cause error, result ReplayResult) *ReplayError {
	return &ReplayError{
		Message:    cause.Error(),
		Items:      result.Items,
		TestRunID:  result.TestRunID,
		TestRunURL: result.TestRunURL,
		Cause:      cause,
	}
}

func (c *Client) startReplay(ctx context.Context, traceFunctionKey string, options ReplayOptions) (startReplayResponse, error) {
	payload := map[string]any{"traceFunctionKey": traceFunctionKey}
	if options.TraceIDs == nil {
		payload["limit"] = options.Limit
	} else {
		payload["traceIds"] = options.TraceIDs
	}
	if options.Name != "" {
		payload["name"] = options.Name
	}
	if options.CodeChangeDescription != nil {
		payload["codeChangeDescription"] = *options.CodeChangeDescription
	}
	if options.CodeChangeFiles != nil {
		payload["codeChangeFiles"] = options.CodeChangeFiles
	}
	if options.DisableCodeChangeCapture && options.CodeChangeFiles == nil {
		payload["codeChangeFiles"] = nil
	}
	if options.DBBranch != nil {
		payload["includeDbBranchLease"] = true
		payload["lazyDbBranchLease"] = true
		if len(options.dbBranchSettings) > 0 {
			payload["dbBranchSettings"] = options.dbBranchSettings
		}
	}
	if options.ExperimentGroupID != "" {
		payload["experimentGroupId"] = options.ExperimentGroupID
	}
	if options.DatasetID != "" {
		payload["datasetId"] = options.DatasetID
	}
	if options.GraderIDs != nil {
		payload["graderIds"] = options.GraderIDs
	}

	timeout := 30 * time.Second
	if options.DBBranch != nil {
		timeout = replayDBBranchRequestTimeout
	}
	response, err := c.httpClient.request(ctx, "/api/sdk/replay/start", payload, timeout)
	if err != nil {
		return startReplayResponse{}, fmt.Errorf("bitfab: start replay: %w", err)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return startReplayResponse{}, fmt.Errorf("bitfab: decode start replay response: %w", err)
	}
	var start startReplayResponse
	if err := json.Unmarshal(encoded, &start); err != nil {
		return startReplayResponse{}, fmt.Errorf("bitfab: decode start replay response: %w", err)
	}
	if start.TestRunID == "" {
		return startReplayResponse{}, fmt.Errorf("bitfab: start replay response omitted testRunId")
	}
	return start, nil
}

func (c *Client) getReplaySpan(ctx context.Context, spanID string) (externalReplaySpan, error) {
	var span externalReplaySpan
	endpoint := "/api/sdk/externalSpans/" + url.PathEscape(spanID) + "?view=replay"
	if err := c.httpClient.get(ctx, endpoint, &span); err != nil {
		return externalReplaySpan{}, fmt.Errorf("bitfab: fetch replay span %s: %w", spanID, err)
	}
	return span, nil
}

func (c *Client) runReplayItems(
	ctx context.Context,
	traceFunctionKey string,
	callable *replayCallable,
	options ReplayOptions,
	start startReplayResponse,
	localTraceIDs []string,
	result *ReplayResult,
) {
	if len(start.Items) == 0 {
		return
	}
	workerCount := min(options.MaxConcurrency, len(start.Items))
	jobs := make(chan int)
	var workers sync.WaitGroup
	var flushMu sync.Mutex
	progress := &replayProgressState{total: len(start.Items)}

	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				serverItem := start.Items[index]
				progress.reportStart(options.OnItemStart, start.TestRunID, serverItem)
				item := c.runReplayItem(
					ctx,
					traceFunctionKey,
					callable,
					options,
					start.TestRunID,
					serverItem,
					localTraceIDs[index],
				)
				c.fillFinishedReplayTraceID(&item, &flushMu)
				result.Items[index] = item
				progress.reportFinish(options.OnItemFinish, start.TestRunID, item)
			}
		}()
	}
	for index := range start.Items {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
}

func (c *Client) fillFinishedReplayTraceID(item *ReplayItem, flushMu *sync.Mutex) {
	if item.localTraceID == "" {
		return
	}
	flushMu.Lock()
	defer flushMu.Unlock()
	if !c.FlushTraces(replayPersistenceTimeout) {
		return
	}
	if traceID := c.httpClient.peekServerTraceID(item.localTraceID); traceID != "" {
		item.TraceID = &traceID
	}
}

type replayProgressState struct {
	mu        sync.Mutex
	started   int
	completed int
	succeeded int
	errored   int
	total     int
}

func (state *replayProgressState) reportStart(callback func(ReplayItemStartProgress), testRunID string, item replayServerItem) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.started++
	if callback == nil {
		return
	}
	originalTraceID := item.originalTraceID()
	originalSpanID := item.originalSpanID()
	safelyCall(func() {
		callback(ReplayItemStartProgress{
			Type:      "started",
			TestRunID: testRunID,
			Started:   state.started,
			Completed: state.completed,
			Total:     state.total,
			Succeeded: state.succeeded,
			Errored:   state.errored,
			Item: AdaptContext{
				OriginalTraceID: originalTraceID,
				OriginalSpanID:  originalSpanID,
				SourceTraceID:   originalTraceID,
				SourceSpanID:    originalSpanID,
			},
		})
	})
}

func (state *replayProgressState) reportFinish(callback func(ReplayItemFinishProgress), testRunID string, item ReplayItem) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.completed++
	if item.Error == nil {
		state.succeeded++
	} else {
		state.errored++
	}
	if callback == nil {
		return
	}
	safelyCall(func() {
		callback(ReplayItemFinishProgress{
			TestRunID: testRunID,
			Completed: state.completed,
			Total:     state.total,
			Succeeded: state.succeeded,
			Errored:   state.errored,
			Item:      item,
		})
	})
}

func safelyCall(fn func()) {
	defer func() { recover() }()
	fn()
}

func replayMetrics(serverItem replayServerItem) (*int64, *TokenUsage, *string) {
	duration := serverItem.OriginalDurationMS
	if duration == nil {
		duration = serverItem.DurationMS
	}
	tokens := serverItem.OriginalTokens
	if tokens == nil {
		tokens = serverItem.Tokens
	}
	model := serverItem.OriginalModel
	if model == nil {
		model = serverItem.Model
	}
	return duration, tokens, model
}

func baseReplayItem(serverItem replayServerItem) ReplayItem {
	originalTraceID := serverItem.originalTraceID()
	originalSpanID := serverItem.originalSpanID()
	duration, tokens, model := replayMetrics(serverItem)
	return ReplayItem{
		OriginalTraceID:    originalTraceID,
		OriginalSpanID:     originalSpanID,
		SourceTraceID:      originalTraceID,
		SourceSpanID:       originalSpanID,
		Input:              []any{},
		OriginalDurationMS: duration,
		OriginalTokens:     tokens,
		OriginalModel:      model,
		Model:              model,
		DBSnapshotRef:      serverItem.DBSnapshotRef,
		DBBranchTimings:    serverItem.DBBranchTimings,
	}
}

func (c *Client) runReplayItem(
	ctx context.Context,
	traceFunctionKey string,
	callable *replayCallable,
	options ReplayOptions,
	testRunID string,
	serverItem replayServerItem,
	localTraceID string,
) ReplayItem {
	item := baseReplayItem(serverItem)
	lease := serverItem.DBBranchLease
	leaseError := serverItem.DBBranchLeaseError
	if options.DBBranch != nil && lease == nil && leaseError == nil {
		resolved, err := c.resolveReplayDBBranch(ctx, testRunID, item.OriginalTraceID, options.dbBranchSettings)
		if err != nil {
			setReplaySetupError(&item, err)
			return item
		}
		lease = resolved.Lease
		leaseError = resolved.LeaseError
		if resolved.DBSnapshotRef != nil {
			item.DBSnapshotRef = resolved.DBSnapshotRef
		}
		if resolved.Timings != nil {
			item.DBBranchTimings = resolved.Timings
		}
	}
	if lease != nil {
		defer c.releaseReplayDBBranch(context.WithoutCancel(ctx), lease.NeonBranchID)
	}
	if err := replayDBBranchError(leaseError, item.OriginalTraceID); err != nil {
		setReplaySetupError(&item, err)
		return item
	}
	span, err := c.getReplaySpan(ctx, serverItem.originalSpanID())
	if err != nil {
		setReplaySetupError(&item, err)
		return item
	}
	item.OriginalOutput = span.RawData.SpanData.Output
	adaptContext := AdaptContext{
		OriginalTraceID: item.OriginalTraceID,
		OriginalSpanID:  item.OriginalSpanID,
		SourceTraceID:   item.OriginalTraceID,
		SourceSpanID:    item.OriginalSpanID,
	}
	var inputs []any
	if options.AdaptInputs != nil {
		recorded, ok := span.RawData.SpanData.Input.([]any)
		if !ok {
			recorded = []any{span.RawData.SpanData.Input}
		}
		inputs, err = options.AdaptInputs(recorded, adaptContext)
		if err != nil {
			setReplaySetupError(&item, err)
			return item
		}
		if err := callable.validateInputs(inputs); err != nil {
			setReplaySetupError(&item, err)
			return item
		}
		item.Input = inputs
	} else {
		inputs, err = callable.inputs(span.RawData.SpanData.Input)
		if err != nil {
			setReplaySetupError(&item, err)
			return item
		}
		item.Input = inputs
	}
	mockTree, err := c.prepareReplayMockTree(
		ctx,
		serverItem.originalSpanID(),
		options.Mock,
		options.MockOverrides,
	)
	if err != nil {
		setReplaySetupError(&item, err)
		return item
	}
	replayCtx := withReplayContext(ctx, &replayContext{
		testRunID:          testRunID,
		traceID:            localTraceID,
		inputSourceSpanID:  span.ID,
		inputSourceTraceID: span.ExternalTraceID,
		sourceTraceID:      item.OriginalTraceID,
		mockStrategy:       options.Mock,
		mockTree:           mockTree,
		mockOverrides:      options.MockOverrides,
		dbBranchLease:      lease,
		dbBranchTimings:    item.DBBranchTimings,
		callCounters:       map[string]int{},
		outputCache:        map[string]*mockOutputCacheEntry{},
	})

	started := time.Now()
	result, traceErr := c.Span(
		replayCtx,
		traceFunctionKey,
		func(spanCtx context.Context) (any, error) {
			return callable.invoke(spanCtx, inputs)
		},
		WithName(traceFunctionKey),
		WithType("function"),
		WithInput(inputs...),
	)
	duration := time.Since(started).Milliseconds()
	item.DurationMS = &duration
	item.Result = result
	item.localTraceID = localTraceID
	if traceErr != nil {
		message := traceErr.Error()
		item.Error = &message
		item.TraceError = traceErr
	}
	return item
}

func setReplaySetupError(item *ReplayItem, err error) {
	message := err.Error()
	if branchErr := asDBBranchReplayError(err); branchErr != nil {
		message = branchErr.itemMessage()
	}
	item.Error = &message
	item.ReplayError = err
}

func (c *Client) waitForReplayPersistence(ctx context.Context, testRunID string, traceIDs []string) (map[string]string, error) {
	if len(traceIDs) == 0 {
		return map[string]string{}, nil
	}
	if !c.httpClient.hasClosedDeliveries(traceIDs) {
		c.httpClient.takeTraceDeliveries(traceIDs)
		return map[string]string{}, nil
	}
	flushed := c.FlushTraces(replayPersistenceTimeout)
	deliveries := c.httpClient.takeTraceDeliveries(traceIDs)
	expected := make(map[string]int, len(deliveries))
	readBackTraceIDs := make(map[string]string, len(deliveries))
	allDelivered := true
	for traceID, delivery := range deliveries {
		if delivery.serverTraceID != "" {
			readBackTraceIDs[traceID] = delivery.serverTraceID
		}
		if !delivery.closed {
			continue
		}
		expected[traceID] = delivery.spanCount
		allDelivered = allDelivered && delivery.delivered
	}
	if allDelivered {
		return readBackTraceIDs, nil
	}

	deadline := time.Now().Add(replayPersistenceTimeout)
	missing := len(expected)
	for {
		status, err := c.getReplayStatus(ctx, testRunID, expected)
		if err == nil {
			missing = 0
			for traceID := range expected {
				if status.TraceIDs[traceID] == "" {
					missing++
				}
			}
			if missing == 0 {
				return readBackTraceIDs, nil
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return nil, fmt.Errorf("bitfab: replay traces were not fully persisted before the deadline: %w", err)
			}
			cause := ""
			if !flushed {
				cause = " Delivery was also not confirmed before the flush deadline, so the spans likely never reached the server."
			}
			return nil, fmt.Errorf(
				"bitfab: replay traces were not fully persisted before the delivery deadline (test run %s, missing %d of %d traces).%s",
				testRunID,
				missing,
				len(expected),
				cause,
			)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Client) getReplayStatus(ctx context.Context, testRunID string, expected map[string]int) (replayStatusResponse, error) {
	response, err := c.httpClient.request(ctx, "/api/sdk/replay/status", map[string]any{
		"testRunId":          testRunID,
		"expectedSpanCounts": expected,
	}, 30*time.Second)
	if err != nil {
		return replayStatusResponse{}, err
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return replayStatusResponse{}, err
	}
	var status replayStatusResponse
	if err := json.Unmarshal(encoded, &status); err != nil {
		return replayStatusResponse{}, err
	}
	if status.TraceIDs == nil {
		status.TraceIDs = map[string]string{}
	}
	return status, nil
}

func (c *Client) completeReplay(ctx context.Context, testRunID string) (completeReplayResponse, error) {
	response, err := c.httpClient.request(ctx, "/api/sdk/replay/complete", map[string]any{
		"testRunId": testRunID,
	}, 30*time.Second)
	if err != nil {
		return completeReplayResponse{}, fmt.Errorf("bitfab: complete replay: %w", err)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return completeReplayResponse{}, fmt.Errorf("bitfab: decode complete replay response: %w", err)
	}
	var complete completeReplayResponse
	if err := json.Unmarshal(encoded, &complete); err != nil {
		return completeReplayResponse{}, fmt.Errorf("bitfab: decode complete replay response: %w", err)
	}
	return complete, nil
}

func (c *Client) enrichReplayItems(result *ReplayResult, complete completeReplayResponse) {
	missing := 0
	executed := 0
	for index := range result.Items {
		item := &result.Items[index]
		if item.OriginalTraceID != "" {
			item.OriginalTraceOutline = complete.OriginalTraceOutlines[item.OriginalTraceID]
		}
		if item.localTraceID == "" {
			continue
		}
		executed++
		if mapped := complete.TraceIDs[item.localTraceID]; mapped != "" {
			if item.TraceID == nil {
				item.TraceID = &mapped
			}
			item.Tokens = complete.Tokens[mapped]
			item.TraceOutline = complete.TraceOutlines[mapped]
		} else if item.TraceID == nil {
			missing++
		}
	}
	if missing > 0 {
		log.Printf("Bitfab: server omitted replay trace IDs for %d of %d executed items", missing, executed)
	}
}

func writeReplayResultFile(result ReplayResult) error {
	path := os.Getenv("BITFAB_REPLAY_RESULT_PATH")
	if path == "" {
		return nil
	}
	serialized, err := SerializeReplayResult(result)
	if err != nil {
		return err
	}
	if parent := filepath.Dir(path); parent != "." {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("create replay result directory: %w", err)
		}
	}
	if err := os.WriteFile(path, []byte(serialized+"\n"), 0o644); err != nil {
		return fmt.Errorf("write replay result file: %w", err)
	}
	return nil
}
