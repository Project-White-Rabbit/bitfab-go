package bitfab

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"time"
)

// SeedOptions configures a seeded execution. Args excludes the optional context argument.
type SeedOptions struct {
	Args      []any
	Metadata  map[string]any
	SessionID string
	Name      string
}

// SeedCaseOptions records positional input and expected output without executing code.
// Function optionally validates the case against a callable's signature.
type SeedCaseOptions struct {
	Input     []any
	Expected  any
	Function  any
	Metadata  map[string]any
	SessionID string
	Name      string
	SpanName  string
	SpanType  string
}

// ReseedResult identifies the adopted trace and its previous run.
type ReseedResult struct {
	TraceID            string `json:"traceId"`
	PreviousRunTraceID string `json:"previousRunTraceId"`
}

// SeedTrace executes fn once, capturing a seeded trace even when ordinary capture is disabled.
// Seeded runs carry no database snapshot pin. Execution failures are recorded and returned.
func (c *Client) SeedTrace(ctx context.Context, traceFunctionKey string, fn any, options *SeedOptions) (string, error) {
	resolved, err := resolveReplayFunction(traceFunctionKey, fn)
	if err != nil {
		return "", err
	}
	callable, err := prepareReplayCallable(resolved)
	if err != nil {
		return "", err
	}
	opts := SeedOptions{}
	if options != nil {
		opts = *options
	}
	if err := callable.validateInputs(opts.Args); err != nil {
		return "", err
	}
	return c.runSeed(ctx, traceFunctionKey, opts, traceFunctionKey, "function", func(ctx context.Context) (any, error) { return callable.invoke(ctx, opts.Args) })
}

// SeedCase creates a seeded trace containing input and expected output, without running Function.
func (c *Client) SeedCase(ctx context.Context, traceFunctionKey string, options SeedCaseOptions) (string, error) {
	if options.Function != nil {
		resolved, err := resolveReplayFunction(traceFunctionKey, options.Function)
		if err != nil {
			return "", err
		}
		callable, err := prepareReplayCallable(resolved)
		if err != nil {
			return "", err
		}
		if err := callable.validateInputs(options.Input); err != nil {
			return "", err
		}
	}
	spanName := options.SpanName
	if spanName == "" {
		spanName = traceFunctionKey
	}
	spanType := options.SpanType
	if spanType == "" {
		spanType = "function"
	}
	return c.runSeed(ctx, traceFunctionKey, SeedOptions{Args: options.Input, Metadata: options.Metadata, SessionID: options.SessionID, Name: options.Name}, spanName, spanType, func(context.Context) (any, error) { return options.Expected, nil })
}

func (c *Client) runSeed(ctx context.Context, key string, options SeedOptions, spanName, spanType string, fn SpanFunc) (string, error) {
	if strings.TrimSpace(key) == "" {
		return "", fmt.Errorf("bitfab: seed trace function key cannot be empty")
	}
	if strings.TrimSpace(c.resolveAPIKey()) == "" {
		return "", fmt.Errorf("bitfab: seeding requires an API key")
	}
	traceID := randomUUID()
	state := createTraceState(traceID)
	state.IngestionType = "seeded"
	state.Metadata = maps.Clone(options.Metadata)
	state.SessionID = options.SessionID
	state.Name = options.Name
	defer deleteTraceState(traceID)
	defer c.FlushTraces(30 * time.Second)
	ctx = context.WithValue(ctx, spanStackKey{}, []spanEntry{})
	ctx = context.WithValue(ctx, replayContextKey{}, (*replayContext)(nil))
	ctx = context.WithValue(ctx, seedContextKey{}, &seedContext{traceID: traceID})
	_, err := c.Span(withManagedTraceRoot(ctx), key, fn, WithName(spanName), WithType(spanType), WithInput(options.Args...))
	if err != nil {
		return "", err
	}
	if getTraceState(traceID) != nil {
		return "", fmt.Errorf("bitfab: seed execution did not record a root trace")
	}
	return traceID, nil
}

// ReseedTrace executes a stored case and adopts the successful fresh run under traceID.
// The server preserves the case's dataset memberships and labels.
func (c *Client) ReseedTrace(ctx context.Context, key string, fn any, traceID string) (ReseedResult, error) {
	endpoint := "/api/sdk/traces/" + url.PathEscape(traceID) + "/reseed"
	var source struct {
		TraceFunctionKey string         `json:"traceFunctionKey"`
		Input            any            `json:"input"`
		Metadata         map[string]any `json:"metadata"`
		SessionID        string         `json:"sessionId"`
		Name             string         `json:"name"`
	}
	if err := c.httpClient.get(ctx, endpoint+"Source", &source); err != nil {
		return ReseedResult{}, err
	}
	if source.TraceFunctionKey != key {
		return ReseedResult{}, fmt.Errorf("bitfab: trace %s belongs to %q, not %q", traceID, source.TraceFunctionKey, key)
	}
	resolved, err := resolveReplayFunction(key, fn)
	if err != nil {
		return ReseedResult{}, err
	}
	callable, err := prepareReplayCallable(resolved)
	if err != nil {
		return ReseedResult{}, err
	}
	args, err := callable.inputs(source.Input)
	if err != nil {
		return ReseedResult{}, err
	}
	runTraceID, err := c.SeedTrace(ctx, key, fn, &SeedOptions{Args: args, Metadata: source.Metadata, SessionID: source.SessionID, Name: source.Name})
	if err != nil {
		return ReseedResult{}, err
	}
	adopted, err := c.httpClient.request(ctx, endpoint, map[string]any{"runTraceId": runTraceID}, 0)
	if err != nil {
		return ReseedResult{}, err
	}
	id, _ := adopted["traceId"].(string)
	if id == "" {
		return ReseedResult{}, fmt.Errorf("bitfab: reseed response omitted traceId")
	}
	previous, _ := adopted["previousRunTraceId"].(string)
	return ReseedResult{TraceID: id, PreviousRunTraceID: previous}, nil
}
