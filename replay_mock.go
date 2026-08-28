package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

// MockStrategy controls which recorded child span outputs replay substitutes.
type MockStrategy string

const (
	// MockNone executes every child span unless a mock override matches it.
	MockNone MockStrategy = "none"
	// MockAll substitutes every child span that has a recorded counterpart.
	MockAll MockStrategy = "all"
	// MockMarked substitutes only child spans configured with WithMockOnReplay.
	MockMarked MockStrategy = "marked"
)

// MockSource identifies where a substituted output came from.
type MockSource string

const (
	MockSourceRecorded MockSource = "recorded"
	MockSourceOverride MockSource = "override"
)

// SpanNodeMeta is the structural identity passed to a mock override matcher.
type SpanNodeMeta struct {
	TraceFunctionKey string
	SpanName         string
	Type             string
	OriginalSpanID   string
}

// MockOverrideContext contains the live call and lazy historical output accessor.
type MockOverrideContext struct {
	Node              SpanNodeMeta
	Inputs            []any
	GetOriginalOutput func() (any, error)
}

// NodeMatcher selects spans using structural metadata. The first match wins.
type NodeMatcher func(SpanNodeMeta) bool

// MockValueFunc computes a replacement output for a matched span.
type MockValueFunc func(MockOverrideContext) (any, error)

// MockOverride substitutes either Value or the result of Resolve for a matched span.
// Resolve takes precedence when both fields are set. A nil Value is a valid flat value.
type MockOverride struct {
	Match   NodeMatcher
	Value   any
	Resolve MockValueFunc
}

type spanTreeNode struct {
	SourceSpanID     string          `json:"sourceSpanId"`
	ExternalSpanID   string          `json:"externalSpanId"`
	TraceFunctionKey string          `json:"traceFunctionKey"`
	SpanName         string          `json:"spanName"`
	Type             string          `json:"type"`
	Output           json.RawMessage `json:"output"`
	OutputMeta       json.RawMessage `json:"outputMeta"`
	Children         []spanTreeNode  `json:"children"`
}

type spanTreeResponse struct {
	Root *spanTreeNode `json:"root"`
}

type mockSpan struct {
	sourceSpanID   string
	externalSpanID string
	output         any
	outputPresent  bool
}

type mockTree struct {
	spans map[string]mockSpan
}

type mockOutputCacheEntry struct {
	done  chan struct{}
	value any
	err   error
}

func normalizeMockStrategy(strategy MockStrategy) (MockStrategy, error) {
	if strategy == "" {
		return MockMarked, nil
	}
	switch strategy {
	case MockNone, MockAll, MockMarked:
		return strategy, nil
	default:
		return "", fmt.Errorf("bitfab: replay mock strategy must be %q, %q, or %q", MockNone, MockAll, MockMarked)
	}
}

func (c *Client) prepareReplayMockTree(
	ctx context.Context,
	originalSpanID string,
	strategy MockStrategy,
	overrides []MockOverride,
) (*mockTree, error) {
	needTree := strategy != MockNone || len(overrides) > 0
	if !needTree {
		return nil, nil
	}
	includeOutputs := strategy == MockAll
	query := url.Values{}
	query.Set("includeRootOutput", "false")
	if !includeOutputs {
		query.Set("includeOutputs", "false")
	}
	var response spanTreeResponse
	endpoint := "/api/sdk/replay/spanTree/" + url.PathEscape(originalSpanID) + "?" + query.Encode()
	if err := c.httpClient.get(ctx, endpoint, &response); err != nil {
		if strategy == MockMarked && len(overrides) == 0 {
			return &mockTree{spans: map[string]mockSpan{}}, nil
		}
		return nil, fmt.Errorf("bitfab: fetch replay span tree: %w", err)
	}
	if response.Root == nil {
		if strategy == MockMarked && len(overrides) == 0 {
			return &mockTree{spans: map[string]mockSpan{}}, nil
		}
		return nil, fmt.Errorf("bitfab: replay mock strategy %q requires a span tree root for original span %s", strategy, originalSpanID)
	}
	return buildReplayMockTree(*response.Root)
}

func buildReplayMockTree(root spanTreeNode) (*mockTree, error) {
	tree := &mockTree{spans: map[string]mockSpan{}}
	counters := map[string]int{}
	var walk func(spanTreeNode) error
	walk = func(node spanTreeNode) error {
		if node.TraceFunctionKey != "" {
			name := node.SpanName
			if name == "" {
				name = node.TraceFunctionKey
			}
			counterKey := node.TraceFunctionKey + ":" + name
			index := counters[counterKey]
			counters[counterKey] = index + 1
			span := mockSpan{
				sourceSpanID:   node.SourceSpanID,
				externalSpanID: node.ExternalSpanID,
			}
			if len(node.Output) > 0 || len(node.OutputMeta) > 0 {
				span.outputPresent = true
				if len(node.Output) > 0 {
					if err := json.Unmarshal(node.Output, &span.output); err != nil {
						return fmt.Errorf("decode recorded output for %s: %w", counterKey, err)
					}
				}
			}
			tree.spans[fmt.Sprintf("%s:%d", counterKey, index)] = span
		}
		for _, child := range node.Children {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	for _, child := range root.Children {
		if err := walk(child); err != nil {
			return nil, err
		}
	}
	return tree, nil
}

func (c *Client) resolveReplayMock(
	ctx context.Context,
	traceFunctionKey string,
	cfg spanConfig,
	isRootSpan bool,
) (any, bool, MockSource, error) {
	replay := currentReplayContext(ctx)
	if replay == nil || replay.mockTree == nil || isRootSpan {
		return nil, false, "", nil
	}

	spanName := cfg.name
	if spanName == "" {
		spanName = traceFunctionKey
	}
	counterKey := traceFunctionKey + ":" + spanName
	replay.mockMu.Lock()
	callIndex := replay.callCounters[counterKey]
	replay.callCounters[counterKey] = callIndex + 1
	span, recorded := replay.mockTree.spans[fmt.Sprintf("%s:%d", counterKey, callIndex)]
	replay.mockMu.Unlock()

	node := SpanNodeMeta{
		TraceFunctionKey: traceFunctionKey,
		SpanName:         spanName,
		Type:             cfg.spanType,
	}
	if recorded {
		node.OriginalSpanID = span.sourceSpanID
	}
	inputs := replayMockInputs(cfg.input)
	for _, override := range replay.mockOverrides {
		matched, err := callNodeMatcher(override.Match, node)
		if err != nil {
			return nil, true, MockSourceOverride, err
		}
		if !matched {
			continue
		}
		value, err := c.resolveMockOverrideValue(ctx, replay, span, recorded, override, node, inputs)
		if err == nil {
			value, err = decodeReplayMockOutput(value, cfg)
		}
		return value, true, MockSourceOverride, err
	}

	shouldMock := replay.mockStrategy == MockAll || (replay.mockStrategy == MockMarked && cfg.mockOnReplay)
	if !shouldMock {
		return nil, false, "", nil
	}
	if !recorded {
		return nil, true, MockSourceRecorded, fmt.Errorf(
			"bitfab: replay selected span %q for mocking, but recorded occurrence %d is unavailable; the real span was not executed",
			counterKey,
			callIndex+1,
		)
	}
	value, err := c.resolveRecordedReplayOutput(ctx, replay, span)
	if err == nil {
		value, err = decodeReplayMockOutput(value, cfg)
	}
	return value, true, MockSourceRecorded, err
}

func decodeReplayMockOutput(value any, cfg spanConfig) (any, error) {
	if cfg.mockOutputType == nil {
		return value, nil
	}
	decoded, err := decodeReplayValue(value, cfg.mockOutputType)
	if err != nil {
		return nil, fmt.Errorf("bitfab: decode mocked output into %s: %w", cfg.mockOutputType, err)
	}
	return decoded.Interface(), nil
}

func replayMockInputs(input any) []any {
	if input == nil {
		return []any{}
	}
	if values, ok := input.([]any); ok {
		return values
	}
	return []any{input}
}

func callNodeMatcher(matcher NodeMatcher, node SpanNodeMeta) (matched bool, err error) {
	if matcher == nil {
		return false, nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("bitfab: replay mock matcher panicked: %v", recovered)
		}
	}()
	return matcher(node), nil
}

func (c *Client) resolveMockOverrideValue(
	ctx context.Context,
	replay *replayContext,
	span mockSpan,
	recorded bool,
	override MockOverride,
	node SpanNodeMeta,
	inputs []any,
) (value any, err error) {
	if override.Resolve == nil {
		return override.Value, nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("bitfab: replay mock value panicked: %v", recovered)
		}
	}()
	return override.Resolve(MockOverrideContext{
		Node:   node,
		Inputs: inputs,
		GetOriginalOutput: func() (any, error) {
			if !recorded {
				return nil, fmt.Errorf("bitfab: no recorded span to source output for %q", node.TraceFunctionKey)
			}
			return c.resolveRecordedReplayOutput(ctx, replay, span)
		},
	})
}

func (c *Client) resolveRecordedReplayOutput(
	ctx context.Context,
	replay *replayContext,
	span mockSpan,
) (any, error) {
	if span.outputPresent {
		return span.output, nil
	}
	if strings.TrimSpace(span.externalSpanID) == "" {
		return nil, fmt.Errorf("bitfab: recorded span omitted both output and external span ID")
	}

	replay.mockMu.Lock()
	entry := replay.outputCache[span.externalSpanID]
	if entry == nil {
		entry = &mockOutputCacheEntry{done: make(chan struct{})}
		replay.outputCache[span.externalSpanID] = entry
		replay.mockMu.Unlock()
		fetched, err := c.getReplaySpan(ctx, span.externalSpanID)
		entry.value = fetched.RawData.SpanData.Output
		entry.err = err
		close(entry.done)
		return entry.value, entry.err
	}
	replay.mockMu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-entry.done:
		return entry.value, entry.err
	}
}
