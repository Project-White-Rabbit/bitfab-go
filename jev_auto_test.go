package bitfab

import (
	"context"
	"errors"
	"testing"
)

type autoJevClient struct {
	reply  any
	err    error
	spanID string
}

func (c *autoJevClient) SystemOne(ctx context.Context, prompt string) (any, error) {
	c.spanID = GetCurrentSpan(ctx).ID()
	return c.reply, c.err
}

type outerJevClient struct {
	inner *autoJevClient
}

func (c *outerJevClient) SystemOne(ctx context.Context, prompt string) (any, error) {
	reply, err := generatedJevCall(c.inner.SystemOne, ctx, prompt)
	if err != nil {
		return nil, err
	}
	rebuilt := map[string]any{}
	for key, value := range reply.(map[string]any) {
		rebuilt[key] = value
	}
	return rebuilt, nil
}

func generatedJevCall[A0, A1, R0, R1 any](call func(A0, A1) (R0, R1), a0 A0, a1 A1) (r0 R0, r1 R1) {
	return generatedNamedJevCall("(*autoJevClient).SystemOne", call, a0, a1)
}

func generatedNamedJevCall[A0, A1, R0, R1 any](name string, call func(A0, A1) (R0, R1), a0 A0, a1 A1) (r0 R0, r1 R1) {
	if AutoCaptureActive() {
		if node := EnterAutoLLMCall(call, any(a0), name, []any{a1}, []any{&r0, &r1}); node != nil {
			defer func() {
				p := recover()
				node.End(p)
				if p != nil {
					panic(p)
				}
			}()
			if ctx, ok := any(node.Context()).(A0); ok {
				a0 = ctx
			}
			if node.Mocked() {
				node.AssignMock()
				return
			}
		}
	}
	return call(a0, a1)
}

var autoJevReply = map[string]any{"model": "jev-1", "usage": map[string]any{"input_tokens": 8, "output_tokens": 2}}

func autoJevSpans(t *testing.T, c *Client, requests func() []map[string]any, opts TraceOptions, fn func(context.Context) error) map[string]map[string]any {
	t.Helper()
	opts.Name = "root"
	if _, err := c.Trace(context.Background(), "jev-auto", func(ctx context.Context) (any, error) { return nil, fn(ctx) }, opts); err != nil {
		t.Fatal(err)
	}
	byName := map[string]map[string]any{}
	for _, raw := range flushedRawSpans(t, c, requests) {
		data := raw["span_data"].(map[string]any)
		byName[data["name"].(string)] = data
	}
	return byName
}

func spanContexts(span map[string]any) []any {
	values, _ := span["contexts"].([]any)
	return values
}

func TestAutoJevCallRecordsLlmSpanWithContext(t *testing.T) {
	c, requests := subtreeClient(t)
	jev := &autoJevClient{reply: autoJevReply}
	spans := autoJevSpans(t, c, requests, TraceOptions{}, func(ctx context.Context) error {
		_, err := generatedJevCall(jev.SystemOne, ctx, "hi")
		return err
	})
	llm := spans["(*autoJevClient).SystemOne"]
	if len(spans) != 2 || llm == nil || llm["type"] != "llm" {
		t.Fatalf("spans = %#v", spans)
	}
	contexts := spanContexts(llm)
	if len(contexts) != 1 {
		t.Fatalf("contexts = %#v", contexts)
	}
	entry := contexts[0].(map[string]any)
	if entry["provider"] != JevProvider || entry["model"] != "jev-1" || entry["inputTokens"] != float64(8) || entry["outputTokens"] != float64(2) {
		t.Fatalf("entry = %#v", entry)
	}
	if len(spanContexts(spans["root"])) != 0 {
		t.Fatalf("root got a context: %#v", spans["root"])
	}
	if jev.spanID == "" {
		t.Fatal("SystemOne ran without the llm span in its context")
	}
}

func TestAutoJevCallWithReplyNotShapedLikeJevRecordsNoContext(t *testing.T) {
	c, requests := subtreeClient(t)
	jev := &autoJevClient{reply: map[string]any{"model": "other", "answer": "no usage"}}
	spans := autoJevSpans(t, c, requests, TraceOptions{}, func(ctx context.Context) error {
		_, err := generatedJevCall(jev.SystemOne, ctx, "hi")
		return err
	})
	llm := spans["(*autoJevClient).SystemOne"]
	if llm == nil || llm["type"] != "llm" || len(spanContexts(llm)) != 0 {
		t.Fatalf("spans = %#v", spans)
	}
}

func TestAutoJevCallPassesErrorThroughAndRecordsNoContext(t *testing.T) {
	c, requests := subtreeClient(t)
	failure := errors.New("jev down")
	jev := &autoJevClient{reply: autoJevReply, err: failure}
	var got error
	spans := autoJevSpans(t, c, requests, TraceOptions{}, func(ctx context.Context) error {
		_, got = generatedJevCall(jev.SystemOne, ctx, "hi")
		return nil
	})
	if got != failure {
		t.Fatalf("error changed: %v", got)
	}
	llm := spans["(*autoJevClient).SystemOne"]
	if llm == nil || llm["error"] != "jev down" || len(spanContexts(llm)) != 0 {
		t.Fatalf("spans = %#v", spans)
	}
}

func TestAutoJevCallInsideWrapJevRecordsOnlyOnTheLlmSpan(t *testing.T) {
	c, requests := subtreeClient(t)
	jev := &autoJevClient{reply: autoJevReply}
	ask := WrapJev(func(ctx context.Context, prompt string) (any, error) {
		return generatedJevCall(jev.SystemOne, ctx, prompt)
	})
	spans := autoJevSpans(t, c, requests, TraceOptions{}, func(ctx context.Context) error {
		_, err := ask(ctx, "hi")
		return err
	})
	if len(spanContexts(spans["(*autoJevClient).SystemOne"])) != 1 || len(spanContexts(spans["root"])) != 0 {
		t.Fatalf("spans = %#v", spans)
	}
}

func TestAutoJevCallExcludedLetsWrapJevRecordOnTheEnclosingSpan(t *testing.T) {
	c, requests := subtreeClient(t)
	jev := &autoJevClient{reply: autoJevReply}
	ask := WrapJev(func(ctx context.Context, prompt string) (any, error) {
		return generatedJevCall(jev.SystemOne, ctx, prompt)
	})
	spans := autoJevSpans(t, c, requests, TraceOptions{Exclude: []string{"SystemOne"}}, func(ctx context.Context) error {
		_, err := ask(ctx, "hi")
		return err
	})
	if len(spans) != 1 || len(spanContexts(spans["root"])) != 1 {
		t.Fatalf("spans = %#v", spans)
	}
}

func TestAutoJevCallNodeOptionsRenameAndKeepLlmType(t *testing.T) {
	c, requests := subtreeClient(t)
	if err := c.Node((*autoJevClient).SystemOne, NodeOptions{Name: "ask-jev"}); err != nil {
		t.Fatal(err)
	}
	jev := &autoJevClient{reply: autoJevReply}
	spans := autoJevSpans(t, c, requests, TraceOptions{}, func(ctx context.Context) error {
		_, err := generatedJevCall(jev.SystemOne, ctx, "hi")
		return err
	})
	llm := spans["ask-jev"]
	if len(spans) != 2 || llm == nil || llm["type"] != "llm" || len(spanContexts(llm)) != 1 {
		t.Fatalf("spans = %#v", spans)
	}
}

func TestAutoJevCallOutsideTraceRecordsNothing(t *testing.T) {
	jev := &autoJevClient{reply: autoJevReply}
	reply, err := generatedJevCall(jev.SystemOne, context.Background(), "hi")
	if err != nil || reply == nil {
		t.Fatalf("reply=%v err=%v", reply, err)
	}
}

func TestNestedAutoJevCallsRecordUsageOnceOnTheInnerCall(t *testing.T) {
	c, requests := subtreeClient(t)
	outer := &outerJevClient{inner: &autoJevClient{reply: autoJevReply}}
	var asked bool
	ask := WrapJev(func(ctx context.Context, prompt string) (any, error) {
		asked = true
		return generatedNamedJevCall("(*outerJevClient).SystemOne", outer.SystemOne, ctx, prompt)
	})
	spans := autoJevSpans(t, c, requests, TraceOptions{}, func(ctx context.Context) error {
		_, err := ask(ctx, "hi")
		return err
	})
	if !asked {
		t.Fatal("wrapped call did not run")
	}
	if len(spanContexts(spans["(*autoJevClient).SystemOne"])) != 1 {
		t.Fatalf("inner contexts = %#v", spans)
	}
	if len(spanContexts(spans["(*outerJevClient).SystemOne"])) != 0 || len(spanContexts(spans["root"])) != 0 {
		t.Fatalf("usage recorded more than once: %#v", spans)
	}
}
