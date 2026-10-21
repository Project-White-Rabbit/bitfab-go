package bitfab

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

type jevUsage struct {
	InputTokens  *int `json:"input_tokens"`
	OutputTokens *int `json:"output_tokens"`
}

type jevReply struct {
	Model   string   `json:"model"`
	Usage   jevUsage `json:"usage"`
	Answers []string `json:"answers"`
}

func jevTokens(n int) *int { return &n }

func runJevUnderSpan[Reply any](t *testing.T, call func(context.Context, string) (Reply, error)) ([]map[string]any, Reply, error) {
	t.Helper()
	c, requests := subtreeClient(t)
	var reply Reply
	var callErr error
	_, _ = c.Span(context.Background(), "jev-root", func(ctx context.Context) (any, error) {
		reply, callErr = call(ctx, "question")
		return nil, nil
	}, WithName("ask"))
	if !c.FlushTraces(time.Second) {
		t.Fatal("flush failed")
	}
	var spans []map[string]any
	for _, group := range sentSpansByName(requests()) {
		spans = append(spans, group...)
	}
	return spans, reply, callErr
}

func onlySpanContexts(t *testing.T, spans []map[string]any) []any {
	t.Helper()
	if len(spans) != 1 || spans[0]["name"] != "ask" {
		t.Fatalf("expected only the ask span, got %#v", spans)
	}
	contexts, _ := spans[0]["contexts"].([]any)
	return contexts
}

func TestWrapJevRecordsStructReplyOnCurrentSpan(t *testing.T) {
	ask := WrapJev(func(_ context.Context, question string) (jevReply, error) {
		return jevReply{Model: "jev-1", Usage: jevUsage{InputTokens: jevTokens(12), OutputTokens: jevTokens(5)}, Answers: []string{question}}, nil
	})
	spans, reply, err := runJevUnderSpan(t, ask)
	if err != nil || reply.Answers[0] != "question" {
		t.Fatalf("reply changed: %#v %v", reply, err)
	}
	contexts := onlySpanContexts(t, spans)
	if len(contexts) != 1 {
		t.Fatalf("expected one context entry, got %#v", contexts)
	}
	entry := contexts[0].(map[string]any)
	if _, ok := entry["durationMs"].(float64); !ok {
		t.Fatalf("missing durationMs: %#v", entry)
	}
	delete(entry, "durationMs")
	want := map[string]any{"model": "jev-1", "provider": "typesafe", "inputTokens": float64(12), "outputTokens": float64(5)}
	if !reflect.DeepEqual(entry, want) {
		t.Fatalf("wrong context: %#v", entry)
	}
}

func TestWrapJevOmitsNullTokenCounts(t *testing.T) {
	ask := WrapJev(func(context.Context, string) (jevReply, error) {
		return jevReply{Model: "jev-1", Usage: jevUsage{OutputTokens: jevTokens(7)}}, nil
	})
	spans, _, _ := runJevUnderSpan(t, ask)
	entry := onlySpanContexts(t, spans)[0].(map[string]any)
	if _, ok := entry["inputTokens"]; ok {
		t.Fatalf("null input tokens recorded: %#v", entry)
	}
	if entry["outputTokens"] != float64(7) {
		t.Fatalf("output tokens lost: %#v", entry)
	}
}

func TestWrapJevContextFromReplyShapes(t *testing.T) {
	cases := []struct {
		label string
		reply any
		want  map[string]any
	}{
		{"map", map[string]any{"model": "jev-1", "usage": map[string]any{"input_tokens": 3, "output_tokens": nil}}, map[string]any{"model": "jev-1", "provider": "typesafe", "inputTokens": int64(3), "durationMs": int64(9)}},
		{"raw json", json.RawMessage(`{"model":"jev-2","usage":{"input_tokens":1,"output_tokens":2}}`), map[string]any{"model": "jev-2", "provider": "typesafe", "inputTokens": int64(1), "outputTokens": int64(2), "durationMs": int64(9)}},
		{"bytes", []byte(`{"model":"jev-3","usage":null}`), map[string]any{"model": "jev-3", "provider": "typesafe", "durationMs": int64(9)}},
		{"string", `{"usage":{"input_tokens":4}}`, map[string]any{"provider": "typesafe", "inputTokens": int64(4), "durationMs": int64(9)}},
		{"model not a string", map[string]any{"model": 5}, map[string]any{"provider": "typesafe", "durationMs": int64(9)}},
		{"struct pointer", &jevReply{Model: "jev-4"}, map[string]any{"model": "jev-4", "provider": "typesafe", "durationMs": int64(9)}},
		{"not an object", []string{"a"}, nil},
		{"nil pointer", (*jevReply)(nil), nil},
		{"unencodable", map[string]any{"bad": make(chan int)}, nil},
	}
	for _, tc := range cases {
		got := jevContext(tc.reply, 9)
		if tc.want == nil && got != nil || tc.want != nil && !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %#v, want %#v", tc.label, got, tc.want)
		}
	}
}

func TestWrapJevRecordsMapReplyOnCurrentSpan(t *testing.T) {
	ask := WrapJev(func(context.Context, string) (map[string]any, error) {
		return map[string]any{"model": "jev-1", "usage": map[string]any{"input_tokens": 2, "output_tokens": 3}}, nil
	})
	spans, _, _ := runJevUnderSpan(t, ask)
	entry := onlySpanContexts(t, spans)[0].(map[string]any)
	if entry["model"] != "jev-1" || entry["inputTokens"] != float64(2) || entry["outputTokens"] != float64(3) || entry["provider"] != "typesafe" {
		t.Fatalf("wrong context: %#v", entry)
	}
}

func TestWrapJevCallsOnReplyAfterRecording(t *testing.T) {
	ctx, state := withSpanEnrichment(withSpanContext(context.Background(), "trace", "span"), "span")
	var seen jevReply
	var contextsAtReply int
	ask := WrapJev(func(context.Context, string) (jevReply, error) {
		return jevReply{Model: "jev-1"}, nil
	}, WithOnReply(func(reply jevReply) {
		seen = reply
		data := map[string]any{}
		state.apply(data, false)
		contextsAtReply = len(data["contexts"].([]ContextEntry))
	}))
	reply, err := ask(ctx, "question")
	if err != nil || reply.Model != "jev-1" || seen.Model != "jev-1" || contextsAtReply != 1 {
		t.Fatalf("onReply not called after recording: reply=%#v seen=%#v contexts=%d err=%v", reply, seen, contextsAtReply, err)
	}
}

func TestWrapJevRecoversOnReplyPanic(t *testing.T) {
	ask := WrapJev(func(context.Context, string) (jevReply, error) {
		return jevReply{Model: "jev-1"}, nil
	}, WithOnReply(func(jevReply) { panic("boom") }))
	reply, err := ask(context.Background(), "question")
	if err != nil || reply.Model != "jev-1" {
		t.Fatalf("panic leaked or reply changed: %#v %v", reply, err)
	}
}

func TestWrapJevPassesErrorThroughAndRecordsNothing(t *testing.T) {
	failure := errors.New("jev down")
	called := false
	ask := WrapJev(func(context.Context, string) (jevReply, error) {
		return jevReply{Model: "partial"}, failure
	}, WithOnReply(func(jevReply) { called = true }))
	spans, reply, err := runJevUnderSpan(t, ask)
	if err != failure || reply.Model != "partial" || called {
		t.Fatalf("error not passed through unchanged: %#v %v called=%v", reply, err, called)
	}
	if contexts := onlySpanContexts(t, spans); len(contexts) != 0 {
		t.Fatalf("recorded on error: %#v", contexts)
	}
}

func TestWrapJevOutsideSpanReturnsReply(t *testing.T) {
	ask := WrapJev(func(context.Context, string) (jevReply, error) {
		return jevReply{Model: "jev-1"}, nil
	})
	reply, err := ask(context.Background(), "question")
	if err != nil || reply.Model != "jev-1" {
		t.Fatalf("reply changed outside a span: %#v %v", reply, err)
	}
}
