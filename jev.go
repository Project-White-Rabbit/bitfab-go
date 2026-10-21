package bitfab

import (
	"bytes"
	"context"
	"encoding/json"
	"sync/atomic"
	"time"
)

// JevProvider is the provider name recorded for Jev calls.
const JevProvider = "typesafe"

// WrapJevOption configures WrapJev.
type WrapJevOption[Reply any] func(*wrapJevConfig[Reply])

type jevClaimKey struct{}

type jevClaim struct {
	recorded atomic.Bool
}

type wrapJevConfig[Reply any] struct {
	onReply func(Reply)
}

// WithOnReply sets a function called with each reply after it is recorded.
// A panic inside it is recovered and does not reach the caller.
func WithOnReply[Reply any](fn func(Reply)) WrapJevOption[Reply] {
	return func(c *wrapJevConfig[Reply]) { c.onReply = fn }
}

// WrapJev wraps a function that calls Jev (TypeSafe's System One model) and
// returns a function with the same shape that records each call.
//
// After a successful call it adds one context entry to the span that is
// current in ctx when the call starts: provider "typesafe", the reply's model,
// its input and output token counts, and the call duration in milliseconds.
// It opens no span of its own, so wrap the call inside a span to see it.
//
// The reply is read as Jev's JSON reply, with a string "model" field and a
// "usage" object holding "input_tokens" and "output_tokens". Any reply that
// encodes to that JSON object works: a struct with json tags, a map,
// json.RawMessage, or a []byte or string holding the JSON. A model that is
// not a string is left out, as is a token count that is null or missing.
// A reply that is not a JSON object records nothing.
//
// When the call returns an error, the error and reply are returned unchanged
// and nothing is recorded. Recording never panics into the caller.
//
// Code built with bitfab-instrument records each SystemOne method call beneath
// a Trace root as its own llm span. When the wrapped function makes such a
// call and that span records the reply, WrapJev records nothing, so each
// reply is counted once.
//
//	ask := bitfab.WrapJev(func(ctx context.Context, req JevRequest) (JevReply, error) {
//		return callJev(ctx, req)
//	})
//	reply, err := ask(ctx, req)
func WrapJev[Req, Reply any](call func(context.Context, Req) (Reply, error), opts ...WrapJevOption[Reply]) func(context.Context, Req) (Reply, error) {
	config := &wrapJevConfig[Reply]{}
	for _, opt := range opts {
		if opt != nil {
			opt(config)
		}
	}
	return func(ctx context.Context, req Req) (Reply, error) {
		span := GetCurrentSpan(ctx)
		claim := &jevClaim{}
		startedAt := time.Now()
		reply, err := call(context.WithValue(ctx, jevClaimKey{}, claim), req)
		if err != nil {
			return reply, err
		}
		if !claim.recorded.Load() {
			recordJevReply(span, reply, time.Since(startedAt).Milliseconds())
		}
		if config.onReply != nil {
			func() {
				defer func() { recover() }()
				config.onReply(reply)
			}()
		}
		return reply, nil
	}
}

func recordJevReply(span *CurrentSpan, reply any, durationMs int64) {
	defer func() { recover() }()
	if entry := jevContext(reply, durationMs); entry != nil {
		span.AddContext(entry)
	}
}

func jevContext(reply any, durationMs int64) map[string]any {
	fields := jevReplyFields(reply)
	if fields == nil {
		return nil
	}
	return jevContextFromFields(fields, durationMs)
}

func looksLikeJevReply(fields map[string]any) bool {
	if _, ok := fields["model"].(string); !ok {
		return false
	}
	_, ok := fields["usage"].(map[string]any)
	return ok
}

func jevContextFromFields(fields map[string]any, durationMs int64) map[string]any {
	entry := map[string]any{"provider": JevProvider}
	if model, ok := fields["model"].(string); ok {
		entry["model"] = model
	}
	usage, _ := fields["usage"].(map[string]any)
	if count, ok := jevTokenCount(usage["input_tokens"]); ok {
		entry["inputTokens"] = count
	}
	if count, ok := jevTokenCount(usage["output_tokens"]); ok {
		entry["outputTokens"] = count
	}
	entry["durationMs"] = durationMs
	return entry
}

func jevReplyFields(reply any) map[string]any {
	var encoded []byte
	switch value := reply.(type) {
	case []byte:
		encoded = value
	case string:
		encoded = []byte(value)
	default:
		marshaled, err := json.Marshal(reply)
		if err != nil {
			return nil
		}
		encoded = marshaled
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var fields map[string]any
	if decoder.Decode(&fields) != nil {
		return nil
	}
	return fields
}

func jevTokenCount(value any) (any, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return nil, false
	}
	if count, err := number.Int64(); err == nil {
		return count, true
	}
	if count, err := number.Float64(); err == nil {
		return count, true
	}
	return nil, false
}
