package bitfab

import (
	"context"
	"sync"
)

type spanEnrichmentKey struct{}
type spanEnrichment struct {
	mu       sync.Mutex
	id       string
	prompt   string
	contexts []ContextEntry
	metadata map[string]any
}

func withSpanEnrichment(ctx context.Context, spanID string) (context.Context, *spanEnrichment) {
	state := &spanEnrichment{id: spanID}
	return context.WithValue(ctx, spanEnrichmentKey{}, state), state
}

func (s *spanEnrichment) apply(data map[string]any, contentOff bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prompt != "" && !contentOff {
		data["prompt"] = s.prompt
	}
	if len(s.contexts) > 0 {
		existing, _ := data["contexts"].([]ContextEntry)
		data["contexts"] = append(append([]ContextEntry{}, existing...), s.contexts...)
	}
	if len(s.metadata) > 0 {
		merged := make(map[string]any, len(s.metadata))
		for k, v := range s.metadata {
			merged[k] = v
		}
		data["metadata"] = merged
	}
}

// SetPrompt records the rendered prompt on the currently active span.
func (cs *CurrentSpan) SetPrompt(prompt string) {
	if cs == nil || cs.enrichment == nil {
		return
	}
	cs.enrichment.mu.Lock()
	defer cs.enrichment.mu.Unlock()
	cs.enrichment.prompt = prompt
}

// AddContext appends structured execution metadata to the active span.
func (cs *CurrentSpan) AddContext(value map[string]any) {
	if cs == nil || cs.enrichment == nil || value == nil {
		return
	}
	copied := make(map[string]any, len(value))
	for k, v := range value {
		copied[k] = v
	}
	cs.enrichment.mu.Lock()
	defer cs.enrichment.mu.Unlock()
	cs.enrichment.contexts = append(cs.enrichment.contexts, copied)
}

// SetMetadata merges metadata into the active span, with later values for a key winning. Use it to label what the span did, for example {"delivery": "blocked"} on a message that was drafted but never sent. Values should be strings, numbers, or booleans; keys are 1 to 40 of A-Z a-z 0-9 _ . : -. Bitfab stores keys and values lowercased, returns them on Spans.List, and narrows that list with its Metadata filter. Sent even when content capture is off, like trace metadata. Safe to call outside a span or on a nil receiver (no-op).
func (cs *CurrentSpan) SetMetadata(metadata map[string]any) {
	defer func() { recover() }()
	if cs == nil || cs.enrichment == nil || len(metadata) == 0 {
		return
	}
	cs.enrichment.mu.Lock()
	defer cs.enrichment.mu.Unlock()
	if cs.enrichment.metadata == nil {
		cs.enrichment.metadata = make(map[string]any, len(metadata))
	}
	for k, v := range metadata {
		cs.enrichment.metadata[k] = v
	}
}
