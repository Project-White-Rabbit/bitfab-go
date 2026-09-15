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
}

func withSpanEnrichment(ctx context.Context, spanID string) (context.Context, *spanEnrichment) {
	state := &spanEnrichment{id: spanID}
	return context.WithValue(ctx, spanEnrichmentKey{}, state), state
}

func (s *spanEnrichment) apply(data map[string]any) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prompt != "" {
		data["prompt"] = s.prompt
	}
	if len(s.contexts) > 0 {
		existing, _ := data["contexts"].([]ContextEntry)
		data["contexts"] = append(append([]ContextEntry{}, existing...), s.contexts...)
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
