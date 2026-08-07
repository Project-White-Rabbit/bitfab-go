package bitfab

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// MaxSpanCarrierBytes is the ceiling on a span's encoded payload.
//
// A span's whole payload (input, output, contexts, prompt, metadata) ships as a
// single bitfab.payload string attribute, and the exporter drops any carrier
// that exceeds the per-request byte ceiling outright rather than trimming it.
// Capping each value on its own cannot prevent that: two values that each fit
// can still add up to an undeliverable span. So the budget is enforced on the
// encoded payload as a whole, and an oversized span ships with its largest
// fields stubbed instead of vanishing.
//
// The budget is measured on the *carrier* (the payload re-escaped into the OTLP
// attribute), not on the payload body, because the carrier is what the exporter
// weighs. Bounding the body instead leaves escape-heavy content to blow the
// request ceiling anyway: a body of escaped JSON, Windows paths, or regexes is
// nearly all backslashes, and every one of them doubles. Measured on a body
// sized exactly to a 2.4 MB cap, prose produced a 2.4 MB carrier but
// backslash-dense content produced 4.8 MB, which the exporter dropped.
//
// 2.8 MB leaves room beneath the 3 MB request ceiling for the span and request
// envelopes wrapped around the attribute.
const MaxSpanCarrierBytes = 2_800_000

// carrierByteLength is the byte length body occupies once re-escaped as a JSON
// string value.
//
// body is itself JSON text, so the first encode already replaced every control
// character with a \uXXXX sequence. Only " and \ are left to escape, and each
// costs exactly one more byte, which bounds the expansion at 2x and is what
// lets fitsCarrierBudget skip this scan for all but the largest payloads.
//
// Go's encoding/json also escapes <, >, and & as \u003c, \u003e, and \u0026,
// so those are already six ASCII bytes in body and cost nothing extra here.
func carrierByteLength(body []byte) int {
	return len(body) + bytes.Count(body, []byte(`"`)) + bytes.Count(body, []byte(`\`)) + 2
}

// fitsCarrierBudget reports whether body fits the carrier budget, measuring
// exactly only when the cheap bounds cannot already decide it. Escaping never
// shrinks the body and can at most double it, so anything under half the budget
// always fits and anything past the budget never does. Ordinary spans settle on
// the first comparison and never pay for the scan.
func fitsCarrierBudget(body []byte) bool {
	size := len(body)
	if size*2+2 <= MaxSpanCarrierBytes {
		return true
	}
	if size+2 > MaxSpanCarrierBytes {
		return false
	}
	return carrierByteLength(body) <= MaxSpanCarrierBytes
}

// structuralSpanKeys are the span fields that identify the span rather than
// carry user data. Trimming one would leave a span that no longer says what it
// is, so they stay whatever the payload costs.
var structuralSpanKeys = map[string]struct{}{
	"name":          {},
	"type":          {},
	"function_name": {},
	"error_source":  {},
}

// payloadBudgetStep is the error step recorded on a payload the budget trimmed.
const payloadBudgetStep = "payload_budget"

type trimCandidate struct {
	container map[string]any
	key       string
	size      int
}

// enforcePayloadBudget returns a payload and body within the budget, plus the
// fields that had to be stubbed.
//
// The payload comes back alongside the body because the transport reads it for
// span naming and timestamps: handing back the untrimmed one would describe a
// span by content the body no longer carries.
func enforcePayloadBudget(
	payload map[string]any,
	body []byte,
	encode func(map[string]any) ([]byte, error),
) (map[string]any, []byte, []string) {
	if fitsCarrierBudget(body) {
		return payload, body, nil
	}
	trimmedPayload, trimmed := trimPayloadToBudget(payload, encode)
	if trimmedPayload == nil {
		return payload, body, nil
	}
	markPayloadTrimmed(trimmedPayload, trimmed)
	trimmedBody, err := encode(trimmedPayload)
	if err != nil {
		return payload, body, nil
	}
	warnOnce(
		"payload:over-budget",
		fmt.Sprintf(
			"a span payload exceeded %d bytes; its largest field(s) (%s) were replaced with placeholders so the span still ships. The span is incomplete and may not be replayable.",
			MaxSpanCarrierBytes,
			strings.Join(uniqueStrings(trimmed), ", "),
		),
	)
	return trimmedPayload, trimmedBody, trimmed
}

// trimPayloadToBudget stubs the largest payload fields until the encoded body
// fits the budget. It returns nil when nothing could be trimmed: the caller then
// ships the oversized body and lets the exporter report the drop, which still
// beats silently emptying a span.
func trimPayloadToBudget(
	payload map[string]any,
	encode func(map[string]any) ([]byte, error),
) (map[string]any, []string) {
	copied, containers := cloneTrimmable(payload)
	candidates := collectTrimCandidates(containers)
	if len(candidates) == 0 {
		return nil, nil
	}

	var trimmed []string
	for _, candidate := range candidates {
		candidate.container[candidate.key] = fmt.Sprintf(
			"<unserializable: too_large_%d_bytes>", candidate.size,
		)
		trimmed = append(trimmed, candidate.key)
		body, err := encode(copied)
		if err != nil {
			return nil, nil
		}
		if fitsCarrierBudget(body) {
			return copied, trimmed
		}
	}
	return nil, nil
}

// cloneTrimmable copies the records holding user data so trimming never mutates
// the caller's maps.
func cloneTrimmable(payload map[string]any) (map[string]any, []map[string]any) {
	copied := make(map[string]any, len(payload))
	for k, v := range payload {
		copied[k] = v
	}
	var containers []map[string]any

	if spanData, ok := copied["span_data"].(map[string]any); ok {
		clone := cloneMap(spanData)
		copied["span_data"] = clone
		containers = append(containers, clone)
	}

	if rawSpan, ok := copied["rawSpan"].(map[string]any); ok {
		if spanData, ok := rawSpan["span_data"].(map[string]any); ok {
			clone := cloneMap(spanData)
			rawSpanCopy := cloneMap(rawSpan)
			rawSpanCopy["span_data"] = clone
			copied["rawSpan"] = rawSpanCopy
			containers = append(containers, clone)
		}
	}

	// No span_data anywhere: a trace-level or otherwise unfamiliar payload. Trim
	// its own fields rather than give up, so an oversized body still ships.
	if len(containers) == 0 {
		containers = append(containers, copied)
	}

	return copied, containers
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func collectTrimCandidates(containers []map[string]any) []trimCandidate {
	var candidates []trimCandidate
	for _, container := range containers {
		for key, value := range container {
			if _, structural := structuralSpanKeys[key]; structural || value == nil {
				continue
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				continue
			}
			candidates = append(candidates, trimCandidate{container, key, len(encoded)})
		}
	}
	// Largest first, with the key as a tiebreak so a payload with equally sized
	// fields trims the same way every run.
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].size != candidates[j].size {
			return candidates[i].size > candidates[j].size
		}
		return candidates[i].key < candidates[j].key
	})
	return candidates
}

// markPayloadTrimmed records the trim in the payload's own errors, which is what
// the server reads to flag a trace as incomplete.
func markPayloadTrimmed(payload map[string]any, trimmed []string) {
	entry := map[string]any{
		"source": "sdk",
		"step":   payloadBudgetStep,
		"error": fmt.Sprintf(
			"trimmed oversized field(s) to fit the %d-byte span carrier budget: %s",
			MaxSpanCarrierBytes,
			strings.Join(uniqueStrings(trimmed), ", "),
		),
	}
	existing, _ := payload["errors"].([]any)
	payload["errors"] = append(existing, entry)
}
