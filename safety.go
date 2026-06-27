package bitfab

import (
	"encoding/json"
	"fmt"
	"log"
	mrand "math/rand"
	"sync"

	"github.com/google/uuid"
)

// MaxSerializedValueBytes caps the JSON size of a single captured span value
// (an input or an output). A large-but-valid value (a big in-memory buffer, a
// map with millions of entries) marshals fine but would spike host memory on
// the user's thread and get the span rejected server-side, silently dropping
// it. Past this ceiling the value is replaced with a stub so the span still
// ships with the rest of its data intact.
const MaxSerializedValueBytes = 512_000

// randomUUID returns a v4 UUID string without ever panicking.
//
// uuid.New() (used previously) is uuid.Must(NewRandom()): it PANICS if
// crypto/rand fails (a locked-down container, seccomp sandbox, or fd
// exhaustion). That panic happens on the user's call path, before the span's
// recover wrapper, so it would crash the host process. This helper uses the
// error-returning uuid.NewRandom() and, only if that fails, falls back to a
// math/rand-filled v4 layout. The fallback is not cryptographically secure,
// which is fine: trace/span ids are correlation-only, never security-sensitive.
func randomUUID() string {
	if id, err := uuid.NewRandom(); err == nil {
		return id.String()
	}
	warnOnce(
		"crypto-unavailable",
		"crypto/rand is unavailable; using a non-cryptographic fallback for trace/span ids. Tracing works normally (ids are correlation-only, not security-sensitive).",
	)
	return fallbackUUIDv4()
}

// fallbackUUIDv4 builds an RFC 4122 version 4 layout from math/rand. Used only
// when crypto/rand is unavailable.
func fallbackUUIDv4() string {
	var b [16]byte
	for i := range b {
		b[i] = byte(mrand.Intn(256))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// capValue bounds the JSON size of a single captured span value. See
// capValueReport; this drops the lossy report for call sites that don't mark.
func capValue(v any) any {
	out, _ := capValueReport(v)
	return out
}

// capValueReport bounds the JSON size of a single captured span value and
// reports whether the cap had to stub it. If v marshals to more than
// MaxSerializedValueBytes it is replaced with a stub string and "too_large" is
// returned in dropped, so the caller can mark the span non-replayable (the cap
// stubs a JSON-clean placeholder, which finalizeSpanPayload's sanitize pass
// cannot otherwise detect). A value that fits is returned unchanged with no
// drops; a value that cannot be marshalled here is left for the http boundary
// sanitizer (and finalizeSpanPayload) to stub and report.
func capValueReport(v any) (any, []string) {
	if v == nil {
		return nil, nil
	}
	body, err := json.Marshal(v)
	if err != nil {
		// Leave it for the boundary sanitizer (marshalPayloadSafe) to stub.
		return v, nil
	}
	if len(body) > MaxSerializedValueBytes {
		warnOnce(
			"value:too_large",
			fmt.Sprintf("a captured span value exceeded %d bytes and was replaced with a placeholder so the span still ships. The captured input/output for this span is incomplete.", MaxSerializedValueBytes),
		)
		return fmt.Sprintf("<unserializable: too_large_%d_bytes>", len(body)), []string{"too_large"}
	}
	return v, nil
}

// warnedKeys dedups warnOnce messages for the life of the process.
var warnedKeys sync.Map

// warnOnce logs a "[bitfab]" warning at most once per distinct key.
//
// The SDK must never crash or spam the host app, so every failure on the user's
// path degrades quietly (a span dropped, a value stubbed, an id generated
// without crypto). Silent is safe but undebuggable; logging on every call from
// a hot path is its own problem. A one-time warning per distinct issue restores
// the signal without the flood. Keys should identify the specific degradation
// so each distinct issue warns once, not just the first one seen.
func warnOnce(key, message string) {
	if _, loaded := warnedKeys.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	func() {
		defer func() { recover() }() // logging must never crash the host app
		log.Printf("bitfab: %s", message)
	}()
}

// resetWarnOnce clears the dedup set so a warning can fire again (test-only).
func resetWarnOnce() {
	warnedKeys.Range(func(k, _ any) bool {
		warnedKeys.Delete(k)
		return true
	})
}
