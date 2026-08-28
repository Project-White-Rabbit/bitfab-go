package bitfab

import (
	"context"
	"sync"
	"sync/atomic"
)

type replayContextKey struct{}

type replayContext struct {
	testRunID          string
	traceID            string
	inputSourceSpanID  string
	inputSourceTraceID string
	sourceTraceID      string
	mockStrategy       MockStrategy
	mockTree           *mockTree
	mockOverrides      []MockOverride
	dbBranchLease      *dbBranchLeaseWire
	dbBranchTimings    *DBBranchTimings
	dbSnapshotAccessed atomic.Bool
	mockMu             sync.Mutex
	callCounters       map[string]int
	outputCache        map[string]*mockOutputCacheEntry
}

func withReplayContext(ctx context.Context, replay *replayContext) context.Context {
	return context.WithValue(ctx, replayContextKey{}, replay)
}

func currentReplayContext(ctx context.Context) *replayContext {
	replay, _ := ctx.Value(replayContextKey{}).(*replayContext)
	return replay
}
