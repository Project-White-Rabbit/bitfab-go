package bitfab

import "context"

type seedContextKey struct{}
type seedContext struct{ traceID string }

func seedFromContext(ctx context.Context) *seedContext {
	seed, _ := ctx.Value(seedContextKey{}).(*seedContext)
	return seed
}
