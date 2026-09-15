package bitfab

import "fmt"

// DBSnapshotConfig optionally fixes the provider used to resolve historical database branches.
// Without a provider configuration, the server resolves the provider at replay time.
type DBSnapshotConfig struct {
	Provider string
}

// WithDBSnapshot validates database snapshot configuration and returns a client option.
// Neon is currently the only supported provider. Configuration performs no database I/O.
func WithDBSnapshot(config DBSnapshotConfig) (Option, error) {
	if config.Provider != "neon" {
		return nil, fmt.Errorf("bitfab: db snapshot provider %q is not supported; supported providers: neon", config.Provider)
	}
	return func(client *Client) { copy := config; client.dbSnapshot = &copy }, nil
}

func (c *Client) buildDBSnapshotRef(startedAt string) *DBSnapshotRef {
	ref := &DBSnapshotRef{SDKWallClockBeforeFn: startedAt}
	if c.dbSnapshot != nil {
		ref.Provider = c.dbSnapshot.Provider
	}
	return ref
}
