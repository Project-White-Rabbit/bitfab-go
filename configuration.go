package bitfab

import (
	"context"
	"log"
	"os"
	"strings"
)

func (c *Client) resolveAPIKey() string {
	c.apiKeyMu.Lock()
	defer c.apiKeyMu.Unlock()
	if c.resolvedAPIKey != "" {
		return c.resolvedAPIKey
	}
	if key := c.candidateAPIKey(); key != "" {
		c.resolvedAPIKey = key
		return key
	}
	if c.strict {
		panic("bitfab: no API key resolved. Set BITFAB_API_KEY or use WithAPIKeyFunc before first use. https://bitfab.ai/settings/api-keys")
	}
	if c.enabled && !c.captureDisabled && !c.apiKeyWarned {
		c.apiKeyWarned = true
		log.Println("Bitfab: API key is empty; tracing is disabled until credentials are available. https://bitfab.ai/settings/api-keys")
	}
	return ""
}

func (c *Client) probeAPIKey() string {
	if override := replayAPIKeyOverride(); override != "" {
		return override
	}
	c.apiKeyMu.Lock()
	defer c.apiKeyMu.Unlock()
	if c.resolvedAPIKey != "" {
		return c.resolvedAPIKey
	}
	return c.candidateAPIKey()
}

func (c *Client) candidateAPIKey() string {
	if override := replayAPIKeyOverride(); override != "" {
		return override
	}
	key := c.apiKey
	if c.apiKeyFunc != nil {
		key = c.apiKeyFunc()
	}
	if strings.TrimSpace(key) == "" {
		key = os.Getenv("BITFAB_API_KEY")
	}
	if strings.TrimSpace(key) == "" {
		return ""
	}
	return key
}

func replayAPIKeyOverride() string {
	override := os.Getenv(replayAPIKeyEnv)
	if strings.TrimSpace(override) == "" {
		return ""
	}
	return override
}

// CaptureEnabled reports whether ordinary calls can currently record traces.
func (c *Client) CaptureEnabled() bool {
	return c.shouldRecord(context.Background())
}

func (c *Client) shouldRecord(ctx context.Context) bool {
	c.httpClient.transportMu.Lock()
	closed := c.httpClient.closed
	c.httpClient.transportMu.Unlock()
	if closed {
		return false
	}
	if (!c.enabled || c.captureDisabled) && currentReplayContext(ctx) == nil && seedFromContext(ctx) == nil {
		return false
	}
	return c.resolveAPIKey() != ""
}

func (h *httpClient) resolveAPIKey() string {
	if h.apiKeyFunc != nil {
		return h.apiKeyFunc()
	}
	return h.apiKey
}
