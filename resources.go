package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

func (h *httpClient) requestInto(ctx context.Context, endpoint string, payload map[string]any, target any) error {
	return h.requestIntoWithTimeout(ctx, endpoint, payload, 0, target)
}

func (h *httpClient) requestIntoWithTimeout(ctx context.Context, endpoint string, payload map[string]any, timeout time.Duration, target any) error {
	response, err := h.request(ctx, endpoint, payload, timeout)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("bitfab: decode SDK response: %w", err)
	}
	if err := json.Unmarshal(encoded, target); err != nil {
		return fmt.Errorf("bitfab: decode SDK response: %w", err)
	}
	return nil
}

func (h *httpClient) requestMethodInto(ctx context.Context, method, endpoint string, payload map[string]any, target any) error {
	body, dropped := marshalPayloadSafe(payload)
	warnForStubbedBody(dropped)
	response, err := h.sendPreparedMethod(ctx, method, endpoint, prepareRequestBody(body), 0)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("bitfab: decode SDK response: %w", err)
	}
	if err := json.Unmarshal(encoded, target); err != nil {
		return fmt.Errorf("bitfab: decode SDK response: %w", err)
	}
	return nil
}
