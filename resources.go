package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
)

func (h *httpClient) requestInto(ctx context.Context, endpoint string, payload map[string]any, target any) error {
	response, err := h.request(ctx, endpoint, payload, 0)
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
