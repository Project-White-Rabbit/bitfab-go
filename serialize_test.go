package bitfab

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

// These tests verify that complex Go types can be serialized to JSON
// without errors, since span data is serialized via json.Marshal in the HTTP client.

type ContentScoreResult struct {
	ID         string     `json:"id"`
	Content    string     `json:"content"`
	Score      float64    `json:"score"`
	Type       string     `json:"type"`
	CompanyID  *uuid.UUID `json:"company_id,omitempty"`
	CustomerID *uuid.UUID `json:"customer_id,omitempty"`
	Number     *int       `json:"number,omitempty"`
	Source     *string    `json:"source,omitempty"`
	Title      *string    `json:"title,omitempty"`
	Attribute  *string    `json:"attribute,omitempty"`
}

type ChatActionType string

const (
	ChatActionTypeSendMessage ChatActionType = "send_message"
	ChatActionTypeSendReply   ChatActionType = "send_reply"
)

type ChatAction struct {
	ID         string                 `json:"id"`
	ActionType ChatActionType         `json:"action_type"`
	Title      string                 `json:"title"`
	Data       map[string]interface{} `json:"data"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

type AccountWithType struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	AccountType string     `json:"account_type"`
	CompanyID   *uuid.UUID `json:"company_id,omitempty"`
	Role        *string    `json:"role,omitempty"`
}

type TableSort struct {
	Column    string `json:"column"`
	Direction string `json:"direction"`
}

type FilterDefinition struct {
	ViewType      string       `json:"view_type"`
	Name          string       `json:"name"`
	TotalCount    int          `json:"total_count"`
	ColumnsSchema []string     `json:"columns_schema,omitempty"`
	SortingSchema *[]TableSort `json:"sorting_schema,omitempty"`
}

func TestJSONMarshal_Assembly(t *testing.T) {
	companyID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	ticketNum := 42
	ticketSource := "email"

	scores := []ContentScoreResult{
		{
			ID:        "score-1",
			Content:   "Great product",
			Score:     0.95,
			Type:      "review",
			CompanyID: &companyID,
			Number:    &ticketNum,
			Source:    &ticketSource,
		},
		{
			ID:      "score-2",
			Content: "Needs improvement",
			Score:   0.3,
			Type:    "feedback",
		},
	}

	tags := []string{"important", "follow-up"}

	actions := []ChatAction{
		{
			ID:         "action-1",
			ActionType: ChatActionTypeSendMessage,
			Title:      "Send greeting",
			Data:       map[string]interface{}{"message": "Hello!"},
			Metadata:   map[string]interface{}{"source": "auto"},
		},
		{
			ID:         "action-2",
			ActionType: ChatActionTypeSendReply,
			Title:      "Reply to ticket",
			Data:       map[string]interface{}{"ticket_id": 123, "body": "Working on it"},
		},
	}

	role := "admin"
	accounts := []AccountWithType{
		{
			ID:          "acc-1",
			Name:        "Acme Corp",
			AccountType: "company",
			CompanyID:   &companyID,
		},
		{
			ID:          "acc-2",
			Name:        "Jane Doe",
			AccountType: "customer",
			Role:        &role,
		},
	}

	filter := &FilterDefinition{
		ViewType:      "accounts",
		Name:          "Enterprise Accounts",
		TotalCount:    42,
		ColumnsSchema: []string{"select", "name", "labels"},
		SortingSchema: &[]TableSort{
			{Column: "name", Direction: "asc"},
		},
	}

	// Build span data the same way bitfab.go does
	spanData := map[string]any{
		"name":   "test-span",
		"type":   "function",
		"input":  "query text",
		"output": []any{scores, tags, actions, accounts, filter},
	}

	payload := map[string]any{
		"type":             "sdk-function",
		"source":           "go-sdk-function",
		"sourceTraceId":    "trace-1",
		"traceFunctionKey": "test-key",
		"rawSpan": map[string]any{
			"id":         "span-1",
			"trace_id":   "trace-1",
			"started_at": "2024-01-01T00:00:00.000Z",
			"ended_at":   "2024-01-01T00:00:01.000Z",
			"span_data":  spanData,
		},
	}

	// This is what the HTTP client does - must not panic or error
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Verify round-trip
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	rawSpan := parsed["rawSpan"].(map[string]any)
	sd := rawSpan["span_data"].(map[string]any)
	output := sd["output"].([]any)

	// Verify scores
	gotScores := output[0].([]any)
	if len(gotScores) != 2 {
		t.Fatalf("scores len = %d, want 2", len(gotScores))
	}
	score1 := gotScores[0].(map[string]any)
	if score1["id"] != "score-1" {
		t.Errorf("score1.id = %v", score1["id"])
	}
	if score1["company_id"] != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" {
		t.Errorf("score1.company_id = %v", score1["company_id"])
	}
	score2 := gotScores[1].(map[string]any)
	if _, ok := score2["company_id"]; ok {
		t.Error("score2 should omit nil company_id with omitempty")
	}

	// Verify actions with custom string type
	gotActions := output[2].([]any)
	action1 := gotActions[0].(map[string]any)
	if action1["action_type"] != "send_message" {
		t.Errorf("action1.action_type = %v", action1["action_type"])
	}

	// Verify filter
	gotFilter := output[4].(map[string]any)
	if gotFilter["name"] != "Enterprise Accounts" {
		t.Errorf("filter.name = %v", gotFilter["name"])
	}
	sorting := gotFilter["sorting_schema"].([]any)
	sort1 := sorting[0].(map[string]any)
	if sort1["column"] != "name" {
		t.Errorf("sort.column = %v", sort1["column"])
	}
}

func TestJSONMarshal_NilPointers(t *testing.T) {
	// Verify nil pointers don't cause panics and serialize as null
	type Record struct {
		ID        string     `json:"id"`
		CompanyID *uuid.UUID `json:"company_id,omitempty"`
		Title     *string    `json:"title"`
	}

	spanData := map[string]any{
		"output": Record{ID: "rec-1", CompanyID: nil, Title: nil},
	}

	data, err := json.Marshal(spanData)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	output := parsed["output"].(map[string]any)
	if _, ok := output["company_id"]; ok {
		t.Error("expected company_id omitted with omitempty")
	}
	if output["title"] != nil {
		t.Errorf("title = %v, want nil", output["title"])
	}
}

// Round-trip tests: verify that serialized span data can be deserialized
// back into the original typed structs with all field values preserved.
// Uses MarshalSpanPayload → UnmarshalSpanPayload[T] to prove the full cycle.

func TestRoundTrip_ContentScoreResult(t *testing.T) {
	companyID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	ticketNum := 42
	ticketSource := "email"

	original := []ContentScoreResult{
		{
			ID:        "score-1",
			Content:   "Great product",
			Score:     0.95,
			Type:      "review",
			CompanyID: &companyID,
			Number:    &ticketNum,
			Source:    &ticketSource,
		},
		{
			ID:      "score-2",
			Content: "Needs improvement",
			Score:   0.3,
			Type:    "feedback",
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	got, err := UnmarshalSpanPayload[[]ContentScoreResult](data)
	if err != nil {
		t.Fatalf("UnmarshalSpanPayload failed: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}

	// First score: all fields populated
	if got[0].ID != "score-1" {
		t.Errorf("got[0].ID = %v", got[0].ID)
	}
	if got[0].Score != 0.95 {
		t.Errorf("got[0].Score = %v", got[0].Score)
	}
	if got[0].CompanyID == nil || *got[0].CompanyID != companyID {
		t.Errorf("got[0].CompanyID = %v, want %v", got[0].CompanyID, companyID)
	}
	if got[0].Number == nil || *got[0].Number != 42 {
		t.Errorf("got[0].Number = %v", got[0].Number)
	}
	if got[0].Source == nil || *got[0].Source != "email" {
		t.Errorf("got[0].Source = %v", got[0].Source)
	}

	// Second score: nil optional fields
	if got[1].CompanyID != nil {
		t.Errorf("got[1].CompanyID should be nil, got %v", got[1].CompanyID)
	}
	if got[1].Number != nil {
		t.Errorf("got[1].Number should be nil, got %v", got[1].Number)
	}
}

func TestRoundTrip_ChatAction(t *testing.T) {
	original := []ChatAction{
		{
			ID:         "action-1",
			ActionType: ChatActionTypeSendMessage,
			Title:      "Send greeting",
			Data:       map[string]interface{}{"message": "Hello!"},
			Metadata:   map[string]interface{}{"source": "auto"},
		},
		{
			ID:         "action-2",
			ActionType: ChatActionTypeSendReply,
			Title:      "Reply to ticket",
			Data:       map[string]interface{}{"ticket_id": float64(123), "body": "Working on it"},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	got, err := UnmarshalSpanPayload[[]ChatAction](data)
	if err != nil {
		t.Fatalf("UnmarshalSpanPayload failed: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}

	// Custom string type preserved
	if got[0].ActionType != ChatActionTypeSendMessage {
		t.Errorf("got[0].ActionType = %v, want %v", got[0].ActionType, ChatActionTypeSendMessage)
	}
	if got[1].ActionType != ChatActionTypeSendReply {
		t.Errorf("got[1].ActionType = %v, want %v", got[1].ActionType, ChatActionTypeSendReply)
	}

	// Nested map data preserved
	if got[0].Data["message"] != "Hello!" {
		t.Errorf("got[0].Data[message] = %v", got[0].Data["message"])
	}
	if got[1].Data["body"] != "Working on it" {
		t.Errorf("got[1].Data[body] = %v", got[1].Data["body"])
	}

	// Metadata omitempty: first has it, second doesn't
	if got[0].Metadata["source"] != "auto" {
		t.Errorf("got[0].Metadata[source] = %v", got[0].Metadata["source"])
	}
	if got[1].Metadata != nil {
		t.Errorf("got[1].Metadata should be nil, got %v", got[1].Metadata)
	}
}

func TestRoundTrip_FilterDefinition(t *testing.T) {
	original := FilterDefinition{
		ViewType:      "accounts",
		Name:          "Enterprise Accounts",
		TotalCount:    42,
		ColumnsSchema: []string{"select", "name", "labels"},
		SortingSchema: &[]TableSort{
			{Column: "name", Direction: "asc"},
			{Column: "created_at", Direction: "desc"},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	got, err := UnmarshalSpanPayload[FilterDefinition](data)
	if err != nil {
		t.Fatalf("UnmarshalSpanPayload failed: %v", err)
	}

	if got.ViewType != "accounts" {
		t.Errorf("ViewType = %v", got.ViewType)
	}
	if got.TotalCount != 42 {
		t.Errorf("TotalCount = %v", got.TotalCount)
	}
	if len(got.ColumnsSchema) != 3 || got.ColumnsSchema[0] != "select" {
		t.Errorf("ColumnsSchema = %v", got.ColumnsSchema)
	}
	if got.SortingSchema == nil {
		t.Fatal("SortingSchema is nil")
	}
	sorting := *got.SortingSchema
	if len(sorting) != 2 {
		t.Fatalf("SortingSchema len = %d, want 2", len(sorting))
	}
	if sorting[0].Column != "name" || sorting[0].Direction != "asc" {
		t.Errorf("sorting[0] = %+v", sorting[0])
	}
	if sorting[1].Column != "created_at" || sorting[1].Direction != "desc" {
		t.Errorf("sorting[1] = %+v", sorting[1])
	}
}

func TestRoundTrip_FullSpanPayload(t *testing.T) {
	companyID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	role := "admin"

	original := AccountWithType{
		ID:          "acc-1",
		Name:        "Acme Corp",
		AccountType: "company",
		CompanyID:   &companyID,
		Role:        &role,
	}

	// Build a full span payload the way the SDK does
	payload := map[string]any{
		"type":             "sdk-function",
		"source":           "go-sdk-function",
		"sourceTraceId":    "trace-1",
		"traceFunctionKey": "test-key",
		"rawSpan": map[string]any{
			"id":         "span-1",
			"trace_id":   "trace-1",
			"started_at": "2024-01-01T00:00:00.000Z",
			"ended_at":   "2024-01-01T00:00:01.000Z",
			"span_data": map[string]any{
				"name":   "test-span",
				"type":   "function",
				"input":  "query text",
				"output": original,
			},
		},
	}

	data, err := MarshalSpanPayload(payload)
	if err != nil {
		t.Fatalf("MarshalSpanPayload failed: %v", err)
	}

	// Server receives the full payload as a generic map
	parsed, err := UnmarshalSpanPayload[map[string]any](data)
	if err != nil {
		t.Fatalf("UnmarshalSpanPayload failed: %v", err)
	}

	// Extract the output JSON from the nested map
	rawSpan := parsed["rawSpan"].(map[string]any)
	sd := rawSpan["span_data"].(map[string]any)
	outputJSON, err := json.Marshal(sd["output"])
	if err != nil {
		t.Fatalf("re-marshal output failed: %v", err)
	}

	// Deserialize back into the original typed struct
	got, err := UnmarshalSpanPayload[AccountWithType](outputJSON)
	if err != nil {
		t.Fatalf("UnmarshalSpanPayload[AccountWithType] failed: %v", err)
	}

	if got.ID != "acc-1" {
		t.Errorf("ID = %v", got.ID)
	}
	if got.Name != "Acme Corp" {
		t.Errorf("Name = %v", got.Name)
	}
	if got.AccountType != "company" {
		t.Errorf("AccountType = %v", got.AccountType)
	}
	if got.CompanyID == nil || *got.CompanyID != companyID {
		t.Errorf("CompanyID = %v, want %v", got.CompanyID, companyID)
	}
	if got.Role == nil || *got.Role != "admin" {
		t.Errorf("Role = %v", got.Role)
	}
}

// TestFinalizeSpanPayloadClean verifies a faithfully-serializable payload is
// returned without an errors entry.
func TestFinalizeSpanPayloadClean(t *testing.T) {
	payload := map[string]any{
		"type":   "sdk-function",
		"source": "go-sdk-function",
		"rawSpan": map[string]any{
			"span_data": map[string]any{"input": []any{"hi"}, "output": "ok"},
		},
	}
	out := finalizeSpanPayload(payload)
	if _, ok := out["errors"]; ok {
		t.Fatalf("clean payload should have no errors, got %v", out["errors"])
	}
}

// TestFinalizeSpanPayloadMarksLossy verifies a payload holding an unserializable
// value is marked serialization_degraded (non-replayable) while control fields
// and the rest of the body survive.
func TestFinalizeSpanPayloadMarksLossy(t *testing.T) {
	payload := map[string]any{
		"type":   "sdk-function",
		"source": "go-sdk-function",
		"rawSpan": map[string]any{
			"span_data": map[string]any{"input": []any{make(chan int)}},
		},
	}
	out := finalizeSpanPayload(payload)

	errs, ok := out["errors"].([]any)
	if !ok || len(errs) == 0 {
		t.Fatalf("lossy payload should carry errors, got %v", out["errors"])
	}
	first, _ := errs[0].(map[string]any)
	if first["source"] != "sdk" {
		t.Fatalf("expected source sdk, got %v", first["source"])
	}
	if first["step"] != serializationDegradedStep {
		t.Fatalf("expected step %q, got %v", serializationDegradedStep, first["step"])
	}
	// Routing/control fields survive sanitization.
	if out["type"] != "sdk-function" || out["source"] != "go-sdk-function" {
		t.Fatalf("control fields lost: %v", out)
	}
	// The whole finalized payload must now JSON-encode without error.
	if _, err := json.Marshal(out); err != nil {
		t.Fatalf("finalized payload should be JSON-safe: %v", err)
	}
}
