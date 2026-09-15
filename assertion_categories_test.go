package bitfab

import (
	"context"
	"net/http"
	"reflect"
	"testing"
)

func TestAssertionCategories_CRUD(t *testing.T) {
	approvedAt := "2026-09-10T00:00:00Z"
	category := map[string]any{
		"id": "category-1", "title": "Timing", "description": "Arrival requirements",
		"organizationId": "org-1", "createdAt": "2026-09-09T00:00:00Z", "updatedAt": "2026-09-09T00:00:00Z",
		"justification": []any{map[string]any{"spanId": "span-1", "text": "Shows the arrival time"}},
		"approvalState": "approved", "approvedBy": map[string]any{"id": "user-1", "fullName": "Ada", "email": nil, "imageUrl": nil},
		"approvedAt": approvedAt,
	}
	server := newDatasetsServer(t, func(r datasetRequest) any {
		if r.method == http.MethodGet && r.path == assertionCategoriesPath {
			return map[string]any{"categories": []any{category}}
		}
		return map[string]any{"category": category}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	description := "Arrival requirements"

	saved, err := client.AssertionCategories.Save(context.Background(), SaveAssertionCategoryParams{
		Title: "Timing", Description: &description,
	})
	if err != nil || saved.ID != "category-1" || saved.OrganizationID != "org-1" || saved.ApprovalState != ApprovalApproved ||
		len(saved.Justification) != 1 || saved.Justification[0].SpanID != "span-1" || saved.ApprovedBy == nil ||
		saved.ApprovedBy.ID != "user-1" || saved.ApprovedBy.FullName == nil || *saved.ApprovedBy.FullName != "Ada" ||
		saved.ApprovedBy.Email != nil || saved.ApprovedBy.ImageURL != nil || saved.ApprovedAt == nil || *saved.ApprovedAt != approvedAt {
		t.Fatalf("Save = %+v, %v", saved, err)
	}
	got, err := client.AssertionCategories.Get(context.Background(), "a/b")
	if err != nil || got.ID != "category-1" {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	listed, err := client.AssertionCategories.List(context.Background())
	if err != nil || len(listed) != 1 || listed[0].ID != "category-1" {
		t.Fatalf("List = %+v, %v", listed, err)
	}
	deleted, err := client.AssertionCategories.Delete(context.Background(), "a/b")
	if err != nil || deleted.ID != "category-1" {
		t.Fatalf("Delete = %+v, %v", deleted, err)
	}

	requests := server.recorded()
	if len(requests) != 4 {
		t.Fatalf("requests = %+v", requests)
	}
	if requests[0].method != http.MethodPost || !reflect.DeepEqual(requests[0].body, map[string]any{
		"title": "Timing", "description": "Arrival requirements",
	}) {
		t.Fatalf("save request = %+v", requests[0])
	}
	if assertionCategoryPath("a/b") != assertionCategoriesPath+"/a%2Fb" {
		t.Fatalf("category ID was not escaped: %s", assertionCategoryPath("a/b"))
	}
	if requests[1].path != assertionCategoriesPath+"/a/b" || requests[2].path != assertionCategoriesPath {
		t.Fatalf("get/list requests = %+v", requests[1:3])
	}
	if requests[3].method != http.MethodDelete || requests[3].path != assertionCategoriesPath+"/a/b" {
		t.Fatalf("delete request = %+v", requests[3])
	}
}

func TestAssertionCategories_SavePreservesOmittedAndSendsEmptyDescription(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"category": map[string]any{"id": "category-1", "title": "Updated"}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	if _, err := client.AssertionCategories.Save(context.Background(), SaveAssertionCategoryParams{
		ID: "category-1", Title: "Updated",
	}); err != nil {
		t.Fatal(err)
	}
	empty := ""
	justification := Justification{{SpanID: "span-1", Text: "Shows the category is useful"}}
	if _, err := client.AssertionCategories.Save(context.Background(), SaveAssertionCategoryParams{
		ID: "category-1", Title: "Updated", Description: &empty, Justification: &justification,
	}); err != nil {
		t.Fatal(err)
	}
	var clearJustification Justification
	if _, err := client.AssertionCategories.Save(context.Background(), SaveAssertionCategoryParams{
		ID: "category-1", Title: "Updated", Justification: &clearJustification,
	}); err != nil {
		t.Fatal(err)
	}
	requests := server.recorded()
	if _, ok := requests[0].body["description"]; ok {
		t.Fatalf("omitted description sent: %+v", requests[0].body)
	}
	if requests[1].body["description"] != "" {
		t.Fatalf("empty description not sent: %+v", requests[1].body)
	}
	if !reflect.DeepEqual(requests[1].body["justification"], []any{map[string]any{
		"spanId": "span-1", "text": "Shows the category is useful",
	}}) {
		t.Fatalf("justification not sent: %+v", requests[1].body)
	}
	if justificationValue, ok := requests[2].body["justification"]; !ok || justificationValue != nil {
		t.Fatalf("justification clear not sent: %+v", requests[2].body)
	}
}
