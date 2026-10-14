package bitfab

import (
	"context"
	"net/http"
	"reflect"
	"testing"
)

func TestCategories_CRUD(t *testing.T) {
	category := map[string]any{
		"id": "category-1", "title": "Timing", "description": "Arrival requirements",
		"organizationId": "org-1", "createdAt": "2026-09-09T00:00:00Z", "updatedAt": "2026-09-09T00:00:00Z",
	}
	server := newDatasetsServer(t, func(r datasetRequest) any {
		if r.method == http.MethodGet && r.path == categoriesPath {
			return map[string]any{"categories": []any{category}}
		}
		return map[string]any{"category": category}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	description := "Arrival requirements"

	saved, err := client.Categories.Save(context.Background(), SaveCategoryParams{
		Title: "Timing", Description: &description,
	})
	if err != nil || saved.ID != "category-1" || saved.OrganizationID != "org-1" {
		t.Fatalf("Save = %+v, %v", saved, err)
	}
	got, err := client.Categories.Get(context.Background(), "a/b")
	if err != nil || got.ID != "category-1" {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	listed, err := client.Categories.List(context.Background())
	if err != nil || len(listed) != 1 || listed[0].ID != "category-1" {
		t.Fatalf("List = %+v, %v", listed, err)
	}
	deleted, err := client.Categories.Delete(context.Background(), "a/b")
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
	if categoryPath("a/b") != categoriesPath+"/a%2Fb" {
		t.Fatalf("category ID was not escaped: %s", categoryPath("a/b"))
	}
	if requests[1].path != categoriesPath+"/a/b" || requests[2].path != categoriesPath {
		t.Fatalf("get/list requests = %+v", requests[1:3])
	}
	if requests[3].method != http.MethodDelete || requests[3].path != categoriesPath+"/a/b" {
		t.Fatalf("delete request = %+v", requests[3])
	}
}

func TestCategories_SavePreservesOmittedAndSendsEmptyDescription(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"category": map[string]any{"id": "category-1", "title": "Updated"}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	if _, err := client.Categories.Save(context.Background(), SaveCategoryParams{
		ID: "category-1", Title: "Updated",
	}); err != nil {
		t.Fatal(err)
	}
	empty := ""
	if _, err := client.Categories.Save(context.Background(), SaveCategoryParams{
		ID: "category-1", Title: "Updated", Description: &empty,
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
}

func TestCategories_DeprecatedAssertionCategoriesIsTheSameClient(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"categories": []any{}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	if client.AssertionCategories != client.Categories {
		t.Fatal("AssertionCategories is not the Categories client")
	}
	var legacy *AssertionCategoriesClient = client.AssertionCategories
	if _, err := legacy.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests := server.recorded(); len(requests) != 1 || requests[0].path != "/api/sdk/categories" {
		t.Fatalf("requests = %+v", requests)
	}
}
