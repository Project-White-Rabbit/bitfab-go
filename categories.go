package bitfab

import (
	"context"
	"net/http"
	"net/url"
)

const categoriesPath = "/api/sdk/categories"

// CategorySummary identifies a category returned with an assertion or a tally.
type CategorySummary struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// Category is an organization-scoped grouping for assertions and scenarios.
type Category struct {
	CategorySummary
	OrganizationID string `json:"organizationId"`
	CreatedAt      string `json:"createdAt"`
	UpdatedAt      string `json:"updatedAt"`
}

// SaveCategoryParams creates a category or updates ID in place.
// A nil Description preserves the existing description; point to an empty string to clear it.
type SaveCategoryParams struct {
	ID          string
	Title       string
	Description *string
}

// CategoriesClient manages the organization's categories.
type CategoriesClient struct {
	httpClient *httpClient
}

// AssertionCategorySummary identifies a category returned with an assertion.
//
// Deprecated: Use CategorySummary instead.
type AssertionCategorySummary = CategorySummary

// AssertionCategory is an organization-scoped grouping.
//
// Deprecated: Use Category instead.
type AssertionCategory = Category

// SaveAssertionCategoryParams creates a category or updates ID in place.
//
// Deprecated: Use SaveCategoryParams instead.
type SaveAssertionCategoryParams = SaveCategoryParams

// AssertionCategoriesClient manages the organization's categories.
//
// Deprecated: Use CategoriesClient instead.
type AssertionCategoriesClient = CategoriesClient

func categoryPath(id string) string {
	return categoriesPath + "/" + url.PathEscape(id)
}

// Save creates a category or updates ID in place.
func (a *CategoriesClient) Save(ctx context.Context, params SaveCategoryParams) (*Category, error) {
	payload := map[string]any{"title": params.Title}
	if params.ID != "" {
		payload["id"] = params.ID
	}
	if params.Description != nil {
		payload["description"] = *params.Description
	}
	var response struct {
		Category Category `json:"category"`
	}
	if err := a.httpClient.requestInto(ctx, categoriesPath, payload, &response); err != nil {
		return nil, err
	}
	return &response.Category, nil
}

// Get reads one category in the API key's organization.
func (a *CategoriesClient) Get(ctx context.Context, id string) (*Category, error) {
	var response struct {
		Category Category `json:"category"`
	}
	if err := a.httpClient.get(ctx, categoryPath(id), &response); err != nil {
		return nil, err
	}
	return &response.Category, nil
}

// List returns the organization's categories ordered by title.
func (a *CategoriesClient) List(ctx context.Context) ([]Category, error) {
	var response struct {
		Categories []Category `json:"categories"`
	}
	if err := a.httpClient.get(ctx, categoriesPath, &response); err != nil {
		return nil, err
	}
	return response.Categories, nil
}

// Delete removes a category and clears its assignments while preserving assertions and verdicts.
func (a *CategoriesClient) Delete(ctx context.Context, id string) (*Category, error) {
	var response struct {
		Category Category `json:"category"`
	}
	if err := a.httpClient.requestMethodInto(ctx, http.MethodDelete, categoryPath(id), map[string]any{}, &response); err != nil {
		return nil, err
	}
	return &response.Category, nil
}
