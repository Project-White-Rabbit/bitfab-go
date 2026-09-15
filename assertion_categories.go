package bitfab

import (
	"context"
	"net/http"
	"net/url"
)

const assertionCategoriesPath = "/api/sdk/assertionCategories"

// AssertionCategorySummary identifies an assertion category returned with an assertion.
type AssertionCategorySummary struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// AssertionCategory is an organization-scoped grouping for assertions.
type AssertionCategory struct {
	ApprovalFields
	AssertionCategorySummary
	OrganizationID string        `json:"organizationId"`
	Justification  Justification `json:"justification"`
	CreatedAt      string        `json:"createdAt"`
	UpdatedAt      string        `json:"updatedAt"`
}

// SaveAssertionCategoryParams creates a category or updates ID in place.
// A nil Description preserves the existing description; point to an empty string to clear it.
// A nil Justification preserves it; point to a nil slice to clear it.
type SaveAssertionCategoryParams struct {
	ID            string
	Title         string
	Description   *string
	Justification *Justification
}

// AssertionCategoriesClient manages the organization's assertion categories.
type AssertionCategoriesClient struct {
	httpClient *httpClient
}

func assertionCategoryPath(id string) string {
	return assertionCategoriesPath + "/" + url.PathEscape(id)
}

// Save creates a category or updates ID in place.
func (a *AssertionCategoriesClient) Save(ctx context.Context, params SaveAssertionCategoryParams) (*AssertionCategory, error) {
	payload := map[string]any{"title": params.Title}
	if params.ID != "" {
		payload["id"] = params.ID
	}
	if params.Description != nil {
		payload["description"] = *params.Description
	}
	if params.Justification != nil {
		payload["justification"] = *params.Justification
	}
	var response struct {
		Category AssertionCategory `json:"category"`
	}
	if err := a.httpClient.requestInto(ctx, assertionCategoriesPath, payload, &response); err != nil {
		return nil, err
	}
	return &response.Category, nil
}

// Get reads one category in the API key's organization.
func (a *AssertionCategoriesClient) Get(ctx context.Context, id string) (*AssertionCategory, error) {
	var response struct {
		Category AssertionCategory `json:"category"`
	}
	if err := a.httpClient.get(ctx, assertionCategoryPath(id), &response); err != nil {
		return nil, err
	}
	return &response.Category, nil
}

// List returns the organization's categories ordered by title.
func (a *AssertionCategoriesClient) List(ctx context.Context) ([]AssertionCategory, error) {
	var response struct {
		Categories []AssertionCategory `json:"categories"`
	}
	if err := a.httpClient.get(ctx, assertionCategoriesPath, &response); err != nil {
		return nil, err
	}
	return response.Categories, nil
}

// Delete removes a category and clears its assignments while preserving assertions and verdicts.
func (a *AssertionCategoriesClient) Delete(ctx context.Context, id string) (*AssertionCategory, error) {
	var response struct {
		Category AssertionCategory `json:"category"`
	}
	if err := a.httpClient.requestMethodInto(ctx, http.MethodDelete, assertionCategoryPath(id), map[string]any{}, &response); err != nil {
		return nil, err
	}
	return &response.Category, nil
}
