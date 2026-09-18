package bitfab

import "context"

const organizationMembersPath = "/api/sdk/organizationMembers"

// OrganizationMember is one person in the API key's organization.
type OrganizationMember struct {
	ID       string  `json:"id"`
	FullName *string `json:"fullName"`
	Email    *string `json:"email"`
	ImageURL *string `json:"imageUrl"`
}

// OrganizationMembersClient reads the people in the API key's organization.
type OrganizationMembersClient struct {
	httpClient *httpClient
}

// List returns the organization's members ordered by name. Read it to find the
// address to pass as an assertion's AssigneeEmail.
func (o *OrganizationMembersClient) List(ctx context.Context) ([]OrganizationMember, error) {
	var response struct {
		Members []OrganizationMember `json:"members"`
	}
	if err := o.httpClient.get(ctx, organizationMembersPath, &response); err != nil {
		return nil, err
	}
	return response.Members, nil
}
