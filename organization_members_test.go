package bitfab

import (
	"context"
	"testing"
)

func TestOrganizationMembers_ListReadsTheAPIKeysOrganization(t *testing.T) {
	email := "dana@bitfab.dev"
	fullName := "Dana Reviewer"
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"members": []any{map[string]any{
			"id": "user-9", "fullName": fullName, "email": email, "imageUrl": nil,
		}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))

	members, err := client.OrganizationMembers.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0].Email == nil || *members[0].Email != email ||
		members[0].FullName == nil || *members[0].FullName != fullName {
		t.Fatalf("members = %+v", members)
	}
	if path := server.recorded()[0].path; path != "/api/sdk/organizationMembers" {
		t.Fatalf("path = %q", path)
	}
}

func TestOrganizationMembers_ListReturnsEmptyWhenNobodyIsMirrored(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"members": []any{}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))

	members, err := client.OrganizationMembers.List(context.Background())
	if err != nil || len(members) != 0 {
		t.Fatalf("members = %+v, %v", members, err)
	}
}
