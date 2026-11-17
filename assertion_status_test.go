package bitfab

import (
	"encoding/json"
	"testing"
)

func TestAssertionStatusFillsDeprecatedApprovalState(t *testing.T) {
	cases := map[AssertionStatus]ApprovalState{
		AssertionStatusDraft:     ApprovalPending,
		AssertionStatusConfirmed: ApprovalApproved,
		AssertionStatusRejected:  ApprovalRejected,
	}
	for status, want := range cases {
		var assertion TraceAssertion
		if err := json.Unmarshal([]byte(`{"id":"a","status":"`+string(status)+`"}`), &assertion); err != nil {
			t.Fatal(err)
		}
		if assertion.Status != status || assertion.ApprovalState != want {
			t.Fatalf("status %q decoded to %q with approval state %q, want %q", status, assertion.Status, assertion.ApprovalState, want)
		}
	}
}
