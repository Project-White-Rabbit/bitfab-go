package bitfab

// JustificationSpan cites one span and explains what it shows.
type JustificationSpan struct {
	SpanID string `json:"spanId"`
	Text   string `json:"text"`
}

// Justification is span-by-span evidence for an assertion decision.
type Justification []JustificationSpan

// ApprovalState reports whether a person reviewed an assertion decision.
type ApprovalState string

const (
	ApprovalPending  ApprovalState = "pending"
	ApprovalApproved ApprovalState = "approved"
	ApprovalRejected ApprovalState = "rejected"
)

// Approver identifies the person who reviewed an assertion decision.
type Approver struct {
	ID       string  `json:"id"`
	FullName *string `json:"fullName"`
	Email    *string `json:"email"`
	ImageURL *string `json:"imageUrl"`
}

// Assignee identifies the organization member on the hook for reviewing an assertion.
type Assignee = Approver

// ApprovalFields are read-only; only a person in Bitfab sets them.
type ApprovalFields struct {
	ApprovalState ApprovalState `json:"approvalState"`
	ApprovedBy    *Approver     `json:"approvedBy"`
	ApprovedAt    *string       `json:"approvedAt"`
}
