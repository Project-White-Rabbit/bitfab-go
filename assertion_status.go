package bitfab

// JustificationSpan cites one span and explains what it shows.
type JustificationSpan struct {
	SpanID string `json:"spanId"`
	Text   string `json:"text"`
}

// Justification is span-by-span evidence for an assertion decision.
type Justification []JustificationSpan

// AssertionStatus is an assertion's review status. Only a confirmed assertion
// is checked on a replay; a draft is checked by nothing.
type AssertionStatus string

const (
	AssertionStatusDraft     AssertionStatus = "draft"
	AssertionStatusConfirmed AssertionStatus = "confirmed"
	AssertionStatusRejected  AssertionStatus = "rejected"
)

// Deprecated: Use AssertionStatus instead. ApprovalState carries the old
// values "pending", "approved", and "rejected".
type ApprovalState string

const (
	// Deprecated: Use AssertionStatusDraft instead.
	ApprovalPending ApprovalState = "pending"
	// Deprecated: Use AssertionStatusConfirmed instead.
	ApprovalApproved ApprovalState = "approved"
	// Deprecated: Use AssertionStatusRejected instead.
	ApprovalRejected ApprovalState = "rejected"
)

func legacyApprovalState(status AssertionStatus) ApprovalState {
	switch status {
	case AssertionStatusDraft:
		return ApprovalPending
	case AssertionStatusConfirmed:
		return ApprovalApproved
	case AssertionStatusRejected:
		return ApprovalRejected
	}
	return ApprovalState(status)
}

// Confirmer identifies the person who confirmed or rejected an assertion.
type Confirmer struct {
	ID       string  `json:"id"`
	FullName *string `json:"fullName"`
	Email    *string `json:"email"`
	ImageURL *string `json:"imageUrl"`
}

// Deprecated: Use Confirmer instead.
type Approver = Confirmer

// Assignee identifies the organization member on the hook for reviewing an assertion.
type Assignee = Confirmer

// StatusFields are read-only; only a person in Bitfab sets them.
type StatusFields struct {
	Status      AssertionStatus `json:"status"`
	ConfirmedBy *Confirmer      `json:"confirmedBy"`
	ConfirmedAt *string         `json:"confirmedAt"`
	// Deprecated: Use Status instead. It carries the old values "pending",
	// "approved", and "rejected".
	ApprovalState ApprovalState `json:"-"`
	// Deprecated: Use ConfirmedBy instead.
	ApprovedBy *Confirmer `json:"-"`
	// Deprecated: Use ConfirmedAt instead.
	ApprovedAt *string `json:"-"`
}

// Deprecated: Use StatusFields instead.
type ApprovalFields = StatusFields

func (fields *StatusFields) fillLegacy() {
	fields.ApprovalState = legacyApprovalState(fields.Status)
	fields.ApprovedBy = fields.ConfirmedBy
	fields.ApprovedAt = fields.ConfirmedAt
}
