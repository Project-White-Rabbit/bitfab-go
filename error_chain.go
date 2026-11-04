package bitfab

import (
	"errors"
	"fmt"
	"strings"
)

// maxErrorCauseDepth is how many causes below an error are followed. A driver
// error is usually one or two levels down; the limit stops a pathological
// chain.
const maxErrorCauseDepth = 10

// errorCauses returns the errors below err, nearest first, following
// errors.Unwrap up to the depth limit.
func errorCauses(err error) []error {
	var causes []error
	current := err
	for len(causes) < maxErrorCauseDepth {
		cause := errors.Unwrap(current)
		if cause == nil {
			break
		}
		causes = append(causes, cause)
		current = cause
	}
	return causes
}

// describeError returns err's message, followed by the deepest cause's message
// when err wraps an error whose message it does not already include. An error
// wrapped with fmt.Errorf("...: %w") already carries its cause's text and is
// left as it is; a driver error type that wraps a connection timeout but
// reports only "failed query" gains "(caused by: ...)".
func describeError(err error) string {
	message := err.Error()
	causes := errorCauses(err)
	if len(causes) == 0 {
		return message
	}
	deepest := strings.TrimSpace(causes[len(causes)-1].Error())
	if deepest == "" || strings.Contains(message, deepest) {
		return message
	}
	return fmt.Sprintf("%s (caused by: %s)", message, deepest)
}

// errorCode returns a database driver's error code when err reports one
// (pgx's *pgconn.PgError and lib/pq's *pq.Error both expose SQLState).
func errorCode(err error) string {
	if coded, ok := err.(interface{ SQLState() string }); ok {
		return coded.SQLState()
	}
	return ""
}
