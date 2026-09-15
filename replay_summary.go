package bitfab

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

func replayValuesEqual(left, right any) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	if errA != nil || errB != nil {
		return reflect.DeepEqual(left, right)
	}
	return bytes.Equal(a, b)
}

func renderReplaySummary(pipeline string, result ReplayResult, stderr io.Writer) {
	same, changed, matched, missed, errors := 0, 0, 0, 0, 0
	for _, item := range result.Items {
		if item.Error != nil {
			errors++
			continue
		}
		equal := replayValuesEqual(item.Result, item.OriginalOutput)
		if item.IngestionType != nil && *item.IngestionType == "seeded" {
			if equal {
				matched++
			} else {
				missed++
			}
		} else if equal {
			same++
		} else {
			changed++
		}
	}
	fmt.Fprintf(stderr, "\nSummary\n  Pipeline: %s\n  Replayed: %d\n", pipeline, len(result.Items))
	if result.Attempts > 1 {
		fmt.Fprintf(stderr, "  Attempts: %d\n", result.Attempts)
	}
	if same > 0 || changed > 0 || matched+missed == 0 {
		fmt.Fprintf(stderr, "  Same:     %d\n  Changed:  %d\n", same, changed)
	}
	if matched > 0 || missed > 0 {
		fmt.Fprintf(stderr, "  Matched expected: %d\n  Missed expected:  %d\n", matched, missed)
	}
	if errors > 0 {
		fmt.Fprintf(stderr, "  Errors:   %d\n", errors)
	}
	fmt.Fprintf(stderr, "\n  %s\n", result.TestRunURL)
}
