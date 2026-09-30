package bitfab

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
)

var invocationEnvironmentVars = []string{"BITFAB_REPLAY_EXECUTION_TARGET", "CI", "GITHUB_ACTIONS", "GITLAB_CI", "CIRCLECI", "BUILDKITE", "JENKINS_URL", "TF_BUILD"}

func clearInvocationEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range invocationEnvironmentVars {
		t.Setenv(name, "")
		os.Unsetenv(name)
	}
}

func TestReplayEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"no marker is local", nil, "local"},
		{"cloud runner wins over CI", map[string]string{"BITFAB_REPLAY_EXECUTION_TARGET": "hosted", "CI": "true", "GITHUB_ACTIONS": "true"}, "cloud_replay"},
		{"cloud marker must be exactly hosted", map[string]string{"BITFAB_REPLAY_EXECUTION_TARGET": "HOSTED"}, "local"},
		{"CI=true", map[string]string{"CI": "true"}, "ci"},
		{"CI=TRUE", map[string]string{"CI": "TRUE"}, "ci"},
		{"CI=1", map[string]string{"CI": "1"}, "ci"},
		{"CI=false", map[string]string{"CI": "false"}, "local"},
		{"CI=0", map[string]string{"CI": "0"}, "local"},
		{"GITHUB_ACTIONS", map[string]string{"GITHUB_ACTIONS": "x"}, "ci"},
		{"GITLAB_CI", map[string]string{"GITLAB_CI": "x"}, "ci"},
		{"CIRCLECI", map[string]string{"CIRCLECI": "x"}, "ci"},
		{"BUILDKITE", map[string]string{"BUILDKITE": "x"}, "ci"},
		{"JENKINS_URL", map[string]string{"JENKINS_URL": "x"}, "ci"},
		{"TF_BUILD", map[string]string{"TF_BUILD": "x"}, "ci"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearInvocationEnvironment(t)
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			if got := replayEnvironment(); got != tc.want {
				t.Fatalf("replayEnvironment() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildReplayInvocationReportsTheSDK(t *testing.T) {
	clearInvocationEnvironment(t)
	invocation := buildReplayInvocation(nil)
	if !reflect.DeepEqual(invocation["sdk"], map[string]any{"language": "go", "version": Version}) {
		t.Fatalf("sdk = %#v", invocation["sdk"])
	}
	if invocation["environment"] != "local" || len(invocation["flags"].(map[string]any)) != 0 {
		t.Fatalf("invocation = %#v", invocation)
	}
}

func TestInvocationFlagsKeepScalarsAndListsAndMarkAnythingElseAsSet(t *testing.T) {
	description := "prompt change"
	flags := invocationFlags([]invocationFlag{
		{"Limit", 5},
		{"Name", "run"},
		{"DryRun", true},
		{"Mock", MockAll},
		{"TraceIDs", []string{"a", "b"}},
		{"CodeChangeDescription", &description},
		{"OnItemStart", func(ReplayItemStartProgress) {}},
		{"DBBranch", &DBBranchOptions{}},
		{"Metadata", map[string]string{"a": "b"}},
		{"MockOverrides", []MockOverride{{}}},
		{"Ratio", math.NaN()},
	})
	want := map[string]any{
		"Limit":                 int64(5),
		"Name":                  "run",
		"DryRun":                true,
		"Mock":                  string(MockAll),
		"TraceIDs":              []any{"a", "b"},
		"CodeChangeDescription": "prompt change",
		"OnItemStart":           true,
		"DBBranch":              true,
		"Metadata":              true,
		"MockOverrides":         true,
		"Ratio":                 true,
	}
	if !reflect.DeepEqual(flags, want) {
		t.Fatalf("flags = %#v, want %#v", flags, want)
	}
}

func TestInvocationFlagsNeverCarryASecretLookingOption(t *testing.T) {
	flags := invocationFlags([]invocationFlag{
		{"APIKey", "k"},
		{"TraceFunctionKey", "fn"},
		{"auth-token", "t"},
		{"clientSecret", "s"},
		{"PASSWORD", "p"},
		{"Name", "run"},
	})
	if !reflect.DeepEqual(flags, map[string]any{"Name": "run"}) {
		t.Fatalf("flags = %#v", flags)
	}
}

func TestInvocationFlagsEnforceTheServerLimits(t *testing.T) {
	list := make([]string, 150)
	for index := range list {
		list[index] = fmt.Sprintf("t-%d", index)
	}
	entries := []invocationFlag{
		{"long", strings.Repeat("x", 1500)},
		{"list", list},
		{"", "empty name"},
		{strings.Repeat("n", 101), "long name"},
	}
	for index := range 150 {
		entries = append(entries, invocationFlag{fmt.Sprintf("flag-%d", index), index})
	}
	flags := invocationFlags(entries)
	if len(flags["long"].(string)) != 1000 {
		t.Fatalf("long flag length = %d", len(flags["long"].(string)))
	}
	if len(flags["list"].([]any)) != 100 {
		t.Fatalf("list flag length = %d", len(flags["list"].([]any)))
	}
	if _, ok := flags[""]; ok {
		t.Fatal("empty flag name was kept")
	}
	if _, ok := flags[strings.Repeat("n", 101)]; ok {
		t.Fatal("overlong flag name was kept")
	}
	if len(flags) != 100 {
		t.Fatalf("flag count = %d", len(flags))
	}
}

func TestInvocationFlagNames(t *testing.T) {
	for alias, want := range map[string]string{"max-concurrency": "concurrency", "dataset-id": "dataset-ids", "dry-run": "dry-run"} {
		if got := canonicalCLIFlag(alias); got != want {
			t.Fatalf("canonicalCLIFlag(%q) = %q, want %q", alias, got, want)
		}
	}
	for field, want := range map[string]string{
		"MaxConcurrency":           "concurrency",
		"DatasetID":                "dataset-ids",
		"DatasetIDs":               "dataset-ids",
		"TraceIDs":                 "trace-ids",
		"DBBranch":                 "db-branch",
		"ExperimentGroupID":        "experiment-group-id",
		"DryRun":                   "dry-run",
		"OnItemStart":              "on-item-start",
		"DisableCodeChangeCapture": "disable-code-change-capture",
		"MockOverrides":            "mock-override",
	} {
		if got := optionFlagName(field); got != want {
			t.Fatalf("optionFlagName(%q) = %q, want %q", field, got, want)
		}
	}
}

func TestInvocationFlagsLetTheDocumentedFlagWin(t *testing.T) {
	flags := invocationFlags(explicitReplayOptions(&ReplayOptions{Concurrency: &ReplayConcurrency{}, MaxConcurrency: 4}))
	if flags["concurrency"] != int64(4) {
		t.Fatalf("flags = %#v", flags)
	}
}

func TestInvocationFlagsSendOnlySortedMetadataKeys(t *testing.T) {
	for _, raw := range []any{
		map[string]string{"team": "search", "owner": "ada"},
		[]string{"team=search", "owner=ada", "team=x"},
	} {
		flags := invocationFlags([]invocationFlag{{"metadata", raw}})
		if !reflect.DeepEqual(flags["metadata"], []any{"owner", "team"}) {
			t.Fatalf("metadata from %#v = %#v", raw, flags["metadata"])
		}
	}
}

func TestSafeReplayInvocation(t *testing.T) {
	clearInvocationEnvironment(t)
	if invocation := safeReplayInvocation(&ReplayOptions{Name: "baseline"}); invocation == nil || !reflect.DeepEqual(invocation["flags"], map[string]any{"name": "baseline"}) {
		t.Fatalf("invocation = %#v", invocation)
	}
	previous := buildInvocation
	buildInvocation = func(*ReplayOptions) map[string]any { panic("boom") }
	t.Cleanup(func() { buildInvocation = previous })
	if invocation := safeReplayInvocation(&ReplayOptions{}); invocation != nil {
		t.Fatalf("invocation after a panic = %#v", invocation)
	}
}
