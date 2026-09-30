package bitfab

import (
	"math"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"unicode"
)

const (
	replayExecutionTargetEnv = "BITFAB_REPLAY_EXECUTION_TARGET"
	hostedExecutionTarget    = "hosted"
	ciEnv                    = "CI"
	maxInvocationFlags       = 100
	maxInvocationFlagName    = 100
	maxInvocationFlagString  = 1000
	maxInvocationFlagList    = 100
	metadataInvocationFlag   = "metadata"
)

var (
	ciPresenceEnvVars   = []string{"GITHUB_ACTIONS", "GITLAB_CI", "CIRCLECI", "BUILDKITE", "JENKINS_URL", "TF_BUILD"}
	secretFlagName      = regexp.MustCompile(`(?i)key|token|secret|password`)
	unsupportedFlagItem = struct{}{}
	cliFlagAliases      = map[string]string{"max-concurrency": "concurrency", "dataset-id": "dataset-ids"}
	optionFlagNames     = map[string]string{
		"MaxConcurrency":    "concurrency",
		"TraceIDs":          "trace-ids",
		"DatasetID":         "dataset-ids",
		"DatasetIDs":        "dataset-ids",
		"GraderIDs":         "grader-ids",
		"ExperimentGroupID": "experiment-group-id",
		"DBBranch":          "db-branch",
		"MockOverrides":     "mock-override",
	}
	buildInvocation       = replayInvocation
	commaSeparatedIDFlags = map[string]bool{"trace-ids": true, "dataset-ids": true, "grader-ids": true}
)

type invocationFlag struct {
	name  string
	value any
}

func replayEnvironment() string {
	if os.Getenv(replayExecutionTargetEnv) == hostedExecutionTarget {
		return "cloud_replay"
	}
	if ci := strings.ToLower(os.Getenv(ciEnv)); ci == "true" || ci == "1" {
		return "ci"
	}
	for _, name := range ciPresenceEnvVars {
		if os.Getenv(name) != "" {
			return "ci"
		}
	}
	return "local"
}

func invocationScalar(value reflect.Value) any {
	switch value.Kind() {
	case reflect.String:
		text := value.String()
		if len(text) > maxInvocationFlagString {
			text = text[:maxInvocationFlagString]
		}
		return text
	case reflect.Bool:
		return value.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.Uint()
	case reflect.Float32, reflect.Float64:
		if math.IsNaN(value.Float()) || math.IsInf(value.Float(), 0) {
			return unsupportedFlagItem
		}
		return value.Float()
	case reflect.Invalid:
		return nil
	case reflect.Pointer, reflect.Interface:
		if value.IsNil() {
			return nil
		}
		return invocationScalar(value.Elem())
	}
	return unsupportedFlagItem
}

func invocationFlagValue(raw any) any {
	value := reflect.ValueOf(raw)
	if value.Kind() == reflect.Slice || value.Kind() == reflect.Array {
		items := make([]any, 0, min(value.Len(), maxInvocationFlagList))
		for index := 0; index < value.Len() && index < maxInvocationFlagList; index++ {
			item := invocationScalar(value.Index(index))
			if item == unsupportedFlagItem {
				return true
			}
			items = append(items, item)
		}
		return items
	}
	scalar := invocationScalar(value)
	if scalar == unsupportedFlagItem {
		return true
	}
	return scalar
}

func canonicalCLIFlag(name string) string {
	if canonical, ok := cliFlagAliases[name]; ok {
		return canonical
	}
	return name
}

func kebabCase(name string) string {
	runes := []rune(name)
	var out strings.Builder
	for index, letter := range runes {
		upper := unicode.IsUpper(letter)
		if upper && index > 0 {
			previous := runes[index-1]
			nextLower := index+1 < len(runes) && unicode.IsLower(runes[index+1])
			if unicode.IsLower(previous) || unicode.IsDigit(previous) || (unicode.IsUpper(previous) && nextLower) {
				out.WriteByte('-')
			}
		}
		out.WriteRune(unicode.ToLower(letter))
	}
	return out.String()
}

func optionFlagName(field string) string {
	if name, ok := optionFlagNames[field]; ok {
		return name
	}
	return kebabCase(field)
}

func metadataKeys(raw any) any {
	var keys []string
	switch value := raw.(type) {
	case map[string]string:
		for key := range value {
			keys = append(keys, key)
		}
	case map[string]any:
		for key := range value {
			keys = append(keys, key)
		}
	case []string:
		for _, pair := range value {
			key, _, _ := strings.Cut(pair, "=")
			keys = append(keys, key)
		}
	default:
		return true
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

func invocationFlags(entries []invocationFlag) map[string]any {
	flags := map[string]any{}
	for _, entry := range entries {
		if len(flags) >= maxInvocationFlags {
			break
		}
		if _, seen := flags[entry.name]; seen || entry.name == "" || len(entry.name) > maxInvocationFlagName || secretFlagName.MatchString(entry.name) {
			continue
		}
		value := entry.value
		if entry.name == metadataInvocationFlag {
			value = metadataKeys(value)
		}
		flags[entry.name] = invocationFlagValue(value)
	}
	return flags
}

func buildReplayInvocation(entries []invocationFlag) map[string]any {
	return map[string]any{
		"flags":       invocationFlags(entries),
		"sdk":         map[string]any{"language": "go", "version": Version},
		"environment": replayEnvironment(),
	}
}

func sortedInvocationFlags(flags map[string]any) []invocationFlag {
	names := make([]string, 0, len(flags))
	for name := range flags {
		names = append(names, name)
	}
	slices.Sort(names)
	entries := make([]invocationFlag, 0, len(names))
	for _, name := range names {
		entries = append(entries, invocationFlag{name: name, value: flags[name]})
	}
	return entries
}

func explicitReplayOptions(options *ReplayOptions) []invocationFlag {
	if options == nil {
		return nil
	}
	value := reflect.ValueOf(*options)
	kind := value.Type()
	mapped := []invocationFlag{}
	rest := []invocationFlag{}
	for index := range kind.NumField() {
		field := kind.Field(index)
		if !field.IsExported() || value.Field(index).IsZero() {
			continue
		}
		entry := invocationFlag{name: optionFlagName(field.Name), value: value.Field(index).Interface()}
		if _, ok := optionFlagNames[field.Name]; ok {
			mapped = append(mapped, entry)
		} else {
			rest = append(rest, entry)
		}
	}
	return append(mapped, rest...)
}

func replayInvocation(options *ReplayOptions) map[string]any {
	if options != nil && options.cliFlags != nil {
		return buildReplayInvocation(sortedInvocationFlags(options.cliFlags))
	}
	return buildReplayInvocation(explicitReplayOptions(options))
}

func safeReplayInvocation(options *ReplayOptions) (invocation map[string]any) {
	defer func() {
		if recover() != nil {
			invocation = nil
		}
	}()
	return buildInvocation(options)
}
