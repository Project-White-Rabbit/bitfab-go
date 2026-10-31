package bitfab

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
)

// ReplayRegistryContext supplies project parameters to the registration's options factory.
type ReplayRegistryContext struct{ Params map[string]any }

// ReplayRegistration binds a project function and its replay configuration.
// OptionsFactory modifies a per-invocation copy after loading --params and --param.
// The command line owns the code change, so Options and OptionsFactory cannot
// set CodeChange, CodeChangeDescription, or CodeChangeFiles.
type ReplayRegistration struct {
	Client           *Client
	Function         any
	TraceFunctionKey string
	Options          ReplayOptions
	OptionsFactory   func(context.Context, ReplayRegistryContext, *ReplayOptions) error
}

// ReplayRegistry is a project-owned collection used by a compiled replay command.
type ReplayRegistry struct{ entries map[string]ReplayRegistration }

// NewReplayRegistry creates an empty project registry.
func NewReplayRegistry() *ReplayRegistry {
	return &ReplayRegistry{entries: map[string]ReplayRegistration{}}
}

// Register adds a uniquely named callable. Bound replay functions supply their key automatically.
func (r *ReplayRegistry) Register(name string, entry ReplayRegistration) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("bitfab: registry name cannot be empty")
	}
	if r.entries == nil {
		r.entries = map[string]ReplayRegistration{}
	}
	if _, ok := r.entries[name]; ok {
		return fmt.Errorf("bitfab: registry already contains %q", name)
	}
	if entry.Client == nil {
		return fmt.Errorf("bitfab: registry client is required")
	}
	if entry.TraceFunctionKey == "" {
		if bound, ok := entry.Function.(ReplayFunction); ok {
			entry.TraceFunctionKey = bound.traceFunctionKey
		} else if bound, ok := entry.Function.(*ReplayFunction); ok && bound != nil {
			entry.TraceFunctionKey = bound.traceFunctionKey
		}
	}
	if strings.TrimSpace(entry.TraceFunctionKey) == "" {
		return fmt.Errorf("bitfab: registry trace function key is required")
	}
	if err := refuseRegistryCodeChange(entry.Options); err != nil {
		return err
	}
	resolved, err := resolveReplayFunction(entry.TraceFunctionKey, entry.Function)
	if err != nil {
		return err
	}
	if _, err = prepareReplayCallable(resolved); err != nil {
		return err
	}
	r.entries[name] = entry
	return nil
}

// Names returns registry names in deterministic order.
func (r *ReplayRegistry) Names() []string {
	names := make([]string, 0, len(r.entries))
	for name := range r.entries {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func refuseRegistryCodeChange(options ReplayOptions) error {
	if options.CodeChange != "" || options.CodeChangeDescription != "" || options.CodeChangeFiles != nil || options.CodeChangeBase != "" {
		return fmt.Errorf("bitfab: registry options cannot set CodeChange, CodeChangeDescription, CodeChangeFiles, or CodeChangeBase; pass --code-change on the command line")
	}
	return nil
}

func (r *ReplayRegistry) fetch(name string) (ReplayRegistration, error) {
	entry, ok := r.entries[name]
	if !ok {
		return entry, fmt.Errorf("bitfab: unknown pipeline %q; registered: %s", name, strings.Join(r.Names(), ", "))
	}
	return entry, nil
}

type registryCLIArgs struct {
	pipeline, traceIDs, datasetIDs, graderIDs, categoryIDs, name, notes, mock, experimentGroupID, experimentGroupName, codeChange, codeChangeFile, codeChangeBase, params, seed, fromTrace, executeItem string
	limit, attempts, concurrency                                                                                                                                                                        int
	assertions, skipJudging, judge, dryRun, failOnError, dbBranch, noDBBranch, run                                                                                                                      bool
	parameters                                                                                                                                                                                          []string
	metadata                                                                                                                                                                                            map[string]string
	visited                                                                                                                                                                                             map[string]bool
	flags                                                                                                                                                                                               map[string]any
}

type registryParameters []string

func (p *registryParameters) String() string         { return strings.Join(*p, ",") }
func (p *registryParameters) Set(value string) error { *p = append(*p, value); return nil }

func parseRegistryCLI(registry *ReplayRegistry, args []string, stderr io.Writer) (registryCLIArgs, error) {
	out := registryCLIArgs{visited: map[string]bool{}}
	if len(args) == 0 {
		return out, fmt.Errorf("usage: replay <%s> [options]", strings.Join(registry.Names(), "|"))
	}
	out.pipeline = args[0]
	for _, arg := range args[1:] {
		if arg == "--" {
			break
		}
		name, _, _ := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if strings.HasPrefix(arg, "-") && name == "no-code-change" {
			return out, fmt.Errorf("bitfab: --no-code-change was replaced by --code-change skip.")
		}
	}
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.IntVar(&out.limit, "limit", 0, "bound the selected traces")
	fs.IntVar(&out.attempts, "attempts", 0, "attempts per trace (1..100)")
	fs.IntVar(&out.concurrency, "concurrency", 0, "maximum concurrent items")
	fs.IntVar(&out.concurrency, "max-concurrency", 0, "maximum concurrent items")
	fs.StringVar(&out.traceIDs, "trace-ids", "", "comma-separated trace IDs")
	fs.StringVar(&out.datasetIDs, "dataset-ids", "", "comma-separated dataset IDs")
	fs.StringVar(&out.datasetIDs, "dataset-id", "", "dataset IDs")
	fs.StringVar(&out.graderIDs, "grader-ids", "", "comma-separated grader IDs")
	fs.StringVar(&out.name, "name", "", "What this run is testing, in a few words, such as 'baseline' or 'shorter system prompt'. Bitfab records the commit, branch, tree state, datasets, and who ran it with every experiment, so do not repeat them here.")
	fs.StringVar(&out.notes, "notes", "", "Run conditions Bitfab cannot see on its own, such as an environment override or a forced feature flag. Kept on the experiment next to its name.")
	fs.StringVar(&out.mock, "mock", "", "marked, all, or none")
	fs.StringVar(&out.experimentGroupID, "experiment-group-id", "", "experiment group ID")
	fs.StringVar(&out.experimentGroupName, "experiment-group-name", "", "Name for the experiment group, applied when this run creates the group or the group has no name yet. A group that already has a name keeps it. Requires --experiment-group-id.")
	fs.StringVar(&out.codeChange, "code-change", "", "`working-tree|last-branch|default-branch|ref|skip|manual` (required: the code change this run tests; working-tree is the uncommitted changes vs HEAD, last-branch the diff against the branch this one was cut from, default-branch the diff against the default branch, ref the diff against --code-change-base, skip none, manual the edits in --code-change-file)")
	fs.StringVar(&out.codeChangeBase, "code-change-base", "", "`BRANCH|COMMIT` (with --code-change ref: the branch or commit to diff against)")
	fs.StringVar(&out.codeChangeFile, "code-change-file", "", "`PATH` (with --code-change manual: a JSON file of { description, files })")
	fs.StringVar(&out.params, "params", "", "parameters JSON file")
	fs.StringVar(&out.seed, "seed", "", "seed cases JSON or JSONL file")
	fs.StringVar(&out.seed, "cases", "", "seed cases JSON or JSONL file")
	fs.StringVar(&out.executeItem, "execute-item", "", "internal replay assignment")
	fs.StringVar(&out.fromTrace, "from-trace", "", "trace IDs to reseed")
	fs.BoolVar(&out.assertions, "only-with-assertions", false, "require approved assertions")
	fs.StringVar(&out.categoryIDs, "category-ids", "", "with --trace-ids or --dataset-ids: replay only the traces carrying an approved assertion in one of these categories; which assertions are judged stays the same")
	fs.BoolVar(&out.judge, "judge-assertions", false, "deprecated: judging is on by default")
	fs.BoolVar(&out.skipJudging, "skip-assertion-judging", false, "don't judge approved assertions on each replay; judging is on by default and costs model calls")
	fs.BoolVar(&out.dryRun, "dry-run", false, "resolve inputs without execution")
	fs.BoolVar(&out.failOnError, "fail-on-error", false, "return an error after printing the result when any replayed item errored; under --dry-run, items whose inputs failed to resolve count")
	fs.BoolVar(&out.dbBranch, "db-branch", false, "use historical database branches")
	fs.BoolVar(&out.noDBBranch, "no-db-branch", false, "disable historical database branches")
	fs.BoolVar(&out.run, "run", false, "execute seed cases")
	var params registryParameters
	fs.Var(&params, "param", "name=value project parameter (repeatable)")
	var metadata registryParameters
	fs.Var(&metadata, "metadata", "key=value label stored on the experiment, filterable when listing experiments (repeatable)")
	if err := fs.Parse(args[1:]); err != nil {
		return out, err
	}
	parsedMetadata, err := registryMetadata(metadata)
	if err != nil {
		return out, err
	}
	out.metadata = parsedMetadata
	if fs.NArg() != 0 {
		return out, fmt.Errorf("bitfab: unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	out.flags = map[string]any{}
	fs.Visit(func(f *flag.Flag) {
		out.visited[f.Name] = true
		recordRegistryFlag(out.flags, f)
	})
	out.parameters = params
	for _, key := range []string{"limit", "attempts", "concurrency", "max-concurrency"} {
		if out.visited[key] {
			value := out.limit
			if key == "attempts" {
				value = out.attempts
			}
			if key == "concurrency" || key == "max-concurrency" {
				value = out.concurrency
			}
			if value < 1 {
				return out, fmt.Errorf("bitfab: --%s must be positive", key)
			}
		}
	}
	if out.visited["code-change"] && !isCodeChangeType(out.codeChange) {
		return out, fmt.Errorf("bitfab: Unknown --code-change '%s'. Pass %s. A file of edits goes in --code-change manual --code-change-file PATH.", out.codeChange, codeChangeChoices)
	}
	if out.attempts > 100 {
		return out, fmt.Errorf("bitfab: --attempts supports at most 100")
	}
	if (out.visited["trace-ids"] && (out.visited["dataset-ids"] || out.visited["dataset-id"])) || (out.dbBranch && out.noDBBranch) {
		return out, fmt.Errorf("bitfab: conflicting replay options")
	}
	return out, nil
}

func requireRegistryCodeChange(parsed registryCLIArgs) error {
	if !parsed.visited["code-change"] {
		return fmt.Errorf(`bitfab: --code-change is required: %s. "ref" takes the branch or commit from --code-change-base, and "manual" takes the edits from --code-change-file PATH.`, codeChangeChoices)
	}
	if (CodeChangeType(parsed.codeChange) == CodeChangeManual) != parsed.visited["code-change-file"] {
		return fmt.Errorf("bitfab: --code-change-file goes with --code-change manual, and --code-change manual needs --code-change-file PATH.")
	}
	if (CodeChangeType(parsed.codeChange) == CodeChangeRef) != parsed.visited["code-change-base"] {
		return fmt.Errorf("bitfab: --code-change-base goes with --code-change ref, and --code-change ref needs --code-change-base BRANCH|COMMIT.")
	}
	return nil
}

var registryFlagsOutsideReplay = map[string]bool{"seed": true, "cases": true, "run": true, "from-trace": true, "execute-item": true}

func recordRegistryFlag(flags map[string]any, f *flag.Flag) {
	if registryFlagsOutsideReplay[f.Name] {
		return
	}
	name := canonicalCLIFlag(f.Name)
	if commaSeparatedIDFlags[name] {
		flags[name] = splitCommaSeparatedIDs(f.Value.String())
		return
	}
	switch value := f.Value.(type) {
	case *registryParameters:
		items := make([]string, 0, len(*value))
		for _, raw := range *value {
			if f.Name == "param" {
				raw, _, _ = strings.Cut(raw, "=")
			}
			items = append(items, raw)
		}
		flags[name] = items
	case flag.Getter:
		flags[name] = value.Get()
	default:
		flags[name] = f.Value.String()
	}
}

func registryMetadata(pairs []string) (map[string]string, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	metadata := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		key, value, found := strings.Cut(pair, "=")
		if !found || key == "" {
			return nil, fmt.Errorf("bitfab: --metadata expects key=value, got %q", pair)
		}
		metadata[key] = value
	}
	return metadata, nil
}

func splitCommaSeparatedIDs(raw string) []string {
	ids := []string{}
	for _, value := range strings.Split(raw, ",") {
		if id := strings.TrimSpace(value); id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

func registryIDs(raw string) ([]string, error) {
	ids := []string{}
	for _, value := range strings.Split(raw, ",") {
		if id := strings.TrimSpace(value); id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("bitfab: selector requires at least one ID")
	}
	return ids, nil
}

func loadRegistryParams(path string, raw []string) (map[string]any, error) {
	params := map[string]any{}
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &params); err != nil {
			return nil, err
		}
		if params == nil {
			return nil, fmt.Errorf("bitfab: --params requires a JSON object")
		}
	}
	for _, value := range raw {
		key, text, found := strings.Cut(value, "=")
		key = strings.TrimSpace(key)
		if !found || key == "" {
			return nil, fmt.Errorf("bitfab: --param requires name=value")
		}
		var parsed any
		if json.Unmarshal([]byte(text), &parsed) != nil {
			parsed = text
		}
		params[key] = parsed
	}
	return params, nil
}

type registryAssertionNarrowing struct {
	onlyWithAssertions bool
	categoryIDs        []string
}

func (n registryAssertionNarrowing) narrows() bool {
	return n.onlyWithAssertions || n.categoryIDs != nil
}

// hasApprovedAssertion reports whether a trace carries an assertion a person has
// approved. GetAssertions returns every state so a reviewer can see drafts, but
// only an approved assertion is checked on a replay, so narrowing on anything
// else would pick traces the run cannot be judged against. The server applies
// the same rule when it revalidates the selection.
func hasApprovedAssertion(assertions []TraceAssertion, categoryIDs []string) bool {
	for _, assertion := range assertions {
		if assertion.ApprovalState != ApprovalApproved {
			continue
		}
		if categoryIDs == nil || (assertion.CategoryID != nil && slices.Contains(categoryIDs, *assertion.CategoryID)) {
			return true
		}
	}
	return false
}

func boundRegistryTraceIDs(ctx context.Context, client *Client, ids []string, limit int, narrowing registryAssertionNarrowing) ([]string, error) {
	if limit < 1 {
		return nil, fmt.Errorf("bitfab: replay limit must be positive")
	}
	if limit >= len(ids) {
		return ids, nil
	}
	if !narrowing.narrows() {
		return ids[:limit], nil
	}
	selected := []string{}
	for _, id := range ids {
		result, err := client.Traces.GetAssertions(ctx, id)
		if err != nil {
			return nil, err
		}
		if result.InheritedFrom == nil && hasApprovedAssertion(result.Assertions, narrowing.categoryIDs) {
			selected = append(selected, id)
			if len(selected) == limit {
				break
			}
		}
	}
	if len(selected) == 0 {
		if narrowing.categoryIDs != nil {
			return nil, fmt.Errorf("bitfab: No traces with an approved assertion in the given categories matched this replay selection.")
		}
		return nil, fmt.Errorf("bitfab: no traces with approved assertions matched this selection")
	}
	return selected, nil
}

type replaySynchronizedWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (w *replaySynchronizedWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.Write(data)
}

var registryInitialEnvironment = os.Environ()

const checkArgumentsFlag = "--check-arguments"

func checkRegistryCLI(registry *ReplayRegistry, args []string, stderr io.Writer) error {
	parsed, err := parseRegistryCLI(registry, args, stderr)
	if err != nil {
		return err
	}
	if parsed.seed == "" && parsed.fromTrace == "" {
		if err = requireRegistryCodeChange(parsed); err != nil {
			return err
		}
	}
	_, err = registry.fetch(parsed.pipeline)
	return err
}

// RunReplayCLI executes a compiled project's registry command and writes machine-readable results.
// The first argument selects the pipeline; subsequent arguments match the other SDK registry CLIs.
func RunReplayCLI(ctx context.Context, registry *ReplayRegistry, args []string, stdout, stderr io.Writer) (any, error) {
	if len(args) > 0 && args[0] == checkArgumentsFlag {
		return nil, checkRegistryCLI(registry, args[1:], stderr)
	}
	if isCloudReplayCommand(args) {
		var check func([]string) error
		if registry != nil {
			check = func(replay []string) error { return checkRegistryCLI(registry, replay, stderr) }
		}
		return nil, runCloudReplayHelper(ctx, cloudReplayHelper, args, stdout, stderr, check)
	}
	stderr = &replaySynchronizedWriter{writer: stderr}
	args, printAPIKey := removeReplayFlag(args, "--print-api-key")
	parsed, err := parseRegistryCLI(registry, args, stderr)
	if err != nil {
		return nil, err
	}
	if printAPIKey {
		return nil, printRegistryAPIKey(registry, parsed.pipeline, stdout)
	}
	if parsed.seed != "" || parsed.fromTrace != "" {
		return runSeedRegistryCLI(ctx, registry, parsed, stdout, stderr)
	}
	if err = requireRegistryCodeChange(parsed); err != nil {
		return nil, err
	}
	entry, err := registry.fetch(parsed.pipeline)
	if err != nil {
		return nil, err
	}
	params, err := loadRegistryParams(parsed.params, parsed.parameters)
	if err != nil {
		return nil, err
	}
	options := entry.Options
	options.TraceIDs = slices.Clone(options.TraceIDs)
	options.DatasetIDs = slices.Clone(options.DatasetIDs)
	options.GraderIDs = slices.Clone(options.GraderIDs)
	options.CategoryIDs = slices.Clone(options.CategoryIDs)
	options.MockOverrides = slices.Clone(options.MockOverrides)
	if options.DBBranch != nil {
		branch := *options.DBBranch
		options.DBBranch = &branch
	}
	if entry.OptionsFactory != nil {
		if err = entry.OptionsFactory(ctx, ReplayRegistryContext{Params: params}, &options); err != nil {
			return nil, err
		}
		if err = refuseRegistryCodeChange(options); err != nil {
			return nil, err
		}
	}
	if parsed.executeItem == "" {
		if options.DatasetID != "" {
			if len(options.DatasetIDs) > 0 {
				return nil, fmt.Errorf("bitfab: dataset_id and dataset_ids are the same selector")
			}
			options.DatasetIDs = []string{options.DatasetID}
			options.DatasetID = ""
		}
		if options.TraceIDs != nil && options.DatasetIDs != nil {
			return nil, fmt.Errorf("bitfab: registry trace_ids and dataset_ids cannot both select a source")
		}
		if parsed.visited["trace-ids"] {
			options.TraceIDs, err = registryIDs(parsed.traceIDs)
			options.DatasetIDs = nil
		}
		if parsed.visited["dataset-ids"] || parsed.visited["dataset-id"] {
			options.DatasetIDs, err = registryIDs(parsed.datasetIDs)
			options.TraceIDs = nil
		}
		if err != nil {
			return nil, err
		}
		if parsed.visited["only-with-assertions"] {
			options.OnlyWithAssertions = parsed.assertions
		}
		if parsed.visited["category-ids"] {
			options.CategoryIDs, err = registryIDs(parsed.categoryIDs)
			if err != nil {
				return nil, err
			}
		}
		if options.CategoryIDs != nil && options.TraceIDs == nil && options.DatasetIDs == nil {
			return nil, fmt.Errorf("bitfab: --category-ids narrows a trace ID or dataset selection. Pass it with --trace-ids or --dataset-ids.")
		}
		narrowing := registryAssertionNarrowing{onlyWithAssertions: options.OnlyWithAssertions, categoryIDs: options.CategoryIDs}
		if parsed.visited["judge-assertions"] {
			fmt.Fprintln(stderr, "bitfab: --judge-assertions is deprecated. Assertion judging is on by default; pass --skip-assertion-judging to turn it off.")
		}
		if parsed.visited["skip-assertion-judging"] && parsed.skipJudging {
			options.SkipAssertionJudging = true
		}
		bound := options.Limit
		if parsed.visited["limit"] {
			bound = parsed.limit
		}
		if bound < 0 {
			return nil, fmt.Errorf("bitfab: replay limit must be positive")
		}
		options.Limit = 0
		if options.TraceIDs != nil {
			if bound > 0 {
				options.TraceIDs, err = boundRegistryTraceIDs(ctx, entry.Client, options.TraceIDs, bound, narrowing)
			}
		} else if options.DatasetIDs != nil {
			if bound > 0 {
				members := []string{}
				for _, id := range options.DatasetIDs {
					result, e := entry.Client.Datasets.ListTraces(ctx, id)
					if e != nil {
						return nil, e
					}
					members = append(members, result.TraceIDs...)
				}
				slices.Sort(members)
				members = slices.Compact(members)
				if bound < len(members) {
					if bound > 100 {
						return nil, fmt.Errorf("bitfab: bounding datasets supports at most 100 pinned trace IDs")
					}
					options.TraceIDs, err = boundRegistryTraceIDs(ctx, entry.Client, members, bound, narrowing)
				}
			}
		} else {
			options.Limit = bound
			if bound == 0 {
				options.Limit = 10
			}
		}
		if err != nil {
			return nil, err
		}
	}
	registryAttempts, registryConcurrency := options.Attempts, options.MaxConcurrency
	if parsed.visited["name"] {
		options.Name = parsed.name
	}
	if parsed.visited["notes"] {
		options.Notes = parsed.notes
	}
	if parsed.visited["metadata"] {
		options.Metadata = parsed.metadata
	}
	if parsed.visited["attempts"] {
		options.Attempts = parsed.attempts
	}
	if parsed.visited["concurrency"] || parsed.visited["max-concurrency"] {
		options.MaxConcurrency = parsed.concurrency
	}
	if parsed.visited["mock"] {
		options.Mock = MockStrategy(parsed.mock)
	}
	if parsed.visited["dry-run"] {
		options.DryRun = parsed.dryRun
	}
	if parsed.visited["experiment-group-id"] {
		options.ExperimentGroupID = parsed.experimentGroupID
	}
	if parsed.visited["experiment-group-name"] {
		options.ExperimentGroupName = parsed.experimentGroupName
	}
	if parsed.visited["grader-ids"] {
		options.GraderIDs, err = registryIDs(parsed.graderIDs)
		if err != nil {
			return nil, err
		}
	}
	if parsed.dbBranch && options.DBBranch == nil {
		options.DBBranch = &DBBranchOptions{}
	}
	if parsed.noDBBranch {
		options.DBBranch = nil
	}
	options.CodeChange = CodeChangeType(parsed.codeChange)
	if options.CodeChange == CodeChangeRef {
		options.CodeChangeBase = parsed.codeChangeBase
	}
	if options.CodeChange == CodeChangeManual {
		data, e := os.ReadFile(parsed.codeChangeFile)
		if e != nil {
			return nil, e
		}
		var change struct {
			Description *string          `json:"description"`
			Files       []CodeChangeFile `json:"files"`
		}
		if json.Unmarshal(data, &change) != nil || change.Description == nil || change.Files == nil {
			return nil, fmt.Errorf("bitfab: Invalid --code-change-file '%s': expected { description, files }.", parsed.codeChangeFile)
		}
		options.CodeChangeDescription = *change.Description
		options.CodeChangeFiles = change.Files
	}
	options.OnItemStart = func(event ReplayItemStartProgress) { ReportReplayProgress(event) }
	if options.Concurrency != nil {
		config := *options.Concurrency
		if registryAttempts != 0 || registryConcurrency != 0 {
			return nil, fmt.Errorf("bitfab: registry concurrency cannot be combined with scalar attempts or max concurrency")
		}
		if parsed.visited["attempts"] {
			config.Attempts = parsed.attempts
			options.Attempts = 0
		}
		if parsed.visited["concurrency"] || parsed.visited["max-concurrency"] {
			config.MaxConcurrency = parsed.concurrency
			options.MaxConcurrency = 0
		}
		options.Concurrency = &config
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	options.processCommand = &replayProcessCommand{executable: executable, args: slices.Clone(args), environ: slices.Clone(registryInitialEnvironment), stderr: stderr}
	if parsed.executeItem != "" {
		return runAssignedReplayItem(ctx, entry, options, parsed.executeItem, stdout, stderr)
	}
	registryCallback := options.OnItemFinish
	options.OnItemFinish = func(event ReplayItemFinishProgress) {
		ReportReplayProgress(event)
		if registryCallback == nil || options.DryRun {
			return
		}
		defer func() {
			if recovered := recover(); recovered != nil {
				fmt.Fprintf(stderr, "[replay] on_item_finish failed for %s: %v\n", event.Item.OriginalTraceID, recovered)
			}
		}()
		registryCallback(event)
	}
	options.OnExperimentStart = func(event ReplayExperimentStart) {
		fmt.Fprintf(stderr, "[replay] Experiment %s: %s\n", event.ExperimentID, event.ExperimentURL)
	}
	fmt.Fprintf(stderr, "[replay] Replaying %q...\n", entry.TraceFunctionKey)
	options.cliFlags = parsed.flags
	result, err := entry.Client.Replay(ctx, entry.TraceFunctionKey, entry.Function, &options)
	if err != nil {
		var replayErr *ReplayError
		if errors.As(err, &replayErr) && replayErr.ExperimentID != "" {
			fmt.Fprintf(stderr, "Experiment %s: %s\n", replayErr.ExperimentID, replayErr.ExperimentURL)
		}
		return result, err
	}
	if len(result.Items) == 0 {
		return result, fmt.Errorf("bitfab: no traces matched this replay selection")
	}
	if options.DryRun {
		for _, item := range result.Items {
			input, _ := json.Marshal(item.Input)
			fmt.Fprintf(stderr, "[dry-run] %s attempt %d inputs=%s\n", item.OriginalTraceID, item.Attempt, input)
		}
	} else {
		renderReplaySummary(parsed.pipeline, result, stderr)
	}
	encoded, err := SerializeReplayResult(result)
	if err != nil {
		return result, err
	}
	if _, err = fmt.Fprintln(stdout, encoded); err != nil {
		return result, err
	}
	if parsed.failOnError {
		return result, erroredItemsError(result, options.DryRun)
	}
	return result, nil
}

func removeReplayFlag(args []string, flag string) ([]string, bool) {
	rest := make([]string, 0, len(args))
	found := false
	for _, arg := range args {
		if arg == flag {
			found = true
			continue
		}
		rest = append(rest, arg)
	}
	return rest, found
}

func printRegistryAPIKey(registry *ReplayRegistry, pipeline string, stdout io.Writer) error {
	entry, err := registry.fetch(pipeline)
	if err != nil {
		return err
	}
	var payload struct {
		APIKey     *string `json:"apiKey"`
		ServiceURL string  `json:"serviceUrl"`
	}
	payload.ServiceURL = entry.Client.serviceURL
	if key := entry.Client.probeAPIKey(); key != "" {
		payload.APIKey = &key
	}
	var line strings.Builder
	line.WriteString("@@bitfab:api-key ")
	encoder := json.NewEncoder(&line)
	encoder.SetEscapeHTML(false)
	if err = encoder.Encode(payload); err != nil {
		return err
	}
	_, err = io.WriteString(stdout, line.String())
	return err
}

func erroredItemsError(result ReplayResult, dryRun bool) error {
	errored := 0
	for _, item := range result.Items {
		if item.Error != nil {
			errored++
		}
	}
	if errored == 0 {
		return nil
	}
	noun := "replayed"
	if dryRun {
		noun = "resolved"
	}
	return fmt.Errorf("[replay] %d of %d %s items errored; exiting 1 because of --fail-on-error", errored, len(result.Items), noun)
}
