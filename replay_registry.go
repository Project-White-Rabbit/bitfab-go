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

func (r *ReplayRegistry) fetch(name string) (ReplayRegistration, error) {
	entry, ok := r.entries[name]
	if !ok {
		return entry, fmt.Errorf("bitfab: unknown pipeline %q; registered: %s", name, strings.Join(r.Names(), ", "))
	}
	return entry, nil
}

type registryCLIArgs struct {
	pipeline, traceIDs, datasetIDs, graderIDs, name, notes, mock, experimentGroupID, codeChange, params, seed, fromTrace, executeItem string
	limit, attempts, concurrency                                                                                                      int
	assertions, dryRun, dbBranch, noDBBranch, noCodeChange, run                                                                       bool
	parameters                                                                                                                        []string
	visited                                                                                                                           map[string]bool
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
	fs.StringVar(&out.codeChange, "code-change", "", "code change JSON file")
	fs.StringVar(&out.params, "params", "", "parameters JSON file")
	fs.StringVar(&out.seed, "seed", "", "seed cases JSON or JSONL file")
	fs.StringVar(&out.seed, "cases", "", "seed cases JSON or JSONL file")
	fs.StringVar(&out.executeItem, "execute-item", "", "internal replay assignment")
	fs.StringVar(&out.fromTrace, "from-trace", "", "trace IDs to reseed")
	fs.BoolVar(&out.assertions, "only-with-assertions", false, "require approved assertions")
	fs.BoolVar(&out.dryRun, "dry-run", false, "resolve inputs without execution")
	fs.BoolVar(&out.dbBranch, "db-branch", false, "use historical database branches")
	fs.BoolVar(&out.noDBBranch, "no-db-branch", false, "disable historical database branches")
	fs.BoolVar(&out.noCodeChange, "no-code-change", false, "disable code change capture")
	fs.BoolVar(&out.run, "run", false, "execute seed cases")
	var params registryParameters
	fs.Var(&params, "param", "name=value project parameter (repeatable)")
	if err := fs.Parse(args[1:]); err != nil {
		return out, err
	}
	if fs.NArg() != 0 {
		return out, fmt.Errorf("bitfab: unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	fs.Visit(func(f *flag.Flag) { out.visited[f.Name] = true })
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
	if out.attempts > 100 {
		return out, fmt.Errorf("bitfab: --attempts supports at most 100")
	}
	if (out.visited["trace-ids"] && (out.visited["dataset-ids"] || out.visited["dataset-id"])) || (out.dbBranch && out.noDBBranch) || (out.codeChange != "" && out.noCodeChange) {
		return out, fmt.Errorf("bitfab: conflicting replay options")
	}
	return out, nil
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

// hasApprovedAssertion reports whether a trace carries an assertion a person has
// approved. GetAssertions returns every state so a reviewer can see drafts, but
// only an approved assertion is checked on a replay, so narrowing on anything
// else would pick traces the run cannot be judged against. The server applies
// the same rule when it revalidates the selection.
func hasApprovedAssertion(assertions []TraceAssertion) bool {
	for _, assertion := range assertions {
		if assertion.ApprovalState == ApprovalApproved {
			return true
		}
	}
	return false
}

func boundRegistryTraceIDs(ctx context.Context, client *Client, ids []string, limit int, assertions bool) ([]string, error) {
	if limit < 1 {
		return nil, fmt.Errorf("bitfab: replay limit must be positive")
	}
	if limit >= len(ids) {
		return ids, nil
	}
	if !assertions {
		return ids[:limit], nil
	}
	selected := []string{}
	for _, id := range ids {
		result, err := client.Traces.GetAssertions(ctx, id)
		if err != nil {
			return nil, err
		}
		if result.InheritedFrom == nil && hasApprovedAssertion(result.Assertions) {
			selected = append(selected, id)
			if len(selected) == limit {
				break
			}
		}
	}
	if len(selected) == 0 {
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

// RunReplayCLI executes a compiled project's registry command and writes machine-readable results.
// The first argument selects the pipeline; subsequent arguments match the other SDK registry CLIs.
func RunReplayCLI(ctx context.Context, registry *ReplayRegistry, args []string, stdout, stderr io.Writer) (any, error) {
	if isCloudReplayCommand(args) {
		return RunCloudReplayCLI(ctx, args, stdout, stderr)
	}
	stderr = &replaySynchronizedWriter{writer: stderr}
	parsed, err := parseRegistryCLI(registry, args, stderr)
	if err != nil {
		return nil, err
	}
	if parsed.seed != "" || parsed.fromTrace != "" {
		return runSeedRegistryCLI(ctx, registry, parsed, stdout, stderr)
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
	options.MockOverrides = slices.Clone(options.MockOverrides)
	options.CodeChangeFiles = slices.Clone(options.CodeChangeFiles)
	if options.DBBranch != nil {
		branch := *options.DBBranch
		options.DBBranch = &branch
	}
	if entry.OptionsFactory != nil {
		if err = entry.OptionsFactory(ctx, ReplayRegistryContext{Params: params}, &options); err != nil {
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
				options.TraceIDs, err = boundRegistryTraceIDs(ctx, entry.Client, options.TraceIDs, bound, options.OnlyWithAssertions)
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
					options.TraceIDs, err = boundRegistryTraceIDs(ctx, entry.Client, members, bound, options.OnlyWithAssertions)
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
	if parsed.noCodeChange {
		options.DisableCodeChangeCapture = true
		options.CodeChangeDescription = nil
		options.CodeChangeFiles = nil
	}
	if parsed.codeChange != "" {
		data, e := os.ReadFile(parsed.codeChange)
		if e != nil {
			return nil, e
		}
		var change struct {
			Description *string          `json:"description"`
			Files       []CodeChangeFile `json:"files"`
		}
		if e = json.Unmarshal(data, &change); e != nil {
			return nil, e
		}
		if change.Description == nil || change.Files == nil {
			return nil, fmt.Errorf("bitfab: code change requires description and files")
		}
		options.CodeChangeDescription = change.Description
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
	_, err = fmt.Fprintln(stdout, encoded)
	return result, err
}
