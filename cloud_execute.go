package bitfab

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	cloudWorkflowFile           = "bitfab-replay.yml"
	cloudWorkflowDirectory      = ".github/workflows"
	cloudSnapshotPrefix         = "bitfab-replay/"
	cloudReplayCommandEnv       = "BITFAB_REPLAY_COMMAND"
	cloudCheckCommandEnv        = "BITFAB_REPLAY_CHECK"
	cloudExecutionTargetEnv     = "BITFAB_REPLAY_EXECUTION_TARGET"
	cloudHostedExecutionTarget  = "hosted"
	cloudAPIKeyEnv              = "BITFAB_REPLAY_BITFAB_API_KEY"
	cloudPrintAPIKeyFlag        = "--print-api-key"
	cloudAPIKeyLine             = "@@bitfab:api-key "
	cloudKeyInput               = "key_id"
	cloudKeyTokenPrefix         = "bfcr_"
	cloudDefaultServiceURL      = "https://bitfab.ai"
	cloudOIDCAudience           = "bitfab-cloud-replay"
	cloudAPIKeyProbe            = 120 * time.Second
	cloudRequestVersion         = 4
	cloudPlainRequestVersion    = 3
	cloudOldestDispatched       = 2
	cloudResultLine             = "bitfab-replay-result "
	cloudResultChunk            = 4000
	cloudOutputBegin            = "bitfab-replay-output-begin"
	cloudOutputEnd              = "bitfab-replay-output-end"
	cloudOutputLimit            = 16 * 1024 * 1024
	cloudDiskLimit              = 1024 * 1024 * 1024
	cloudItemErrorLines         = 20
	cloudItemErrorLength        = 500
	cloudSummaryErrorItems      = 100
	cloudEnvFlag                = "--cloud-env"
	cloudCodeChangeFlag         = "--code-change"
	cloudCodeChangeBaseFlag     = "--code-change-base"
	cloudGitTokenEnv            = "BITFAB_REPLAY_GIT_TOKEN"
	cloudFetchTimeout           = 600 * time.Second
	cloudCodeChangePathEnv      = "BITFAB_CODE_CHANGE_PATH"
	cloudCodeChangeMaxFiles     = 60
	cloudCodeChangeMaxFileBytes = 500_000
	cloudCodeChangeMaxTotal     = 2_000_000
	cloudCommandTimeout         = 60 * time.Second
	cloudDetached               = "Detached. A GitHub run that already started continues; follow it with --cloud-watch and the printed execution UUID, or stop it with --cloud-cancel and remove its snapshot branch with --cloud-cleanup once it completes"
	cloudProgressPrefix         = "@@bitfab:progress "
	cloudSecretAdvice           = "Map it in the workflow's replay step env from a GitHub secret instead, then set it with bitfab-replay --cloud-secrets"
)

var (
	cloudWorkflowNames      = []string{cloudWorkflowFile, "bitfab-replay.yaml"}
	cloudItemErrorFields    = []string{"error", "traceError", "trace_error", "replayError", "replay_error"}
	cloudItemIDFields       = []string{"originalTraceId", "original_trace_id", "traceId", "trace_id"}
	cloudExperimentFields   = [][]string{{"experimentId", "experimentId", "experiment_id"}, {"experimentUrl", "experimentUrl", "experiment_url"}, {"testRunId", "testRunId", "test_run_id"}, {"testRunUrl", "testRunUrl", "test_run_url"}, {"attempts", "attempts"}}
	cloudProgressFields     = []string{"input", "result", "originalOutput", "original_output", "selectiveReplay", "selective_replay"}
	cloudReservedPrefixes   = []string{"GITHUB_", "RUNNER_", "ACTIONS_", "BITFAB_REPLAY_"}
	cloudBranchChanges      = []string{"last-branch", "default-branch", "ref"}
	cloudFetchedChanges     = []string{"default-branch", "ref"}
	cloudCheckoutOnly       = []string{"working-tree", "last-branch"}
	cloudUUIDPattern        = regexp.MustCompile(`\A[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\z`)
	cloudSHAPattern         = regexp.MustCompile(`\A[0-9a-f]{40}\z`)
	cloudExperimentLine     = regexp.MustCompile(`\A\[replay\] Experiment ([0-9a-f-]{36}):`)
	cloudSecretReference    = regexp.MustCompile(`["']?([A-Za-z_][A-Za-z0-9_]*)["']?[ \t]*:[ \t]*["']?\$\{\{\s*secrets(?:\.([A-Za-z_][A-Za-z0-9_]*)|\[\s*["']([A-Za-z_][A-Za-z0-9_]*)["']\s*\])\s*\}\}`)
	cloudBranchName         = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9._/-]*\z`)
	cloudEnvName            = regexp.MustCompile(`\A[A-Za-z_][A-Za-z0-9_]*\z`)
	cloudSecretEnvName      = regexp.MustCompile(`(?i)(?:^|_)(?:KEY|SECRET|TOKEN|PASSWORD|PASSWD|CREDENTIALS?)(?:_|$)`)
	cloudURLPassword        = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]*:[^/\s@]+@`)
	cloudDescribedRef       = regexp.MustCompile(`\A[^\x00-\x1f\x7f]+\z`)
	cloudWorkflowReference  = regexp.MustCompile(`(\.github/workflows/[^@]+)@`)
	cloudHTTPStatus         = regexp.MustCompile(`\bHTTP (\d{3})\b`)
	cloudLineBreaks         = regexp.MustCompile("\r\n|[\n\r\v\f\x1c\x1d\x1e\u0085  ]")
	errCloudReplayInterrupt = errors.New("interrupted")
)

type cloudReplayExitError struct {
	code int
}

func (failure *cloudReplayExitError) Error() string {
	return fmt.Sprintf("bitfab: cloud replay exited %d", failure.code)
}

func (failure *cloudReplayExitError) ExitCode() int {
	return failure.code
}

type cloudCommandError struct {
	message string
}

func (failure *cloudCommandError) Error() string {
	return failure.message
}

type cloudClient struct {
	apiKey     string
	serviceURL string
}

type cloudFile struct {
	path   string
	before string
	after  string
}

type cloudCodeChange struct {
	description string
	files       []cloudFile
}

type cloudResult struct {
	executionID any
	commit      string
	exitCode    int
	stdout      string
	omitted     int64
	stopped     string
	testRunID   string
}

type cloudTimeout struct {
	minutes float64
	label   string
}

type cloudRunner struct {
	ctx           context.Context
	env           map[string]string
	stdout        io.Writer
	stderr        io.Writer
	output        sync.Mutex
	detached      atomic.Bool
	running       atomic.Bool
	runInterrupts chan struct{}
}

func runCloudExecute(ctx context.Context, stdout, stderr io.Writer) int {
	commands, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &cloudRunner{ctx: commands, env: map[string]string{}, stdout: stdout, stderr: stderr, runInterrupts: make(chan struct{}, 1)}
	for _, entry := range os.Environ() {
		if name, value, ok := strings.Cut(entry, "="); ok && name != "" {
			runner.env[name] = value
		}
	}
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	defer signal.Stop(interrupts)
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		done := ctx.Done()
		for {
			select {
			case <-finished:
				return
			case <-interrupts:
				runner.interrupt(cancel)
			case <-done:
				done = nil
				runner.interrupt(cancel)
			}
		}
	}()
	result, err := runner.execute()
	if runner.detached.Load() {
		runner.writeError("[cloud] " + cloudDetached)
		return 130
	}
	if err != nil {
		runner.writeError("Cloud replay: " + err.Error())
		return 1
	}
	runner.annotate(result)
	return cloudRunnerExitCode(result)
}

func (runner *cloudRunner) interrupt(cancel context.CancelFunc) {
	if runner.running.Load() {
		select {
		case runner.runInterrupts <- struct{}{}:
		default:
		}
		return
	}
	runner.detached.Store(true)
	cancel()
}

func (runner *cloudRunner) say(message string) {
	runner.output.Lock()
	defer runner.output.Unlock()
	io.WriteString(runner.stdout, message+"\n")
}

func (runner *cloudRunner) writeError(message string) {
	runner.output.Lock()
	defer runner.output.Unlock()
	io.WriteString(runner.stderr, message+"\n")
}

func (runner *cloudRunner) warn(message string) {
	runner.say("::warning title=Bitfab replay::" + message)
}

func (runner *cloudRunner) getenv(name string) string {
	return runner.env[name]
}

func (runner *cloudRunner) environ(extra map[string]string) []string {
	merged := map[string]string{}
	for name, value := range runner.env {
		merged[name] = value
	}
	for name, value := range extra {
		merged[name] = value
	}
	entries := make([]string, 0, len(merged))
	for name, value := range merged {
		entries = append(entries, name+"="+value)
	}
	sort.Strings(entries)
	return entries
}

func (runner *cloudRunner) command(args []string, directory string, env []string, timeout time.Duration, input *string) (string, error) {
	ctx, cancel := context.WithTimeout(runner.ctx, timeout)
	defer cancel()
	child := exec.CommandContext(ctx, args[0], args[1:]...)
	child.Dir = directory
	if env == nil {
		env = runner.environ(nil)
	}
	child.Env = env
	if input != nil {
		child.Stdin = strings.NewReader(*input)
	}
	var stdout, stderr bytes.Buffer
	child.Stdout, child.Stderr = &stdout, &stderr
	err := child.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "", fmt.Errorf("Command '%s' timed out after %d seconds", strings.Join(args, " "), int(timeout.Seconds()))
	}
	if runner.ctx.Err() != nil {
		return "", errCloudReplayInterrupt
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		message := fmt.Sprintf("%s %s failed (exit %d", args[0], args[1], exit.ExitCode())
		if status := cloudHTTPStatus.FindStringSubmatch(stderr.String()); status != nil {
			message += ", HTTP " + status[1]
		}
		return "", &cloudCommandError{message: message + "); check authentication and permissions"}
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(stdout.String()), nil
}

func (runner *cloudRunner) git(root string, args ...string) (string, error) {
	return runner.command(append([]string{"git", "-C", root}, args...), "", nil, cloudCommandTimeout, nil)
}

func (runner *cloudRunner) quietGit(root string, args ...string) string {
	output, err := runner.git(root, args...)
	if err != nil {
		return ""
	}
	return output
}

func (runner *cloudRunner) rootDirectory() (string, error) {
	output, err := runner.command([]string{"git", "rev-parse", "--show-toplevel"}, "", nil, cloudCommandTimeout, nil)
	if err != nil {
		return "", err
	}
	return resolveCloudPath(output), nil
}

func resolveCloudPath(path string) string {
	path = filepath.Clean(path)
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return resolved
	}
	parent := filepath.Dir(path)
	if parent == path {
		return path
	}
	return filepath.Join(resolveCloudPath(parent), filepath.Base(path))
}

func cloudRelativePath(value any) (string, error) {
	text, ok := value.(string)
	if !ok || text == "" || strings.Contains(text, `\`) || strings.Contains(text, "\x00") {
		return "", errors.New("Expected a repository-relative path")
	}
	escapes := strings.HasPrefix(text, "/") || strings.HasPrefix(text, "-")
	for _, part := range strings.Split(text, "/") {
		if part == ".." {
			escapes = true
		}
	}
	if escapes {
		return "", errors.New("Paths must stay inside the repository")
	}
	return text, nil
}

func cloudWithin(root string, value any) (string, error) {
	relative, err := cloudRelativePath(value)
	if err != nil {
		return "", err
	}
	path := resolveCloudPath(filepath.Join(root, filepath.FromSlash(relative)))
	inside, err := filepath.Rel(root, path)
	if err != nil || inside == ".." || strings.HasPrefix(inside, ".."+string(filepath.Separator)) {
		return "", errors.New("Paths must stay inside the repository")
	}
	return path, nil
}

func cloudIsFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

func (runner *cloudRunner) replayCommand() ([]string, error) {
	configured, ok := runner.env[cloudReplayCommandEnv]
	if !ok {
		executable, err := os.Executable()
		if err != nil {
			return nil, err
		}
		return []string{executable}, nil
	}
	refused := fmt.Errorf("%s must be a JSON list of the command that starts the replay", cloudReplayCommandEnv)
	parsed, err := parseCloudJSON(configured)
	items, ok := parsed.([]any)
	if err != nil || !ok || len(items) == 0 {
		return nil, refused
	}
	command := make([]string, 0, len(items))
	for _, item := range items {
		part, ok := item.(string)
		if !ok || part == "" {
			return nil, refused
		}
		command = append(command, part)
	}
	return command, nil
}

func cloudFlagValue(args []string, flag string) (string, bool) {
	for index, value := range args {
		if strings.HasPrefix(value, flag+"=") {
			return value[len(flag)+1:], true
		}
		if value == flag && index+1 < len(args) {
			return args[index+1], true
		}
	}
	return "", false
}

func cloudContains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func validateCloudArguments(value any) ([]string, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, errors.New("Replay arguments must be a list of strings")
	}
	args := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, errors.New("Replay arguments must be a list of strings")
		}
		args = append(args, text)
	}
	total := 0
	for _, arg := range args {
		if strings.ContainsAny(arg, "\x00\n") {
			return nil, errors.New("Replay options must be single-line values")
		}
		if strings.HasPrefix(arg, "--cloud") {
			return nil, errors.New("The runner replays locally and never dispatches again")
		}
		total += cloudUTF16Length(arg)
	}
	if total > 16000 {
		return nil, errors.New("Replay options exceed 16000 characters")
	}
	return args, nil
}

func validateCloudEnvironment(value any) (*cloudObject, error) {
	object, ok := value.(*cloudObject)
	if !ok {
		return nil, errors.New("Replay environment must map names to string values")
	}
	for _, name := range object.keys {
		if _, ok := object.values[name].(string); !ok {
			return nil, errors.New("Replay environment must map names to string values")
		}
	}
	total := 0
	for _, name := range object.keys {
		text := object.values[name].(string)
		upper := strings.ToUpper(name)
		if !cloudEnvName.MatchString(name) {
			return nil, fmt.Errorf("%s is not an environment variable name; use letters, digits, and underscores", pythonCloudRepr(name))
		}
		reserved := upper == "CI"
		for _, prefix := range cloudReservedPrefixes {
			reserved = reserved || strings.HasPrefix(upper, prefix)
		}
		if reserved {
			return nil, fmt.Errorf("%s belongs to GitHub Actions or the cloud runner and cannot be set per replay", name)
		}
		if cloudSecretEnvName.MatchString(name) {
			return nil, fmt.Errorf("%s looks like a secret, and %s values are stored in the snapshot commit on GitHub. %s", name, cloudEnvFlag, cloudSecretAdvice)
		}
		if strings.ContainsAny(text, "\x00\n\r") {
			return nil, fmt.Errorf("%s must be a single-line value", name)
		}
		if cloudURLPassword.MatchString(text) {
			return nil, fmt.Errorf("%s holds a URL with a password, and %s values are stored in the snapshot commit on GitHub. %s", name, cloudEnvFlag, cloudSecretAdvice)
		}
		total += cloudUTF16Length(name) + cloudUTF16Length(text)
	}
	if total > 16000 {
		return nil, errors.New("Replay environment exceeds 16000 characters")
	}
	return object, nil
}

func refuseCloudGitCodeChange(args []string, where string) error {
	kind, _ := cloudFlagValue(args, cloudCodeChangeFlag)
	if cloudContains(cloudCheckoutOnly, kind) {
		return fmt.Errorf("%s as it is on GitHub, with no snapshot of a checkout, so --code-change %s has nothing to compare against there; pass --code-change default-branch, --code-change ref with %s, --code-change skip, or --code-change manual with --code-change-file, or start it with bitfab-replay --cloud and no --cloud-ref, which resolves %s from your checkout before upload", where, kind, cloudCodeChangeBaseFlag, kind)
	}
	base, _ := cloudFlagValue(args, cloudCodeChangeBaseFlag)
	if kind == "ref" && !cloudBranchName.MatchString(base) {
		return fmt.Errorf("%s as it is on GitHub, so --code-change ref needs %s naming a branch or commit SHA on GitHub to fetch there", where, cloudCodeChangeBaseFlag)
	}
	return nil
}

func (runner *cloudRunner) runningWorkflow(root string) (string, error) {
	if match := cloudWorkflowReference.FindStringSubmatch(runner.getenv("GITHUB_WORKFLOW_REF")); match != nil {
		return cloudWithin(root, match[1])
	}
	name := cloudWorkflowFile
	for _, candidate := range cloudWorkflowNames {
		path, err := cloudWithin(root, cloudWorkflowDirectory+"/"+candidate)
		if err != nil {
			return "", err
		}
		if cloudIsFile(path) {
			name = candidate
			break
		}
	}
	return cloudWithin(root, cloudWorkflowDirectory+"/"+name)
}

func cloudWorkflowSecrets(text string) *cloudObject {
	secrets := newCloudObject()
	for _, match := range cloudSecretReference.FindAllStringSubmatch(text, -1) {
		secret := match[2]
		if secret == "" {
			secret = match[3]
		}
		secrets.set(match[1], secret)
	}
	return secrets
}

func (runner *cloudRunner) mappedSecrets(root string) (*cloudObject, error) {
	path, err := runner.runningWorkflow(root)
	if err != nil {
		return nil, err
	}
	if !cloudIsFile(path) {
		return newCloudObject(), nil
	}
	text, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return cloudWorkflowSecrets(validCloudText(text)), nil
}

func (runner *cloudRunner) workflowEnvironmentConflicts(root string, values *cloudObject) error {
	mapped, err := runner.mappedSecrets(root)
	if err != nil {
		return err
	}
	var conflicts []string
	for _, name := range values.keys {
		if _, ok := mapped.lookup(name); ok {
			conflicts = append(conflicts, name)
		}
	}
	sort.Strings(conflicts)
	if len(conflicts) > 0 {
		return fmt.Errorf("The workflow already reads %s from a GitHub secret, so a replay cannot set it; update the secret with bitfab-replay --cloud-secrets", strings.Join(conflicts, ", "))
	}
	return nil
}

func splitCloudLines(text string) []string {
	if text == "" {
		return nil
	}
	parts := cloudLineBreaks.Split(text, -1)
	if parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

func (runner *cloudRunner) checkSecrets(root string) error {
	targets, err := runner.mappedSecrets(root)
	if err != nil {
		return err
	}
	var empty []string
	for _, name := range targets.keys {
		if value, ok := runner.env[name]; ok && value == "" {
			empty = append(empty, fmt.Sprintf("%s (secret %s)", name, targets.get(name)))
		}
	}
	if len(empty) > 0 {
		return fmt.Errorf("These replay environment variables are empty on the runner: %s. GitHub passes a secret that does not exist as an empty string. Create each one under Settings, Secrets and variables, Actions, in the repository or in the job's Environment", strings.Join(empty, ", "))
	}
	for _, name := range targets.keys {
		lines := splitCloudLines(runner.env[name])
		if len(lines) > 0 {
			lines = lines[1:]
		}
		for _, line := range lines {
			length := utf8.RuneCountInString(strings.TrimSpace(line))
			if length > 0 && length < 4 {
				runner.warn(name + " has a line of three characters or fewer, such as a lone brace in multi-line JSON, and GitHub masks that text everywhere in this log. Store it on one line; bitfab-replay --cloud-secrets does this for JSON")
				break
			}
		}
	}
	return nil
}

func (runner *cloudRunner) snapshotRequest(root, commit string) (*cloudObject, error) {
	message, err := runner.git(root, "log", "-1", "--format=%B", commit)
	if err != nil {
		return nil, err
	}
	_, body, _ := strings.Cut(message, "\n\n")
	parsed, _ := parseCloudJSON(body)
	request, isObject := parsed.(*cloudObject)
	version, isInteger := float64(0), false
	if isObject {
		version, isInteger = cloudInteger(request.get("version"))
	}
	if !isInteger || (version != cloudPlainRequestVersion && version != cloudRequestVersion) {
		newer := "different"
		if isObject {
			raw, present := request.lookup("version")
			if !present {
				raw = nil
			}
			if number, ok := cloudInteger(raw); ok && number > cloudRequestVersion {
				newer = "newer"
			}
		}
		return nil, fmt.Errorf("This replay was submitted by a %s SDK than the one the snapshot installs; match the SDK you run locally to the one in the lockfile", newer)
	}
	snapshotID := "None"
	if id := request.get("id"); id != nil {
		snapshotID = cloudJSString(id)
	}
	if runner.getenv("GITHUB_REF_NAME") != cloudSnapshotPrefix+snapshotID {
		return nil, errors.New("The runner replays only the snapshot branch its request names; start replays with bitfab-replay --cloud")
	}
	args, err := validateCloudArguments(request.get("args"))
	if err != nil {
		return nil, err
	}
	request.set("args", args)
	if value, ok := request.lookup("env"); ok {
		if _, err := validateCloudEnvironment(value); err != nil {
			return nil, err
		}
	}
	return request, nil
}

func (runner *cloudRunner) dispatchEvent() (*cloudObject, error) {
	path := runner.getenv("GITHUB_EVENT_PATH")
	if path == "" || !cloudIsFile(path) {
		return newCloudObject(), nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	parsed, err := parseCloudJSON(string(data))
	if err != nil {
		return nil, err
	}
	event, ok := parsed.(*cloudObject)
	if !ok {
		return newCloudObject(), nil
	}
	return event, nil
}

func (runner *cloudRunner) dispatchedInputs() (*cloudObject, error) {
	event, err := runner.dispatchEvent()
	if err != nil {
		return nil, err
	}
	inputs, ok := event.get("inputs").(*cloudObject)
	if !ok {
		return newCloudObject(), nil
	}
	return inputs, nil
}

func (runner *cloudRunner) eventDefaultBranch() (string, error) {
	event, err := runner.dispatchEvent()
	if err != nil {
		return "", err
	}
	branch := ""
	if repository, ok := event.get("repository").(*cloudObject); ok {
		branch, _ = repository.get("default_branch").(string)
	}
	if !cloudBranchName.MatchString(branch) {
		return "", errors.New("The run's event names no default branch, so --code-change default-branch has nothing to compare against; pass --code-change ref with --code-change-base")
	}
	return branch, nil
}

func decodeCloudRequest(text string) (any, error) {
	if value, err := parseCloudJSON(text); err == nil {
		return value, nil
	}
	refused := errors.New("The request input is neither JSON nor base64-encoded JSON")
	decoded, err := base64.StdEncoding.DecodeString(text)
	if err != nil || !utf8.Valid(decoded) {
		return nil, refused
	}
	value, err := parseCloudJSON(string(decoded))
	if err != nil {
		return nil, refused
	}
	return value, nil
}

func (runner *cloudRunner) dispatchedRequest() (*cloudObject, error) {
	branch := runner.getenv("GITHUB_REF_NAME")
	inputs, err := runner.dispatchedInputs()
	if err != nil {
		return nil, err
	}
	text := ""
	if raw := inputs.get("request"); cloudTruthy(raw) {
		text = strings.TrimFunc(cloudJSString(raw), cloudJSWhitespace)
	}
	if text == "" || text == "-" {
		return nil, fmt.Errorf("A replay of %s takes its request from the workflow_dispatch request input, and this run has none; start replays with bitfab-replay --cloud, or --cloud --cloud-ref %s to replay the branch as it is", branch, branch)
	}
	decoded, err := decodeCloudRequest(text)
	if err != nil {
		return nil, err
	}
	request, ok := decoded.(*cloudObject)
	if !ok {
		return nil, errors.New("The request input must be a JSON object")
	}
	rawVersion, present := request.lookup("version")
	version := float64(cloudRequestVersion)
	if present {
		number, isInteger := cloudInteger(rawVersion)
		if !isInteger || number < cloudOldestDispatched {
			return nil, fmt.Errorf("Request version %s is not supported; send version %d, or leave version out", pythonCloudRepr(rawVersion), cloudRequestVersion)
		}
		version = number
	}
	if version > cloudRequestVersion {
		return nil, fmt.Errorf("This replay was submitted by a newer SDK than the one %s installs; the request is version %s and this SDK reads up to %d", branch, cloudJSNumber(version), cloudRequestVersion)
	}
	executionID := request.get("id")
	if !cloudTruthy(executionID) {
		executionID = inputs.get("execution_id")
	}
	if !cloudTruthy(executionID) {
		executionID = uuid.NewString()
	}
	id, ok := executionID.(string)
	if !ok || !cloudUUIDPattern.MatchString(id) {
		return nil, errors.New("The request id must be a UUID")
	}
	if inputID := inputs.get("execution_id"); cloudTruthy(inputID) && inputID != any(id) {
		return nil, errors.New("The request id does not match the execution_id input")
	}
	args, err := validateCloudArguments(request.get("args"))
	if err != nil {
		return nil, err
	}
	if err := refuseCloudGitCodeChange(args, "A dispatched replay runs "+branch); err != nil {
		return nil, err
	}
	if request.get("check") == true && !cloudContains(args, "--dry-run") {
		args = append(args, "--dry-run")
	}
	normalized := newCloudObject()
	normalized.set("version", cloudRequestVersion)
	normalized.set("id", id)
	cwd := request.get("cwd")
	if !cloudTruthy(cwd) {
		cwd = "."
	}
	normalized.set("cwd", cwd)
	normalized.set("args", args)
	if timeout := request.get("timeoutMinutes"); timeout != nil {
		minutes, isInteger := cloudInteger(timeout)
		if !isInteger || minutes < 1 || minutes > 7200 {
			return nil, errors.New("timeoutMinutes must be 1..7200")
		}
		normalized.set("timeoutMinutes", minutes)
	}
	if request.get("fullOutput") == true {
		normalized.set("fullOutput", true)
	}
	if environment := request.get("env"); cloudTruthy(environment) && !cloudEmptyObject(environment) {
		if _, err := validateCloudEnvironment(environment); err != nil {
			return nil, err
		}
		normalized.set("env", environment)
	}
	return normalized, nil
}

func (runner *cloudRunner) runnerRequest(root, commit string) (*cloudObject, error) {
	if strings.HasPrefix(runner.getenv("GITHUB_REF_NAME"), cloudSnapshotPrefix) {
		return runner.snapshotRequest(root, commit)
	}
	return runner.dispatchedRequest()
}

func (runner *cloudRunner) checkOutSnapshotBase(root, commit string, branch any) (string, bool, error) {
	base, err := runner.git(root, "rev-parse", "--verify", "-q", commit+"^")
	if err != nil {
		if errors.Is(err, errCloudReplayInterrupt) {
			return "", false, err
		}
		runner.say("This checkout does not include the commit the snapshot was taken on, so the experiment records no code change and the snapshot commit instead of your branch; set fetch-depth: 2 on the workflow's checkout step, or run bitfab-replay --cloud-update")
		return "", false, nil
	}
	if name, ok := branch.(string); ok && cloudBranchName.MatchString(name) {
		if _, err := runner.git(root, "update-ref", "refs/heads/"+name, base); err == nil {
			if _, err := runner.git(root, "symbolic-ref", "HEAD", "refs/heads/"+name); err == nil {
				return base, true, nil
			}
		}
	}
	if _, err := runner.git(root, "reset", "-q", "--soft", base); err != nil {
		return "", false, err
	}
	return base, true, nil
}

func (runner *cloudRunner) gitBlob(root, ref, path string) (string, bool) {
	sizeText, err := runner.git(root, "cat-file", "-s", ref+":"+path)
	if err != nil {
		return "", false
	}
	size, err := strconv.Atoi(sizeText)
	if err != nil || size > cloudCodeChangeMaxFileBytes {
		return "", false
	}
	child := exec.CommandContext(runner.ctx, "git", "-C", root, "show", ref+":"+path)
	child.Env = runner.environ(nil)
	content, err := child.Output()
	if err != nil || bytes.IndexByte(content[:min(len(content), 8000)], 0) >= 0 {
		return "", false
	}
	return strings.ReplaceAll(validCloudText(content), "\r\n", "\n"), true
}

type cloudChangedPath struct {
	status string
	before string
	path   string
}

func (runner *cloudRunner) changedPaths(root, base, commit string) ([]cloudChangedPath, error) {
	raw, err := runner.git(root, "diff", "--name-status", "--find-renames", "-z", base, commit, "--", ":!.bitfab")
	if err != nil {
		return nil, err
	}
	var parts []string
	for _, part := range strings.Split(raw, "\x00") {
		if part != "" {
			parts = append(parts, part)
		}
	}
	var changed []cloudChangedPath
	index := 0
	for index+1 < len(parts) {
		status, before := parts[index][:1], parts[index+1]
		index += 2
		path := before
		if (status == "R" || status == "C") && index < len(parts) {
			path = parts[index]
			index++
		}
		changed = append(changed, cloudChangedPath{status: status, before: before, path: path})
	}
	return changed, nil
}

func (runner *cloudRunner) snapshotCodeChange(root, base, commit, name, against, subjectRef string) (*cloudCodeChange, error) {
	changed, err := runner.changedPaths(root, base, commit)
	if err != nil {
		return nil, err
	}
	var files []cloudFile
	total := 0
	for _, entry := range changed {
		if len(files) >= cloudCodeChangeMaxFiles {
			break
		}
		before, beforeOK := "", true
		if entry.status != "A" {
			before, beforeOK = runner.gitBlob(root, base, entry.before)
		}
		after, afterOK := "", true
		if entry.status != "D" {
			after, afterOK = runner.gitBlob(root, commit, entry.path)
		}
		if !beforeOK || !afterOK || before == after {
			continue
		}
		size := len(before) + len(after)
		if total+size > cloudCodeChangeMaxTotal {
			continue
		}
		total += size
		files = append(files, cloudFile{path: entry.path, before: before, after: after})
	}
	if len(files) == 0 {
		return nil, nil
	}
	head := strings.TrimSpace(name)
	if head == "" {
		if subjectRef == "" {
			subjectRef = base
		}
		head, err = runner.git(root, "log", "-1", "--format=%s", subjectRef)
		if err != nil {
			return nil, err
		}
	}
	if head == "" {
		head = "Working-tree change"
	}
	word := "files"
	if len(files) == 1 {
		word = "file"
	}
	return &cloudCodeChange{description: fmt.Sprintf("%s (%d %s changed %s)", head, len(files), word, against), files: files}, nil
}

func (runner *cloudRunner) branchCodeChange(root, base, commit string, request *cloudObject, args []string) (*cloudCodeChange, error) {
	codeChangeBase, ok := request.get("codeChangeBase").(*cloudObject)
	if !ok {
		return nil, nil
	}
	sha, shaOK := codeChangeBase.get("sha").(string)
	ref, refOK := codeChangeBase.get("ref").(string)
	if !shaOK || !cloudSHAPattern.MatchString(sha) || !refOK || !cloudDescribedRef.MatchString(ref) || cloudUTF16Length(ref) > 200 {
		return nil, nil
	}
	if sha != base && runner.quietGit(root, "rev-parse", "--verify", "-q", commit+"^2") != sha {
		return nil, nil
	}
	name, _ := cloudFlagValue(args, "--name")
	return runner.snapshotCodeChange(root, sha, commit, name, "vs "+ref, base)
}

func (runner *cloudRunner) runnerCodeChange(root, base, commit string, request *cloudObject, args []string) (*cloudCodeChange, error) {
	kind, _ := cloudFlagValue(args, cloudCodeChangeFlag)
	var change *cloudCodeChange
	var err error
	switch {
	case kind == "working-tree":
		name, _ := cloudFlagValue(args, "--name")
		change, err = runner.snapshotCodeChange(root, base, commit, name, "uncommitted (vs HEAD)", "")
	case cloudContains(cloudBranchChanges, kind):
		change, err = runner.branchCodeChange(root, base, commit, request, args)
	default:
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if change == nil {
		change = &cloudCodeChange{}
	}
	return change, nil
}

func (runner *cloudRunner) gitTokenEnvironment(token string) []string {
	extra := map[string]string{"GIT_TERMINAL_PROMPT": "0"}
	if token != "" {
		basic := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + token))
		extra["GIT_CONFIG_COUNT"] = "1"
		extra["GIT_CONFIG_KEY_0"] = "http.https://github.com/.extraheader"
		extra["GIT_CONFIG_VALUE_0"] = "AUTHORIZATION: basic " + basic
	}
	return runner.environ(extra)
}

func (runner *cloudRunner) fetchedBase(root, ref, token string) (string, error) {
	args := []string{"git", "-C", root, "fetch", "-q", "--no-tags"}
	if runner.quietGit(root, "rev-parse", "--is-shallow-repository") == "true" {
		args = append(args, "--unshallow")
	}
	args = append(args, "origin", ref)
	if _, err := runner.command(args, "", runner.gitTokenEnvironment(token), cloudFetchTimeout, nil); err != nil {
		if errors.Is(err, errCloudReplayInterrupt) {
			return "", err
		}
		hint := "check that it exists on GitHub"
		if token == "" {
			hint = "the workflow passes no " + cloudGitTokenEnv + " to fetch it with, so run bitfab-replay --cloud-update and commit the workflow it updates"
		}
		return "", fmt.Errorf("Could not fetch %s from origin to compare against (%s); %s, or pass --code-change skip", ref, err, hint)
	}
	base := runner.quietGit(root, "merge-base", "HEAD", "FETCH_HEAD")
	if base == "" {
		return "", fmt.Errorf("This branch shares no history with %s, so there is nothing to compare against", ref)
	}
	return base, nil
}

func (runner *cloudRunner) dispatchedCodeChange(root, commit string, args []string, token string) (*cloudCodeChange, error) {
	kind, _ := cloudFlagValue(args, cloudCodeChangeFlag)
	if !cloudContains(cloudFetchedChanges, kind) {
		return nil, nil
	}
	ref, _ := cloudFlagValue(args, cloudCodeChangeBaseFlag)
	if kind != "ref" {
		var err error
		if ref, err = runner.eventDefaultBranch(); err != nil {
			return nil, err
		}
	}
	runner.say("Fetching " + ref + " to record the code change against it")
	base, err := runner.fetchedBase(root, ref, token)
	if err != nil {
		return nil, err
	}
	name, _ := cloudFlagValue(args, "--name")
	change, err := runner.snapshotCodeChange(root, base, commit, name, "vs "+ref, commit)
	if err != nil {
		return nil, err
	}
	if change == nil {
		change = &cloudCodeChange{}
	}
	return change, nil
}

func (change *cloudCodeChange) object() *cloudObject {
	object := newCloudObject()
	if change.description != "" {
		object.set("description", change.description)
	}
	files := []any{}
	for _, file := range change.files {
		entry := newCloudObject()
		entry.set("path", file.path)
		entry.set("before", file.before)
		entry.set("after", file.after)
		files = append(files, entry)
	}
	object.set("files", files)
	return object
}

func (runner *cloudRunner) writeCodeChange(change *cloudCodeChange) (string, error) {
	file, err := os.CreateTemp(runner.getenv("RUNNER_TEMP"), "bitfab-code-change-*.json")
	if err != nil {
		return "", err
	}
	defer file.Close()
	if _, err := file.WriteString(compactCloudJSON(change.object())); err != nil {
		return "", err
	}
	return file.Name(), nil
}

func (runner *cloudRunner) warnIfWorkflowOutdated() {
	var problems []string
	if _, ok := runner.env[cloudAPIKeyEnv]; ok {
		problems = append(problems, "still sets "+cloudAPIKeyEnv+" from a secret named by the run's inputs, which makes GitHub give this run every secret in the repository")
	}
	if runner.getenv("ACTIONS_ID_TOKEN_REQUEST_URL") == "" {
		problems = append(problems, "does not grant id-token: write, so replays use the workflow's BITFAB_API_KEY secret instead of running as the person who started them")
	}
	if len(problems) > 0 {
		runner.warn("This workflow " + strings.Join(problems, " and ") + ". Run bitfab-replay --cloud-update and commit the workflow it updates")
	}
}

func (runner *cloudRunner) localClient(directory string, args []string) *cloudClient {
	command, err := runner.replayCommand()
	if err != nil {
		return nil
	}
	empty := ""
	output, err := runner.command(append(append(command, args...), cloudPrintAPIKeyFlag), directory, nil, cloudAPIKeyProbe, &empty)
	if err != nil {
		return nil
	}
	lines := strings.Split(output, "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		line := strings.TrimSuffix(lines[index], "\r")
		if !strings.HasPrefix(line, cloudAPIKeyLine) {
			continue
		}
		parsed, err := parseCloudJSON(line[len(cloudAPIKeyLine):])
		reported, ok := parsed.(*cloudObject)
		if err != nil || !ok {
			return nil
		}
		key, ok := reported.get("apiKey").(string)
		if !ok || strings.TrimSpace(key) == "" {
			return nil
		}
		serviceURL, ok := reported.get("serviceUrl").(string)
		if !ok || strings.TrimSpace(serviceURL) == "" {
			serviceURL = cloudDefaultServiceURL
		}
		return &cloudClient{apiKey: key, serviceURL: strings.TrimRight(serviceURL, "/")}
	}
	return nil
}

func (runner *cloudRunner) bitfabAPI(client cloudClient, path, method string, payload *cloudObject) (any, error) {
	var body io.Reader
	if payload != nil {
		body = strings.NewReader(compactCloudJSON(payload))
	}
	request, err := http.NewRequest(method, client.serviceURL+"/api/sdk/"+path, body)
	if err != nil {
		return nil, &cloudCommandError{message: fmt.Sprintf("Could not reach %s: %s", client.serviceURL, err)}
	}
	if client.apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+client.apiKey)
	}
	request.Header.Set("User-Agent", "bitfab-cloud-replay")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := (&http.Client{Timeout: 60 * time.Second}).Do(request)
	if err != nil {
		return nil, &cloudCommandError{message: fmt.Sprintf("Could not reach %s: %s", client.serviceURL, cloudTransportReason(err))}
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, &cloudCommandError{message: fmt.Sprintf("Could not reach %s: %s", client.serviceURL, err)}
	}
	if response.StatusCode >= 400 {
		message := fmt.Sprintf("Bitfab %s %s failed (HTTP %d)", method, path, response.StatusCode)
		if parsed, err := parseCloudJSON(validCloudText(data)); err == nil {
			if object, ok := parsed.(*cloudObject); ok && cloudTruthy(object.get("error")) {
				message += ": " + cloudJSString(object.get("error"))
			}
		}
		return nil, &cloudCommandError{message: message}
	}
	if strings.TrimSpace(string(data)) == "" {
		return newCloudObject(), nil
	}
	parsed, err := parseCloudJSON(validCloudText(data))
	if err != nil {
		return nil, &cloudCommandError{message: fmt.Sprintf("Bitfab %s %s returned a response that is not JSON", method, path)}
	}
	return parsed, nil
}

func cloudTransportReason(err error) string {
	var failure *url.Error
	if errors.As(err, &failure) {
		return failure.Err.Error()
	}
	return err.Error()
}

func (runner *cloudRunner) githubRunIdentity() (string, error) {
	address := runner.getenv("ACTIONS_ID_TOKEN_REQUEST_URL")
	bearer := runner.getenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
	if address == "" || bearer == "" {
		return "", &cloudCommandError{message: "The workflow does not grant id-token: write, so this run cannot prove to Bitfab which run it is; run bitfab-replay --cloud-update to add it"}
	}
	separator := "?"
	if strings.Contains(address, "?") {
		separator = "&"
	}
	refused := func(reason string) error {
		return &cloudCommandError{message: "GitHub did not issue this run's OIDC token: " + reason}
	}
	request, err := http.NewRequest(http.MethodGet, address+separator+url.Values{"audience": {cloudOIDCAudience}}.Encode(), nil)
	if err != nil {
		return "", refused(err.Error())
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	request.Header.Set("User-Agent", "bitfab-cloud-replay")
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return "", refused(cloudTransportReason(err))
	}
	defer response.Body.Close()
	if response.StatusCode >= 400 {
		return "", refused(http.StatusText(response.StatusCode))
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return "", refused(err.Error())
	}
	parsed, err := parseCloudJSON(validCloudText(data))
	if err != nil {
		return "", refused(err.Error())
	}
	object, ok := parsed.(*cloudObject)
	if !ok {
		return "", refused("the response is not a JSON object")
	}
	value, ok := object.get("value").(string)
	if !ok || value == "" {
		return "", &cloudCommandError{message: "GitHub returned no OIDC token for this run"}
	}
	return value, nil
}

func (runner *cloudRunner) starterKey(directory string, request *cloudObject, args []string) (*cloudClient, error) {
	runner.warnIfWorkflowOutdated()
	if strings.TrimSpace(runner.getenv(cloudAPIKeyEnv)) != "" {
		runner.say("Replaying with the Bitfab API key of the person who started this run")
		return nil, nil
	}
	inputs, err := runner.dispatchedInputs()
	if err != nil {
		return nil, err
	}
	token := ""
	if raw := inputs.get(cloudKeyInput); cloudTruthy(raw) {
		token = strings.TrimFunc(cloudJSString(raw), cloudJSWhitespace)
	}
	if !strings.HasPrefix(token, cloudKeyTokenPrefix) {
		return nil, nil
	}
	shared := "so this run uses the workflow's BITFAB_API_KEY secret"
	local := runner.localClient(directory, args)
	if local == nil {
		runner.warn("The replay's Bitfab client resolved no API key on the runner, " + shared)
		return nil, nil
	}
	identity, err := runner.githubRunIdentity()
	if err != nil {
		runner.warn(err.Error() + ", " + shared)
		return nil, nil
	}
	payload := newCloudObject()
	payload.set("token", token)
	payload.set("oidcToken", identity)
	if timeout := request.get("timeoutMinutes"); cloudTruthy(timeout) {
		payload.set("minutes", timeout)
	}
	redeemed, err := runner.bitfabAPI(cloudClient{serviceURL: local.serviceURL}, "replay/cloudKey/redeem", http.MethodPost, payload)
	if err != nil {
		runner.warn(err.Error() + ", " + shared)
		return nil, nil
	}
	key := ""
	if object, ok := redeemed.(*cloudObject); ok {
		key, _ = object.get("apiKey").(string)
	}
	if strings.TrimSpace(key) == "" {
		runner.warn("Bitfab returned no key for the replay token, " + shared)
		return nil, nil
	}
	runner.say("::add-mask::" + key)
	runner.env[cloudAPIKeyEnv] = key
	runner.say("Replaying as the person who started this run, with a key that is turned off when the replay ends")
	return &cloudClient{apiKey: key, serviceURL: local.serviceURL}, nil
}

func (runner *cloudRunner) revokeStarterKey(starter *cloudClient) {
	if starter == nil {
		return
	}
	if _, err := runner.bitfabAPI(*starter, "replay/cloudKey", http.MethodDelete, nil); err != nil {
		runner.warn("Could not turn off this replay's key (" + err.Error() + "); it expires on its own 30 minutes after the replay's timeout")
	}
}

func cloudShellSplit(text string) ([]string, error) {
	var parts []string
	var current strings.Builder
	started := false
	var quote rune
	characters := []rune(text)
	for index := 0; index < len(characters); index++ {
		character := characters[index]
		if quote == '\'' {
			if character == '\'' {
				quote = 0
			} else {
				current.WriteRune(character)
			}
			continue
		}
		if quote == '"' {
			switch {
			case character == '"':
				quote = 0
			case character == '\\' && index+1 < len(characters) && (characters[index+1] == '"' || characters[index+1] == '\\'):
				current.WriteRune(characters[index+1])
				index++
			default:
				current.WriteRune(character)
			}
			continue
		}
		if strings.ContainsRune(" \t\n\r\v\f", character) {
			if started {
				parts = append(parts, current.String())
				current.Reset()
				started = false
			}
			continue
		}
		started = true
		switch character {
		case '\'', '"':
			quote = character
		case '\\':
			if index+1 >= len(characters) {
				return nil, errors.New("No escaped character")
			}
			current.WriteRune(characters[index+1])
			index++
		default:
			current.WriteRune(character)
		}
	}
	if quote != 0 {
		return nil, errors.New("No closing quotation")
	}
	if started {
		parts = append(parts, current.String())
	}
	return parts, nil
}

func (runner *cloudRunner) runCheckCommand(directory string, environment map[string]string) error {
	check := strings.TrimSpace(runner.getenv(cloudCheckCommandEnv))
	if check == "" {
		return nil
	}
	runner.say("Running " + cloudCheckCommandEnv + ": " + check)
	args, err := cloudShellSplit(check)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return nil
	}
	child := exec.CommandContext(runner.ctx, args[0], args[1:]...)
	child.Dir = directory
	child.Env = runner.environ(environment)
	child.Stdout, child.Stderr = runner.stdout, runner.stderr
	err = child.Run()
	if runner.ctx.Err() != nil {
		return errCloudReplayInterrupt
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		code := exit.ExitCode()
		if code < 0 {
			code = 1
		}
		return fmt.Errorf("%s exited %d; its output is above", cloudCheckCommandEnv, code)
	}
	if err != nil {
		return fmt.Errorf("%s: %s", args[0], err)
	}
	return nil
}

func cloudItemErrored(item *cloudObject) bool {
	for _, field := range cloudItemErrorFields {
		if item.get(field) != nil {
			return true
		}
	}
	return false
}

func cloudCarriedOver(item *cloudObject) bool {
	return item.get("carriedOver") == true || item.get("carried_over") == true
}

func cloudItemErrors(items []*cloudObject) []any {
	errorsFound := []any{}
	for _, item := range items {
		if !cloudItemErrored(item) {
			continue
		}
		var failure any
		for _, field := range cloudItemErrorFields {
			if value := item.get(field); value != nil {
				failure = value
				break
			}
		}
		if object, ok := failure.(*cloudObject); ok {
			if message := object.get("message"); cloudTruthy(message) {
				failure = message
			} else {
				failure = pythonCloudJSON(object)
			}
		}
		trace := "unknown trace"
		for _, field := range cloudItemIDFields {
			if value, ok := item.get(field).(string); ok {
				trace = value
				break
			}
		}
		text := strings.Join(strings.Fields(pythonCloudText(failure)), " ")
		if cloudUTF16Length(text) > cloudItemErrorLength {
			text = cloudUTF16Prefix(text, cloudItemErrorLength) + "..."
		}
		errored := newCloudObject()
		errored.set("originalTraceId", trace)
		errored.set("error", text)
		if attempt, ok := cloudInteger(item.get("attempt")); ok {
			errored.set("attempt", attempt)
		}
		errorsFound = append(errorsFound, errored)
	}
	return errorsFound
}

func cloudItemField(item *cloudObject, camel, snake string) any {
	if value, ok := item.lookup(camel); ok {
		return value
	}
	return item.get(snake)
}

func cloudReplaySummary(parsed *cloudObject) *cloudObject {
	var items, replayed []*cloudObject
	for _, entry := range parsed.get("items").([]any) {
		if item, ok := entry.(*cloudObject); ok {
			items = append(items, item)
			if !cloudCarriedOver(item) {
				replayed = append(replayed, item)
			}
		}
	}
	errored := cloudItemErrors(replayed)
	same, changed, matched, missed := 0, 0, 0, 0
	for _, item := range replayed {
		if cloudItemErrored(item) {
			continue
		}
		equal := compactCloudJSON(item.get("result")) == compactCloudJSON(cloudItemField(item, "originalOutput", "original_output"))
		seeded := cloudItemField(item, "ingestionType", "ingestion_type") == "seeded"
		switch {
		case seeded && equal:
			matched++
		case seeded:
			missed++
		case equal:
			same++
		default:
			changed++
		}
	}
	counts := newCloudObject()
	counts.set("replayed", len(replayed))
	counts.set("carriedOver", len(items)-len(replayed))
	counts.set("same", same)
	counts.set("changed", changed)
	counts.set("matchedExpected", matched)
	counts.set("missedExpected", missed)
	counts.set("errored", len(errored))
	summary := newCloudObject()
	summary.set("cloudSummary", true)
	for _, spellings := range cloudExperimentFields {
		for _, key := range spellings[1:] {
			if value := parsed.get(key); value != nil {
				summary.set(spellings[0], value)
				break
			}
		}
	}
	summary.set("counts", counts)
	summary.set("erroredItems", errored[:min(len(errored), cloudSummaryErrorItems)])
	if len(errored) > cloudSummaryErrorItems {
		summary.set("erroredItemsOmitted", len(errored)-cloudSummaryErrorItems)
	}
	return summary
}

func cloudTrailingJSON(text string) (int, *cloudObject) {
	offsets := []int{0}
	for index := 0; index < len(text); index++ {
		if text[index] == '\n' {
			offsets = append(offsets, index+1)
		}
	}
	for _, offset := range offsets {
		if offset >= len(text) || text[offset] != '{' {
			continue
		}
		parsed, err := parseCloudJSON(text[offset:])
		if err != nil {
			continue
		}
		if object, ok := parsed.(*cloudObject); ok {
			return offset, object
		}
	}
	return -1, nil
}

func cloudResultSummary(stdout string) *cloudObject {
	_, parsed := cloudTrailingJSON(stdout)
	if parsed == nil {
		return nil
	}
	if parsed.get("cloudSummary") == true {
		return parsed
	}
	if _, ok := parsed.get("items").([]any); ok {
		return cloudReplaySummary(parsed)
	}
	return nil
}

func cloudRelayedStdout(output *os.File, code int, summarize bool) (string, int64, error) {
	if summarize {
		data, err := os.ReadFile(output.Name())
		if err != nil {
			return "", 0, err
		}
		if _, parsed := cloudTrailingJSON(validCloudText(data)); parsed != nil {
			if _, ok := parsed.get("items").([]any); ok {
				summary := cloudReplaySummary(parsed)
				summary.set("exitCode", code)
				return compactCloudJSON(summary) + "\n", 0, nil
			}
		}
	}
	info, err := output.Stat()
	if err != nil {
		return "", 0, err
	}
	start := max(0, info.Size()-cloudOutputLimit)
	data := make([]byte, info.Size()-start)
	if _, err := output.ReadAt(data, start); err != nil && err != io.EOF {
		return "", 0, err
	}
	text := validCloudText(data)
	if start == 0 {
		return text, 0, nil
	}
	offset, _ := cloudTrailingJSON(text)
	if offset < 0 {
		offset = 0
	}
	return text[offset:], start + int64(offset), nil
}

func cloudField(value any, name string) any {
	if object, ok := value.(*cloudObject); ok {
		return object.field(name)
	}
	return cloudMissing{}
}

func cloudEmptyObject(value any) bool {
	object, ok := value.(*cloudObject)
	return ok && len(object.keys) == 0
}

func cloudSummaryMarkdown(result cloudResult) string {
	summary := cloudResultSummary(result.stdout)
	if summary == nil {
		summary = newCloudObject()
	}
	counts := summary.get("counts")
	var testRun any = result.testRunID
	if result.testRunID == "" {
		testRun = summary.field("testRunId")
	}
	lines := []string{"Commit: `" + result.commit + "`", fmt.Sprintf("Exit code: %d", result.exitCode)}
	if cloudTruthy(testRun) {
		lines = append([]string{"Test run: `" + cloudJSString(testRun) + "`"}, lines...)
	}
	if result.stopped != "" {
		lines = append(lines, "Stopped early: "+result.stopped)
	} else if cloudTruthy(cloudField(counts, "replayed")) || cloudTruthy(cloudField(counts, "carriedOver")) {
		lines = append(lines, fmt.Sprintf("Replayed: %s, same: %s, changed: %s, errored: %s", cloudJSString(cloudField(counts, "replayed")), cloudJSString(cloudField(counts, "same")), cloudJSString(cloudField(counts, "changed")), cloudJSString(cloudField(counts, "errored"))))
	}
	text := "### Bitfab replay\n\n" + strings.Join(lines, "\n\n") + "\n"
	errored, _ := summary.get("erroredItems").([]any)
	if len(errored) == 0 {
		return text
	}
	var shown []string
	for _, entry := range errored[:min(len(errored), cloudItemErrorLines)] {
		shown = append(shown, "trace "+cloudJSString(cloudField(entry, "originalTraceId"))+": "+cloudJSString(cloudField(entry, "error")))
	}
	total := cloudField(counts, "errored")
	if _, missing := total.(cloudMissing); missing || total == nil {
		total = 0
	}
	if number := cloudJSToNumber(total); number > float64(len(shown)) {
		shown = append(shown, "and "+cloudJSNumber(number-float64(len(shown)))+" more errored items")
	}
	text += "\n#### Errored items\n\n"
	for _, line := range shown {
		text += "- `" + strings.ReplaceAll(line, "`", "'") + "`\n"
	}
	return text
}

func (result cloudResult) object() *cloudObject {
	object := newCloudObject()
	object.set("executionId", result.executionID)
	object.set("commitSha", result.commit)
	object.set("exitCode", result.exitCode)
	object.set("stdout", result.stdout)
	if result.omitted != 0 {
		object.set("stdoutOmittedBytes", result.omitted)
	}
	if result.stopped != "" {
		object.set("stoppedEarly", result.stopped)
		if result.testRunID != "" {
			object.set("testRunId", result.testRunID)
		}
	}
	return object
}

func (runner *cloudRunner) writeResult(result cloudResult) error {
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	if _, err := writer.Write([]byte(compactCloudJSON(result.object()))); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	encoded := base64.StdEncoding.EncodeToString(compressed.Bytes())
	for start := 0; start < len(encoded); start += cloudResultChunk {
		runner.say(cloudResultLine + encoded[start:min(len(encoded), start+cloudResultChunk)])
	}
	path := runner.getenv("GITHUB_STEP_SUMMARY")
	if path == "" {
		return nil
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(cloudSummaryMarkdown(result))
	return err
}

func (runner *cloudRunner) annotate(result cloudResult) {
	switch {
	case result.stopped != "":
		runner.warn("Replay stopped early: " + result.stopped)
	case result.exitCode != 0:
		runner.warn(fmt.Sprintf("Replay exited %d, so this job exits with it too", result.exitCode))
	}
}

func cloudRunnerExitCode(result cloudResult) int {
	if result.stopped != "" && result.exitCode == 0 {
		return 1
	}
	return result.exitCode
}

func (runner *cloudRunner) withoutItemPayload(line []byte) []byte {
	parsed, err := parseCloudJSON(string(line[len(cloudProgressPrefix):]))
	event, ok := parsed.(*cloudObject)
	if err != nil || !ok {
		return line
	}
	item, ok := event.get("item").(*cloudObject)
	if !ok {
		return line
	}
	kept := newCloudObject()
	for _, key := range item.keys {
		if !cloudContains(cloudProgressFields, key) {
			kept.set(key, item.values[key])
		}
	}
	event.set("item", kept)
	return []byte(cloudProgressPrefix + compactCloudJSON(event) + "\n")
}

func (runner *cloudRunner) forwardStderr(source io.Reader, summarize bool, experiment *string) {
	reader := bufio.NewReader(source)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if summarize && bytes.HasPrefix(line, []byte(cloudProgressPrefix)) {
				line = runner.withoutItemPayload(line)
			}
			text := validCloudText(line)
			runner.output.Lock()
			io.WriteString(runner.stdout, text)
			runner.output.Unlock()
			if *experiment == "" {
				if match := cloudExperimentLine.FindStringSubmatch(text); match != nil {
					*experiment = match[1]
				}
			}
		}
		if err != nil {
			return
		}
	}
}

func stopCloudReplayChild(child *exec.Cmd, exited <-chan struct{}) {
	signalCloudReplayChild(child, false)
	select {
	case <-exited:
	case <-time.After(30 * time.Second):
	}
	signalCloudReplayChild(child, true)
	<-exited
}

func (runner *cloudRunner) runReplay(directory string, args []string, timeout *cloudTimeout, summarize bool, environment map[string]string) (int, string, int64, string, string, error) {
	output, err := os.CreateTemp("", "bitfab-replay-stdout-*")
	if err != nil {
		return 0, "", 0, "", "", err
	}
	defer os.Remove(output.Name())
	defer output.Close()
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		return 0, "", 0, "", "", err
	}
	defer stderrReader.Close()
	extra := map[string]string{}
	for name, value := range environment {
		extra[name] = value
	}
	extra[cloudExecutionTargetEnv] = cloudHostedExecutionTarget
	child := exec.Command(args[0], args[1:]...)
	child.Dir = directory
	child.Env = runner.environ(extra)
	child.Stdout = output
	child.Stderr = stderrWriter
	prepareCloudReplayChild(child)
	terminations := make(chan os.Signal, 1)
	signal.Notify(terminations, syscall.SIGTERM)
	defer signal.Stop(terminations)
	runner.running.Store(true)
	defer runner.running.Store(false)
	if err := child.Start(); err != nil {
		stderrWriter.Close()
		return 0, "", 0, "", "", err
	}
	stderrWriter.Close()
	experiment := ""
	forwarded := make(chan struct{})
	go func() {
		defer close(forwarded)
		runner.forwardStderr(stderrReader, summarize, &experiment)
	}()
	exited := make(chan struct{})
	go func() {
		child.Wait()
		close(exited)
	}()
	var deadline time.Time
	if timeout != nil {
		deadline = time.Now().Add(time.Duration(timeout.minutes * float64(time.Minute)))
	}
	parent := os.Getppid()
	checked := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	stopped := ""
	var failure error
	cancelled := func() {
		stopped = "the GitHub run was cancelled"
		stopCloudReplayChild(child, exited)
	}
watch:
	for {
		select {
		case <-exited:
			break watch
		case <-terminations:
			cancelled()
			break watch
		case <-runner.runInterrupts:
			cancelled()
			break watch
		case <-ticker.C:
			if info, err := output.Stat(); err == nil && info.Size() > cloudDiskLimit {
				failure = errors.New("Replay printed more than 1 GiB to stdout")
				stopCloudReplayChild(child, exited)
				break watch
			}
			if timeout != nil && !time.Now().Before(deadline) {
				stopped = fmt.Sprintf("it reached the %s-minute --cloud-timeout", timeout.label)
				stopCloudReplayChild(child, exited)
				break watch
			}
			if time.Since(checked) >= time.Second {
				checked = time.Now()
				if os.Getppid() != parent {
					cancelled()
					break watch
				}
			}
		}
	}
	signalCloudReplayChild(child, true)
	select {
	case <-forwarded:
	case <-time.After(5 * time.Second):
		stderrReader.Close()
		<-forwarded
	}
	if failure != nil {
		return 0, "", 0, "", "", failure
	}
	code := child.ProcessState.ExitCode()
	if code < 0 {
		code = 1
	}
	text, omitted, err := cloudRelayedStdout(output, code, summarize)
	if err != nil {
		return 0, "", 0, "", "", err
	}
	return code, text, omitted, stopped, experiment, nil
}

func cloudRequestTimeout(value any) *cloudTimeout {
	if value == nil {
		return nil
	}
	minutes := cloudJSToNumber(value)
	if math.IsNaN(minutes) || minutes > 1e9 {
		return nil
	}
	return &cloudTimeout{minutes: max(minutes, -1e9), label: cloudJSString(value)}
}

func (runner *cloudRunner) execute() (cloudResult, error) {
	if runner.getenv("GITHUB_RUN_ATTEMPT") != "1" {
		return cloudResult{}, errors.New("Submit a new replay instead of rerunning an Actions job")
	}
	gitToken := runner.env[cloudGitTokenEnv]
	delete(runner.env, cloudGitTokenEnv)
	root, err := runner.rootDirectory()
	if err != nil {
		return cloudResult{}, err
	}
	commit, ok := runner.env["GITHUB_SHA"]
	if !ok {
		return cloudResult{}, errors.New("'GITHUB_SHA'")
	}
	head, err := runner.git(root, "rev-parse", "HEAD")
	if err != nil {
		return cloudResult{}, err
	}
	if head != commit {
		return cloudResult{}, errors.New("Runner checkout does not match the dispatched commit")
	}
	request, err := runner.runnerRequest(root, commit)
	if err != nil {
		return cloudResult{}, err
	}
	requestArgs, _ := request.get("args").([]string)
	snapshot := strings.HasPrefix(runner.getenv("GITHUB_REF_NAME"), cloudSnapshotPrefix)
	base, hasBase := "", false
	if snapshot {
		if base, hasBase, err = runner.checkOutSnapshotBase(root, commit, request.get("branch")); err != nil {
			return cloudResult{}, err
		}
	}
	directory, err := cloudWithin(root, request.get("cwd"))
	if err != nil {
		return cloudResult{}, err
	}
	if err := runner.checkSecrets(root); err != nil {
		return cloudResult{}, err
	}
	environment := map[string]string{}
	if object, ok := request.get("env").(*cloudObject); ok && len(object.keys) > 0 {
		if err := runner.workflowEnvironmentConflicts(root, object); err != nil {
			return cloudResult{}, err
		}
		names := append([]string{}, object.keys...)
		sort.Strings(names)
		for _, name := range names {
			environment[name] = object.get(name).(string)
		}
		runner.say("Setting " + strings.Join(names, ", ") + " from the request")
	}
	var change *cloudCodeChange
	switch {
	case snapshot && hasBase:
		change, err = runner.runnerCodeChange(root, base, commit, request, requestArgs)
	case !snapshot:
		change, err = runner.dispatchedCodeChange(root, commit, requestArgs, gitToken)
	}
	if err != nil {
		return cloudResult{}, err
	}
	runEnvironment := map[string]string{}
	for name, value := range environment {
		runEnvironment[name] = value
	}
	if change != nil {
		path, err := runner.writeCodeChange(change)
		if err != nil {
			return cloudResult{}, err
		}
		runEnvironment[cloudCodeChangePathEnv] = path
	}
	command, err := runner.replayCommand()
	if err != nil {
		return cloudResult{}, err
	}
	args := append(append([]string{}, command...), requestArgs...)
	if cloudContains(requestArgs, "--dry-run") {
		if err := runner.runCheckCommand(directory, environment); err != nil {
			return cloudResult{}, err
		}
	}
	starter, err := runner.starterKey(directory, request, requestArgs)
	if err != nil {
		return cloudResult{}, err
	}
	if runner.detached.Load() {
		runner.revokeStarterKey(starter)
		return cloudResult{}, errCloudReplayInterrupt
	}
	runner.say(cloudOutputBegin)
	code, stdout, omitted, stopped, experiment, err := runner.runReplay(directory, args, cloudRequestTimeout(request.get("timeoutMinutes")), request.get("fullOutput") != true, runEnvironment)
	runner.say(cloudOutputEnd)
	runner.revokeStarterKey(starter)
	if err != nil {
		return cloudResult{}, err
	}
	result := cloudResult{executionID: request.get("id"), commit: commit, exitCode: code, stdout: stdout, omitted: omitted, stopped: stopped}
	if stopped != "" && cloudUUIDPattern.MatchString(experiment) {
		result.testRunID = experiment
	}
	if err := runner.writeResult(result); err != nil {
		return cloudResult{}, err
	}
	return result, nil
}
