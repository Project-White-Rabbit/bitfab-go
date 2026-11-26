package bitfab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func cloudTestRepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if out, err := exec.Command("git", "init", root).CombinedOutput(); err != nil {
		t.Fatalf("%s: %v", out, err)
	}
	t.Chdir(root)
	return root
}

func TestCloudShipsHelperInsteadOfReadingRepository(t *testing.T) {
	cloudTestRepo(t)
	var errOutput bytes.Buffer
	args := []string{"--cloud", "pipeline", "--trace-ids", "22222222-2222-4222-8222-222222222222"}
	_, err := RunReplayCLI(context.Background(), nil, args, io.Discard, &errOutput)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("expected exit 1, got %v", err)
	}
	if !strings.Contains(errOutput.String(), "Run bitfab-replay --cloud-init") {
		t.Fatalf("missing setup message: %s", errOutput.String())
	}
}

func TestCloudPassesArgumentsAndTheReplayCommand(t *testing.T) {
	root := cloudTestRepo(t)
	mark := filepath.Join(root, "mark.json")
	helper := []byte("import { writeFileSync } from \"node:fs\"\nwriteFileSync(" + strconv.Quote(mark) + ", JSON.stringify({ args: process.argv.slice(2), command: JSON.parse(process.env.BITFAB_REPLAY_COMMAND), language: process.env.BITFAB_SDK_LANGUAGE }))\nconsole.log(\"replay output\")\nprocess.exit(3)\n")
	args := []string{"--cloud", "pipeline", "--trace-ids", "id", "--fail-on-error"}
	var output bytes.Buffer
	err := runCloudReplayHelper(context.Background(), helper, args, &output, io.Discard, nil)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 3 {
		t.Fatalf("expected exit 3, got %v", err)
	}
	if output.String() != "replay output\n" {
		t.Fatalf("output not streamed: %q", output.String())
	}
	data, err := os.ReadFile(mark)
	if err != nil {
		t.Fatal(err)
	}
	var passed struct {
		Args     []string `json:"args"`
		Command  []string `json:"command"`
		Language string   `json:"language"`
	}
	if err := json.Unmarshal(data, &passed); err != nil {
		t.Fatal(err)
	}
	executable, _ := os.Executable()
	if strings.Join(passed.Args, " ") != strings.Join(args, " ") || len(passed.Command) != 1 || passed.Command[0] != executable || passed.Language != "go" {
		t.Fatalf("wrong delegation: %+v", passed)
	}
}

func TestCloudRoutesEveryCloudOption(t *testing.T) {
	for _, operation := range []string{"status", "watch", "cancel", "cleanup", "preview", "detach", "init", "secrets", "execute"} {
		if !isCloudReplayCommand([]string{"--cloud-" + operation, "id"}) {
			t.Fatalf("--cloud-%s not routed", operation)
		}
	}
	if isCloudReplayCommand([]string{"pipeline", "--trace-ids", "x"}) {
		t.Fatal("routed a local replay")
	}
}

func TestCloudHelpComesFromThePackagedHelper(t *testing.T) {
	cloudTestRepo(t)
	var output bytes.Buffer
	if err := RunCloudReplayCLI(context.Background(), []string{"--cloud", "--help"}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "--cloud-init") {
		t.Fatalf("help missing setup flags: %s", output.String())
	}
}

func TestCheckArgumentsParsesReplayOptionsWithoutRunning(t *testing.T) {
	registry := NewReplayRegistry()
	calls := 0
	if err := registry.Register("pipeline", ReplayRegistration{Client: newTestClient("http://127.0.0.1:1"), Function: BindReplayFunction("registry", func(string) { calls++ })}); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		args    []string
		message string
	}{
		{[]string{"pipeline", "--code-change", "skip", "--experiment-group-id", "group-1", "--trace-ids", "t1", "--fail-on-error"}, ""},
		{[]string{"pipeline", "--resume", "e1"}, "flag provided but not defined: -resume"},
		{[]string{"other", "--code-change", "skip", "--experiment-group-id", "group-1", "--trace-ids", "t1"}, "other"},
		{[]string{"pipeline", "--trace-ids", "t1"}, "--code-change is required"},
		{[]string{"pipeline", "--code-change", "manual", "--experiment-group-id", "group-1", "--trace-ids", "t1"}, "--code-change manual needs --code-change-file PATH"},
	} {
		var stdout, stderr bytes.Buffer
		_, err := RunReplayCLI(context.Background(), registry, append([]string{"--check-arguments"}, check.args...), &stdout, &stderr)
		if check.message == "" && err != nil {
			t.Fatalf("%v: %v", check.args, err)
		}
		if check.message != "" && (err == nil || !strings.Contains(err.Error(), check.message)) {
			t.Fatalf("%v: got %v, want %q", check.args, err, check.message)
		}
		if stdout.Len() != 0 {
			t.Fatalf("%v printed %q", check.args, stdout.String())
		}
	}
	if calls != 0 {
		t.Fatal("check ran the replay")
	}
}

func TestCloudChecksReplayOptionsInThisProcessBeforeHandingOff(t *testing.T) {
	root := cloudTestRepo(t)
	mark := filepath.Join(root, "mark.json")
	helper := []byte("import { writeFileSync } from \"node:fs\"\nif (process.argv[2] === \"--replay-arguments\") {\n  console.log(JSON.stringify(process.argv.slice(3, -1)))\n  process.exit(0)\n}\nwriteFileSync(" + strconv.Quote(mark) + ", process.env.BITFAB_REPLAY_ARGUMENTS_CHECKED ?? \"\")\n")
	args := []string{"pipeline", "--trace-ids", "t1", "--cloud"}
	var checked []string
	refused := errors.New("flag provided but not defined: -resume")
	err := runCloudReplayHelper(context.Background(), helper, args, io.Discard, io.Discard, func(replay []string) error {
		checked = replay
		return refused
	})
	if !errors.Is(err, refused) {
		t.Fatalf("expected the check's error, got %v", err)
	}
	if strings.Join(checked, " ") != "pipeline --trace-ids t1" {
		t.Fatalf("checked %v", checked)
	}
	if _, err := os.Stat(mark); err == nil {
		t.Fatal("handed off after the check refused the options")
	}
	if err := runCloudReplayHelper(context.Background(), helper, args, io.Discard, io.Discard, func([]string) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(mark); string(data) != "1" {
		t.Fatalf("the script was not told the options were checked: %q", data)
	}
}

func TestCloudRefusesABadOptionWithTheLocalError(t *testing.T) {
	cloudTestRepo(t)
	registry := NewReplayRegistry()
	if err := registry.Register("pipeline", ReplayRegistration{Client: newTestClient("http://127.0.0.1:1"), Function: BindReplayFunction("registry", func(string) {})}); err != nil {
		t.Fatal(err)
	}
	_, err := RunReplayCLI(context.Background(), registry, []string{"pipeline", "--resume", "e1", "--cloud"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined: -resume") {
		t.Fatalf("expected the local parse error, got %v", err)
	}
}

func TestCloudSaysNodeIsNeededWhenItIsMissing(t *testing.T) {
	root := cloudTestRepo(t)
	t.Setenv("PATH", root)
	err := runCloudReplayHelper(context.Background(), []byte(""), []string{"--cloud-status", "id"}, io.Discard, io.Discard, nil)
	if err == nil || !strings.Contains(err.Error(), "needs Node.js 18+ on PATH as node") {
		t.Fatalf("expected the Node.js requirement, got %v", err)
	}
}

func TestCloudEntryPointRefusesTheOptionCheckInsteadOfLooping(t *testing.T) {
	err := RunCloudReplayCLI(context.Background(), []string{"--check-arguments", "pipeline", "--trace-ids", "t1"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "route arguments without --cloud to RunReplayCLI") {
		t.Fatalf("expected an immediate routing error, got %v", err)
	}
}

func dispatchCloudExecute(t *testing.T, request map[string]any) (string, string, error) {
	t.Helper()
	prepareCloudExecute(t, request)
	var stdout, stderr bytes.Buffer
	err := RunCloudReplayCLI(context.Background(), []string{"--cloud-execute"}, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func prepareCloudExecute(t *testing.T, request map[string]any) {
	t.Helper()
	root := cloudTestRepo(t)
	if out, err := exec.Command("git", "-c", "user.email=test@example.com", "-c", "user.name=test", "commit", "-q", "--allow-empty", "-m", "init").CombinedOutput(); err != nil {
		t.Fatalf("%s: %v", out, err)
	}
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	event, err := json.Marshal(map[string]any{"inputs": map[string]string{"request": string(encoded)}})
	if err != nil {
		t.Fatal(err)
	}
	eventPath := filepath.Join(root, "event.json")
	if err := os.WriteFile(eventPath, event, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITHUB_RUN_ATTEMPT", "1")
	t.Setenv("GITHUB_SHA", strings.TrimSpace(string(head)))
	t.Setenv("GITHUB_REF_NAME", "dev")
	t.Setenv("GITHUB_EVENT_PATH", eventPath)
	t.Setenv("BITFAB_API_KEY", "test")
	t.Setenv(replayProgramEnv, "1")
}

var dispatchedReplay = []string{"pipeline", "--trace-ids", "22222222-2222-4222-8222-222222222222"}

func TestCloudExecuteExitsNonZeroWhenTheReplayFails(t *testing.T) {
	stdout, _, err := dispatchCloudExecute(t, map[string]any{"args": append(append([]string{}, dispatchedReplay...), "--code-change", "skip")})
	var exit interface{ ExitCode() int }
	if !errors.As(err, &exit) || exit.ExitCode() == 0 {
		t.Fatalf("expected a non-zero exit, got %v", err)
	}
	if !strings.Contains(stdout, "Replay exited "+strconv.Itoa(exit.ExitCode())+", so this job exits with it too") {
		t.Fatalf("missing the replay's exit code: %s", stdout)
	}
}

func TestCloudExecuteRefusesADispatchedGitCodeChange(t *testing.T) {
	_, stderr, err := dispatchCloudExecute(t, map[string]any{"args": append(append([]string{}, dispatchedReplay...), "--code-change", "working-tree")})
	var exit interface{ ExitCode() int }
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("expected exit 1, got %v", err)
	}
	if !strings.Contains(stderr, "--code-change working-tree has nothing to compare against") {
		t.Fatalf("missing the refusal: %s", stderr)
	}
}

func TestCloudExecuteChecksTheRequestEnvironment(t *testing.T) {
	_, stderr, err := dispatchCloudExecute(t, map[string]any{"args": append(append([]string{}, dispatchedReplay...), "--code-change", "skip"), "env": map[string]string{"API_TOKEN": "x"}})
	var exit interface{ ExitCode() int }
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("expected exit 1, got %v", err)
	}
	if !strings.Contains(stderr, "API_TOKEN looks like a secret") {
		t.Fatalf("missing the environment check: %s", stderr)
	}
}

func TestReplayCLIRunsCloudExecuteWithoutNode(t *testing.T) {
	prepareCloudExecute(t, map[string]any{"args": append(append([]string{}, dispatchedReplay...), "--code-change", "skip")})
	tools := t.TempDir()
	for _, tool := range []string{"git", "sh"} {
		path, err := exec.LookPath(tool)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(path, filepath.Join(tools, tool)); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", tools)
	if _, err := exec.LookPath("node"); err == nil {
		t.Fatal("node is still on PATH")
	}
	var stdout, stderr bytes.Buffer
	_, err := RunReplayCLI(context.Background(), NewReplayRegistry(), []string{"--cloud-execute"}, &stdout, &stderr)
	var exit interface{ ExitCode() int }
	if !errors.As(err, &exit) || exit.ExitCode() == 0 {
		t.Fatalf("expected the replay's non-zero exit, got %v\n%s\n%s", err, stdout.String(), stderr.String())
	}
	if strings.Contains(err.Error(), "Node.js") || strings.Contains(stderr.String(), "Node.js") {
		t.Fatalf("went through the Node.js helper: %v\n%s", err, stderr.String())
	}
	for _, text := range []string{"bitfab-replay-output-begin", "bitfab-replay-result ", "Replay exited " + strconv.Itoa(exit.ExitCode()) + ", so this job exits with it too"} {
		if !strings.Contains(stdout.String(), text) {
			t.Fatalf("missing %q from the native runner: %s", text, stdout.String())
		}
	}
}
