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
	helper := []byte("import json, os, sys\nopen(" + strconv.Quote(mark) + ", 'w').write(json.dumps({'args': sys.argv[1:], 'command': json.loads(os.environ['BITFAB_REPLAY_COMMAND']), 'language': os.environ['BITFAB_SDK_LANGUAGE']}))\nprint('replay output')\nraise SystemExit(3)\n")
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
		{[]string{"pipeline", "--trace-ids", "t1", "--fail-on-error"}, ""},
		{[]string{"pipeline", "--resume", "e1"}, "flag provided but not defined: -resume"},
		{[]string{"other", "--trace-ids", "t1"}, "other"},
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
	helper := []byte("import json, os, sys\nif sys.argv[1] == '--replay-arguments':\n    print(json.dumps(sys.argv[2:-1]))\n    raise SystemExit(0)\nopen(" + strconv.Quote(mark) + ", 'w').write(os.environ.get('BITFAB_REPLAY_ARGUMENTS_CHECKED', ''))\n")
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

func TestCloudEntryPointRefusesTheOptionCheckInsteadOfLooping(t *testing.T) {
	err := RunCloudReplayCLI(context.Background(), []string{"--check-arguments", "pipeline", "--trace-ids", "t1"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "route arguments without --cloud to RunReplayCLI") {
		t.Fatalf("expected an immediate routing error, got %v", err)
	}
}
