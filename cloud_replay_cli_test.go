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
	err := runCloudReplayHelper(context.Background(), helper, args, &output, io.Discard)
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
