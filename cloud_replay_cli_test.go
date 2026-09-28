package bitfab

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

var fakeCloudHelper = []byte("import json, os, sys\nprint(json.dumps({'args': sys.argv[1:], 'command': json.loads(os.environ['BITFAB_REPLAY_COMMAND']), 'language': os.environ['BITFAB_SDK_LANGUAGE']}))\n")

func cloudTestRepo(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	if out, err := exec.Command("git", "init", root).CombinedOutput(); err != nil {
		t.Fatalf("%s: %v", out, err)
	}
	t.Chdir(root)
}

func TestCloudShipsHelperInsteadOfReadingRepository(t *testing.T) {
	cloudTestRepo(t)
	var errors bytes.Buffer
	args := []string{"--cloud", "pipeline", "--trace-ids", "22222222-2222-4222-8222-222222222222"}
	if _, err := RunReplayCLI(context.Background(), nil, args, io.Discard, &errors); err == nil {
		t.Fatal("accepted a repository without setup")
	}
	if !strings.Contains(errors.String(), "Run bitfab-replay --cloud-init") {
		t.Fatalf("missing setup message: %s", errors.String())
	}
}

func TestCloudDelegationWithoutRegistry(t *testing.T) {
	cloudTestRepo(t)
	for _, operation := range []string{"status", "watch", "cancel", "cleanup", "dry-run", "detach", "init", "secrets", "execute"} {
		args := []string{"--cloud-" + operation, "id"}
		if !isCloudReplayCommand(args) {
			t.Fatal("not routed")
		}
		result, err := runCloudReplayHelper(context.Background(), fakeCloudHelper, args, io.Discard, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		got := result["args"].([]any)
		if got[0] != args[0] || got[1] != args[1] {
			t.Fatalf("wrong args: %v", got)
		}
		executable, _ := os.Executable()
		if command := result["command"].([]any); len(command) != 1 || command[0] != executable || result["language"] != "go" {
			t.Fatalf("wrong replay command: %v", result)
		}
	}
}

func TestCloudHelpComesFromThePackagedHelper(t *testing.T) {
	cloudTestRepo(t)
	var output bytes.Buffer
	if _, err := RunCloudReplayCLI(context.Background(), []string{"--cloud", "--help"}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "--cloud-init") {
		t.Fatalf("help missing setup flags: %s", output.String())
	}
}

func TestCloudHelperFailure(t *testing.T) {
	cloudTestRepo(t)
	failing := []byte("import sys\nprint('recover execution', file=sys.stderr)\nraise SystemExit(1)\n")
	var errors bytes.Buffer
	if _, err := runCloudReplayHelper(context.Background(), failing, []string{"--cloud-status", "id"}, io.Discard, &errors); err == nil {
		t.Fatal("accepted failure")
	}
	if !strings.Contains(errors.String(), "recover execution") {
		t.Fatal("lost diagnostics")
	}
}

func TestCloudForwardsWorkflowCommandsAndParsesTheResult(t *testing.T) {
	cloudTestRepo(t)
	for _, want := range []string{`{"state": "completed", "conclusion": "success", "testRunId": "run"}`, `{"check": "passed", "resolved": 1, "commitSha": "abc"}`} {
		helper := []byte("import json\nprint('::notice title=x::abc')\nprint(json.dumps(json.loads('" + want + "'), indent=2))\n")
		var output bytes.Buffer
		result, err := runCloudReplayHelper(context.Background(), helper, []string{"--cloud-execute"}, &output, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		var expected map[string]any
		if err := json.Unmarshal([]byte(want), &expected); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("wrong result: %v", result)
		}
		if first, _, _ := strings.Cut(output.String(), "\n"); first != "::notice title=x::abc" {
			t.Fatalf("notice not forwarded: %q", output.String())
		}
	}
}

func TestCloudParsesJSONOnlyOutput(t *testing.T) {
	cloudTestRepo(t)
	helper := []byte("import json\nprint(json.dumps({'state': 'completed'}, indent=2))\n")
	var output bytes.Buffer
	result, err := runCloudReplayHelper(context.Background(), helper, []string{"--cloud", "pipeline"}, &output, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if result["state"] != "completed" || strings.Contains(output.String(), "::") {
		t.Fatalf("wrong result %v or output %q", result, output.String())
	}
}

func TestCloudRejectsOutputWithoutAResult(t *testing.T) {
	cloudTestRepo(t)
	var output bytes.Buffer
	_, err := runCloudReplayHelper(context.Background(), []byte("print('not json')\n"), []string{"--cloud", "pipeline"}, &output, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "did not print a JSON result") {
		t.Fatalf("wrong error: %v", err)
	}
	if !strings.Contains(output.String(), "not json") {
		t.Fatal("lost output")
	}
}
