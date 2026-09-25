package bitfab

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"strings"
	"testing"
)

var fakeCloudHelper = []byte("import json, sys\nprint(json.dumps({'args': sys.argv[1:]}))\n")

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
	if !strings.Contains(errors.String(), "Run bitfab:setup cloud first") {
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
	}
}

func TestCloudHelpWithoutRunningHelper(t *testing.T) {
	cloudTestRepo(t)
	var output bytes.Buffer
	if _, err := runCloudReplayHelper(context.Background(), nil, []string{"--cloud", "--help"}, &output, io.Discard); err != nil {
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
