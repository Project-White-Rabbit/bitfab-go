package bitfab

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
	if err := os.Mkdir(".bitfab", 0700); err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(root, ".bitfab", "cloudReplay.py")
	if err := os.WriteFile(helper, []byte("import json, sys\nprint(json.dumps({'args': sys.argv[1:]}))\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return helper
}

func TestCloudDelegationWithoutRegistry(t *testing.T) {
	cloudTestRepo(t)
	for _, operation := range []string{"status", "watch", "cancel", "cleanup", "dry-run", "detach"} {
		args := []string{"--cloud-" + operation, "id"}
		if !isCloudReplayCommand(args) {
			t.Fatal("not routed")
		}
		result, err := RunReplayCLI(context.Background(), nil, args, io.Discard, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		got := result.(map[string]any)["args"].([]any)
		if got[0] != args[0] || got[1] != args[1] {
			t.Fatalf("wrong args: %v", got)
		}
	}
}
func TestCloudHelpAndMissingSetup(t *testing.T) {
	helper := cloudTestRepo(t)
	if err := os.Remove(helper); err != nil {
		t.Fatal(err)
	}
	if _, err := RunCloudReplayCLI(context.Background(), []string{"--cloud", "--help"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, err := RunCloudReplayCLI(context.Background(), []string{"--cloud-status", "id"}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "setup cloud") {
		t.Fatalf("missing setup: %v", err)
	}
}
func TestCloudRefusesSymlink(t *testing.T) {
	helper := cloudTestRepo(t)
	if err := os.Rename(helper, "other.py"); err != nil {
		t.Fatal(err)
	}
	other, _ := filepath.Abs("other.py")
	if err := os.Symlink(other, helper); err != nil {
		t.Fatal(err)
	}
	if _, err := RunCloudReplayCLI(context.Background(), []string{"--cloud-status", "id"}, io.Discard, io.Discard); err == nil {
		t.Fatal("accepted symlink")
	}
}
func TestCloudHelperFailure(t *testing.T) {
	helper := cloudTestRepo(t)
	if err := os.WriteFile(helper, []byte("import sys\nprint('recover execution', file=sys.stderr)\nraise SystemExit(1)\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var errors bytes.Buffer
	if _, err := RunCloudReplayCLI(context.Background(), []string{"--cloud-status", "id"}, io.Discard, &errors); err == nil {
		t.Fatal("accepted failure")
	}
	if !strings.Contains(errors.String(), "recover execution") {
		t.Fatal("lost diagnostics")
	}
}
