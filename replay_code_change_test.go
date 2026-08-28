package bitfab

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResolveAutoCodeChangeReadsOverrideFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "change.json")
	content := `{"description":"candidate","files":[{"path":"prompt.go","before":"old","after":"new"}]}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write override: %v", err)
	}
	t.Setenv("BITFAB_CODE_CHANGE_PATH", path)
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "")

	change := resolveAutoCodeChange(context.Background(), "ignored")
	if change == nil || change.Description == nil || *change.Description != "candidate" || len(change.Files) != 1 {
		t.Fatalf("change = %#v", change)
	}
}

func TestCaptureCodeChangeFromGitPreservesRename(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	for _, args := range [][]string{
		{"init", "-b", "main"},
		{"config", "user.email", "test@example.com"},
		{"config", "user.name", "Replay Test"},
	} {
		if output, ok := runReplayGit(ctx, root, args...); !ok {
			t.Fatalf("git %v failed: %s", args, output)
		}
	}
	original := "line one\nline two\nline three\nline four\n"
	oldPath := filepath.Join(root, "old.go")
	if err := os.WriteFile(oldPath, []byte(original), 0o600); err != nil {
		t.Fatalf("write original: %v", err)
	}
	for _, args := range [][]string{{"add", "old.go"}, {"commit", "-m", "base"}} {
		if output, ok := runReplayGit(ctx, root, args...); !ok {
			t.Fatalf("git %v failed: %s", args, output)
		}
	}
	newPath := filepath.Join(root, "new.go")
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatalf("rename: %v", err)
	}
	updated := "line one\nline two changed\nline three\nline four\n"
	if err := os.WriteFile(newPath, []byte(updated), 0o600); err != nil {
		t.Fatalf("write renamed file: %v", err)
	}
	if output, ok := runReplayGit(ctx, root, "add", "-A"); !ok {
		t.Fatalf("stage rename: %s", output)
	}

	change := captureCodeChangeFromGit(ctx, root, "rename candidate")
	if change == nil || len(change.Files) != 1 {
		t.Fatalf("change = %#v", change)
	}
	file := change.Files[0]
	if file.Path != "new.go" || file.Before != original || file.After != updated {
		t.Fatalf("file = %#v", file)
	}
	if change.Description == nil || !strings.Contains(*change.Description, "1 file changed vs trunk") {
		t.Fatalf("description = %v", change.Description)
	}
}

func TestReplayAutoCapturesCodeChangeOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "change.json")
	content := `{"description":"automatic","files":[{"path":"workflow.go","before":"a","after":"b"}]}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write override: %v", err)
	}
	t.Setenv("BITFAB_CODE_CHANGE_PATH", path)
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "")
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, nil))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	if _, err := client.Replay(context.Background(), "capture-workflow", func() {}, nil); err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.startBody["codeChangeDescription"] != "automatic" {
		t.Fatalf("start body = %#v", state.startBody)
	}
	files := state.startBody["codeChangeFiles"].([]any)
	if len(files) != 1 || files[0].(map[string]any)["path"] != "workflow.go" {
		t.Fatalf("code change files = %#v", files)
	}
}

func TestReplayCanDisableAutomaticCodeCapture(t *testing.T) {
	path := filepath.Join(t.TempDir(), "change.json")
	if err := os.WriteFile(path, []byte(`{"files":[{"path":"ignored.go","before":"","after":"x"}]}`), 0o600); err != nil {
		t.Fatalf("write override: %v", err)
	}
	t.Setenv("BITFAB_CODE_CHANGE_PATH", path)
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, nil))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	if _, err := client.Replay(
		context.Background(),
		"capture-workflow",
		func() {},
		&ReplayOptions{DisableCodeChangeCapture: true},
	); err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	files, present := state.startBody["codeChangeFiles"]
	if !present || files != nil {
		t.Fatalf("start body = %#v", state.startBody)
	}
}

func TestReplayRejectsBoundFunctionKeyMismatch(t *testing.T) {
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, nil))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	_, err := client.Replay(
		context.Background(),
		"selected-key",
		BindReplayFunction("declared-key", func() {}),
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "declared-key") || !strings.Contains(err.Error(), "selected-key") {
		t.Fatalf("error = %v", err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.startBody != nil {
		t.Fatalf("mismatch started a replay: %#v", state.startBody)
	}
}

func TestResolveAutoCodeChangeCanBeDisabled(t *testing.T) {
	t.Setenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE", "1")
	t.Setenv("BITFAB_CODE_CHANGE_PATH", filepath.Join(t.TempDir(), "missing.json"))
	if change := resolveAutoCodeChange(context.Background(), "ignored"); change != nil {
		t.Fatalf("change = %#v, want nil", change)
	}
}

func TestReadCodeChangeOverrideHandlesMalformedAndPartialFiles(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		wantNil     bool
		wantFiles   bool
		wantMessage string
	}{
		{name: "malformed JSON", content: `{`, wantNil: true},
		{name: "unusable values", content: `{"description":42,"files":"bad"}`, wantNil: true},
		{name: "description only", content: `{"description":"candidate"}`, wantMessage: "candidate"},
		{name: "files only", content: `{"files":[{"path":"a.go","before":"a","after":"b"}]}`, wantFiles: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "change.json")
			if err := os.WriteFile(path, []byte(test.content+"\n"), 0o600); err != nil {
				t.Fatalf("write override: %v", err)
			}
			t.Setenv("BITFAB_CODE_CHANGE_PATH", path)
			change := readCodeChangeOverride()
			if test.wantNil {
				if change != nil {
					t.Fatalf("change = %#v, want nil", change)
				}
				return
			}
			if change == nil {
				t.Fatal("change = nil")
			}
			if test.wantMessage != "" && (change.Description == nil || *change.Description != test.wantMessage) {
				t.Fatalf("description = %v", change.Description)
			}
			if test.wantFiles && len(change.Files) != 1 {
				t.Fatalf("files = %#v", change.Files)
			}
		})
	}
}

func TestCaptureCodeChangeFromGitReturnsNilOutsideRepoAndForCleanTree(t *testing.T) {
	if change := captureCodeChangeFromGit(context.Background(), t.TempDir(), "outside"); change != nil {
		t.Fatalf("outside-repo change = %#v", change)
	}

	root := initReplayGitRepo(t, "main")
	if change := captureCodeChangeFromGit(context.Background(), root, "clean"); change != nil {
		t.Fatalf("clean-tree change = %#v", change)
	}
}

func TestCaptureCodeChangeFromGitCapturesAddsDeletesAndSkipsUnsafeFiles(t *testing.T) {
	root := initReplayGitRepo(t, "main")
	deletedPath := filepath.Join(root, "deleted.txt")
	if err := os.WriteFile(deletedPath, []byte("before delete\r\n"), 0o600); err != nil {
		t.Fatalf("write deleted fixture: %v", err)
	}
	if output, ok := runReplayGit(context.Background(), root, "add", "deleted.txt"); !ok {
		t.Fatalf("git add failed: %s", output)
	}
	if output, ok := runReplayGit(context.Background(), root, "commit", "-m", "add deleted fixture"); !ok {
		t.Fatalf("git commit failed: %s", output)
	}
	if err := os.Remove(deletedPath); err != nil {
		t.Fatalf("remove fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "added.txt"), []byte("new file\r\n"), 0o600); err != nil {
		t.Fatalf("write added fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "binary.dat"), []byte{'a', 0, 'b'}, 0o600); err != nil {
		t.Fatalf("write binary fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "oversized.txt"), []byte(strings.Repeat("x", maxCodeChangeFileBytes+1)), 0o600); err != nil {
		t.Fatalf("write oversized fixture: %v", err)
	}

	change := captureCodeChangeFromGit(context.Background(), root, "mixed changes")
	if change == nil {
		t.Fatal("change = nil")
	}
	files := make(map[string]CodeChangeFile, len(change.Files))
	for _, file := range change.Files {
		files[file.Path] = file
	}
	if len(files) != 2 {
		t.Fatalf("files = %#v", files)
	}
	if files["deleted.txt"].Before != "before delete\n" || files["deleted.txt"].After != "" {
		t.Fatalf("deleted file = %#v", files["deleted.txt"])
	}
	if files["added.txt"].Before != "" || files["added.txt"].After != "new file\n" {
		t.Fatalf("added file = %#v", files["added.txt"])
	}
	if _, exists := files["binary.dat"]; exists {
		t.Fatal("binary file should be excluded")
	}
	if _, exists := files["oversized.txt"]; exists {
		t.Fatal("oversized file should be excluded")
	}
}

func TestResolveCodeChangeBaseHonorsForcedRefAndFallsBackToHead(t *testing.T) {
	root := initReplayGitRepo(t, "main")
	if output, ok := runReplayGit(context.Background(), root, "checkout", "-b", "feature"); !ok {
		t.Fatalf("git checkout failed: %s", output)
	}
	t.Setenv("BITFAB_CODE_CHANGE_BASE", "main")
	base, fromTrunk, ok := resolveCodeChangeBase(context.Background(), root)
	if !ok || !fromTrunk || strings.TrimSpace(base) == "" || base == "main" {
		t.Fatalf("forced base=%q fromTrunk=%t ok=%t", base, fromTrunk, ok)
	}

	headOnly := initReplayGitRepo(t, "feature-only")
	t.Setenv("BITFAB_CODE_CHANGE_BASE", "missing")
	base, fromTrunk, ok = resolveCodeChangeBase(context.Background(), headOnly)
	if !ok || fromTrunk || base != "HEAD" {
		t.Fatalf("fallback base=%q fromTrunk=%t ok=%t", base, fromTrunk, ok)
	}
}

func initReplayGitRepo(t *testing.T, branch string) string {
	t.Helper()
	root := t.TempDir()
	ctx := context.Background()
	for _, args := range [][]string{
		{"init", "-b", branch},
		{"config", "user.email", "test@example.com"},
		{"config", "user.name", "Replay Test"},
	} {
		if output, ok := runReplayGit(ctx, root, args...); !ok {
			t.Fatalf("git %v failed: %s", args, output)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "base.txt"), []byte("base\n"), 0o600); err != nil {
		t.Fatalf("write base fixture: %v", err)
	}
	for _, args := range [][]string{{"add", "base.txt"}, {"commit", "-m", "base"}} {
		if output, ok := runReplayGit(ctx, root, args...); !ok {
			t.Fatalf("git %v failed: %s", args, output)
		}
	}
	return root
}

func TestNormalizeDBBranchOptions(t *testing.T) {
	settings, err := normalizeDBBranchOptions(&DBBranchOptions{MinCU: 2, MaxCU: 8, WarmupSQL: "SELECT 1"})
	if err != nil {
		t.Fatalf("normalize valid options: %v", err)
	}
	encoded, _ := json.Marshal(settings)
	if string(encoded) != `{"maxCu":8,"minCu":2,"warmupSql":"SELECT 1"}` {
		t.Fatalf("settings = %s", encoded)
	}
	for _, options := range []*DBBranchOptions{
		{MinCU: 0.75},
		{MinCU: 4, MaxCU: 2},
		{MinCU: 1, MaxCU: 16},
		{MinCU: 18},
	} {
		if _, err := normalizeDBBranchOptions(options); err == nil {
			t.Fatalf("options %#v should fail", options)
		}
	}
}
