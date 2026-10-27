package bitfab

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

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

	change := captureCodeChangeFromGit(ctx, root, CodeChangeWorkingTree, "rename candidate")
	if change == nil || len(change.Files) != 1 {
		t.Fatalf("change = %#v", change)
	}
	file := change.Files[0]
	if file.Path != "new.go" || file.Before != original || file.After != updated {
		t.Fatalf("file = %#v", file)
	}
	if change.Description == nil || !strings.Contains(*change.Description, "1 file changed uncommitted (vs HEAD)") {
		t.Fatalf("description = %v", change.Description)
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
		&ReplayOptions{CodeChange: CodeChangeSkip},
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
	if change := captureCodeChangeFromGit(context.Background(), t.TempDir(), CodeChangeWorkingTree, "outside"); change != nil {
		t.Fatalf("outside-repo change = %#v", change)
	}

	root := initReplayGitRepo(t, "main")
	for _, codeChange := range []CodeChangeType{CodeChangeWorkingTree, CodeChangeLastBranch, CodeChangeDefaultBranch} {
		if change := captureCodeChangeFromGit(context.Background(), root, codeChange, "clean"); change != nil {
			t.Fatalf("%s clean-tree change = %#v", codeChange, change)
		}
	}
}

func TestCaptureLastBranchDiffsBranchAgainstDefaultBranch(t *testing.T) {
	root := initReplayGitRepo(t, "main")
	ctx := context.Background()
	if output, ok := runReplayGit(ctx, root, "checkout", "-b", "feature"); !ok {
		t.Fatalf("git checkout failed: %s", output)
	}
	if err := os.WriteFile(filepath.Join(root, "base.txt"), []byte("committed\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if output, ok := runReplayGit(ctx, root, "commit", "-am", "committed experiment"); !ok {
		t.Fatalf("git commit failed: %s", output)
	}

	if change := captureCodeChangeFromGit(ctx, root, CodeChangeWorkingTree, "clean"); change != nil {
		t.Fatalf("working-tree change on a clean tree = %#v", change)
	}
	change := captureCodeChangeFromGit(ctx, root, CodeChangeLastBranch, "clean")
	if change == nil {
		t.Fatal("change = nil")
	}
	want := []CodeChangeFile{{Path: "base.txt", Before: "base\n", After: "committed\n"}}
	if !reflect.DeepEqual(change.Files, want) {
		t.Fatalf("files = %#v", change.Files)
	}
	if change.Description == nil || !strings.Contains(*change.Description, "vs main") {
		t.Fatalf("description = %v", change.Description)
	}
}

func TestCaptureLastBranchIgnoresUnpushedCommitsOnDefaultBranch(t *testing.T) {
	root := initReplayGitRepo(t, "main")
	remote := t.TempDir()
	ctx := context.Background()
	steps := [][]string{
		{"init", "-q", "--bare", remote},
		{"-C", root, "remote", "add", "origin", remote},
		{"-C", root, "push", "-q", "-u", "origin", "main"},
	}
	for _, args := range steps {
		if output, ok := runReplayGit(ctx, root, args...); !ok {
			t.Fatalf("git %v failed: %s", args, output)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "base.txt"), []byte("local\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if output, ok := runReplayGit(ctx, root, "commit", "-am", "unpushed"); !ok {
		t.Fatalf("git commit failed: %s", output)
	}

	if change := captureCodeChangeFromGit(ctx, root, CodeChangeLastBranch, "clean"); change != nil {
		t.Fatalf("change = %#v, want nil", change)
	}
}

func TestCaptureLastAndDefaultBranchOnStackedBranch(t *testing.T) {
	root := initReplayGitRepo(t, "main")
	ctx := context.Background()
	commit := func(branch string, content string) {
		if output, ok := runReplayGit(ctx, root, "checkout", "-b", branch); !ok {
			t.Fatalf("git checkout failed: %s", output)
		}
		if err := os.WriteFile(filepath.Join(root, "base.txt"), []byte(content), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		if output, ok := runReplayGit(ctx, root, "commit", "-am", branch); !ok {
			t.Fatalf("git commit failed: %s", output)
		}
	}
	commit("parent", "parent\n")
	commit("child", "child\n")

	change := captureCodeChangeFromGit(ctx, root, CodeChangeLastBranch, "clean")
	if change == nil {
		t.Fatal("change = nil")
	}
	want := []CodeChangeFile{{Path: "base.txt", Before: "parent\n", After: "child\n"}}
	if !reflect.DeepEqual(change.Files, want) {
		t.Fatalf("files = %#v", change.Files)
	}
	if change.Description == nil || !strings.Contains(*change.Description, "vs parent") {
		t.Fatalf("description = %v", change.Description)
	}

	change = captureCodeChangeFromGit(ctx, root, CodeChangeDefaultBranch, "clean")
	if change == nil {
		t.Fatal("default-branch change = nil")
	}
	want = []CodeChangeFile{{Path: "base.txt", Before: "base\n", After: "child\n"}}
	if !reflect.DeepEqual(change.Files, want) {
		t.Fatalf("default-branch files = %#v", change.Files)
	}
	if change.Description == nil || !strings.Contains(*change.Description, "vs main") {
		t.Fatalf("default-branch description = %v", change.Description)
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

	change := captureCodeChangeFromGit(context.Background(), root, CodeChangeWorkingTree, "mixed changes")
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

func replayCodeChangeStart(t *testing.T, options *ReplayOptions) (map[string]any, error) {
	t.Helper()
	state := &replayTestServerState{}
	server := newLegacyCarrierServer(t, replayTestHandler(t, state, nil))
	defer server.Close()
	client := newTestClient(server.URL)
	defer client.Close(5 * time.Second)

	_, err := client.Replay(context.Background(), "code-change-workflow", func() {}, options)
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.startBody, err
}

func captureReplayWarnings(t *testing.T) *strings.Builder {
	t.Helper()
	var output strings.Builder
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	return &output
}

func TestReplayRefusesInvalidCodeChangeBeforeAnyRequest(t *testing.T) {
	files := []CodeChangeFile{{Path: "a.go", Before: "a", After: "b"}}
	tests := []struct {
		name    string
		options *ReplayOptions
		want    string
	}{
		{name: "nil options", options: nil, want: `bitfab: replay needs a CodeChange naming the code this run tests: ` + codeChangeChoices + `.`},
		{name: "missing", options: &ReplayOptions{}, want: `bitfab: replay needs a CodeChange naming the code this run tests: ` + codeChangeChoices + `.`},
		{name: "unknown", options: &ReplayOptions{CodeChange: "staged"}, want: `bitfab: Unknown CodeChange "staged". Pass ` + codeChangeChoices + `.`},
		{name: "manual without files", options: &ReplayOptions{CodeChange: CodeChangeManual}, want: `bitfab: CodeChange "manual" needs CodeChangeFiles: the path and before/after contents of each edited file.`},
		{name: "files without manual", options: &ReplayOptions{CodeChange: CodeChangeWorkingTree, CodeChangeFiles: files}, want: `bitfab: CodeChangeFiles only applies with CodeChange "manual" (got "working-tree").`},
		{name: "skip with description", options: &ReplayOptions{CodeChange: CodeChangeSkip, CodeChangeDescription: "x"}, want: `bitfab: CodeChange "skip" attaches no code change, so it cannot take a CodeChangeDescription.`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body, err := replayCodeChangeStart(t, test.options)
			if err == nil || err.Error() != test.want {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if body != nil {
				t.Fatalf("refused replay sent a start request: %#v", body)
			}
		})
	}
}

func TestReplaySkipSendsNoCodeChange(t *testing.T) {
	body, err := replayCodeChangeStart(t, &ReplayOptions{CodeChange: CodeChangeSkip})
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if body["codeChangeType"] != "skip" {
		t.Fatalf("code change type = %#v", body["codeChangeType"])
	}
	if _, present := body["codeChangeFiles"]; present {
		t.Fatalf("start body = %#v", body)
	}
	if _, present := body["codeChangeDescription"]; present {
		t.Fatalf("start body = %#v", body)
	}
	if flags := body["invocation"].(map[string]any)["flags"].(map[string]any); flags["code-change"] != "skip" {
		t.Fatalf("invocation flags = %#v", flags)
	}
}

func TestReplayManualSendsCallerFiles(t *testing.T) {
	body, err := replayCodeChangeStart(t, &ReplayOptions{
		CodeChange:            CodeChangeManual,
		CodeChangeDescription: "shorter prompt",
		CodeChangeFiles:       []CodeChangeFile{{Path: "prompt.go", Before: "old", After: "new"}},
	})
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if body["codeChangeType"] != "manual" || body["codeChangeDescription"] != "shorter prompt" {
		t.Fatalf("start body = %#v", body)
	}
	files := body["codeChangeFiles"].([]any)
	if len(files) != 1 || files[0].(map[string]any)["path"] != "prompt.go" {
		t.Fatalf("code change files = %#v", files)
	}
}

func TestReplayWorkingTreeSendsUncommittedChanges(t *testing.T) {
	root := initReplayGitRepo(t, "main")
	if output, ok := runReplayGit(context.Background(), root, "checkout", "-b", "feature"); !ok {
		t.Fatalf("git checkout failed: %s", output)
	}
	if err := os.WriteFile(filepath.Join(root, "base.txt"), []byte("committed\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if output, ok := runReplayGit(context.Background(), root, "commit", "-am", "committed"); !ok {
		t.Fatalf("git commit failed: %s", output)
	}
	if err := os.WriteFile(filepath.Join(root, "new.txt"), []byte("untracked\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	t.Chdir(root)

	body, err := replayCodeChangeStart(t, &ReplayOptions{CodeChange: CodeChangeWorkingTree, Name: "candidate"})
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	files := body["codeChangeFiles"].([]any)
	if len(files) != 1 || files[0].(map[string]any)["path"] != "new.txt" {
		t.Fatalf("code change files = %#v", files)
	}
	if body["codeChangeDescription"] != "candidate (1 file changed uncommitted (vs HEAD))" {
		t.Fatalf("description = %#v", body["codeChangeDescription"])
	}
	if body["codeChangeType"] != "working-tree" {
		t.Fatalf("code change type = %#v", body["codeChangeType"])
	}
}

func TestReplayGitTypesDiffAgainstTheirBase(t *testing.T) {
	root := initReplayGitRepo(t, "main")
	ctx := context.Background()
	for _, branch := range []string{"parent", "child"} {
		if output, ok := runReplayGit(ctx, root, "checkout", "-b", branch); !ok {
			t.Fatalf("git checkout failed: %s", output)
		}
		if err := os.WriteFile(filepath.Join(root, "base.txt"), []byte(branch+"\n"), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		if output, ok := runReplayGit(ctx, root, "commit", "-am", branch); !ok {
			t.Fatalf("git commit failed: %s", output)
		}
	}
	t.Chdir(root)

	tests := []struct {
		codeChange CodeChangeType
		before     string
		against    string
	}{
		{codeChange: CodeChangeLastBranch, before: "parent\n", against: "vs parent"},
		{codeChange: CodeChangeDefaultBranch, before: "base\n", against: "vs main"},
	}
	for _, test := range tests {
		t.Run(string(test.codeChange), func(t *testing.T) {
			body, err := replayCodeChangeStart(t, &ReplayOptions{CodeChange: test.codeChange, CodeChangeDescription: "caller text"})
			if err != nil {
				t.Fatalf("Replay returned error: %v", err)
			}
			files := body["codeChangeFiles"].([]any)
			file := files[0].(map[string]any)
			if len(files) != 1 || file["before"] != test.before || file["after"] != "child\n" {
				t.Fatalf("code change files = %#v", files)
			}
			if body["codeChangeDescription"] != "caller text" {
				t.Fatalf("description = %#v", body["codeChangeDescription"])
			}
			captured := captureCodeChangeFromGit(ctx, root, test.codeChange, "")
			if captured == nil || !strings.Contains(*captured.Description, test.against) {
				t.Fatalf("captured = %#v", captured)
			}
		})
	}
}

func TestReplayGitTypeWarnsWhenNothingChanged(t *testing.T) {
	root := initReplayGitRepo(t, "main")
	t.Chdir(root)
	for _, codeChange := range []CodeChangeType{CodeChangeWorkingTree, CodeChangeLastBranch, CodeChangeDefaultBranch} {
		t.Run(string(codeChange), func(t *testing.T) {
			warnings := captureReplayWarnings(t)
			body, err := replayCodeChangeStart(t, &ReplayOptions{CodeChange: codeChange, CodeChangeDescription: "kept"})
			if err != nil {
				t.Fatalf("Replay returned error: %v", err)
			}
			if _, present := body["codeChangeFiles"]; present {
				t.Fatalf("start body = %#v", body)
			}
			if body["codeChangeDescription"] != "kept" {
				t.Fatalf("description = %#v", body["codeChangeDescription"])
			}
			want := `Bitfab: CodeChange "` + string(codeChange) + `" found no changed files, so this replay has no code change attached.`
			if !strings.Contains(warnings.String(), want) {
				t.Fatalf("warnings = %q", warnings.String())
			}
		})
	}
}

func TestReplayGitTypeUsesCodeChangePathHandoff(t *testing.T) {
	root := initReplayGitRepo(t, "main")
	if err := os.WriteFile(filepath.Join(root, "base.txt"), []byte("ignored local edit\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	t.Chdir(root)
	path := filepath.Join(t.TempDir(), "change.json")
	content := `{"description":"resolved by runner","files":[{"path":"workflow.go","before":"a","after":"b"}]}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write handoff: %v", err)
	}
	t.Setenv("BITFAB_CODE_CHANGE_PATH", path)

	body, err := replayCodeChangeStart(t, &ReplayOptions{CodeChange: CodeChangeLastBranch})
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if body["codeChangeDescription"] != "resolved by runner" {
		t.Fatalf("start body = %#v", body)
	}
	files := body["codeChangeFiles"].([]any)
	if len(files) != 1 || files[0].(map[string]any)["path"] != "workflow.go" {
		t.Fatalf("code change files = %#v", files)
	}
}

func TestReplayGitTypeTreatsEmptyHandoffAsNothingFound(t *testing.T) {
	root := initReplayGitRepo(t, "main")
	if err := os.WriteFile(filepath.Join(root, "base.txt"), []byte("local edit git would find\n"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	t.Chdir(root)
	path := filepath.Join(t.TempDir(), "change.json")
	if err := os.WriteFile(path, []byte(`{"files":[]}`), 0o600); err != nil {
		t.Fatalf("write handoff: %v", err)
	}
	t.Setenv("BITFAB_CODE_CHANGE_PATH", path)
	warnings := captureReplayWarnings(t)

	body, err := replayCodeChangeStart(t, &ReplayOptions{CodeChange: CodeChangeWorkingTree})
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}
	if _, present := body["codeChangeFiles"]; present {
		t.Fatalf("empty handoff fell back to git capture: %#v", body)
	}
	if !strings.Contains(warnings.String(), `Bitfab: CodeChange "working-tree" found no changed files`) {
		t.Fatalf("warnings = %q", warnings.String())
	}
}
