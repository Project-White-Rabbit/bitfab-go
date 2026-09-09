package bitfab

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func initGitStateRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	testGit(t, dir, "init", "-q", "-b", "main")
	testGit(t, dir, "config", "user.email", "ada@example.com")
	testGit(t, dir, "config", "user.name", "Ada Lovelace")
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a\n"), 0o644); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	testGit(t, dir, "add", ".")
	testGit(t, dir, "commit", "-q", "-m", "init")
	return dir
}

func value(t *testing.T, pointer *string) string {
	t.Helper()
	if pointer == nil {
		t.Fatal("expected a value, got nil")
	}
	return *pointer
}

func TestGitStateReadsEmailBranchCommitAndBothTreeSHAs(t *testing.T) {
	dir := initGitStateRepo(t)

	state := resolveGitState(context.Background(), dir)

	if state == nil {
		t.Fatal("expected a git state")
	}
	if got := value(t, state.GithubEmail); got != "ada@example.com" {
		t.Errorf("email = %q", got)
	}
	if got := value(t, state.Branch); got != "main" {
		t.Errorf("branch = %q", got)
	}
	if got, want := value(t, state.CommitSHA), testGit(t, dir, "rev-parse", "HEAD"); got != want {
		t.Errorf("commit = %q, want %q", got, want)
	}
	tree := testGit(t, dir, "rev-parse", "HEAD^{tree}")
	if got := value(t, state.BaseSHA); got != tree {
		t.Errorf("base = %q, want %q", got, tree)
	}
	if got := value(t, state.ExperimentSHA); got != tree {
		t.Errorf("experiment = %q, want %q", got, tree)
	}
}

func TestGitStateMovesExperimentSHAOffBaseWhenDirty(t *testing.T) {
	dir := initGitStateRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("changed\n"), 0o644); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}

	state := resolveGitState(context.Background(), dir)

	if value(t, state.ExperimentSHA) == value(t, state.BaseSHA) {
		t.Error("experiment sha should differ from base on a dirty tree")
	}
}

func TestGitStateCapturesUntrackedFile(t *testing.T) {
	dir := initGitStateRepo(t)
	clean := resolveGitState(context.Background(), dir)
	if err := os.WriteFile(filepath.Join(dir, "brand_new.go"), []byte("package x\n"), 0o644); err != nil {
		t.Fatalf("write brand_new.go: %v", err)
	}

	dirty := resolveGitState(context.Background(), dir)

	if value(t, dirty.ExperimentSHA) == value(t, clean.ExperimentSHA) {
		t.Error("an untracked file should change the experiment sha")
	}
}

func TestGitStateLeavesTheCallersIndexUntouched(t *testing.T) {
	dir := initGitStateRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("changed\n"), 0o644); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "untracked.txt"), []byte("u\n"), 0o644); err != nil {
		t.Fatalf("write untracked.txt: %v", err)
	}

	resolveGitState(context.Background(), dir)

	status := testGit(t, dir, "status", "--porcelain")
	if !strings.Contains(status, "a.txt") || !strings.Contains(status, "?? untracked.txt") {
		t.Errorf("status = %q", status)
	}
	if staged := testGit(t, dir, "diff", "--cached", "--name-only"); staged != "" {
		t.Errorf("index should be untouched, staged = %q", staged)
	}
}

func TestGitStateDiffBetweenBaseAndExperimentIsTheWorkingChange(t *testing.T) {
	dir := initGitStateRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("changed\n"), 0o644); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	state := resolveGitState(context.Background(), dir)

	diff := testGit(t, dir, "diff", "--name-only", value(t, state.BaseSHA), value(t, state.ExperimentSHA))

	if diff != "a.txt" {
		t.Errorf("diff = %q, want a.txt", diff)
	}
}

func TestGitStateIsNilOutsideARepository(t *testing.T) {
	if state := resolveGitState(context.Background(), t.TempDir()); state != nil {
		t.Errorf("expected nil outside a repo, got %#v", state)
	}
}

func TestGitStateReportsDetachedHead(t *testing.T) {
	dir := initGitStateRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte("b\n"), 0o644); err != nil {
		t.Fatalf("write b.txt: %v", err)
	}
	testGit(t, dir, "add", ".")
	testGit(t, dir, "commit", "-q", "-m", "second")
	testGit(t, dir, "checkout", "-q", "--detach", "HEAD")

	state := resolveGitState(context.Background(), dir)

	if state.Branch != nil {
		t.Errorf("branch = %q, want nil on a detached head", *state.Branch)
	}
	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(value(t, state.CommitSHA)) {
		t.Errorf("commit = %q", value(t, state.CommitSHA))
	}
}

func TestGitStateRereadsTheTreeSoALaterReplayIsNotStale(t *testing.T) {
	dir := initGitStateRepo(t)

	before := resolveGitState(context.Background(), dir)
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("changed after the first replay\n"), 0o644); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	after := resolveGitState(context.Background(), dir)

	if value(t, before.ExperimentSHA) == value(t, after.ExperimentSHA) {
		t.Error("a second replay must not reuse the first run's experiment sha")
	}
}

func TestResolvedGitStateIsNilWhenSwitchedOff(t *testing.T) {
	t.Setenv(disableGitStateEnv, "1")

	if state := resolvedGitState(context.Background()); state != nil {
		t.Errorf("expected nil when switched off, got %#v", state)
	}
}
