package bitfab

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type GitState struct {
	GithubEmail   *string `json:"githubEmail"`
	Branch        *string `json:"branch"`
	CommitSHA     *string `json:"commitSha"`
	BaseSHA       *string `json:"baseSha"`
	ExperimentSHA *string `json:"experimentSha"`
}

const (
	gitStateTimeout    = 30 * time.Second
	disableGitStateEnv = "BITFAB_DISABLE_GIT_STATE"
)

var shaPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

func gitStateText(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func gitStateSHA(value string) *string {
	trimmed := strings.TrimSpace(value)
	if !shaPattern.MatchString(trimmed) {
		return nil
	}
	return &trimmed
}

func (state *GitState) isEmpty() bool {
	return state.GithubEmail == nil &&
		state.Branch == nil &&
		state.CommitSHA == nil &&
		state.BaseSHA == nil &&
		state.ExperimentSHA == nil
}

func resolveWorkingTreeSHA(ctx context.Context, root string) *string {
	dir, err := os.MkdirTemp("", "bitfab-git-")
	if err != nil {
		return nil
	}
	defer os.RemoveAll(dir)

	env := []string{"GIT_INDEX_FILE=" + filepath.Join(dir, "index")}
	if _, ok := runGitWithEnv(ctx, root, gitStateTimeout, env, "read-tree", "HEAD"); !ok {
		return nil
	}
	if _, ok := runGitWithEnv(ctx, root, gitStateTimeout, env, "add", "-A", "--", root); !ok {
		return nil
	}
	out, ok := runGitWithEnv(ctx, root, gitStateTimeout, env, "write-tree")
	if !ok {
		return nil
	}
	return gitStateSHA(out)
}

func resolveGitState(ctx context.Context, cwd string) *GitState {
	refs, ok := runGit(ctx, cwd, gitStateTimeout, "rev-parse", "--show-toplevel", "HEAD", "HEAD^{tree}")
	if !ok {
		return nil
	}
	lines := strings.Split(strings.TrimSpace(refs), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) == "" {
		return nil
	}
	root := strings.TrimSpace(lines[0])

	at := func(index int) string {
		if index < len(lines) {
			return lines[index]
		}
		return ""
	}
	email, _ := runGit(ctx, cwd, gitStateTimeout, "config", "user.email")
	branch, _ := runGit(ctx, cwd, gitStateTimeout, "symbolic-ref", "--short", "-q", "HEAD")

	state := &GitState{
		GithubEmail:   gitStateText(email),
		Branch:        gitStateText(branch),
		CommitSHA:     gitStateSHA(at(1)),
		BaseSHA:       gitStateSHA(at(2)),
		ExperimentSHA: resolveWorkingTreeSHA(ctx, root),
	}
	if state.isEmpty() {
		return nil
	}
	return state
}

func resolvedGitState(ctx context.Context) *GitState {
	if os.Getenv(disableGitStateEnv) != "" {
		return nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return nil
	}
	return resolveGitState(ctx, cwd)
}
