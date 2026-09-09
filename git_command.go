package bitfab

import (
	"context"
	"os"
	"os/exec"
	"time"
)

func runGit(ctx context.Context, cwd string, timeout time.Duration, args ...string) (string, bool) {
	return runGitWithEnv(ctx, cwd, timeout, nil, args...)
}

func runGitWithEnv(ctx context.Context, cwd string, timeout time.Duration, env []string, args ...string) (string, bool) {
	commandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	command := exec.CommandContext(commandCtx, "git", args...)
	command.Dir = cwd
	if len(env) > 0 {
		command.Env = append(os.Environ(), env...)
	}
	output, err := command.Output()
	if err != nil {
		return "", false
	}
	return string(output), true
}
