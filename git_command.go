package bitfab

import (
	"context"
	"os/exec"
	"time"
)

func runGit(ctx context.Context, cwd string, timeout time.Duration, args ...string) (string, bool) {
	commandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	command := exec.CommandContext(commandCtx, "git", args...)
	command.Dir = cwd
	output, err := command.Output()
	if err != nil {
		return "", false
	}
	return string(output), true
}
