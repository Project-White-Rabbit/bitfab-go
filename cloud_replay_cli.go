package bitfab

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const cloudReplayHelp = "Direct GitHub replay: --cloud PIPELINE --trace-ids UUID[,UUID] [--max-concurrency 1..32] [--cloud-include FILE] [--cloud-dry-run] [--cloud-detach] [--cloud-request-id UUID]. Lifecycle: --cloud-status|--cloud-watch|--cloud-cancel|--cloud-cleanup UUID. Requires git, gh auth login, Python 3.10+, and bitfab:setup cloud."

func isCloudReplayCommand(args []string) bool {
	for _, arg := range args {
		if arg == "--cloud" || strings.HasPrefix(arg, "--cloud-") {
			return true
		}
	}
	return false
}

// RunCloudReplayCLI runs the repository's direct GitHub helper without loading a replay registry.
// It leaves HEAD, the index, and files untouched and streams the recovery UUID to stderr.
func RunCloudReplayCLI(ctx context.Context, args []string, stdout, stderr io.Writer) (map[string]any, error) {
	if slices.Contains(args, "--help") || slices.Contains(args, "-h") {
		fmt.Fprintln(stdout, cloudReplayHelp)
		return map[string]any{}, nil
	}
	gitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(gitCtx, "git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return nil, fmt.Errorf("bitfab: cloud replay must run inside a Git repository: %w", err)
	}
	root, err := filepath.EvalSymlinks(strings.TrimSpace(string(out)))
	if err != nil {
		return nil, err
	}
	helper := filepath.Join(root, ".bitfab", "cloudReplay.py")
	resolved, err := filepath.EvalSymlinks(helper)
	if err != nil || resolved != helper {
		return nil, fmt.Errorf("bitfab: run bitfab:setup cloud first; expected .bitfab/cloudReplay.py inside this repository")
	}
	info, err := os.Stat(helper)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("bitfab: cloud replay helper must be a regular file")
	}
	command := exec.CommandContext(ctx, "python3", append([]string{helper}, args...)...)
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, stderr
	if err := command.Run(); err != nil {
		if output.Len() > 0 {
			fmt.Fprint(stdout, output.String())
		}
		return nil, fmt.Errorf("bitfab: cloud replay failed; see diagnostics and recover using the execution UUID: %w", err)
	}
	var result map[string]any
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		return nil, err
	}
	if _, err := stdout.Write(output.Bytes()); err != nil {
		return nil, err
	}
	return result, nil
}
