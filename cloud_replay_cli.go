package bitfab

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
)

//go:embed cloudReplay.py
var cloudReplayHelper []byte

const cloudReplayHelp = "Direct GitHub replay: --cloud PIPELINE --trace-ids UUID[,UUID] [--max-concurrency 1..32] [--cloud-include FILE] [--cloud-dry-run] [--cloud-detach] [--cloud-request-id UUID]. Lifecycle: --cloud-status|--cloud-watch|--cloud-cancel|--cloud-cleanup UUID. Setup: --cloud-init --config FILE, --cloud-secrets --env-file FILE [NAME ...]. Requires git, gh auth login, Python 3.10+, and bitfab:setup cloud."

func isCloudReplayCommand(args []string) bool {
	for _, arg := range args {
		if arg == "--cloud" || strings.HasPrefix(arg, "--cloud-") {
			return true
		}
	}
	return false
}

func RunCloudReplayCLI(ctx context.Context, args []string, stdout, stderr io.Writer) (map[string]any, error) {
	return runCloudReplayHelper(ctx, cloudReplayHelper, args, stdout, stderr)
}

func runCloudReplayHelper(ctx context.Context, script []byte, args []string, stdout, stderr io.Writer) (map[string]any, error) {
	if slices.Contains(args, "--help") || slices.Contains(args, "-h") {
		fmt.Fprintln(stdout, cloudReplayHelp)
		return map[string]any{}, nil
	}
	file, err := os.CreateTemp("", "bitfab-cloud-replay-*.py")
	if err != nil {
		return nil, err
	}
	helper := file.Name()
	defer os.Remove(helper)
	if _, err := file.Write(script); err != nil {
		file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
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
