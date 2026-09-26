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
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	replayCommand, err := json.Marshal([]string{executable})
	if err != nil {
		return nil, err
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
	command.Env = append(os.Environ(), "BITFAB_REPLAY_COMMAND="+string(replayCommand), "BITFAB_SDK_LANGUAGE=go")
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, stderr
	if err := command.Run(); err != nil {
		if output.Len() > 0 {
			fmt.Fprint(stdout, output.String())
		}
		return nil, fmt.Errorf("bitfab: cloud replay failed; see diagnostics and recover using the execution UUID: %w", err)
	}
	if slices.Contains(args, "--help") || slices.Contains(args, "-h") {
		_, err := stdout.Write(output.Bytes())
		return map[string]any{}, err
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
