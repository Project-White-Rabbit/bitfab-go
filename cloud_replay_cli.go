package bitfab

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
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

func RunCloudReplayCLI(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return runCloudReplayHelper(ctx, cloudReplayHelper, args, stdout, stderr)
}

func runCloudReplayHelper(ctx context.Context, script []byte, args []string, stdout, stderr io.Writer) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	replayCommand, err := json.Marshal([]string{executable})
	if err != nil {
		return err
	}
	file, err := os.CreateTemp("", "bitfab-cloud-replay-*.py")
	if err != nil {
		return err
	}
	helper := file.Name()
	defer os.Remove(helper)
	if _, err := file.Write(script); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, "python3", append([]string{helper}, args...)...)
	command.Env = append(os.Environ(), "BITFAB_REPLAY_COMMAND="+string(replayCommand), "BITFAB_SDK_LANGUAGE=go")
	command.Stdout, command.Stderr = stdout, stderr
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	defer signal.Stop(interrupts)
	err = command.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return fmt.Errorf("bitfab: cloud replay exited %d: %w", exit.ExitCode(), err)
	}
	if errors.Is(err, exec.ErrNotFound) {
		return fmt.Errorf("bitfab: cloud replay needs Python 3.10+ on PATH as python3: %w", err)
	}
	return err
}
