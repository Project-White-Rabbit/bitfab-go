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

//go:embed cloudReplay.mjs
var cloudReplayHelper []byte

func isCloudReplayCommand(args []string) bool {
	for _, arg := range args {
		if arg == "--cloud" || strings.HasPrefix(arg, "--cloud-") {
			return true
		}
	}
	return false
}

func isCloudExecuteCommand(args []string) bool {
	return len(args) == 1 && args[0] == "--cloud-execute"
}

func runNativeCloudExecute(ctx context.Context, stdout, stderr io.Writer) error {
	if code := runCloudExecute(ctx, stdout, stderr); code != 0 {
		return &cloudReplayExitError{code: code}
	}
	return nil
}

func RunCloudReplayCLI(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == checkArgumentsFlag {
		return errors.New("bitfab: cloud replay checks replay options with RunReplayCLI and your registry; route arguments without --cloud to RunReplayCLI, and only --cloud commands to RunCloudReplayCLI")
	}
	if isCloudExecuteCommand(args) {
		return runNativeCloudExecute(ctx, stdout, stderr)
	}
	return runCloudReplayHelper(ctx, cloudReplayHelper, args, stdout, stderr, nil)
}

func cloudReplayArguments(ctx context.Context, helper string, args []string) []string {
	output, err := exec.CommandContext(ctx, "node", append([]string{helper, "--replay-arguments"}, args...)...).Output()
	if err != nil {
		return nil
	}
	var replay []string
	if json.Unmarshal(output, &replay) != nil {
		return nil
	}
	return replay
}

func runCloudReplayHelper(ctx context.Context, script []byte, args []string, stdout, stderr io.Writer, check func([]string) error) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	replayCommand, err := json.Marshal([]string{executable})
	if err != nil {
		return err
	}
	file, err := os.CreateTemp("", "bitfab-cloud-replay-*.mjs")
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
	env := append(os.Environ(), "BITFAB_REPLAY_COMMAND="+string(replayCommand), "BITFAB_SDK_LANGUAGE=go")
	if check != nil {
		if replay := cloudReplayArguments(ctx, helper, args); replay != nil {
			if err := check(replay); err != nil {
				return err
			}
			env = append(env, "BITFAB_REPLAY_ARGUMENTS_CHECKED=1")
		}
	}
	command := exec.CommandContext(ctx, "node", append([]string{helper}, args...)...)
	command.Env = env
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
		return fmt.Errorf("bitfab: cloud replay needs Node.js 18+ on PATH as node: %w", err)
	}
	return err
}
