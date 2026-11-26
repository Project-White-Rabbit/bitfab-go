package bitfab

import (
	"context"
	"errors"
	"os"
	"testing"
)

const replayProgramEnv = "BITFAB_TEST_AS_REPLAY_PROGRAM"

const cloudExecuteProgramEnv = "BITFAB_TEST_AS_CLOUD_EXECUTE"

func TestMain(m *testing.M) {
	if os.Getenv(cloudExecuteProgramEnv) == "1" {
		err := RunCloudReplayCLI(context.Background(), []string{"--cloud-execute"}, os.Stdout, os.Stderr)
		var exit interface{ ExitCode() int }
		if errors.As(err, &exit) {
			os.Exit(exit.ExitCode())
		}
		if err != nil {
			os.Stderr.WriteString(err.Error() + "\n")
			os.Exit(1)
		}
		os.Exit(0)
	}
	if os.Getenv(replayProgramEnv) == "1" {
		if _, err := RunReplayCLI(context.Background(), NewReplayRegistry(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
			os.Stderr.WriteString(err.Error() + "\n")
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Setenv("BITFAB_DISABLE_SIM_PLAN", "1")
	home, err := os.MkdirTemp("", "bitfab-go-test-home-")
	if err != nil {
		panic(err)
	}
	os.Setenv("HOME", home)
	code := m.Run()
	os.RemoveAll(home)
	os.Exit(code)
}
