package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/Project-White-Rabbit/bitfab-go/instrument"
)

const usage = `usage: bitfab-instrument [-include-packages=<module>,<module>] run|build|test [go arguments]

-include-packages lists dependency modules, by module path, whose packages are
instrumented like the main module's, so every Client.Trace root records calls
into them. A client that passes WithIncludePackages records only the modules it
and each root's IncludePackages name. It may also follow run,
build, or test. The Bitfab SDK cannot be listed.`

func main() {
	var libraries []string
	args := os.Args[1:]
	for len(args) > 0 && strings.HasPrefix(args[0], "-") {
		flag, value, hasValue := strings.Cut(strings.TrimLeft(args[0], "-"), "=")
		if flag == "h" || flag == "help" {
			fmt.Println(usage)
			os.Exit(0)
		}
		if flag != instrument.IncludePackagesFlag {
			break
		}
		args = args[1:]
		if !hasValue {
			if len(args) == 0 {
				fmt.Fprintln(os.Stderr, usage)
				os.Exit(2)
			}
			value, args = args[0], args[1:]
		}
		libraries = append(libraries, instrument.SplitPackages(value)...)
	}
	if len(args) < 1 || (args[0] != "run" && args[0] != "build" && args[0] != "test") {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	command, goArgs := args[0], args[1:]
	for _, flag := range strings.Fields(os.Getenv("GOFLAGS")) {
		flag = strings.TrimLeft(flag, "\"'")
		if flag == "-overlay" || strings.HasPrefix(flag, "-overlay=") {
			fmt.Fprintln(os.Stderr, "bitfab-instrument: pass -overlay explicitly instead of in GOFLAGS so its files can be composed with instrumentation")
			os.Exit(2)
		}
	}
	dir, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	selection, err := instrument.ParseListOptions(command, goArgs)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	selection.IncludePackages = append(libraries, selection.IncludePackages...)
	build, err := instrument.PrepareBuild(dir, selection)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cmd := exec.Command("go", build.Args(command, goArgs)...)
	if len(build.Env) > 0 {
		cmd.Env = append(os.Environ(), build.Env...)
	}
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	build.Cleanup()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			os.Exit(exit.ExitCode())
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
