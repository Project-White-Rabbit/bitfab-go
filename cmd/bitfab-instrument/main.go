package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/Project-White-Rabbit/bitfab-go/instrument"
)

func main() {
	if len(os.Args) < 2 || (os.Args[1] != "run" && os.Args[1] != "build" && os.Args[1] != "test") {
		fmt.Fprintln(os.Stderr, "usage: bitfab-instrument run|build|test [go arguments]")
		os.Exit(2)
	}
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
	selection, err := instrument.ParseListOptions(os.Args[1], os.Args[2:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	overlay, cleanup, err := instrument.OverlayWithOptions(dir, selection)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	args := append([]string{os.Args[1], "-overlay", overlay}, selection.GoArgs(os.Args[2:])...)
	cmd := exec.Command("go", args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	cleanup()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			os.Exit(exit.ExitCode())
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
