//go:build windows

package bitfab

import "os/exec"

func prepareReplayChild(cmd *exec.Cmd) {}
func killReplayChild(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
