//go:build !unix

package bitfab

import "os/exec"

func prepareCloudReplayChild(child *exec.Cmd) {}

func signalCloudReplayChild(child *exec.Cmd, force bool) {
	if child.Process != nil {
		_ = child.Process.Kill()
	}
}
