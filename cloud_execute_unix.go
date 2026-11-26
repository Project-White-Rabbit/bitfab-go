//go:build unix

package bitfab

import (
	"os/exec"
	"syscall"
)

func prepareCloudReplayChild(child *exec.Cmd) {
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func signalCloudReplayChild(child *exec.Cmd, force bool) {
	if child.Process == nil {
		return
	}
	signal := syscall.SIGTERM
	if force {
		signal = syscall.SIGKILL
	}
	_ = syscall.Kill(-child.Process.Pid, signal)
}
