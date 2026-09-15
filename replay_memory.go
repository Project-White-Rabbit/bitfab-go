package bitfab

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const replayGiB int64 = 1024 * 1024 * 1024

type replayMemoryThrottle struct {
	mu                  sync.Mutex
	resident            map[int]int64
	peak, budget, floor int64
	available           func() (int64, bool)
	swap                func() (float64, bool)
	poll                time.Duration
}

func replayMemoryDisabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("BITFAB_REPLAY_MEMORY_THROTTLE"))) {
	case "0", "off", "false", "no":
		return true
	}
	return false
}
func replayEnvBytes(name string, fallback int64) int64 {
	value, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil || value < 1 {
		return fallback
	}
	return value * 1024 * 1024
}
func newReplayMemoryThrottle() *replayMemoryThrottle {
	return &replayMemoryThrottle{resident: map[int]int64{}, budget: replayEnvBytes("BITFAB_REPLAY_CHILD_MEMORY_MB", 2*replayGiB), floor: replayEnvBytes("BITFAB_REPLAY_MEMORY_FLOOR_MB", 2*replayGiB), available: replayAvailableMemory, swap: replaySwapRatio, poll: 2 * time.Second}
}
func (t *replayMemoryThrottle) childBudget() int64 {
	budget := t.budget
	if t.peak > 0 {
		budget = t.peak
	}
	for _, resident := range t.resident {
		budget = max(budget, resident)
	}
	return budget
}
func (t *replayMemoryThrottle) headroom() bool {
	if len(t.resident) == 0 {
		return true
	}
	if swap, known := t.swap(); known && swap >= 0.85 {
		return false
	}
	available, known := t.available()
	if !known {
		return true
	}
	budget := t.childBudget()
	var reserved int64
	for _, resident := range t.resident {
		reserved += max(int64(0), budget-resident)
	}
	return available-reserved-budget >= t.floor
}
func (t *replayMemoryThrottle) admit(ctx context.Context, index int) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		t.mu.Lock()
		allowed := t.headroom()
		if allowed {
			t.resident[index] = 0
		}
		t.mu.Unlock()
		if allowed {
			return nil
		}
		timer := time.NewTimer(t.poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
func (t *replayMemoryThrottle) observe(index int, rss int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.resident[index]; exists {
		t.resident[index] = max(t.resident[index], rss)
	}
}
func (t *replayMemoryThrottle) release(index int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peak = max(t.peak, t.resident[index])
	delete(t.resident, index)
}

func replayReadCommand(args ...string) (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(ctx, args[0], args[1:]...).Output()
	return strings.TrimSpace(string(raw)), err == nil
}
func replayMeminfo() map[string]int64 {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return nil
	}
	values := map[string]int64{}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			value, err := strconv.ParseInt(fields[1], 10, 64)
			if err == nil {
				values[strings.TrimSuffix(fields[0], ":")] = value * 1024
			}
		}
	}
	return values
}
func replayAvailableMemory() (int64, bool) {
	if runtime.GOOS == "darwin" {
		raw, ok := replayReadCommand("sysctl", "-n", "kern.memorystatus_level")
		if !ok {
			return 0, false
		}
		percent, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, false
		}
		raw, ok = replayReadCommand("sysctl", "-n", "hw.memsize")
		if !ok {
			return 0, false
		}
		total, err := strconv.ParseInt(raw, 10, 64)
		return total * min(max(percent, 0), 100) / 100, err == nil
	}
	values := replayMeminfo()
	value, ok := values["MemAvailable"]
	return value, ok
}
func replaySwapRatio() (float64, bool) {
	if runtime.GOOS == "darwin" {
		raw, ok := replayReadCommand("sysctl", "-n", "vm.swapusage")
		if !ok {
			return 0, false
		}
		fields := strings.Fields(strings.ReplaceAll(raw, "=", " "))
		values := map[string]float64{}
		for index := 0; index+1 < len(fields); index++ {
			key := fields[index]
			if key != "total" && key != "used" {
				continue
			}
			value := strings.ToLower(fields[index+1])
			if value == "" {
				continue
			}
			scale := float64(1)
			switch value[len(value)-1] {
			case 'k':
				scale = 1024
				value = value[:len(value)-1]
			case 'm':
				scale = 1024 * 1024
				value = value[:len(value)-1]
			case 'g':
				scale = float64(replayGiB)
				value = value[:len(value)-1]
			}
			number, err := strconv.ParseFloat(value, 64)
			if err == nil {
				values[key] = number * scale
			}
		}
		if values["total"] <= 0 {
			return 0, false
		}
		return values["used"] / values["total"], true
	}
	values := replayMeminfo()
	total := values["SwapTotal"]
	if total <= 0 {
		return 0, false
	}
	return float64(total-values["SwapFree"]) / float64(total), true
}
func replayProcessRSS(pid int) (int64, bool) {
	if runtime.GOOS == "darwin" {
		raw, ok := replayReadCommand("ps", "-o", "rss=", "-p", strconv.Itoa(pid))
		if !ok {
			return 0, false
		}
		value, err := strconv.ParseInt(raw, 10, 64)
		return value * 1024, err == nil
	}
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	return pages * int64(os.Getpagesize()), err == nil
}
