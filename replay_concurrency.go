package bitfab

import (
	"fmt"
	"io"
	"time"
)

// ReplayConcurrency selects in-process goroutines or isolated registry command processes.
// Process workers default to four at a time and memory admission enabled.
type ReplayConcurrency struct {
	Attempts                   int
	Primitive                  string
	MaxConcurrency             int
	MemoryThrottle             *bool
	ChildTimeout               time.Duration
	ChildDeliveryTimeout       time.Duration
	OnItemFinishInChildProcess func(ReplayItemFinishEvent)
}

// ReplayItemFinishEvent is emitted in an isolated child after the item is persisted.
type ReplayItemFinishEvent struct {
	TestRunID string     `json:"testRunId"`
	Item      ReplayItem `json:"item"`
}

type replayProcessCommand struct {
	executable string
	args       []string
	environ    []string
	stderr     io.Writer
}

func normalizeReplayConcurrency(options ReplayOptions) (ReplayOptions, error) {
	if options.Concurrency == nil {
		return options, nil
	}
	if options.Attempts != 0 || options.MaxConcurrency != 0 {
		return options, fmt.Errorf("bitfab: concurrency cannot be combined with scalar attempts or max concurrency")
	}
	config := *options.Concurrency
	if config.Primitive == "" {
		config.Primitive = "goroutine"
	}
	if config.Primitive != "goroutine" && config.Primitive != "process" {
		return options, fmt.Errorf("bitfab: concurrency primitive must be goroutine or process")
	}
	if config.Attempts == 0 {
		config.Attempts = 1
	}
	if config.MaxConcurrency == 0 {
		config.MaxConcurrency = defaultReplayConcurrency
		if config.Primitive == "process" {
			config.MaxConcurrency = 4
		}
	}
	if config.Attempts < 1 || config.Attempts > 100 || config.MaxConcurrency < 1 {
		return options, fmt.Errorf("bitfab: concurrency requires attempts 1..100 and positive max concurrency")
	}
	if config.ChildTimeout < 0 {
		return options, fmt.Errorf("bitfab: child timeout must be positive")
	}
	if config.ChildTimeout == 0 {
		config.ChildTimeout = 40 * time.Minute
	}
	if config.ChildDeliveryTimeout < 0 {
		return options, fmt.Errorf("bitfab: child delivery timeout must be positive")
	}
	if config.Primitive != "process" && config.ChildDeliveryTimeout != 0 {
		return options, fmt.Errorf("bitfab: child delivery timeout requires process concurrency, since goroutine concurrency starts no child process")
	}
	if config.Primitive == "process" && config.ChildDeliveryTimeout == 0 {
		config.ChildDeliveryTimeout = replayPersistenceTimeout
	}
	if config.MemoryThrottle == nil {
		enabled := config.Primitive == "process"
		config.MemoryThrottle = &enabled
	}
	if config.Primitive != "process" && (*config.MemoryThrottle || config.OnItemFinishInChildProcess != nil) {
		return options, fmt.Errorf("bitfab: memory throttle and child callbacks require process concurrency")
	}
	if config.Primitive == "process" && options.processCommand == nil {
		return options, fmt.Errorf("bitfab: process concurrency requires RunReplayCLI with a project registry")
	}
	options.Concurrency = &config
	options.Attempts = config.Attempts
	options.MaxConcurrency = config.MaxConcurrency
	return options, nil
}
