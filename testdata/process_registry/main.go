package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"

	bitfab "github.com/Project-White-Rabbit/bitfab-go"
)

var calls int

func main() {
	if !slices.Contains(os.Args, "--execute-item") {
		_ = os.Setenv("BITFAB_PARENT_MUTATION", "parent-only")
	}
	client := bitfab.NewClient("key", bitfab.WithServiceURL(os.Getenv("BITFAB_TEST_SERVICE_URL")), bitfab.WithCaptureEnabled(os.Getenv("BITFAB_TEST_HOOK_SPAN") == "1"), bitfab.WithSimulationPlan(false))
	defer client.Close(time.Second)
	registry := bitfab.NewReplayRegistry()
	memory := false
	timeout := time.Duration(0)
	if millis, err := strconv.Atoi(os.Getenv("BITFAB_TEST_CHILD_TIMEOUT_MS")); err == nil {
		timeout = time.Duration(millis) * time.Millisecond
	}
	deliveryTimeout := time.Duration(0)
	if millis, err := strconv.Atoi(os.Getenv("BITFAB_TEST_CHILD_DELIVERY_TIMEOUT_MS")); err == nil {
		deliveryTimeout = time.Duration(millis) * time.Millisecond
	}
	err := registry.Register("pipeline", bitfab.ReplayRegistration{Client: client, TraceFunctionKey: "process", Function: func(ctx context.Context, name string, count int) (map[string]any, error) {
		calls++
		if os.Getenv("BITFAB_PARENT_MUTATION") != "" {
			return nil, fmt.Errorf("parent environment leaked")
		}
		if os.Getenv("BITFAB_TEST_SLEEP") == "1" {
			time.Sleep(10 * time.Second)
		}
		if os.Getenv("BITFAB_TEST_FAIL") == "1" {
			return nil, fmt.Errorf("intentional failure")
		}
		branch := ""
		if value := bitfab.GetCurrentReplayBranch(ctx); value != nil {
			branch = value.DatabaseURL()
		}
		return map[string]any{"pid": os.Getpid(), "calls": calls, "name": name, "count": count, "branch": branch}, nil
	}, Options: bitfab.ReplayOptions{Mock: bitfab.MockNone, DisableCodeChangeCapture: true, Concurrency: &bitfab.ReplayConcurrency{Primitive: "process", Attempts: 2, MaxConcurrency: 2, MemoryThrottle: &memory, ChildTimeout: timeout, ChildDeliveryTimeout: deliveryTimeout, OnItemFinishInChildProcess: func(event bitfab.ReplayItemFinishEvent) {
		if os.Getenv("BITFAB_TEST_HOOK_SPAN") == "1" {
			_, _ = client.Span(context.Background(), "grading", func(ctx context.Context) (any, error) { return "graded", nil })
		}
		if event.Item.TraceID == nil {
			panic("child hook lacks persisted trace ID")
		}
		if file := os.Getenv("BITFAB_TEST_HOOK_FILE"); file != "" {
			handle, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
			if err != nil {
				panic(err)
			}
			fmt.Fprintln(handle, *event.Item.TraceID)
			handle.Close()
		}
		if os.Getenv("BITFAB_TEST_HOOK_PANIC") == "1" {
			panic("intentional hook error")
		}
	}}}})
	if err == nil {
		_, err = bitfab.RunReplayCLI(context.Background(), registry, os.Args[1:], os.Stdout, os.Stderr)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
