package bitfab

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReplayProcessesRunFreshWorkersWithOneExperiment(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "registry")
	build := exec.Command("go", "build", "-o", binary, "./testdata/process_registry")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, output)
	}
	for _, mode := range []string{"success", "function-error", "timeout", "hook-error", "dry-run"} {
		t.Run(mode, func(t *testing.T) {
			state := &replayTestServerState{}
			base := replayTestHandler(t, state, replayItems())
			var mu sync.Mutex
			starts, completes, resolves, releases := 0, 0, 0, 0
			server := newLegacyCarrierServer(t, func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				switch r.URL.Path {
				case "/api/sdk/replay/start":
					starts++
				case "/api/sdk/replay/complete":
					completes++
				}
				mu.Unlock()
				switch r.URL.Path {
				case "/api/sdk/replay/resolveDbBranchLease":
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					mu.Lock()
					resolves++
					mu.Unlock()
					branch := fmt.Sprintf("%s-%v", body["traceId"], body["attempt"])
					writeReplayTestJSON(t, w, map[string]any{"lease": map[string]any{"neonBranchId": branch, "databaseUrl": "postgres://" + branch, "envKey": "DATABASE_URL"}})
				case "/api/sdk/replay/releaseDbBranchLease":
					mu.Lock()
					releases++
					mu.Unlock()
					writeReplayTestJSON(t, w, map[string]any{})
				default:
					base(w, r)
				}
			})
			defer server.Close()
			hookFile := filepath.Join(t.TempDir(), "hooks.txt")
			args := []string{"pipeline", "--db-branch"}
			if mode == "dry-run" {
				args = append(args, "--dry-run")
			}
			command := exec.Command(binary, args...)
			command.Env = append(os.Environ(), "BITFAB_TEST_SERVICE_URL="+server.URL, "BITFAB_TEST_HOOK_FILE="+hookFile, "BITFAB_DISABLE_CODE_CHANGE_CAPTURE=1")
			switch mode {
			case "function-error":
				command.Env = append(command.Env, "BITFAB_TEST_FAIL=1")
			case "timeout":
				command.Env = append(command.Env, "BITFAB_TEST_SLEEP=1", "BITFAB_TEST_CHILD_TIMEOUT_MS=200")
			case "hook-error":
				command.Env = append(command.Env, "BITFAB_TEST_HOOK_PANIC=1")
			}
			var stdout, stderr bytes.Buffer
			command.Stdout = &stdout
			command.Stderr = &stderr
			if err := command.Run(); err != nil {
				t.Fatalf("run: %v\n%s\n%s", err, stdout.String(), stderr.String())
			}
			var result ReplayResult
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
				t.Fatalf("decode: %v %s", err, stdout.String())
			}
			if strings.Count(stderr.String(), "@@bitfab:progress") != 8 {
				t.Fatalf("expected exactly one start/finish per item: %s", stderr.String())
			}
			if len(result.Items) != 4 {
				t.Fatalf("items: %+v", result.Items)
			}
			mu.Lock()
			defer mu.Unlock()
			if starts != 1 || completes != 1 {
				t.Fatalf("experiment lifecycle start=%d complete=%d", starts, completes)
			}
			pids := map[float64]bool{}
			traceIDs := map[string]bool{}
			for index, item := range result.Items {
				if item.Attempt != index/2 {
					t.Fatalf("attempt-major order lost: %+v", result.Items)
				}
				if mode == "timeout" || mode == "function-error" {
					if item.Error == nil {
						t.Fatal("missing item error")
					}
					continue
				}
				if mode == "dry-run" {
					if item.TraceID != nil {
						t.Fatal("dry run persisted")
					}
					continue
				}
				if item.Error != nil || item.TraceID == nil {
					t.Fatalf("item: %+v stderr:%s", item, stderr.String())
				}
				traceIDs[*item.TraceID] = true
				value := item.Result.(map[string]any)
				if value["calls"] != float64(1) {
					t.Fatalf("worker reused: %+v", value)
				}
				pids[value["pid"].(float64)] = true
				if !strings.Contains(value["branch"].(string), item.OriginalTraceID) {
					t.Fatalf("wrong branch: %+v", value)
				}
			}
			if mode == "success" || mode == "hook-error" {
				if len(pids) != 4 || len(traceIDs) != 4 || resolves != 4 || releases != 4 {
					t.Fatalf("isolation pids=%v ids=%v resolves=%d releases=%d", pids, traceIDs, resolves, releases)
				}
				raw, err := os.ReadFile(hookFile)
				if err != nil || len(strings.Fields(string(raw))) != 4 {
					t.Fatalf("hooks: %s %v", raw, err)
				}
			}
			if mode == "hook-error" && !strings.Contains(stderr.String(), "intentional hook error") {
				t.Fatal("child hook error not surfaced")
			}
			if mode == "dry-run" && (resolves != 0 || releases != 0) {
				t.Fatal("dry run acquired database")
			}
		})
	}
}

func TestReplayProcessConcurrencyValidation(t *testing.T) {
	enabled := true
	for _, options := range []ReplayOptions{
		{Concurrency: &ReplayConcurrency{Primitive: "process"}},
		{Concurrency: &ReplayConcurrency{Primitive: "unknown"}},
		{Concurrency: &ReplayConcurrency{Primitive: "goroutine", MemoryThrottle: &enabled}},
		{Concurrency: &ReplayConcurrency{Primitive: "goroutine", OnItemFinishInChildProcess: func(ReplayItemFinishEvent) {}}},
		{Concurrency: &ReplayConcurrency{}, Attempts: 2},
		{Concurrency: &ReplayConcurrency{MaxConcurrency: -1}},
	} {
		if _, err := normalizeReplayOptions(&options); err == nil {
			t.Fatalf("accepted invalid concurrency: %+v", options)
		}
	}
}

const (
	replayStallCalm     = "some avg10=35.00 avg60=20.00 avg300=5.00 total=100\nfull avg10=0.40 avg60=0.10 avg300=0.00 total=10\n"
	replayStallCritical = "some avg10=80.00 avg60=60.00 avg300=20.00 total=100\nfull avg10=12.50 avg60=4.00 avg300=1.00 total=10\n"
)

func TestReplayMemoryAdmissionWeighsPressureBeforeSwap(t *testing.T) {
	cases := []struct {
		name                  string
		critical, known       bool
		swap                  float64
		wantSecondChildAdmits bool
	}{
		{"calm pressure ignores full swap", false, true, .99, true},
		{"critical pressure blocks despite free memory", true, true, 0, false},
		{"unreadable pressure falls back to the swap limit", false, false, .9, false},
		{"unreadable pressure admits below the swap limit", false, false, .5, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			throttle := newReplayMemoryThrottle()
			throttle.budget = 100
			throttle.floor = 50
			throttle.poll = time.Millisecond
			throttle.available = func() (int64, bool) { return 1000, true }
			throttle.pressure = func() (bool, bool) { return c.critical, c.known }
			throttle.swap = func() (float64, bool) { return c.swap, true }
			if err := throttle.admit(context.Background(), 0); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			err := throttle.admit(ctx, 1)
			if admitted := err == nil; admitted != c.wantSecondChildAdmits {
				t.Fatalf("second child admitted = %v, want %v", admitted, c.wantSecondChildAdmits)
			}
		})
	}
}

func TestReplayPressureCriticalFromLevel(t *testing.T) {
	cases := []struct {
		raw             string
		critical, known bool
	}{{"1", false, true}, {"2", false, true}, {"4\n", true, true}, {"0", false, false}, {"warn", false, false}, {"", false, false}}
	for _, c := range cases {
		critical, known := replayPressureCriticalFromLevel(c.raw)
		if critical != c.critical || known != c.known {
			t.Errorf("level %q = (%v, %v), want (%v, %v)", c.raw, critical, known, c.critical, c.known)
		}
	}
}

func TestReplayPressureCriticalFromStall(t *testing.T) {
	cases := []struct {
		raw             string
		critical, known bool
	}{
		{replayStallCalm, false, true},
		{replayStallCritical, true, true},
		{"some avg10=90.00 avg60=0.00 avg300=0.00 total=1\n", false, false},
		{"full avg10=abc avg60=0.00 avg300=0.00 total=1\n", false, false},
		{"", false, false},
	}
	for _, c := range cases {
		critical, known := replayPressureCriticalFromStall(c.raw)
		if critical != c.critical || known != c.known {
			t.Errorf("stall %q = (%v, %v), want (%v, %v)", c.raw, critical, known, c.critical, c.known)
		}
	}
}

func TestReplayPressurePrefersTheContainerStallFile(t *testing.T) {
	dir := t.TempDir()
	container := filepath.Join(dir, "memory.pressure")
	host := filepath.Join(dir, "pressure-memory")
	if err := os.WriteFile(host, []byte(replayStallCritical), 0o600); err != nil {
		t.Fatal(err)
	}
	if critical, known := replayPressureCriticalFromStallFiles([]string{filepath.Join(dir, "missing"), host}); !known || !critical {
		t.Fatalf("host fallback = (%v, %v), want (true, true)", critical, known)
	}
	if err := os.WriteFile(container, []byte(replayStallCalm), 0o600); err != nil {
		t.Fatal(err)
	}
	if critical, known := replayPressureCriticalFromStallFiles([]string{container, host}); !known || critical {
		t.Fatalf("container file = (%v, %v), want (false, true)", critical, known)
	}
	if _, known := replayPressureCriticalFromStallFiles([]string{filepath.Join(dir, "missing")}); known {
		t.Fatal("missing stall files reported a reading")
	}
}

func TestReplayMemoryAdmissionReservesBudgetAndCancels(t *testing.T) {
	throttle := newReplayMemoryThrottle()
	throttle.budget = 100
	throttle.floor = 50
	throttle.poll = time.Millisecond
	throttle.available = func() (int64, bool) { return 200, true }
	throttle.swap = func() (float64, bool) { return 0, true }
	throttle.pressure = func() (bool, bool) { return false, true }
	if err := throttle.admit(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := throttle.admit(ctx, 1); err == nil {
		t.Fatal("overcommitted reserved memory")
	}
	throttle.observe(0, 40)
	throttle.release(0)
	if throttle.childBudget() != 40 {
		t.Fatal("observed worker peak not retained")
	}
	if err := throttle.admit(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	throttle.pressure = func() (bool, bool) { return true, true }
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()
	if err := throttle.admit(ctx2, 2); err == nil {
		t.Fatal("critical memory pressure ignored")
	}
	throttle.release(1)
	if err := throttle.admit(context.Background(), 2); err != nil {
		t.Fatal("first worker must make progress")
	}
}
