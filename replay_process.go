package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type replayAssignment struct {
	ExperimentID      string           `json:"experimentId"`
	ServerItem        replayServerItem `json:"serverItem"`
	Attempt           int              `json:"attempt"`
	LocalTraceID      string           `json:"localTraceId"`
	ResultPath        string           `json:"resultPath"`
	HookErrorPath     string           `json:"hookErrorPath"`
	DeliveryErrorPath string           `json:"deliveryErrorPath"`
}

type replayChildResult struct {
	Item     ReplayItem `json:"item"`
	Executed bool       `json:"executed"`
}

func runAssignedReplayItem(ctx context.Context, entry ReplayRegistration, options ReplayOptions, path string, stdout, stderr io.Writer) (ReplayItem, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ReplayItem{}, err
	}
	var assignment replayAssignment
	if err = json.Unmarshal(raw, &assignment); err != nil {
		return ReplayItem{}, err
	}
	if assignment.ExperimentID == "" || assignment.LocalTraceID == "" || assignment.ResultPath == "" {
		return ReplayItem{}, fmt.Errorf("bitfab: invalid replay assignment")
	}
	assignment.ServerItem.attempt = assignment.Attempt
	normalized, err := normalizeReplayOptions(&options)
	if err != nil {
		return ReplayItem{}, err
	}
	fn, err := resolveReplayFunction(entry.TraceFunctionKey, entry.Function)
	if err != nil {
		return ReplayItem{}, err
	}
	callable, err := prepareReplayCallable(fn)
	if err != nil {
		return ReplayItem{}, err
	}
	deliveryTimeout := replayPersistenceTimeout
	if normalized.Concurrency != nil && normalized.Concurrency.ChildDeliveryTimeout > 0 {
		deliveryTimeout = normalized.Concurrency.ChildDeliveryTimeout
	}
	problems := collectReplayDeliveryProblems()
	defer problems.stop()
	client := entry.Client
	client.httpClient.trackTraceDeliveries([]string{assignment.LocalTraceID})
	item := client.runReplayItem(ctx, entry.TraceFunctionKey, callable, normalized, assignment.ExperimentID, assignment.ServerItem, assignment.LocalTraceID)
	if item.localTraceID != "" {
		persisted, persistErr := client.waitForReplayPersistence(ctx, assignment.ExperimentID, []string{item.localTraceID}, deliveryTimeout)
		if persistErr != nil {
			setReplaySetupError(&item, persistErr)
			problems.add(persistErr.Error())
		} else if id := persisted[item.localTraceID]; id != "" {
			item.TraceID = &id
		}
	}
	serialized, err := json.Marshal(replayChildResult{Item: item, Executed: item.localTraceID != ""})
	if err != nil {
		return item, err
	}
	if err = os.WriteFile(assignment.ResultPath+".tmp", serialized, 0600); err != nil {
		return item, err
	}
	if err = os.Rename(assignment.ResultPath+".tmp", assignment.ResultPath); err != nil {
		return item, err
	}
	publicItem, _ := json.Marshal(item)
	fmt.Fprintln(stdout, string(publicItem))
	if normalized.Concurrency != nil && normalized.Concurrency.OnItemFinishInChildProcess != nil && !normalized.DryRun {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					message := fmt.Sprintf("[replay] child grading hook failed for %s: %v", item.OriginalTraceID, recovered)
					fmt.Fprintln(stderr, message)
					if assignment.HookErrorPath != "" {
						_ = os.WriteFile(assignment.HookErrorPath, []byte(message+"\n"), 0600)
					}
				}
			}()
			normalized.Concurrency.OnItemFinishInChildProcess(ReplayItemFinishEvent{ExperimentID: assignment.ExperimentID, TestRunID: assignment.ExperimentID, Item: item})
		}()
	}
	if !normalized.DryRun && !client.FlushTraces(deliveryTimeout) {
		problems.add(fmt.Sprintf("delivery was not confirmed within ChildDeliveryTimeout (%s). Spans that failed or were still sending when the child exited may never reach Bitfab.", deliveryTimeout))
	}
	if report := problems.report(fmt.Sprintf("%s#%d", item.OriginalTraceID, item.Attempt)); report != "" {
		fmt.Fprint(stderr, report)
		if assignment.DeliveryErrorPath != "" {
			_ = os.WriteFile(assignment.DeliveryErrorPath, []byte(report), 0600)
		}
	}
	return item, nil
}

type replayDeliveryProblems struct {
	mu       sync.Mutex
	messages []string
	counts   map[string]int
}

var activeReplayDeliveryProblems atomic.Pointer[replayDeliveryProblems]

func collectReplayDeliveryProblems() *replayDeliveryProblems {
	problems := &replayDeliveryProblems{counts: map[string]int{}}
	activeReplayDeliveryProblems.Store(problems)
	return problems
}

func recordReplayDeliveryProblem(message string) {
	if problems := activeReplayDeliveryProblems.Load(); problems != nil {
		problems.add(message)
	}
}

func (p *replayDeliveryProblems) stop() {
	activeReplayDeliveryProblems.CompareAndSwap(p, nil)
}

func (p *replayDeliveryProblems) add(message string) {
	message = strings.TrimPrefix(message, "bitfab: ")
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.counts[message] == 0 {
		p.messages = append(p.messages, message)
	}
	p.counts[message]++
}

func (p *replayDeliveryProblems) report(label string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var report strings.Builder
	for _, message := range p.messages {
		fmt.Fprintf(&report, "[replay] span delivery problem for %s: %s", label, message)
		if count := p.counts[message]; count > 1 {
			fmt.Fprintf(&report, " (x%d)", count)
		}
		report.WriteString("\n")
	}
	return report.String()
}

func (c *Client) runReplayProcesses(ctx context.Context, options ReplayOptions, start startReplayResponse, traceIDs []string, result *ReplayResult) {
	if len(start.Items) == 0 {
		return
	}
	dir, err := os.MkdirTemp("", "bitfab-replay-work-")
	if err != nil {
		for i, source := range start.Items {
			item := baseReplayItem(source)
			setReplaySetupError(&item, err)
			result.Items[i] = item
		}
		return
	}
	defer os.RemoveAll(dir)
	var throttle *replayMemoryThrottle
	if *options.Concurrency.MemoryThrottle && !replayMemoryDisabled() {
		throttle = newReplayMemoryThrottle()
	}
	progress := &replayProgressState{total: len(start.Items)}
	var memoryNotice sync.Once
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range min(options.MaxConcurrency, len(start.Items)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				source := start.Items[index]
				var item ReplayItem
				if throttle != nil {
					admissionStarted := time.Now()
					admissionErr := throttle.admit(ctx, index)
					if time.Since(admissionStarted) >= throttle.poll {
						memoryNotice.Do(func() {
							fmt.Fprintln(options.processCommand.stderr, "[replay] Low memory: holding child processes back. BITFAB_REPLAY_MEMORY_THROTTLE=off disables this.")
						})
					}
					if admissionErr != nil {
						item = baseReplayItem(source)
						setReplaySetupError(&item, admissionErr)
						result.Items[index] = item
						progress.reportFinish(options.OnItemFinish, start.ExperimentID, item)
						continue
					}
				}
				progress.reportStart(options.OnItemStart, start.ExperimentID, source)
				item = runReplayChild(ctx, options, dir, index, throttle, replayAssignment{ExperimentID: start.ExperimentID, ServerItem: source, Attempt: source.attempt, LocalTraceID: traceIDs[index]})
				if throttle != nil {
					throttle.release(index)
				}
				result.Items[index] = item
				progress.reportFinish(options.OnItemFinish, start.ExperimentID, item)
			}
		}()
	}
	for index := range start.Items {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
}

func runReplayChild(ctx context.Context, options ReplayOptions, dir string, index int, throttle *replayMemoryThrottle, assignment replayAssignment) ReplayItem {
	assignment.ResultPath = filepath.Join(dir, fmt.Sprintf("result-%d.json", index))
	assignment.HookErrorPath = filepath.Join(dir, fmt.Sprintf("hook-%d.txt", index))
	assignment.DeliveryErrorPath = filepath.Join(dir, fmt.Sprintf("delivery-%d.txt", index))
	path := filepath.Join(dir, fmt.Sprintf("assignment-%d.json", index))
	item := baseReplayItem(assignment.ServerItem)
	if err := ctx.Err(); err != nil {
		setReplaySetupError(&item, err)
		return item
	}
	raw, err := json.Marshal(assignment)
	if err != nil {
		setReplaySetupError(&item, err)
		return item
	}
	if err = os.WriteFile(path, raw, 0600); err != nil {
		setReplaySetupError(&item, err)
		return item
	}
	logPath := filepath.Join(dir, fmt.Sprintf("child-%d.log", index))
	log, err := os.Create(logPath)
	if err != nil {
		setReplaySetupError(&item, err)
		return item
	}
	defer log.Close()
	childCtx, cancel := context.WithTimeout(ctx, options.Concurrency.ChildTimeout)
	defer cancel()
	command := options.processCommand
	args := append(append([]string{}, command.args...), "--execute-item", path)
	cmd := exec.Command(command.executable, args...)
	cmd.Env = append([]string{}, command.environ...)
	cmd.Stdout = log
	cmd.Stderr = log
	prepareReplayChild(cmd)
	if err = cmd.Start(); err != nil {
		setReplaySetupError(&item, err)
		return item
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	waiting := true
	for waiting {
		select {
		case err = <-done:
			waiting = false
		case <-ticker.C:
			if throttle != nil {
				if rss, known := replayProcessRSS(cmd.Process.Pid); known {
					throttle.observe(index, rss)
				}
			}
		case <-childCtx.Done():
			killReplayChild(cmd)
			<-done
			err = childCtx.Err()
			waiting = false
		}
	}
	if report, readErr := os.ReadFile(assignment.DeliveryErrorPath); readErr == nil {
		fmt.Fprint(command.stderr, string(report))
	}
	if raw, readErr := os.ReadFile(assignment.ResultPath); readErr == nil {
		var childResult replayChildResult
		if decodeErr := json.Unmarshal(raw, &childResult); decodeErr != nil {
			setReplaySetupError(&item, fmt.Errorf("bitfab: child result decode: %w", decodeErr))
		} else {
			item = childResult.Item
			if childResult.Executed {
				item.localTraceID = assignment.LocalTraceID
			}
			if hookError, readErr := os.ReadFile(assignment.HookErrorPath); readErr == nil {
				fmt.Fprint(command.stderr, string(hookError))
			}
			if err != nil {
				fmt.Fprintf(command.stderr, "[replay] child for %s exited after writing its result: %v\n", item.OriginalTraceID, err)
			}
		}
		return item
	}
	detail := replayChildLogTail(logPath)
	setReplaySetupError(&item, fmt.Errorf("bitfab: replay child exited without writing a result: %v%s", err, detail))
	return item
}

func replayChildLogTail(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return ""
	}
	offset := max(int64(0), stat.Size()-64*1024)
	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		return ""
	}
	raw, err := io.ReadAll(io.LimitReader(file, 64*1024))
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) > 20 {
		lines = lines[len(lines)-20:]
	}
	tail := strings.Join(lines, "\n")
	if tail == "" {
		return ""
	}
	return " Last output:\n" + tail
}
