package bitfab

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

func isolatedAgentSessionHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, name := range agentSessionIDEnvVars {
		t.Setenv(name, "")
	}
	return home
}

func agentSessionFile(home, sessionID string) string {
	return filepath.Join(home, ".bitfab", "agent-sessions", sessionID, "experiments.jsonl")
}

func readAgentSessionLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	var lines []map[string]any
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var line map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("parse line: %v", err)
		}
		lines = append(lines, line)
	}
	return lines
}

func TestRecordAgentSessionExperimentAppendsOneLinePerExperiment(t *testing.T) {
	home := isolatedAgentSessionHome(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-a")

	recordAgentSessionExperiment("first", "http://localhost:3001/x?experimentId=first", "baseline", "http://localhost:3001/", "")
	recordAgentSessionExperiment("second", "http://localhost:3001/x?experimentId=second", "", "http://localhost:3001", "")

	lines := readAgentSessionLines(t, agentSessionFile(home, "session-a"))
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d", len(lines))
	}
	if lines[0]["experimentId"] != "first" || lines[0]["name"] != "baseline" || lines[0]["serviceUrl"] != "http://localhost:3001" {
		t.Fatalf("unexpected first line: %v", lines[0])
	}
	if _, ok := lines[0]["startedAt"].(string); !ok {
		t.Fatalf("startedAt missing: %v", lines[0])
	}
	if lines[1]["experimentId"] != "second" || lines[1]["name"] != nil {
		t.Fatalf("unexpected second line: %v", lines[1])
	}
}

func TestRecordAgentSessionExperimentPrefersTheExplicitSession(t *testing.T) {
	home := isolatedAgentSessionHome(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "from-env")

	recordAgentSessionExperiment("first", "url", "", "http://localhost:3001", "explicit-session")

	if _, err := os.Stat(agentSessionFile(home, "explicit-session")); err != nil {
		t.Fatalf("explicit session file missing: %v", err)
	}
	if _, err := os.Stat(agentSessionFile(home, "from-env")); err == nil {
		t.Fatalf("wrote the environment session too")
	}
}

func TestRecordAgentSessionExperimentSkipsMissingOrUnsafeSessions(t *testing.T) {
	home := isolatedAgentSessionHome(t)

	recordAgentSessionExperiment("first", "url", "", "http://localhost:3001", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "../escape")
	recordAgentSessionExperiment("first", "url", "", "http://localhost:3001", "")

	if _, err := os.Stat(filepath.Join(home, ".bitfab")); err == nil {
		t.Fatalf("wrote a session folder")
	}
}

func TestRecordAgentSessionExperimentIgnoresWriteFailures(t *testing.T) {
	home := isolatedAgentSessionHome(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-b")
	if err := os.WriteFile(filepath.Join(home, ".bitfab"), []byte("not a folder"), 0o644); err != nil {
		t.Fatal(err)
	}

	recordAgentSessionExperiment("first", "url", "", "http://localhost:3001", "")

	content, err := os.ReadFile(filepath.Join(home, ".bitfab"))
	if err != nil || string(content) != "not a folder" {
		t.Fatalf("file changed: %q %v", content, err)
	}
}

func TestRecordAgentSessionExperimentKeepsLinesWholeUnderConcurrentAppends(t *testing.T) {
	home := isolatedAgentSessionHome(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-concurrent")
	name := strings.Repeat("x", 2000)
	want := make([]string, 200)
	var group sync.WaitGroup
	for index := range want {
		want[index] = fmt.Sprintf("experiment-%d", index)
		group.Add(1)
		go func(experimentID string) {
			defer group.Done()
			recordAgentSessionExperiment(experimentID, "url", name, "http://localhost:3001", "")
		}(want[index])
	}
	group.Wait()

	lines := readAgentSessionLines(t, agentSessionFile(home, "session-concurrent"))
	got := make([]string, len(lines))
	for index, line := range lines {
		got[index] = line["experimentId"].(string)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("lines lost or mangled: got %d lines", len(got))
	}
}
