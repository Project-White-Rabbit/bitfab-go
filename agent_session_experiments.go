package bitfab

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var safeAgentSessionID = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]*$`)

type agentSessionExperiment struct {
	ExperimentID  string  `json:"experimentId"`
	ExperimentURL string  `json:"experimentUrl"`
	Name          *string `json:"name"`
	ServiceURL    string  `json:"serviceUrl"`
	StartedAt     string  `json:"startedAt"`
}

func recordAgentSessionExperiment(experimentID, experimentURL, name, serviceURL, explicitAgentSessionID string) {
	sessionID := resolvedAgentSessionID(explicitAgentSessionID)
	if sessionID == "" || !safeAgentSessionID.MatchString(sessionID) {
		return
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	directory := filepath.Join(home, ".bitfab", "agent-sessions", sessionID)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return
	}
	var recordedName *string
	if name != "" {
		recordedName = &name
	}
	line, err := json.Marshal(agentSessionExperiment{
		ExperimentID:  experimentID,
		ExperimentURL: experimentURL,
		Name:          recordedName,
		ServiceURL:    strings.TrimRight(serviceURL, "/"),
		StartedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return
	}
	file, err := os.OpenFile(filepath.Join(directory, "experiments.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.Write(append(line, '\n'))
}
