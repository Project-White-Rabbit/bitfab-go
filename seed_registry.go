package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// SeedRegistryCase is a positional input row read from JSON or JSONL.
type SeedRegistryCase struct {
	Input       []any          `json:"input"`
	Expected    any            `json:"expected,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	SessionID   string         `json:"sessionId,omitempty"`
	Name        string         `json:"name,omitempty"`
	hasExpected bool
}

// SeedRegistryResult is the machine-readable result of seed or reseed commands.
type SeedRegistryResult struct {
	Pipeline         string         `json:"pipeline"`
	TraceFunctionKey string         `json:"traceFunctionKey"`
	TraceIDs         []string       `json:"traceIds,omitempty"`
	Reseeded         []ReseedResult `json:"reseeded,omitempty"`
}

// MarshalJSON preserves the distinct seed and reseed result shapes, including empty arrays.
func (result SeedRegistryResult) MarshalJSON() ([]byte, error) {
	value := map[string]any{"pipeline": result.Pipeline, "traceFunctionKey": result.TraceFunctionKey}
	if result.Reseeded != nil {
		value["reseeded"] = result.Reseeded
	} else {
		ids := result.TraceIDs
		if ids == nil {
			ids = []string{}
		}
		value["traceIds"] = ids
	}
	return json.Marshal(value)
}

func parseSeedRegistryCases(raw []byte, run bool) ([]SeedRegistryCase, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return nil, fmt.Errorf("bitfab: seed file is empty")
	}
	var rows []json.RawMessage
	if strings.HasPrefix(text, "[") {
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, err
		}
	} else {
		for _, line := range strings.Split(text, "\n") {
			if strings.TrimSpace(line) != "" {
				rows = append(rows, json.RawMessage(line))
			}
		}
	}
	cases := make([]SeedRegistryCase, 0, len(rows))
	for index, row := range rows {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(row, &fields); err != nil {
			return nil, fmt.Errorf("bitfab: seed case %d must be an object: %w", index, err)
		}
		var value SeedRegistryCase
		if err := json.Unmarshal(row, &value); err != nil {
			return nil, fmt.Errorf("bitfab: seed case %d: %w", index, err)
		}
		if value.Input == nil {
			return nil, fmt.Errorf("bitfab: seed case %d requires an input array", index)
		}
		_, value.hasExpected = fields["expected"]
		if run && value.hasExpected {
			return nil, fmt.Errorf("bitfab: seed case %d carries expected, which --run does not record; remove expected or drop --run", index)
		}
		cases = append(cases, value)
	}
	return cases, nil
}

// SeedFromRegistry records cases or executes each registered function once when run is true.
func SeedFromRegistry(ctx context.Context, registry *ReplayRegistry, pipeline string, cases []SeedRegistryCase, run bool) (SeedRegistryResult, error) {
	entry, err := registry.fetch(pipeline)
	if err != nil {
		return SeedRegistryResult{}, err
	}
	result := SeedRegistryResult{Pipeline: pipeline, TraceFunctionKey: entry.TraceFunctionKey, TraceIDs: []string{}}
	for _, row := range cases {
		if row.Input == nil {
			return result, fmt.Errorf("bitfab: seed cases require an input array")
		}
		if run && (row.hasExpected || row.Expected != nil) {
			return result, fmt.Errorf("bitfab: run-based seed cases cannot carry expected")
		}
	}
	for _, row := range cases {
		var id string
		if run {
			id, err = entry.Client.SeedTrace(ctx, entry.TraceFunctionKey, entry.Function, &SeedOptions{Args: row.Input, Metadata: row.Metadata, SessionID: row.SessionID, Name: row.Name})
		} else {
			id, err = entry.Client.SeedCase(ctx, entry.TraceFunctionKey, SeedCaseOptions{Input: row.Input, Expected: row.Expected, Function: entry.Function, Metadata: row.Metadata, SessionID: row.SessionID, Name: row.Name})
		}
		if err != nil {
			return result, err
		}
		result.TraceIDs = append(result.TraceIDs, id)
	}
	return result, nil
}

// ReseedFromRegistry reruns each selected trace through its registered function.
func ReseedFromRegistry(ctx context.Context, registry *ReplayRegistry, pipeline string, ids []string) (SeedRegistryResult, error) {
	entry, err := registry.fetch(pipeline)
	if err != nil {
		return SeedRegistryResult{}, err
	}
	result := SeedRegistryResult{Pipeline: pipeline, TraceFunctionKey: entry.TraceFunctionKey, Reseeded: []ReseedResult{}}
	for _, id := range ids {
		value, err := entry.Client.ReseedTrace(ctx, entry.TraceFunctionKey, entry.Function, id)
		if err != nil {
			return result, err
		}
		result.Reseeded = append(result.Reseeded, value)
	}
	return result, nil
}

// RunSeedCLI runs --cases PATH [--run] or --from-trace IDS against a project registry.
func RunSeedCLI(ctx context.Context, registry *ReplayRegistry, args []string, stdout, stderr io.Writer) (SeedRegistryResult, error) {
	parsed, err := parseRegistryCLI(registry, args, stderr)
	if err != nil {
		return SeedRegistryResult{}, err
	}
	return runSeedRegistryCLI(ctx, registry, parsed, stdout, stderr)
}

func runSeedRegistryCLI(ctx context.Context, registry *ReplayRegistry, args registryCLIArgs, stdout, stderr io.Writer) (SeedRegistryResult, error) {
	if (args.seed == "") == (args.fromTrace == "") || (args.fromTrace != "" && args.run) {
		return SeedRegistryResult{}, fmt.Errorf("bitfab: supply --cases PATH [--run] or --from-trace IDS")
	}
	var result SeedRegistryResult
	var err error
	if args.fromTrace != "" {
		ids, e := registryIDs(args.fromTrace)
		if e != nil {
			return result, e
		}
		fmt.Fprintf(stderr, "[seed] Re-seeding %d trace(s) through %q...\n", len(ids), args.pipeline)
		result, err = ReseedFromRegistry(ctx, registry, args.pipeline, ids)
	} else {
		raw, e := os.ReadFile(args.seed)
		if e != nil {
			return result, e
		}
		cases, e := parseSeedRegistryCases(raw, args.run)
		if e != nil {
			return result, e
		}
		fmt.Fprintf(stderr, "[seed] Recording %d case(s) through %q...\n", len(cases), args.pipeline)
		result, err = SeedFromRegistry(ctx, registry, args.pipeline, cases, args.run)
	}
	if err != nil {
		return result, err
	}
	err = json.NewEncoder(stdout).Encode(result)
	return result, err
}
