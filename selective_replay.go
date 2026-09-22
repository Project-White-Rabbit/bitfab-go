package bitfab

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"
)

// ReplayNodeIdentity selects every occurrence of a node in a workflow.
type ReplayNodeIdentity struct {
	TraceFunctionKey string `json:"traceFunctionKey"`
	SpanName         string `json:"spanName"`
}

// SelectiveReplayOptions protects changed code and unchanged code with required effects.
// Each selected subtree and its ancestors stay live; the server adds assertion protection.
type SelectiveReplayOptions struct {
	MustRun []ReplayNodeIdentity `json:"mustRun"`
}

// SelectiveReplayDecision records what happened at an intercepted call.
type SelectiveReplayDecision struct {
	ReplayNodeIdentity
	OriginalSpanID *string `json:"originalSpanId"`
	Action         string  `json:"action"`
	Reason         string  `json:"reason"`
}

// SelectiveReplayReport is execution coverage, not assertion verdicts.
type SelectiveReplayReport struct {
	Decisions                  []SelectiveReplayDecision `json:"decisions"`
	MustRunNotReached          []ReplayNodeIdentity      `json:"mustRunNotReached"`
	UnresolvedMustRun          []ReplayNodeIdentity      `json:"unresolvedMustRun"`
	AssertionIDs               []string                  `json:"assertionIds"`
	UnresolvedAssertions       []string                  `json:"unresolvedAssertions"`
	AssertionTargetsNotReached []string                  `json:"assertionTargetsNotReached"`
}

type selectiveRecording struct {
	Input  []any           `json:"input"`
	Kwargs map[string]any  `json:"kwargs,omitempty"`
	Output json.RawMessage `json:"output"`
}
type selectiveNode struct {
	ReplayNodeIdentity
	ID        string              `json:"id"`
	ParentID  *string             `json:"parentId"`
	MustRun   *bool               `json:"mustRun"`
	Reusable  *bool               `json:"reusable"`
	Reason    string              `json:"reason"`
	Recording *selectiveRecording `json:"recording"`
}
type selectiveAssertion struct {
	ID     string `json:"id"`
	Target *struct {
		Kind       string `json:"kind"`
		Name       string `json:"name"`
		Occurrence any    `json:"occurrence"`
	} `json:"target"`
}
type selectivePlan struct {
	Version           int                    `json:"version"`
	RootID            string                 `json:"rootId"`
	Options           SelectiveReplayOptions `json:"options"`
	UnresolvedMustRun []ReplayNodeIdentity   `json:"unresolvedMustRun"`
	Nodes             []selectiveNode        `json:"nodes"`
	Assertions        []selectiveAssertion   `json:"assertions"`
}
type selectiveRuntime struct {
	mu                   sync.Mutex
	plan                 *selectivePlan
	nodes                map[string]*selectiveNode
	children             map[string][]*selectiveNode
	parents              map[string]string
	matchingParents      map[string]string
	consumed             map[string]map[string]bool
	required             map[ReplayNodeIdentity]bool
	reached              map[ReplayNodeIdentity]bool
	requiredScopes       map[string]bool
	assertionScopes      map[string]bool
	assertionNames       map[string]bool
	observed             map[string]int
	fullTrace            bool
	unresolvedAssertions []string
	decisions            []SelectiveReplayDecision
	failure              error
}

func validateSelectiveOptions(options *SelectiveReplayOptions) error {
	if options == nil || len(options.MustRun) < 1 || len(options.MustRun) > 100 {
		return fmt.Errorf("bitfab: selective replay requires 1–100 MustRun identities")
	}
	for _, node := range options.MustRun {
		if len(node.TraceFunctionKey) < 1 || len(node.TraceFunctionKey) > 500 || len(node.SpanName) < 1 || len(node.SpanName) > 500 {
			return fmt.Errorf("bitfab: invalid MustRun identity")
		}
	}
	return nil
}

func newSelectiveRuntime(plan *selectivePlan, options *SelectiveReplayOptions) (*selectiveRuntime, error) {
	if err := validateSelectiveOptions(options); err != nil {
		return nil, err
	}
	if plan == nil || plan.Version != 4 || len(plan.Nodes) < 1 || len(plan.Nodes) > 500 || plan.Assertions == nil || plan.UnresolvedMustRun == nil {
		return nil, fmt.Errorf("bitfab: incompatible selective replay plan")
	}
	if !reflect.DeepEqual(plan.Options, *options) {
		return nil, fmt.Errorf("bitfab: selective replay plan does not match MustRun")
	}
	r := &selectiveRuntime{plan: plan, nodes: map[string]*selectiveNode{}, children: map[string][]*selectiveNode{}, parents: map[string]string{}, matchingParents: map[string]string{}, consumed: map[string]map[string]bool{}, required: map[ReplayNodeIdentity]bool{}, reached: map[ReplayNodeIdentity]bool{}, requiredScopes: map[string]bool{}, assertionScopes: map[string]bool{}, assertionNames: map[string]bool{}, observed: map[string]int{}, unresolvedAssertions: []string{}, decisions: []SelectiveReplayDecision{}}
	for _, key := range options.MustRun {
		r.required[key] = true
	}
	recorded := map[ReplayNodeIdentity]bool{}
	for i := range plan.Nodes {
		node := &plan.Nodes[i]
		if node.ID == "" || r.nodes[node.ID] != nil || node.MustRun == nil || node.Reusable == nil || node.TraceFunctionKey == "" || node.SpanName == "" {
			return nil, fmt.Errorf("bitfab: invalid selective replay node")
		}
		r.nodes[node.ID] = node
		recorded[node.ReplayNodeIdentity] = true
		if node.ParentID != nil {
			r.children[*node.ParentID] = append(r.children[*node.ParentID], node)
		}
	}
	root := r.nodes[plan.RootID]
	if root == nil || root.ParentID != nil || !*root.MustRun {
		return nil, fmt.Errorf("bitfab: selective replay requires a live root")
	}
	missing := map[ReplayNodeIdentity]bool{}
	for key := range r.required {
		if !recorded[key] {
			missing[key] = true
		}
	}
	unresolved := map[ReplayNodeIdentity]bool{}
	for _, key := range plan.UnresolvedMustRun {
		unresolved[key] = true
	}
	if !reflect.DeepEqual(missing, unresolved) {
		return nil, fmt.Errorf("bitfab: incomplete MustRun coverage")
	}
	ids := map[string]bool{}
	for _, assertion := range plan.Assertions {
		if assertion.ID == "" || ids[assertion.ID] {
			return nil, fmt.Errorf("bitfab: invalid selective assertion")
		}
		ids[assertion.ID] = true
		target := assertion.Target
		if target == nil {
			r.fullTrace = true
			continue
		}
		if target.Kind == "output" {
			continue
		}
		if target.Kind != "span" || target.Name == "" {
			return nil, fmt.Errorf("bitfab: invalid selective assertion target")
		}
		occurrence, err := selectiveOccurrence(target.Occurrence)
		if err != nil {
			return nil, err
		}
		r.assertionNames[target.Name] = true
		count := 0
		for _, node := range plan.Nodes {
			if node.SpanName == target.Name {
				count++
			}
		}
		if count <= occurrence {
			r.unresolvedAssertions = append(r.unresolvedAssertions, assertion.ID)
			r.fullTrace = true
		}
	}
	for _, node := range r.nodes {
		visited := map[string]bool{node.ID: true}
		required := r.required[node.ReplayNodeIdentity] || len(missing) > 0
		asserted := r.fullTrace || r.assertionNames[node.SpanName]
		parentID := node.ParentID
		for parentID != nil {
			parent := r.nodes[*parentID]
			if parent == nil || visited[*parentID] || (*node.MustRun && !*parent.MustRun) {
				return nil, fmt.Errorf("bitfab: invalid selective replay ancestry")
			}
			visited[*parentID] = true
			required = required || r.required[parent.ReplayNodeIdentity]
			asserted = asserted || r.assertionNames[parent.SpanName]
			parentID = parent.ParentID
		}
		if !visited[plan.RootID] || ((required || asserted) && !*node.MustRun) {
			return nil, fmt.Errorf("bitfab: selective plan could hide required code or assertion evidence")
		}
	}
	return r, nil
}

func selectiveOccurrence(value any) (int, error) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case string:
		if v == "first" || v == "last" {
			return 0, nil
		}
	case float64:
		if v >= 0 && v <= 9007199254740991 && v == math.Trunc(v) {
			return int(v), nil
		}
	}
	return 0, fmt.Errorf("bitfab: invalid selective assertion occurrence")
}

// Only ordinary JSON-shaped values are allowed; custom serialization, pointers,
// cycles and aliasing cannot prove output-only replay equivalence.
func selectiveFingerprint(value any) string {
	seen := map[uintptr]bool{}
	var valid func(reflect.Value) bool
	valid = func(v reflect.Value) bool {
		if !v.IsValid() {
			return true
		}
		if v.Kind() == reflect.Interface {
			return valid(v.Elem())
		}
		if v.Type().PkgPath() != "" {
			return false
		}
		switch v.Kind() {
		case reflect.Bool:
			return true
		case reflect.String:
			return utf8.ValidString(v.String()) && !strings.HasPrefix(v.String(), "<unserializable")
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return v.Int() >= -9007199254740991 && v.Int() <= 9007199254740991
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return v.Uint() <= 9007199254740991
		case reflect.Float64:
			return !math.IsNaN(v.Float()) && !math.IsInf(v.Float(), 0) && !(v.Float() == 0 && math.Signbit(v.Float()))
		case reflect.Slice, reflect.Map:
			if v.IsNil() {
				return true
			}
			ptr := v.Pointer()
			if ptr != 0 && seen[ptr] {
				return false
			}
			seen[ptr] = true
			if v.Kind() == reflect.Map {
				if v.Type().Key() != reflect.TypeFor[string]() {
					return false
				}
				it := v.MapRange()
				for it.Next() {
					if !utf8.ValidString(it.Key().String()) || !valid(it.Value()) {
						return false
					}
				}
				return true
			}
			if v.Type().Elem().Kind() == reflect.Uint8 {
				return false
			}
			for i := 0; i < v.Len(); i++ {
				if !valid(v.Index(i)) {
					return false
				}
			}
			return true
		}
		return false
	}
	if !valid(reflect.ValueOf(value)) {
		return ""
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func selectiveOutputTypeSafe(t reflect.Type) bool {
	if t == nil {
		return true
	}
	if t.PkgPath() != "" {
		return false
	}
	switch t.Kind() {
	case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float64:
		return true
	case reflect.Interface:
		return t.NumMethod() == 0
	case reflect.Slice:
		return t.Elem().Kind() != reflect.Uint8 && selectiveOutputTypeSafe(t.Elem())
	case reflect.Map:
		return t.Key() == reflect.TypeFor[string]() && selectiveOutputTypeSafe(t.Elem())
	}
	return false
}

func (r *selectiveRuntime) enter(key ReplayNodeIdentity, spanID, parentID string, cfg spanConfig) (any, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fingerprint := selectiveFingerprint(replayMockInputs(cfg.input))
	matchingParentID := parentID
	if exportedParent, hidden := r.matchingParents[parentID]; hidden {
		matchingParentID = exportedParent
	}
	var node *selectiveNode
	if parentID == "" {
		node = r.nodes[r.plan.RootID]
	} else {
		if r.consumed[matchingParentID] == nil {
			r.consumed[matchingParentID] = map[string]bool{}
		}
		candidates, exact := []*selectiveNode{}, []*selectiveNode{}
		for _, candidate := range r.children[r.parents[matchingParentID]] {
			if candidate.ReplayNodeIdentity != key || r.consumed[matchingParentID][candidate.ID] {
				continue
			}
			candidates = append(candidates, candidate)
			if fingerprint != "" && candidate.Recording != nil && len(candidate.Recording.Kwargs) == 0 && fingerprint == selectiveFingerprint(candidate.Recording.Input) {
				exact = append(exact, candidate)
			}
		}
		if len(exact) == 1 {
			node = exact[0]
		} else if len(exact) == 0 && len(candidates) == 1 {
			node = candidates[0]
		}
		if node != nil {
			r.consumed[matchingParentID][node.ID] = true
		} else {
			for _, candidate := range candidates {
				r.consumed[matchingParentID][candidate.ID] = true
			}
		}
	}
	if node != nil {
		r.parents[spanID] = node.ID
	} else if cfg.selectiveInterceptionOnly {
		// Only parents absent from the recording are transparent to matching.
		// Matched parents retain recorded ancestry even when now capture-hidden.
		// Safety scopes always follow the actual parent.
		r.matchingParents[spanID] = matchingParentID
	}
	required := r.required[key] || r.requiredScopes[parentID]
	asserted := r.fullTrace || r.assertionNames[key.SpanName] || r.assertionScopes[parentID]
	r.requiredScopes[spanID], r.assertionScopes[spanID] = required, asserted
	mustRun := parentID == "" || required || asserted || len(r.plan.UnresolvedMustRun) > 0 || (node != nil && *node.MustRun)
	reusable := false
	var output any
	equalInputs := node != nil && node.Recording != nil && fingerprint != "" && fingerprint == selectiveFingerprint(node.Recording.Input) && len(node.Recording.Kwargs) == 0
	outputSupported := cfg.finalize == nil && !cfg.selectiveUnsupportedOutput && selectiveOutputTypeSafe(cfg.mockOutputType)
	if !mustRun && equalInputs && outputSupported && len(node.Recording.Output) > 0 {
		if json.Unmarshal(node.Recording.Output, &output) == nil && selectiveFingerprint(output) != "" {
			var err error
			output, err = decodeReplayMockOutput(output, cfg)
			reusable = err == nil
		}
	}
	reason := "unmatched-or-ambiguous-call"
	if node != nil {
		reason = node.Reason
	}
	if !mustRun && node != nil {
		if !equalInputs || len(node.Recording.Output) == 0 {
			reason = "input-drift-or-missing-recording"
		} else if !outputSupported || !reusable {
			reason = "unsupported-output-boundary"
		} else if !cfg.mockOnReplay && !cfg.replayReusable {
			reason = "reuse-not-declared"
		}
	}
	if required {
		reason = "must-run"
	} else if asserted {
		reason = "assertion"
	} else if len(r.plan.UnresolvedMustRun) > 0 {
		reason = "unresolved-must-run"
	}
	decision := SelectiveReplayDecision{ReplayNodeIdentity: key, Reason: reason, Action: "run"}
	if node != nil {
		id := node.ID
		decision.OriginalSpanID = &id
	}
	if cfg.mockOnReplay && (mustRun || !reusable) {
		decision.Action = "blocked"
		decision.Reason = "safety-conflict:" + reason
		r.decisions = append(r.decisions, decision)
		r.failure = fmt.Errorf("bitfab: selective replay cannot execute or safely reuse safety-mocked node %s (%s)", key.SpanName, reason)
		return nil, true, r.failure
	}
	if !mustRun && reusable && (cfg.mockOnReplay || (node != nil && *node.Reusable && cfg.replayReusable)) {
		decision.Action = "mock"
		decision.Reason = "unchanged-inputs"
		r.decisions = append(r.decisions, decision)
		return output, true, nil
	}
	r.decisions = append(r.decisions, decision)
	r.observed[key.SpanName]++
	if r.required[key] {
		r.reached[key] = true
	}
	return nil, false, nil
}

func (r *selectiveRuntime) report() *SelectiveReplayReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := &SelectiveReplayReport{Decisions: slices.Clone(r.decisions), MustRunNotReached: []ReplayNodeIdentity{}, UnresolvedMustRun: slices.Clone(r.plan.UnresolvedMustRun), AssertionIDs: []string{}, UnresolvedAssertions: slices.Clone(r.unresolvedAssertions), AssertionTargetsNotReached: []string{}}
	for _, key := range r.plan.Options.MustRun {
		if !r.reached[key] {
			report.MustRunNotReached = append(report.MustRunNotReached, key)
		}
	}
	for _, assertion := range r.plan.Assertions {
		report.AssertionIDs = append(report.AssertionIDs, assertion.ID)
		if assertion.Target != nil && assertion.Target.Kind == "span" {
			occurrence, _ := selectiveOccurrence(assertion.Target.Occurrence)
			if r.observed[assertion.Target.Name] <= occurrence {
				report.AssertionTargetsNotReached = append(report.AssertionTargetsNotReached, assertion.ID)
			}
		}
	}
	return report
}

func recordSelectiveJSON(data map[string]any, inputJSON string, output any, cfg spanConfig) {
	data["replay_reusable"] = cfg.replayReusable
	encoded := selectiveFingerprint(output)
	if inputJSON == "" || encoded == "" || cfg.finalize != nil || cfg.selectiveUnsupportedOutput || !selectiveOutputTypeSafe(cfg.mockOutputType) {
		return
	}
	var clone any
	if json.Unmarshal([]byte(encoded), &clone) != nil {
		return
	}
	decoded, err := decodeReplayMockOutput(clone, cfg)
	if err != nil || !reflect.DeepEqual(output, decoded) {
		return
	}
	var inputs []any
	if json.Unmarshal([]byte(inputJSON), &inputs) != nil {
		return
	}
	data["replay_json_safe"] = true
	data["replay_recording"] = map[string]any{"input": inputs, "kwargs": map[string]any{}, "output": clone}
}
